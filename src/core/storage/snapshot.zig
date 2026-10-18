//! Storage-owned snapshot read and write for the current runtime state shape.
//! Time Complexity: Snapshot write and load are O(n + b), where `n` is retained key and TTL entry count and `b` is total serialized bytes.
//! Allocator: Uses explicit allocators for temporary write sorting, full-file load buffers, decode scratch, and temporary loaded shard state.
//! Thread Safety: Snapshot write acquires the shared visibility gate plus one shard shared lock for each serialization window, which blocks writers globally during those windows but allows active reads to continue. Snapshot load must not race with live engine access.

const std = @import("std");
const codec = @import("../internal/codec.zig");
const art = @import("../index/art/tree.zig");
const runtime_shard = @import("../runtime/shard.zig");
const runtime_state = @import("../runtime/state.zig");
const Value = @import("../types/value.zig").Value;

/// Snapshot file magic header used for format identification.
const MAGIC = [4]u8{ 'Z', 'E', 'N', 'S' };

/// Current snapshot format version for `zeno-core`.
const VERSION: u32 = 2;

/// Minimum valid snapshot size: magic, version, checkpoint LSN, shard count, and CRC trailer.
const MIN_SNAPSHOT_SIZE: usize = 24;

/// One borrowed TTL entry collected from a shard for deterministic serialization.
const BorrowedTtlEntry = struct {
    key: []const u8,
    expireAt: i64,
};

/// One temporary loaded shard populated before atomic publish into runtime state.
const LoadedShard = struct {
    base_allocator: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    ttl_index: std.StringHashMapUnmanaged(i64) = .{},
    tree: art.Tree,

    /// Releases all loaded shard storage that has not been published.
    ///
    /// Time Complexity: O(t), where `t` is TTL count.
    ///
    /// Allocator: Does not allocate; frees TTL metadata and tears down arena-owned ART storage through `base_allocator`.
    fn deinit(self: *LoadedShard) void {
        var ttl_iterator = self.ttl_index.iterator();
        while (ttl_iterator.next()) |entry| {
            self.base_allocator.free(entry.key_ptr.*);
        }
        self.ttl_index.deinit(self.base_allocator);
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Writes bytes directly to one file while accumulating a rolling CRC32.
const CrcFileWriter = struct {
    io: std.Io,
    file: std.Io.File,
    hasher: std.hash.crc.Crc32,

    /// Initializes a CRC-aware file writer.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    fn init(io: std.Io, file: std.Io.File) CrcFileWriter {
        return .{
            .io = io,
            .file = file,
            .hasher = std.hash.crc.Crc32.init(),
        };
    }

    /// Writes bytes and updates the rolling CRC.
    ///
    /// Time Complexity: O(n), where `n` is `bytes.len`.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Borrows `bytes` for the duration of the write only.
    fn writeAll(self: *CrcFileWriter, bytes: []const u8) !void {
        try self.file.writeStreamingAll(self.io, bytes);
        self.hasher.update(bytes);
    }

    /// Writes one little-endian `u16`.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    fn writeU16Le(self: *CrcFileWriter, value: u16) !void {
        var buf: [2]u8 = undefined;
        std.mem.writeInt(u16, &buf, value, .little);
        try self.writeAll(&buf);
    }

    /// Writes one little-endian `u32`.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    fn writeU32Le(self: *CrcFileWriter, value: u32) !void {
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, value, .little);
        try self.writeAll(&buf);
    }

    /// Writes one little-endian `u64`.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    fn writeU64Le(self: *CrcFileWriter, value: u64) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, value, .little);
        try self.writeAll(&buf);
    }

    /// Returns the CRC32 accumulated over all prior writes.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    fn final(self: *CrcFileWriter) u32 {
        return self.hasher.final();
    }
};

/// Metadata returned after a successful snapshot write.
///
/// Ownership: Pure value metadata; does not own external memory.
pub const SnapshotWriteResult = struct {
    checkpoint_lsn: u64,
    records_written: usize,
};

/// Metadata returned after a successful snapshot load.
///
/// Ownership: Pure value metadata; does not own external memory.
pub const SnapshotLoadResult = struct {
    checkpoint_lsn: u64,
    records_loaded: usize,
};

/// Writes a snapshot of the current runtime state to `path`.
///
/// Time Complexity: O(n + b), where `n` is retained key and TTL entry count and `b` is total serialized bytes.
///
/// Allocator: Uses `allocator` for deterministic sorting scratch, temporary serialized values, and the temporary path.
///
/// Thread Safety: Acquires the shared visibility gate plus each shard's shared lock while serializing that shard. This blocks writers globally during each shard window, but active reads may continue and may delay completion.
pub fn write(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    path: []const u8,
    checkpoint_lsn: u64,
) !SnapshotWriteResult {
    return writeSnapshotFile(state, allocator, path, checkpoint_lsn, VERSION);
}

/// Loads snapshot state from `path` into `state`.
///
/// Returns `error.FileNotFound` when the snapshot file does not exist and `error.SnapshotCorrupted` when the file fails format or CRC validation.
///
/// Time Complexity: O(n + b), where `n` is retained key and TTL entry count and `b` is total snapshot bytes parsed and decoded.
///
/// Allocator: Uses `allocator` for the temporary file buffer, parse scratch, and temporary per-shard load containers; persistent loaded state uses `state.base_allocator`.
///
/// Thread Safety: Not safe to race with live engine access. On success this replaces shard contents atomically per shard after the full file has validated.
pub fn load(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    path: []const u8,
) !SnapshotLoadResult {
    const file = std.Io.Dir.cwd().openFile(state.io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return error.FileNotFound,
        else => return err,
    };
    defer file.close(state.io);

    const file_size_u64 = file.length(state.io) catch return error.SnapshotCorrupted;
    const file_size = std.math.cast(usize, file_size_u64) orelse return error.OutOfMemory;
    if (file_size < MIN_SNAPSHOT_SIZE) return error.SnapshotCorrupted;

    const file_bytes = try allocator.alloc(u8, file_size);
    defer allocator.free(file_bytes);
    if (try file.readPositionalAll(state.io, file_bytes, 0) != file_size) return error.SnapshotCorrupted;

    const data_len = file_bytes.len - 4;
    const stored_crc = std.mem.readInt(u32, file_bytes[data_len..][0..4], .little);
    const computed_crc = std.hash.crc.Crc32.hash(file_bytes[0..data_len]);
    if (stored_crc != computed_crc) return error.SnapshotCorrupted;

    var stream = std.Io.Reader.fixed(file_bytes[0..data_len]);
    const reader = &stream;

    var magic: [4]u8 = undefined;
    reader.readSliceAll(&magic) catch return error.SnapshotCorrupted;
    if (!std.mem.eql(u8, &magic, &MAGIC)) return error.SnapshotCorrupted;

    const version = readU32Le(reader) catch return error.SnapshotCorrupted;
    if (version != 1 and version != VERSION) return error.SnapshotCorrupted;

    const checkpoint_lsn = readU64Le(reader) catch return error.SnapshotCorrupted;
    const shard_count = readU32Le(reader) catch return error.SnapshotCorrupted;
    if (shard_count != state.shards.len) return error.SnapshotCorrupted;

    const loaded_shards = try allocator.alloc(LoadedShard, state.shards.len);
    defer allocator.free(loaded_shards);

    var loaded_initialized: usize = 0;
    var committed = false;
    defer if (!committed) {
        for (loaded_shards[0..loaded_initialized]) |*loaded_shard| loaded_shard.deinit();
    };

    for (0..state.shards.len) |index| {
        loaded_shards[index] = .{
            .base_allocator = state.base_allocator,
            .arena = std.heap.ArenaAllocator.init(state.base_allocator),
            .ttl_index = .{},
            .tree = undefined,
        };
        loaded_shards[index].tree = art.Tree.init(loaded_shards[index].arena.allocator());
        loaded_initialized += 1;
    }

    const seen = try allocator.alloc(bool, state.shards.len);
    defer allocator.free(seen);
    @memset(seen, false);

    var value_buf = std.ArrayList(u8).empty;
    defer value_buf.deinit(allocator);

    var total_records: usize = 0;
    const now = runtime_shard.unixNow(state.io);

    for (0..shard_count) |_| {
        const shard_idx_u32 = readU32Le(reader) catch return error.SnapshotCorrupted;
        if (shard_idx_u32 >= state.shards.len) return error.SnapshotCorrupted;
        const shard_idx: usize = @intCast(shard_idx_u32);
        if (seen[shard_idx]) return error.SnapshotCorrupted;
        seen[shard_idx] = true;

        const record_count = readU32Le(reader) catch return error.SnapshotCorrupted;
        var loaded_shard = &loaded_shards[shard_idx];
        const shard_allocator = loaded_shard.arena.allocator();

        for (0..record_count) |_| {
            const key_len = readU16Le(reader) catch return error.SnapshotCorrupted;
            if (key_len > codec.MAX_KEY_LEN) return error.SnapshotCorrupted;

            const key = try shard_allocator.alloc(u8, key_len);
            reader.readSliceAll(key) catch return error.SnapshotCorrupted;

            const value_len = readU32Le(reader) catch return error.SnapshotCorrupted;
            if (value_len > codec.MAX_VAL_LEN) return error.SnapshotCorrupted;

            try value_buf.resize(allocator, value_len);
            reader.readSliceAll(value_buf.items) catch return error.SnapshotCorrupted;

            var value_stream = std.Io.Reader.fixed(value_buf.items);
            const decoded = codec.deserializeValue(&value_stream, shard_allocator, 0) catch return error.SnapshotCorrupted;
            if (value_stream.seek != value_buf.items.len) return error.SnapshotCorrupted;
            if (loaded_shard.tree.lookup(key) != null) return error.SnapshotCorrupted;

            const value_ptr = try shard_allocator.create(Value);
            value_ptr.* = decoded;
            try loaded_shard.tree.insert(key, value_ptr);
            total_records += 1;
        }

        if (version >= 2) {
            const ttl_count = readU32Le(reader) catch return error.SnapshotCorrupted;
            for (0..ttl_count) |_| {
                const key_len = readU16Le(reader) catch return error.SnapshotCorrupted;
                if (key_len > codec.MAX_KEY_LEN) return error.SnapshotCorrupted;

                const key = try allocator.alloc(u8, key_len);
                defer allocator.free(key);
                reader.readSliceAll(key) catch return error.SnapshotCorrupted;

                const expireAt: i64 = @bitCast(readU64Le(reader) catch return error.SnapshotCorrupted);
                if (expireAt <= now) {
                    _ = try loaded_shard.tree.delete(key);
                    continue;
                }
                if (loaded_shard.tree.lookup(key) == null) continue;
                if (loaded_shard.ttl_index.contains(key)) return error.SnapshotCorrupted;

                const owned_ttl_key = try state.base_allocator.dupe(u8, key);
                errdefer state.base_allocator.free(owned_ttl_key);
                try loaded_shard.ttl_index.put(state.base_allocator, owned_ttl_key, expireAt);
            }
        }
    }

    if (stream.seek != data_len) return error.SnapshotCorrupted;

    for (&state.shards, 0..) |*live_shard, index| {
        live_shard.lock.lockUncancelable(live_shard.io);
        defer live_shard.lock.unlock(live_shard.io);

        live_shard.replaceStorageUnlocked(
            loaded_shards[index].arena,
            loaded_shards[index].ttl_index,
            loaded_shards[index].tree,
        );
    }
    committed = true;

    return .{
        .checkpoint_lsn = checkpoint_lsn,
        .records_loaded = total_records,
    };
}

/// Writes one snapshot file with one requested on-disk format version.
///
/// Time Complexity: O(n + b), where `n` is retained key and TTL entry count and `b` is total serialized bytes.
///
/// Allocator: Uses `allocator` for sorting scratch, temporary serialized values, and the temporary path.
///
/// Thread Safety: Acquires the shared visibility gate plus one shard shared lock at a time. The resulting snapshot fully reflects all mutations with `lsn <= checkpoint_lsn`, while newer mutations may also appear and remain recoverable from retained WAL records.
fn writeSnapshotFile(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    path: []const u8,
    checkpoint_lsn: u64,
    version: u32,
) !SnapshotWriteResult {
    std.debug.assert(version == 1 or version == VERSION);

    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp", .{path});
    defer allocator.free(tmp_path);

    if (std.Io.Dir.path.dirname(path)) |dir_path| {
        if (dir_path.len != 0) {
            const dir = try std.Io.Dir.cwd().createDirPathOpen(state.io, dir_path, .{});
            dir.close(state.io);
        }
    }

    const tmp_file = try std.Io.Dir.cwd().createFile(state.io, tmp_path, .{ .truncate = true });
    defer tmp_file.close(state.io);
    errdefer std.Io.Dir.cwd().deleteFile(state.io, tmp_path) catch {};

    var writer = CrcFileWriter.init(state.io, tmp_file);
    var value_buf = std.ArrayList(u8).empty;
    defer value_buf.deinit(allocator);

    try writer.writeAll(&MAGIC);
    try writer.writeU32Le(version);
    try writer.writeU64Le(checkpoint_lsn);
    try writer.writeU32Le(@intCast(state.shards.len));

    var total_records: usize = 0;

    for (&state.shards, 0..) |*shard, shard_idx| {
        total_records += try writeOneShardSnapshot(allocator, &writer, &value_buf, shard, shard_idx, version);
    }

    var crc_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &crc_buf, writer.final(), .little);
    try tmp_file.writeStreamingAll(state.io, &crc_buf);
    try tmp_file.sync(state.io);
    try std.Io.Dir.cwd().rename(tmp_path, std.Io.Dir.cwd(), path, state.io);

    return .{
        .checkpoint_lsn = checkpoint_lsn,
        .records_written = total_records,
    };
}

/// Writes one shard's currently visible state into the snapshot stream.
///
/// Time Complexity: O(n log n + b), where `n` is the shard entry count and `b` is total serialized value bytes for that shard.
///
/// Allocator: Uses `allocator` for sorted borrowed-entry slices and temporary serialized value bytes.
///
/// Thread Safety: Acquires the shared visibility gate plus this shard's shared lock for the duration of the shard-serialization window, which blocks writers globally during that window.
fn writeOneShardSnapshot(
    allocator: std.mem.Allocator,
    writer: *CrcFileWriter,
    value_buf: *std.ArrayList(u8),
    shard: *const runtime_shard.Shard,
    shard_idx: usize,
    version: u32,
) !usize {
    const live_shard = @constCast(shard);
    live_shard.visibility_gate.lockShared();
    defer live_shard.visibility_gate.unlockShared();
    live_shard.lock.lockSharedUncancelable(shard.io);
    defer live_shard.lock.unlockShared(shard.io);

    try writer.writeU32Le(@intCast(shard_idx));
    var dummy_ctx: u8 = 0;
    const entry_count = try shard.tree.forEach(&dummy_ctx, noopVisit);
    try writer.writeU32Le(@intCast(entry_count));

    var write_ctx = EntryWriteCtx{
        .allocator = allocator,
        .writer = writer,
        .value_buf = value_buf,
    };
    _ = try shard.tree.forEach(&write_ctx, writeEntryVisit);

    if (version >= 2) {
        const ttl_entries = try collectSortedTtlEntries(allocator, shard);
        defer allocator.free(ttl_entries);

        try writer.writeU32Le(@intCast(ttl_entries.len));
        for (ttl_entries) |entry| {
            std.debug.assert(entry.key.len <= codec.MAX_KEY_LEN);
            try writer.writeU16Le(@intCast(entry.key.len));
            try writer.writeAll(entry.key);
            try writer.writeU64Le(@bitCast(entry.expireAt));
        }
    }

    return entry_count;
}

fn noopVisit(_: *anyopaque, _: []const u8, _: *const Value) !void {}

const EntryWriteCtx = struct {
    allocator: std.mem.Allocator,
    writer: *CrcFileWriter,
    value_buf: *std.ArrayList(u8),
};

fn writeEntryVisit(ctx_ptr: *anyopaque, key: []const u8, value: *const Value) !void {
    const ctx: *EntryWriteCtx = @ptrCast(@alignCast(ctx_ptr));
    std.debug.assert(key.len <= codec.MAX_KEY_LEN);

    try ctx.writer.writeU16Le(@intCast(key.len));
    try ctx.writer.writeAll(key);

    ctx.value_buf.clearRetainingCapacity();
    try codec.serializeValue(ctx.allocator, value, ctx.value_buf, 0);
    std.debug.assert(ctx.value_buf.items.len <= codec.MAX_VAL_LEN);

    try ctx.writer.writeU32Le(@intCast(ctx.value_buf.items.len));
    try ctx.writer.writeAll(ctx.value_buf.items);
}

/// Collects one shard's TTL metadata into a lexicographically sorted borrowed view.
///
/// Time Complexity: O(n log n), where `n` is the shard TTL entry count.
///
/// Allocator: Allocates the returned borrowed-entry slice with `allocator`.
///
/// Ownership: Returned entries borrow shard-owned keys and are valid only while the caller still holds the shard lock.
fn collectSortedTtlEntries(allocator: std.mem.Allocator, shard: *const runtime_shard.Shard) ![]BorrowedTtlEntry {
    const entries = try allocator.alloc(BorrowedTtlEntry, shard.ttl_index.count());
    var index: usize = 0;
    var iterator = shard.ttl_index.iterator();
    while (iterator.next()) |entry| : (index += 1) {
        entries[index] = .{
            .key = entry.key_ptr.*,
            .expireAt = entry.value_ptr.*,
        };
    }
    std.mem.sort(BorrowedTtlEntry, entries, {}, borrowedTtlLessThan);
    return entries;
}

/// Orders borrowed TTL entries lexicographically by key bytes.
///
/// Time Complexity: O(min(a, b)), where `a` and `b` are the compared key lengths.
///
/// Allocator: Does not allocate.
fn borrowedTtlLessThan(_: void, left: BorrowedTtlEntry, right: BorrowedTtlEntry) bool {
    return std.mem.lessThan(u8, left.key, right.key);
}

/// Reads one little-endian `u16` from `reader`.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
fn readU16Le(reader: anytype) !u16 {
    var buf: [2]u8 = undefined;
    try reader.readSliceAll(&buf);
    return std.mem.readInt(u16, &buf, .little);
}

/// Reads one little-endian `u32` from `reader`.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
fn readU32Le(reader: anytype) !u32 {
    var buf: [4]u8 = undefined;
    try reader.readSliceAll(&buf);
    return std.mem.readInt(u32, &buf, .little);
}

/// Reads one little-endian `u64` from `reader`.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
fn readU64Le(reader: anytype) !u64 {
    var buf: [8]u8 = undefined;
    try reader.readSliceAll(&buf);
    return std.mem.readInt(u64, &buf, .little);
}

/// Inserts one owned key/value pair into runtime state for snapshot tests.
///
/// Time Complexity: O(k + v), where `k` is `key.len` and `v` is deep-clone work for `value`.
///
/// Allocator: Uses the target shard arena for the owned key and cloned value.
fn putTestValue(state: *runtime_state.DatabaseState, key: []const u8, value: Value) !void {
    const shard_idx = runtime_shard.getShardIndex(key);
    const shard = &state.shards[shard_idx];
    shard.lock.lockUncancelable(shard.io);
    defer shard.lock.unlock(shard.io);

    shard.rebindTreeAllocator();
    const shard_allocator = shard.arena.allocator();
    const value_ptr = try shard_allocator.create(Value);
    value_ptr.* = value;
    try shard.tree.insert(try shard_allocator.dupe(u8, key), value_ptr);
}

/// Inserts one owned TTL entry into runtime state for snapshot tests.
///
/// Time Complexity: O(k), where `k` is `key.len`.
///
/// Allocator: Uses `state.base_allocator` for the owned TTL key and map growth.
fn putTestTtl(state: *runtime_state.DatabaseState, key: []const u8, expireAt: i64) !void {
    const shard_idx = runtime_shard.getShardIndex(key);
    const shard = &state.shards[shard_idx];
    shard.lock.lockUncancelable(shard.io);
    defer shard.lock.unlock(shard.io);

    const owned_key = try state.base_allocator.dupe(u8, key);
    errdefer state.base_allocator.free(owned_key);
    try shard.ttl_index.put(state.base_allocator, owned_key, expireAt);
    shard.has_ttl_entries = true;
}

fn allocTmpPathTest(allocator: std.mem.Allocator, tmp: std.testing.TmpDir, basename: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, basename });
}

/// Reads one whole snapshot file into owned memory for test assertions.
///
/// Time Complexity: O(n), where `n` is the file size.
///
/// Allocator: Allocates the returned byte slice with `allocator`.
///
/// Ownership: Caller owns the returned slice.
fn readAllTest(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{});
    defer file.close(std.testing.io);
    const file_size_u64 = try file.length(std.testing.io);
    const file_size = std.math.cast(usize, file_size_u64) orelse return error.OutOfMemory;
    return try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, allocator, .limited(file_size + 1));
}

/// Corrupts one byte in the target file for snapshot corruption tests.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
fn xorByteTest(path: []const u8, offset: u64, mask: u8) !void {
    const file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
    defer file.close(std.testing.io);

    var byte: [1]u8 = undefined;
    if ((try file.readPositionalAll(std.testing.io, &byte, offset)) != byte.len) return error.EndOfStream;
    byte[0] ^= mask;
    try file.writePositionalAll(std.testing.io, &byte, offset);
}

/// Rewrites one snapshot file from payload bytes and appends a fresh CRC trailer.
///
/// Time Complexity: O(n), where `n` is `payload.len`.
///
/// Allocator: Does not allocate.
///
/// Ownership: Borrows `payload` for the duration of the rewrite only.
fn writeSnapshotPayloadWithCrcTest(path: []const u8, payload: []const u8) !void {
    const file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{ .truncate = true });
    defer file.close(std.testing.io);

    try file.writeStreamingAll(std.testing.io, payload);

    var crc_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &crc_buf, std.hash.crc.Crc32.hash(payload), .little);
    try file.writeStreamingAll(std.testing.io, &crc_buf);
}

test "snapshot write and load roundtrip values and ttl metadata" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "roundtrip.snapshot");
    defer testing.allocator.free(path);

    var source = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer source.deinit();

    try putTestValue(&source, "alpha", .{ .string = "hello" });
    try putTestValue(&source, "{user:1}:beta", .{ .integer = 7 });
    try putTestValue(&source, "{user:2}:gamma", .{ .boolean = true });
    try putTestTtl(&source, "alpha", runtime_shard.unixNow(std.testing.io) + 60);
    try putTestTtl(&source, "{user:2}:gamma", runtime_shard.unixNow(std.testing.io) - 60);

    const write_result = try write(&source, testing.allocator, path, 41);
    try testing.expectEqual(@as(u64, 41), write_result.checkpoint_lsn);
    try testing.expectEqual(@as(usize, 3), write_result.records_written);

    var loaded = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer loaded.deinit();

    const load_result = try load(&loaded, testing.allocator, path);
    try testing.expectEqual(@as(u64, 41), load_result.checkpoint_lsn);
    try testing.expectEqual(@as(usize, 3), load_result.records_loaded);

    try testing.expectEqualStrings("hello", loaded.shards[runtime_shard.getShardIndex("alpha")].tree.lookup("alpha").?.string);
    try testing.expectEqual(@as(i64, 7), loaded.shards[runtime_shard.getShardIndex("{user:1}:beta")].tree.lookup("{user:1}:beta").?.integer);
    try testing.expect(loaded.shards[runtime_shard.getShardIndex("{user:2}:gamma")].tree.lookup("{user:2}:gamma") == null);
    try testing.expectEqual(@as(?i64, source.shards[runtime_shard.getShardIndex("alpha")].ttl_index.get("alpha").?), loaded.shards[runtime_shard.getShardIndex("alpha")].ttl_index.get("alpha"));
    try testing.expect(loaded.shards[runtime_shard.getShardIndex("{user:2}:gamma")].ttl_index.get("{user:2}:gamma") == null);
}

test "snapshot write is deterministic for the same state" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const first_path = try allocTmpPathTest(testing.allocator, tmp, "first.snapshot");
    defer testing.allocator.free(first_path);
    const second_path = try allocTmpPathTest(testing.allocator, tmp, "second.snapshot");
    defer testing.allocator.free(second_path);

    var state = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer state.deinit();

    try putTestValue(&state, "zeta", .{ .integer = 1 });
    try putTestValue(&state, "alpha", .{ .integer = 2 });
    try putTestValue(&state, "{user:3}:beta", .{ .string = "ok" });
    try putTestTtl(&state, "zeta", 123);
    try putTestTtl(&state, "alpha", 456);

    _ = try write(&state, testing.allocator, first_path, 9);
    _ = try write(&state, testing.allocator, second_path, 9);

    const first_bytes = try readAllTest(testing.allocator, first_path);
    defer testing.allocator.free(first_bytes);
    const second_bytes = try readAllTest(testing.allocator, second_path);
    defer testing.allocator.free(second_bytes);

    try testing.expectEqualSlices(u8, first_bytes, second_bytes);
}

test "snapshot load supports version 1 files without ttl sections" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "v1.snapshot");
    defer testing.allocator.free(path);

    var source = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer source.deinit();

    try putTestValue(&source, "alpha", .{ .integer = 5 });
    try putTestValue(&source, "beta", .{ .boolean = false });
    try putTestTtl(&source, "alpha", 999);

    _ = try writeSnapshotFile(&source, testing.allocator, path, 17, 1);

    var loaded = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer loaded.deinit();

    const result = try load(&loaded, testing.allocator, path);
    try testing.expectEqual(@as(u64, 17), result.checkpoint_lsn);
    try testing.expectEqual(@as(usize, 2), result.records_loaded);
    try testing.expectEqual(@as(i64, 5), loaded.shards[runtime_shard.getShardIndex("alpha")].tree.lookup("alpha").?.integer);
    try testing.expectEqual(false, loaded.shards[runtime_shard.getShardIndex("beta")].tree.lookup("beta").?.boolean);
    try testing.expect(loaded.shards[runtime_shard.getShardIndex("alpha")].ttl_index.get("alpha") == null);
}

test "snapshot load returns file not found for missing files" {
    const testing = std.testing;

    var state = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer state.deinit();

    try testing.expectError(error.FileNotFound, load(&state, testing.allocator, "missing.snapshot"));
}

test "snapshot load rejects bad magic" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "bad-magic.snapshot");
    defer testing.allocator.free(path);

    var state = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer state.deinit();
    try putTestValue(&state, "alpha", .{ .integer = 1 });
    _ = try write(&state, testing.allocator, path, 1);

    const bytes = try readAllTest(testing.allocator, path);
    defer testing.allocator.free(bytes);

    var payload = std.ArrayList(u8).empty;
    defer payload.deinit(testing.allocator);
    try payload.appendSlice(testing.allocator, bytes[0 .. bytes.len - 4]);
    payload.items[0] ^= 0xff;
    try writeSnapshotPayloadWithCrcTest(path, payload.items);

    try testing.expectError(error.SnapshotCorrupted, load(&state, testing.allocator, path));
}

test "snapshot load rejects bad version" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "bad-version.snapshot");
    defer testing.allocator.free(path);

    var state = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer state.deinit();
    try putTestValue(&state, "alpha", .{ .integer = 1 });
    _ = try write(&state, testing.allocator, path, 1);

    const bytes = try readAllTest(testing.allocator, path);
    defer testing.allocator.free(bytes);

    var payload = std.ArrayList(u8).empty;
    defer payload.deinit(testing.allocator);
    try payload.appendSlice(testing.allocator, bytes[0 .. bytes.len - 4]);
    std.mem.writeInt(u32, @ptrCast(payload.items[4..8]), 99, .little);
    try writeSnapshotPayloadWithCrcTest(path, payload.items);

    try testing.expectError(error.SnapshotCorrupted, load(&state, testing.allocator, path));
}

test "snapshot load rejects bad crc" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "bad-crc.snapshot");
    defer testing.allocator.free(path);

    var state = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer state.deinit();
    try putTestValue(&state, "alpha", .{ .integer = 1 });
    _ = try write(&state, testing.allocator, path, 1);
    try xorByteTest(path, 8, 0x01);

    try testing.expectError(error.SnapshotCorrupted, load(&state, testing.allocator, path));
}

test "snapshot load rejects duplicate shard indices and trailing bytes" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const dup_path = try allocTmpPathTest(testing.allocator, tmp, "duplicate-shard.snapshot");
    defer testing.allocator.free(dup_path);
    const trailing_path = try allocTmpPathTest(testing.allocator, tmp, "trailing.snapshot");
    defer testing.allocator.free(trailing_path);

    var state = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer state.deinit();
    try putTestValue(&state, "alpha", .{ .integer = 1 });
    try putTestValue(&state, "beta", .{ .integer = 2 });

    _ = try write(&state, testing.allocator, dup_path, 1);
    _ = try write(&state, testing.allocator, trailing_path, 1);

    const dup_bytes = try readAllTest(testing.allocator, dup_path);
    defer testing.allocator.free(dup_bytes);

    const trailing_bytes = try readAllTest(testing.allocator, trailing_path);
    defer testing.allocator.free(trailing_bytes);

    var shard_indices = std.ArrayList(usize).empty;
    defer shard_indices.deinit(testing.allocator);

    var pos: usize = 20;
    for (0..runtime_state.NUM_SHARDS) |_| {
        try shard_indices.append(testing.allocator, pos);
        const value_count = std.mem.readInt(u32, dup_bytes[pos + 4 ..][0..4], .little);
        pos += 8;
        for (0..value_count) |_| {
            const key_len = std.mem.readInt(u16, dup_bytes[pos..][0..2], .little);
            pos += 2 + key_len;
            const value_len = std.mem.readInt(u32, dup_bytes[pos..][0..4], .little);
            pos += 4 + value_len;
        }
        const ttl_count = std.mem.readInt(u32, dup_bytes[pos..][0..4], .little);
        pos += 4;
        for (0..ttl_count) |_| {
            const key_len = std.mem.readInt(u16, dup_bytes[pos..][0..2], .little);
            pos += 2 + key_len + 8;
        }
    }

    {
        var payload = std.ArrayList(u8).empty;
        defer payload.deinit(testing.allocator);
        try payload.appendSlice(testing.allocator, dup_bytes[0 .. dup_bytes.len - 4]);
        const first_shard_idx = std.mem.readInt(u32, dup_bytes[shard_indices.items[0]..][0..4], .little);
        std.mem.writeInt(u32, @ptrCast(payload.items[shard_indices.items[1] .. shard_indices.items[1] + 4]), first_shard_idx, .little);
        try writeSnapshotPayloadWithCrcTest(dup_path, payload.items);
    }

    {
        var payload = std.ArrayList(u8).empty;
        defer payload.deinit(testing.allocator);
        try payload.appendSlice(testing.allocator, trailing_bytes[0 .. trailing_bytes.len - 4]);
        try payload.append(testing.allocator, 'x');
        try writeSnapshotPayloadWithCrcTest(trailing_path, payload.items);
    }

    try testing.expectError(error.SnapshotCorrupted, load(&state, testing.allocator, dup_path));
    try testing.expectError(error.SnapshotCorrupted, load(&state, testing.allocator, trailing_path));
}

test "snapshot load rejects malformed lengths and truncated payloads" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const bad_len_path = try allocTmpPathTest(testing.allocator, tmp, "bad-len.snapshot");
    defer testing.allocator.free(bad_len_path);
    const truncated_path = try allocTmpPathTest(testing.allocator, tmp, "truncated.snapshot");
    defer testing.allocator.free(truncated_path);

    var state = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer state.deinit();
    try putTestValue(&state, "alpha", .{ .integer = 1 });

    _ = try write(&state, testing.allocator, bad_len_path, 1);
    _ = try write(&state, testing.allocator, truncated_path, 1);

    {
        const bytes = try readAllTest(testing.allocator, bad_len_path);
        defer testing.allocator.free(bytes);

        var payload = std.ArrayList(u8).empty;
        defer payload.deinit(testing.allocator);
        try payload.appendSlice(testing.allocator, bytes[0 .. bytes.len - 4]);

        var pos: usize = 20;
        for (0..runtime_state.NUM_SHARDS) |_| {
            pos += 4;
            const value_count = std.mem.readInt(u32, payload.items[pos..][0..4], .little);
            pos += 4;
            if (value_count > 0) {
                std.mem.writeInt(u16, @ptrCast(payload.items[pos .. pos + 2]), codec.MAX_KEY_LEN + 1, .little);
                break;
            }
            const ttl_count = std.mem.readInt(u32, payload.items[pos..][0..4], .little);
            pos += 4;
            for (0..ttl_count) |_| {
                const key_len = std.mem.readInt(u16, payload.items[pos..][0..2], .little);
                pos += 2 + key_len + 8;
            }
        }

        try writeSnapshotPayloadWithCrcTest(bad_len_path, payload.items);
    }

    {
        const file = try std.Io.Dir.cwd().openFile(std.testing.io, truncated_path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        const size = try file.length(std.testing.io);
        try file.setLength(std.testing.io, size - 5);
    }

    try testing.expectError(error.SnapshotCorrupted, load(&state, testing.allocator, bad_len_path));
    try testing.expectError(error.SnapshotCorrupted, load(&state, testing.allocator, truncated_path));
}

test "snapshot load rejects duplicate value records within one shard" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "duplicate-value.snapshot");
    defer testing.allocator.free(path);

    var source = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer source.deinit();
    try putTestValue(&source, "alpha", .{ .integer = 1 });
    _ = try write(&source, testing.allocator, path, 1);

    const bytes = try readAllTest(testing.allocator, path);
    defer testing.allocator.free(bytes);

    var payload = std.ArrayList(u8).empty;
    defer payload.deinit(testing.allocator);
    try payload.appendSlice(testing.allocator, bytes[0 .. bytes.len - 4]);

    var pos: usize = 20;
    for (0..runtime_state.NUM_SHARDS) |_| {
        pos += 4;
        const value_count_offset = pos;
        const value_count = std.mem.readInt(u32, bytes[pos..][0..4], .little);
        pos += 4;
        if (value_count > 0) {
            const first_record_start = pos;
            const key_len = std.mem.readInt(u16, bytes[pos..][0..2], .little);
            pos += 2 + key_len;
            const value_len = std.mem.readInt(u32, bytes[pos..][0..4], .little);
            pos += 4 + value_len;
            const first_record_len = pos - first_record_start;

            std.mem.writeInt(u32, @ptrCast(payload.items[value_count_offset .. value_count_offset + 4]), value_count + 1, .little);

            var rewritten = std.ArrayList(u8).empty;
            defer rewritten.deinit(testing.allocator);
            try rewritten.appendSlice(testing.allocator, payload.items[0..pos]);
            try rewritten.appendSlice(testing.allocator, payload.items[first_record_start .. first_record_start + first_record_len]);
            try rewritten.appendSlice(testing.allocator, payload.items[pos..]);
            try writeSnapshotPayloadWithCrcTest(path, rewritten.items);
            break;
        }

        const ttl_count = std.mem.readInt(u32, bytes[pos..][0..4], .little);
        pos += 4;
        for (0..ttl_count) |_| {
            const key_len = std.mem.readInt(u16, bytes[pos..][0..2], .little);
            pos += 2 + key_len + 8;
        }
    }

    var loaded = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer loaded.deinit();
    try testing.expectError(error.SnapshotCorrupted, load(&loaded, testing.allocator, path));
}

test "corrupted snapshot does not partially mutate existing state" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "no-partial-mutate.snapshot");
    defer testing.allocator.free(path);

    var source = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer source.deinit();
    try putTestValue(&source, "alpha", .{ .integer = 1 });
    _ = try write(&source, testing.allocator, path, 1);
    try xorByteTest(path, 10, 0x80);

    var target = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer target.deinit();
    try putTestValue(&target, "sentinel", .{ .integer = 99 });

    try testing.expectError(error.SnapshotCorrupted, load(&target, testing.allocator, path));
    try testing.expectEqual(@as(i64, 99), target.shards[runtime_shard.getShardIndex("sentinel")].tree.lookup("sentinel").?.integer);
    try testing.expect(target.shards[runtime_shard.getShardIndex("alpha")].tree.lookup("alpha") == null);
}

test "snapshot load preserves shard lock usability after publishing replacement storage" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "lock-usable.snapshot");
    defer testing.allocator.free(path);

    var source = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer source.deinit();
    try putTestValue(&source, "alpha", .{ .integer = 1 });
    _ = try write(&source, testing.allocator, path, 1);

    var loaded = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer loaded.deinit();
    _ = try load(&loaded, testing.allocator, path);

    const shard = &loaded.shards[runtime_shard.getShardIndex("alpha")];

    shard.lock.lockSharedUncancelable(shard.io);
    try testing.expectEqual(@as(i64, 1), shard.tree.lookup("alpha").?.integer);
    shard.lock.unlockShared(shard.io);

    shard.lock.lockUncancelable(shard.io);
    defer shard.lock.unlock(shard.io);
    shard.resetUnlocked();
    try testing.expect(shard.tree.lookup("alpha") == null);
}

test "snapshot load ignores ttl entries for missing keys" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "orphan-ttl.snapshot");
    defer testing.allocator.free(path);

    var source = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer source.deinit();
    try putTestValue(&source, "alpha", .{ .integer = 1 });
    try putTestTtl(&source, "ghost", 1234);
    _ = try write(&source, testing.allocator, path, 1);

    var loaded = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer loaded.deinit();
    _ = try load(&loaded, testing.allocator, path);

    try testing.expectEqual(@as(i64, 1), loaded.shards[runtime_shard.getShardIndex("alpha")].tree.lookup("alpha").?.integer);
    try testing.expect(loaded.shards[runtime_shard.getShardIndex("ghost")].ttl_index.get("ghost") == null);
}

test "snapshot load rejects duplicate ttl keys" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "duplicate-ttl.snapshot");
    defer testing.allocator.free(path);

    var source = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer source.deinit();
    try putTestValue(&source, "alpha", .{ .integer = 1 });
    try putTestTtl(&source, "alpha", runtime_shard.unixNow(std.testing.io) + 10);
    _ = try write(&source, testing.allocator, path, 1);

    const bytes = try readAllTest(testing.allocator, path);
    defer testing.allocator.free(bytes);

    var payload = std.ArrayList(u8).empty;
    defer payload.deinit(testing.allocator);
    try payload.appendSlice(testing.allocator, bytes[0 .. bytes.len - 4]);

    var pos: usize = 20;
    for (0..runtime_state.NUM_SHARDS) |shard_idx| {
        _ = shard_idx;
        pos += 4;
        const value_count = std.mem.readInt(u32, bytes[pos..][0..4], .little);
        pos += 4;
        for (0..value_count) |_| {
            const key_len = std.mem.readInt(u16, bytes[pos..][0..2], .little);
            pos += 2 + key_len;
            const value_len = std.mem.readInt(u32, bytes[pos..][0..4], .little);
            pos += 4 + value_len;
        }
        const ttl_count_offset = pos;
        const ttl_count = std.mem.readInt(u32, bytes[pos..][0..4], .little);
        pos += 4;
        if (ttl_count == 1) {
            const ttl_record_len = 2 + std.mem.readInt(u16, bytes[pos..][0..2], .little) + 8;
            std.mem.writeInt(u32, @ptrCast(payload.items[ttl_count_offset .. ttl_count_offset + 4]), 2, .little);

            var rewritten = std.ArrayList(u8).empty;
            defer rewritten.deinit(testing.allocator);
            try rewritten.appendSlice(testing.allocator, payload.items[0 .. pos + ttl_record_len]);
            try rewritten.appendSlice(testing.allocator, payload.items[pos .. pos + ttl_record_len]);
            try rewritten.appendSlice(testing.allocator, payload.items[pos + ttl_record_len ..]);
            try writeSnapshotPayloadWithCrcTest(path, rewritten.items);
            break;
        }
        for (0..ttl_count) |_| {
            const key_len = std.mem.readInt(u16, bytes[pos..][0..2], .little);
            pos += 2 + key_len + 8;
        }
    }

    var loaded = runtime_state.DatabaseState.init(std.testing.io, testing.allocator, null);
    defer loaded.deinit();
    try testing.expectError(error.SnapshotCorrupted, load(&loaded, testing.allocator, path));
}
