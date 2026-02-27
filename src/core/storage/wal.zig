//! Storage-owned append-side WAL for live durability ordering.
//! Cost: Appends are O(k + v) by key and serialized value size; `open` is O(1) for empty-file setup.
//! Allocator: Uses the caller-provided allocator for owned path/state and temporary serialization scratch.

const std = @import("std");
const builtin = @import("builtin");
const codec = @import("../internal/codec.zig");
const Value = @import("../types/value.zig").Value;

/// Record tag for insert and update payload records.
const tag_put: u8 = 0x01;

/// Record tag for delete tombstone records.
const tag_delete: u8 = 0x02;

/// Record tag for absolute-expiration metadata updates.
const tag_expire: u8 = 0x03;

/// Record tag for multi-record batch begin markers.
const tag_batch_begin: u8 = 0x05;

/// Record tag for multi-record batch commit markers.
const tag_batch_commit: u8 = 0x06;

/// Fixed payload length for one batch BEGIN marker.
const batch_begin_payload_len: usize = 4;

/// Fixed payload length for one batch COMMIT marker.
const batch_commit_payload_len: usize = 12;

/// Maximum nesting depth allowed in durability payload encoding.
pub const MAX_DEPTH: usize = codec.MAX_DEPTH;

/// Maximum accepted key length for durability records.
pub const MAX_KEY_LEN: u32 = codec.MAX_KEY_LEN;

/// Maximum accepted serialized value size for durability records.
pub const MAX_VAL_LEN: u32 = codec.MAX_VAL_LEN;

/// Durability policy for WAL fsync behavior.
pub const FsyncMode = enum { always, none, batched_async };

/// WAL open/runtime configuration with replay floor and fsync policy.
pub const WalOptions = struct {
    fsync_mode: FsyncMode = .always,
    fsync_interval_ms: u32 = 2,
    min_lsn: u64 = 0,
};

/// Replay callback table used by later recovery steps.
///
/// Ownership: Callback arguments are borrowed for the duration of each replay callback only.
pub const ReplayApplier = struct {
    ctx: *anyopaque,
    put: *const fn (ctx: *anyopaque, key: []const u8, value: *const Value) anyerror!void,
    delete: *const fn (ctx: *anyopaque, key: []const u8) anyerror!void,
    expire: *const fn (ctx: *anyopaque, key: []const u8, expire_at_sec: i64) anyerror!void,
};

/// One borrowed plain-key PUT member serialized into a WAL batch envelope.
///
/// Ownership: Borrows `key` and `value` for the duration of one `append_put_batch` call only.
pub const PutBatchWrite = struct {
    key: []const u8,
    value: *const Value,
};

/// Shared mutable runtime state guarded by the WAL mutex and atomics.
const FlushState = struct {
    file: std.fs.File,
    mutex: std.Thread.Mutex,
    dirty: std.atomic.Value(bool),
    last_appended_lsn: std.atomic.Value(u64),
    last_fsync_lsn: std.atomic.Value(u64),
    flush_error: std.atomic.Value(u8),
    stop_flush_thread: std.atomic.Value(bool),
    fsync_interval_ms: u32,
    flush_thread: ?std.Thread,
    bytes_written_total: std.atomic.Value(u64),
};

/// One materialized WAL record slice inside a scratch append buffer.
const EncodedAppendRecord = struct {
    start_offset: usize,
    lsn_offset: usize,
    value_offset: usize,
    end_offset: usize,
    tag: u8,
};

/// Storage-owned WAL handle for live append operations over a single file.
///
/// Thread Safety: Point and batch append operations serialize through the internal WAL mutex and coordinate with the optional async flush thread through atomics.
pub const Wal = struct {
    path: []u8,
    next_lsn: u64,
    fsync_mode: FsyncMode,
    allocator: std.mem.Allocator,
    state: *FlushState,

    /// Opens or creates an append-only WAL file at `path`.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Allocates owned path/state using `allocator`.
    ///
    /// Ownership: Owns the returned path copy and flush state until `close`.
    ///
    /// Thread Safety: Safe for concurrent append callers; they serialize on the internal WAL mutex after the caller has entered the engine visibility window.
    pub fn open(
        path: []const u8,
        options: WalOptions,
        applier: ReplayApplier,
        allocator: std.mem.Allocator,
    ) !Wal {
        _ = applier;

        const file = try std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
        });
        errdefer file.close();

        if ((try file.getEndPos()) != 0) return error.NotImplemented;

        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);

        const state = try allocator.create(FlushState);
        errdefer allocator.destroy(state);
        state.* = .{
            .file = file,
            .mutex = .{},
            .dirty = std.atomic.Value(bool).init(false),
            .last_appended_lsn = std.atomic.Value(u64).init(0),
            .last_fsync_lsn = std.atomic.Value(u64).init(0),
            .flush_error = std.atomic.Value(u8).init(0),
            .stop_flush_thread = std.atomic.Value(bool).init(false),
            .fsync_interval_ms = options.fsync_interval_ms,
            .flush_thread = null,
            .bytes_written_total = std.atomic.Value(u64).init(0),
        };

        var wal = Wal{
            .path = owned_path,
            .next_lsn = options.min_lsn + 1,
            .fsync_mode = options.fsync_mode,
            .allocator = allocator,
            .state = state,
        };
        errdefer wal.close();

        if (wal.fsync_mode == .batched_async) {
            wal.state.flush_thread = try std.Thread.spawn(.{}, flush_thread_main, .{wal.state});
        }

        return wal;
    }

    /// Appends a PUT record to the WAL before a live in-memory publication.
    ///
    /// Time Complexity: O(k + v), where `k` is `key.len` and `v` is the serialized value size.
    ///
    /// Allocator: Uses `self.allocator` for temporary serialization scratch.
    ///
    /// Thread Safety: Serializes with other append and close operations through the internal WAL mutex.
    pub fn append_put(self: *Wal, key: []const u8, value: *const Value) !void {
        var value_buf = std.ArrayList(u8).init(self.allocator);
        defer value_buf.deinit();

        try codec.serialize_value(self.allocator, value, &value_buf, 0);
        _ = try self.append_record(tag_put, key, value_buf.items);
    }

    /// Appends a DELETE record to the WAL before a live in-memory publication.
    ///
    /// Time Complexity: O(k), where `k` is `key.len`.
    ///
    /// Allocator: Uses `self.allocator` for the temporary encoded record.
    ///
    /// Thread Safety: Serializes with other append and close operations through the internal WAL mutex.
    pub fn append_delete(self: *Wal, key: []const u8) !void {
        _ = try self.append_record(tag_delete, key, "");
    }

    /// Appends an EXPIRE record to the WAL before a live in-memory publication.
    ///
    /// Time Complexity: O(k), where `k` is `key.len`.
    ///
    /// Allocator: Uses `self.allocator` for the temporary encoded record.
    ///
    /// Thread Safety: Serializes with other append and close operations through the internal WAL mutex.
    pub fn append_expire(self: *Wal, key: []const u8, expire_at_sec: i64) !void {
        var payload: [8]u8 = undefined;
        std.mem.writeInt(i64, &payload, expire_at_sec, .little);
        _ = try self.append_record(tag_expire, key, &payload);
    }

    /// Appends one contiguous WAL batch envelope for plain-key PUT writes.
    ///
    /// Time Complexity: O(n * (k + v)), where `n` is `writes.len`.
    ///
    /// Allocator: Uses `self.allocator` for temporary envelope and serialization scratch.
    ///
    /// Ownership: Borrows every `key` and `value` pointer for the duration of the call only.
    ///
    /// Thread Safety: Serializes with other append and close operations through the internal WAL mutex.
    pub fn append_put_batch(self: *Wal, writes: []const PutBatchWrite) !u64 {
        if (writes.len == 0) return error.EmptyBatch;

        var envelope = std.ArrayList(u8).empty;
        defer envelope.deinit(self.allocator);

        var records = std.ArrayList(EncodedAppendRecord).empty;
        defer records.deinit(self.allocator);
        try records.ensureTotalCapacityPrecise(self.allocator, writes.len + 2);

        var begin_payload: [batch_begin_payload_len]u8 = undefined;
        @memset(begin_payload, 0);
        try records.append(self.allocator, try append_encoded_record_placeholder(
            self.allocator,
            &envelope,
            tag_batch_begin,
            "",
            &begin_payload,
        ));

        var value_buf = std.ArrayList(u8).empty;
        defer value_buf.deinit(self.allocator);

        for (writes) |write| {
            value_buf.clearRetainingCapacity();
            try codec.serialize_value(self.allocator, write.value, &value_buf, 0);
            try records.append(self.allocator, try append_encoded_record_placeholder(
                self.allocator,
                &envelope,
                tag_put,
                write.key,
                value_buf.items,
            ));
        }

        var commit_payload: [batch_commit_payload_len]u8 = undefined;
        @memset(commit_payload, 0);
        try records.append(self.allocator, try append_encoded_record_placeholder(
            self.allocator,
            &envelope,
            tag_batch_commit,
            "",
            &commit_payload,
        ));

        if (self.fsync_mode == .batched_async and self.state.flush_error.load(.acquire) != 0) {
            return error.WalFlushFailed;
        }

        self.state.mutex.lock();
        defer self.state.mutex.unlock();

        if (self.fsync_mode == .batched_async and self.state.flush_error.load(.acquire) != 0) {
            return error.WalFlushFailed;
        }

        const begin_lsn = self.next_lsn;
        const member_record_count: u32 = @intCast(writes.len);

        for (records.items, 0..) |record, index| {
            const lsn = self.next_lsn;
            self.next_lsn += 1;

            switch (record.tag) {
                tag_batch_begin => {
                    const value_offset = record.value_offset;
                    const begin_slice = envelope.items[value_offset .. value_offset + batch_begin_payload_len];
                    std.mem.writeInt(u32, begin_slice, member_record_count, .little);
                },
                tag_batch_commit => {
                    const value_offset = record.value_offset;
                    const lsn_slice = envelope.items[value_offset .. value_offset + 8];
                    const count_slice = envelope.items[value_offset + 8 .. value_offset + batch_commit_payload_len];
                    std.mem.writeInt(u64, lsn_slice, begin_lsn, .little);
                    std.mem.writeInt(u32, count_slice, member_record_count, .little);
                },
                tag_put => std.debug.assert(index > 0 and index + 1 < records.items.len),
                else => unreachable,
            }

            patch_encoded_record(envelope.items, record, lsn);
        }

        const commit_lsn = begin_lsn + @as(u64, @intCast(writes.len)) + 1;
        try write_all_tracked(self.state.file, envelope.items);
        _ = self.state.bytes_written_total.fetchAdd(envelope.items.len, .monotonic);
        self.state.last_appended_lsn.store(commit_lsn, .release);
        self.state.dirty.store(true, .release);
        try self.maybe_fsync_inline(commit_lsn);
        return commit_lsn;
    }

    /// Flushes buffered WAL state to stable storage.
    ///
    /// Time Complexity: O(1) CPU work plus filesystem-dependent fsync latency.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Duplicates the file descriptor under the internal WAL mutex, then performs the actual `fsync` without holding that mutex.
    pub fn fsync(self: *Wal) !void {
        const handle = blk: {
            self.state.mutex.lock();
            errdefer self.state.mutex.unlock();
            const dup_fd = try std.posix.dup(self.state.file.handle);
            self.state.mutex.unlock();
            break :blk dup_fd;
        };
        defer std.posix.close(handle);

        try perform_fsync(handle);

        const appended = self.state.last_appended_lsn.load(.acquire);
        store_max_lsn(&self.state.last_fsync_lsn, appended);
        self.state.dirty.store(false, .release);
    }

    /// Closes the WAL handle and releases storage-owned resources.
    ///
    /// Time Complexity: O(1) CPU work plus optional async flush join latency.
    ///
    /// Allocator: Frees the owned path and flush state through `self.allocator`.
    ///
    /// Thread Safety: Not safe to race with other operations; callers must ensure the enclosing engine state has stopped issuing WAL appends before `close`.
    pub fn close(self: *Wal) void {
        if (self.fsync_mode == .batched_async) {
            self.state.stop_flush_thread.store(true, .release);
            if (self.state.flush_thread) |thread| thread.join();

            const dirty = self.state.dirty.load(.acquire);
            const last_appended = self.state.last_appended_lsn.load(.acquire);
            const last_fsync = self.state.last_fsync_lsn.load(.acquire);
            if (dirty or last_fsync < last_appended) {
                self.fsync() catch {};
            }
        }

        self.state.file.close();
        self.allocator.destroy(self.state);
        self.allocator.free(self.path);
    }

    fn maybe_fsync_inline(self: *Wal, lsn: u64) !void {
        switch (self.fsync_mode) {
            .always => {
                try perform_fsync(self.state.file.handle);
                store_max_lsn(&self.state.last_fsync_lsn, lsn);
                self.state.dirty.store(false, .release);
            },
            .none => {},
            .batched_async => {},
        }
    }

    fn append_record(self: *Wal, tag: u8, key: []const u8, value_bytes: []const u8) !u64 {
        if (self.fsync_mode == .batched_async and self.state.flush_error.load(.acquire) != 0) {
            return error.WalFlushFailed;
        }

        self.state.mutex.lock();
        defer self.state.mutex.unlock();

        if (self.fsync_mode == .batched_async and self.state.flush_error.load(.acquire) != 0) {
            return error.WalFlushFailed;
        }

        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(self.allocator);

        const lsn = self.next_lsn;
        self.next_lsn += 1;

        try buf.appendNTimes(self.allocator, 0, 4);
        try write_u64_le(self.allocator, &buf, lsn);
        try buf.append(self.allocator, tag);
        try write_u16_le(self.allocator, &buf, @intCast(key.len));
        try buf.appendSlice(self.allocator, key);
        try write_u32_le(self.allocator, &buf, @intCast(value_bytes.len));
        try buf.appendSlice(self.allocator, value_bytes);

        const crc = std.hash.crc.Crc32.hash(buf.items[4..]);
        std.mem.writeInt(u32, buf.items[0..4], crc, .little);

        try write_all_tracked(self.state.file, buf.items);
        _ = self.state.bytes_written_total.fetchAdd(buf.items.len, .monotonic);
        self.state.last_appended_lsn.store(lsn, .release);
        self.state.dirty.store(true, .release);
        try self.maybe_fsync_inline(lsn);
        return lsn;
    }
};

/// Opens a WAL handle and reserves the owned storage state.
///
/// Time Complexity: O(1).
///
/// Allocator: Allocates the owned path and flush state from `allocator`.
///
/// Ownership: Returns a WAL handle that owns its copied path and flush state until `close`.
///
/// Thread Safety: The returned handle is safe for concurrent append callers; Step 6 still rejects non-empty WAL files because replay is not implemented yet.
pub fn open(
    path: []const u8,
    options: WalOptions,
    applier: ReplayApplier,
    allocator: std.mem.Allocator,
) !Wal {
    return Wal.open(path, options, applier, allocator);
}

fn flush_thread_main(state: *FlushState) void {
    const interval_ns = @max(@as(u64, state.fsync_interval_ms), 1) * std.time.ns_per_ms;
    while (!state.stop_flush_thread.load(.acquire)) {
        std.Thread.sleep(interval_ns);
        if (state.stop_flush_thread.load(.acquire)) break;
        flush_once(state) catch {};
    }
}

fn flush_once(state: *FlushState) !void {
    const snapshot = blk: {
        state.mutex.lock();
        errdefer state.mutex.unlock();

        if (!state.dirty.load(.acquire)) {
            state.mutex.unlock();
            return;
        }

        const target_lsn = state.last_appended_lsn.load(.acquire);
        const dup_fd = try std.posix.dup(state.file.handle);
        state.dirty.store(false, .release);
        state.mutex.unlock();
        break :blk .{ .target_lsn = target_lsn, .dup_fd = dup_fd };
    };
    defer std.posix.close(snapshot.dup_fd);

    perform_fsync(snapshot.dup_fd) catch |err| {
        state.flush_error.store(1, .release);
        state.dirty.store(true, .release);
        return err;
    };

    store_max_lsn(&state.last_fsync_lsn, snapshot.target_lsn);
}

fn store_max_lsn(dst: *std.atomic.Value(u64), candidate: u64) void {
    var current = dst.load(.acquire);
    while (candidate > current) {
        if (dst.cmpxchgWeak(current, candidate, .acq_rel, .acquire)) |actual| {
            current = actual;
        } else {
            return;
        }
    }
}

fn append_encoded_record_placeholder(
    allocator: std.mem.Allocator,
    buf: *std.ArrayList(u8),
    tag: u8,
    key: []const u8,
    value_bytes: []const u8,
) !EncodedAppendRecord {
    const start_offset = buf.items.len;
    try buf.appendNTimes(allocator, 0, 4);

    const lsn_offset = buf.items.len;
    try buf.appendNTimes(allocator, 0, 8);
    try buf.append(allocator, tag);
    try write_u16_le(allocator, buf, @intCast(key.len));
    try buf.appendSlice(allocator, key);
    try write_u32_le(allocator, buf, @intCast(value_bytes.len));

    const value_offset = buf.items.len;
    try buf.appendSlice(allocator, value_bytes);

    return .{
        .start_offset = start_offset,
        .lsn_offset = lsn_offset,
        .value_offset = value_offset,
        .end_offset = buf.items.len,
        .tag = tag,
    };
}

fn patch_encoded_record(buf: []u8, record: EncodedAppendRecord, lsn: u64) void {
    std.mem.writeInt(u64, buf[record.lsn_offset .. record.lsn_offset + 8], lsn, .little);
    const crc = std.hash.crc.Crc32.hash(buf[record.start_offset + 4 .. record.end_offset]);
    std.mem.writeInt(u32, buf[record.start_offset .. record.start_offset + 4], crc, .little);
}

fn write_u16_le(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), value: u16) !void {
    var tmp: [2]u8 = undefined;
    std.mem.writeInt(u16, &tmp, value, .little);
    try buf.appendSlice(allocator, &tmp);
}

fn write_u32_le(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), value: u32) !void {
    var tmp: [4]u8 = undefined;
    std.mem.writeInt(u32, &tmp, value, .little);
    try buf.appendSlice(allocator, &tmp);
}

fn write_u64_le(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), value: u64) !void {
    var tmp: [8]u8 = undefined;
    std.mem.writeInt(u64, &tmp, value, .little);
    try buf.appendSlice(allocator, &tmp);
}

var g_fail_next_write = std.atomic.Value(bool).init(false);
var g_fail_next_fsync = std.atomic.Value(bool).init(false);

fn write_all_tracked(file: std.fs.File, bytes: []const u8) !void {
    if (builtin.is_test and g_fail_next_write.swap(false, .acq_rel)) {
        return error.SimulatedWriteFailure;
    }
    try file.writeAll(bytes);
}

fn perform_fsync(fd: std.posix.fd_t) !void {
    if (builtin.is_test and g_fail_next_fsync.swap(false, .acq_rel)) {
        return error.SimulatedFsyncFailure;
    }
    try std.posix.fsync(fd);
}

/// Test-only fault injection hooks for WAL append and fsync paths.
///
/// Time Complexity: O(1) per hook arm.
///
/// Allocator: Does not allocate.
pub const test_hooks = if (builtin.is_test) struct {
    /// Arms one-shot WAL append failure injection for test builds.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    pub fn failNextWrite() void {
        g_fail_next_write.store(true, .release);
    }

    /// Arms one-shot fsync failure injection for test builds.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    pub fn failNextFsync() void {
        g_fail_next_fsync.store(true, .release);
    }
} else struct {};

fn read_all_test(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    return try file.readToEndAlloc(allocator, 1 << 20);
}

fn collect_record_tags_test(allocator: std.mem.Allocator, bytes: []const u8) ![]u8 {
    var tags = std.ArrayList(u8).empty;
    errdefer tags.deinit(allocator);

    var cursor: usize = 0;
    while (cursor < bytes.len) {
        if (bytes.len - cursor < 19) return error.EndOfStream;

        const tag = bytes[cursor + 12];
        const key_len = std.mem.readInt(u16, bytes[cursor + 13 .. cursor + 15], .little);
        const value_len = std.mem.readInt(u32, bytes[cursor + 15 + key_len .. cursor + 19 + key_len], .little);
        const record_len = 19 + @as(usize, key_len) + @as(usize, value_len);
        if (cursor + record_len > bytes.len) return error.EndOfStream;

        try tags.append(allocator, tag);
        cursor += record_len;
    }

    return try tags.toOwnedSlice(allocator);
}

fn noop_put(ctx: *anyopaque, key: []const u8, value: *const Value) !void {
    _ = ctx;
    _ = key;
    _ = value;
}

fn noop_delete(ctx: *anyopaque, key: []const u8) !void {
    _ = ctx;
    _ = key;
}

fn noop_expire(ctx: *anyopaque, key: []const u8, expire_at_sec: i64) !void {
    _ = ctx;
    _ = key;
    _ = expire_at_sec;
}

fn noop_applier() ReplayApplier {
    return .{
        .ctx = undefined,
        .put = noop_put,
        .delete = noop_delete,
        .expire = noop_expire,
    };
}

test "open succeeds for a new empty wal file" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step6-empty.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    var wal = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
    defer wal.close();
}

test "open rejects non-empty wal files until replay lands" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step6-non-empty.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    {
        const file = try std.fs.cwd().createFile(path, .{ .truncate = true });
        defer file.close();
        try file.writeAll("seed");
    }

    try testing.expectError(error.NotImplemented, open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator));
}

test "append point records writes put delete and expire tags" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step6-point-records.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    var wal = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
    defer wal.close();

    const value = Value{ .string = "hello" };
    try wal.append_put("alpha", &value);
    try wal.append_delete("alpha");
    try wal.append_expire("beta", 123);
    try wal.fsync();

    const bytes = try read_all_test(testing.allocator, path);
    defer testing.allocator.free(bytes);

    const tags = try collect_record_tags_test(testing.allocator, bytes);
    defer testing.allocator.free(tags);

    try testing.expectEqualSlices(u8, &.{ tag_put, tag_delete, tag_expire }, tags);
}

test "append_put_batch emits one committed batch envelope" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step6-batch.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    var wal = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
    defer wal.close();

    const one = Value{ .integer = 1 };
    const two = Value{ .integer = 2 };
    _ = try wal.append_put_batch(&.{
        .{ .key = "alpha", .value = &one },
        .{ .key = "beta", .value = &two },
    });
    try wal.fsync();

    const bytes = try read_all_test(testing.allocator, path);
    defer testing.allocator.free(bytes);

    const tags = try collect_record_tags_test(testing.allocator, bytes);
    defer testing.allocator.free(tags);

    try testing.expectEqualSlices(u8, &.{ tag_batch_begin, tag_put, tag_put, tag_batch_commit }, tags);
}

test "fsync failure propagates from always mode append" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step6-fsync-failure.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    var wal = try open(path, .{ .fsync_mode = .always }, noop_applier(), testing.allocator);
    defer wal.close();

    const value = Value{ .integer = 1 };
    test_hooks.failNextFsync();
    try testing.expectError(error.SimulatedFsyncFailure, wal.append_put("alpha", &value));
}
