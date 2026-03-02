//! Storage-owned WAL handle for live append paths and WAL-backed open.
//! Time Complexity: Appends are O(k + v) by key and serialized value size; open is O(n) over existing WAL bytes when replay runs.
//! Allocator: Uses the caller-provided allocator for owned path/state and temporary serialization scratch.
//! Thread Safety: Shared `Wal` instances serialize append state internally and coordinate with the optional async flush thread through atomics and a mutex.

const std = @import("std");
const builtin = @import("builtin");
const codec = @import("../internal/codec.zig");
const storage_replay = @import("replay.zig");
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
///
/// Ownership: Alias of the shared codec limit; does not own storage.
pub const MAX_KEY_LEN: u32 = codec.MAX_KEY_LEN;

/// Maximum accepted serialized value size for durability records.
///
/// Ownership: Alias of the shared codec limit; does not own storage.
pub const MAX_VAL_LEN: u32 = codec.MAX_VAL_LEN;

/// Selects how WAL appends reach stable storage.
///
/// Ownership: Pure value configuration.
pub const FsyncMode = enum { always, none, batched_async };

/// Configures WAL replay floor and fsync policy for one handle.
///
/// Ownership: Pure value configuration copied by `open`.
pub const WalOptions = struct {
    /// Chooses inline fsync, no fsync, or the background batched flusher.
    fsync_mode: FsyncMode = .always,
    /// Sets the background flush cadence for `.batched_async`.
    fsync_interval_ms: u32 = 2,
    /// Skips accepted replay records whose LSN is at or below this floor.
    min_lsn: u64 = 0,
};

/// Provides physical replay callbacks for WAL recovery.
///
/// Ownership: Stores borrowed callback pointers and context only. Callback arguments are borrowed for the duration of each replay callback.
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
/// Allocator: Owns the copied path and flush state through `allocator`; per-call append scratch also uses that allocator.
///
/// Thread Safety: Point and batch append operations serialize through the internal WAL mutex and coordinate with the optional async flush thread through atomics.
pub const Wal = struct {
    path: []u8,
    next_lsn: u64,
    fsync_mode: FsyncMode,
    allocator: std.mem.Allocator,
    state: *FlushState,

    /// Opens or creates one WAL file at `path` and replays any surviving prefix.
    ///
    /// Time Complexity: O(n) over existing WAL bytes, or O(1) when the file is empty.
    ///
    /// Allocator: Allocates owned path/state using `allocator`.
    ///
    /// Ownership: Owns the returned path copy and flush state until `close`.
    ///
    /// Thread Safety: Safe for concurrent append callers after open returns; replay itself runs before the handle is published to other threads.
    pub fn open(
        path: []const u8,
        options: WalOptions,
        applier: anytype,
        allocator: std.mem.Allocator,
    ) !Wal {
        const file = try std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
        });
        errdefer file.close();

        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);

        const replay_result = try storage_replay.replay(applier, allocator, file, options.min_lsn);
        try file.seekFromEnd(0);

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
            .next_lsn = @max(replay_result.last_lsn, options.min_lsn) + 1,
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

    /// Compacts this WAL by removing records with `lsn <= max_lsn_inclusive`.
    ///
    /// Standalone records newer than the cutoff are preserved. Committed batches
    /// are retained or dropped as a whole based on the `COMMIT` record LSN.
    /// Incomplete or structurally invalid trailing batches stop compaction at the
    /// corresponding `BEGIN` record so no partial batch fragment is retained.
    ///
    /// Time Complexity: O(n + m), where `n` is WAL bytes scanned and `m` is retained bytes rewritten.
    ///
    /// Allocator: Uses `self.allocator` for the temporary compaction path and copied-record scratch.
    ///
    /// Thread Safety: Serializes with other append and close operations through the internal WAL mutex.
    pub fn truncate_up_to_lsn(self: *Wal, max_lsn_inclusive: u64) !void {
        self.state.mutex.lock();
        defer self.state.mutex.unlock();

        const newest_lsn: u64 = if (self.next_lsn > 0) self.next_lsn - 1 else 0;
        if (newest_lsn <= max_lsn_inclusive) {
            try self.state.file.setEndPos(0);
            try self.state.file.seekTo(0);
            store_max_lsn(&self.state.last_fsync_lsn, newest_lsn);
            self.state.dirty.store(false, .release);
            return;
        }

        const tmp_path = try std.fmt.allocPrint(self.allocator, "{s}.compact.tmp", .{self.path});
        defer self.allocator.free(tmp_path);

        const old_file = self.state.file;
        var tmp_file = try std.fs.cwd().createFile(tmp_path, .{
            .read = true,
            .truncate = true,
        });
        var promoted_tmp_file = false;
        defer if (!promoted_tmp_file) tmp_file.close();
        errdefer std.fs.cwd().deleteFile(tmp_path) catch {};

        try storage_replay.compact_up_to_lsn(self.allocator, old_file, tmp_file, max_lsn_inclusive);
        try std.posix.fsync(tmp_file.handle);
        try tmp_file.seekFromEnd(0);

        try std.fs.cwd().rename(tmp_path, self.path);
        old_file.close();
        self.state.file = tmp_file;
        promoted_tmp_file = true;
        store_max_lsn(&self.state.last_fsync_lsn, newest_lsn);
        self.state.dirty.store(false, .release);
    }

    /// Returns whether close should force one final flush before teardown.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Uses atomics only; safe to call while coordinating shutdown from one owner thread.
    pub fn needs_close_fsync(self: *const Wal) bool {
        if (self.fsync_mode != .batched_async) return false;
        const dirty = self.state.dirty.load(.acquire);
        const last_appended = self.state.last_appended_lsn.load(.acquire);
        const last_fsync = self.state.last_fsync_lsn.load(.acquire);
        return dirty or last_fsync < last_appended;
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

    /// Applies the inline fsync policy after one successful append.
    ///
    /// Time Complexity: O(1) CPU work plus optional filesystem-dependent fsync latency.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Must run while the caller still owns the WAL append serialization path.
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

    /// Encodes and appends one standalone WAL record.
    ///
    /// Time Complexity: O(k + v), where `k` is `key.len` and `v` is `value_bytes.len`.
    ///
    /// Allocator: Uses `self.allocator` for the temporary encoded record buffer.
    ///
    /// Thread Safety: Serializes through the WAL mutex and must not race with `close`.
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

/// Opens a WAL handle and replays any surviving WAL prefix before returning.
///
/// Time Complexity: O(n) over existing WAL bytes, or O(1) when the file is empty.
///
/// Allocator: Allocates the owned path and flush state from `allocator`.
///
/// Ownership: Returns a WAL handle that owns its copied path and flush state until `close`.
///
/// Thread Safety: The returned handle is safe for concurrent append callers after open returns; replay happens before the handle becomes shared.
pub fn open(
    path: []const u8,
    options: WalOptions,
    applier: anytype,
    allocator: std.mem.Allocator,
) !Wal {
    return Wal.open(path, options, applier, allocator);
}

/// Runs the background fsync loop for `.batched_async` WAL handles.
///
/// Time Complexity: O(1) per idle iteration plus optional filesystem-dependent fsync latency when dirty data is present.
///
/// Allocator: Does not allocate.
///
/// Thread Safety: Coordinates only through `FlushState` atomics and the WAL mutex.
fn flush_thread_main(state: *FlushState) void {
    const interval_ns = @max(@as(u64, state.fsync_interval_ms), 1) * std.time.ns_per_ms;
    while (!state.stop_flush_thread.load(.acquire)) {
        std.Thread.sleep(interval_ns);
        if (state.stop_flush_thread.load(.acquire)) break;
        flush_once(state) catch {};
    }
}

/// Attempts one asynchronous flush pass against the current durable prefix.
///
/// Time Complexity: O(1) CPU work plus filesystem-dependent fsync latency when dirty data is present.
///
/// Allocator: Does not allocate.
///
/// Thread Safety: Uses the WAL mutex to snapshot the target file descriptor and LSN before fsyncing without the mutex held.
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

/// Raises one atomic LSN watermark when `candidate` is newer.
///
/// Time Complexity: O(1) expected, with retry cost bounded by concurrent atomic contention.
///
/// Allocator: Does not allocate.
///
/// Thread Safety: Lock-free atomic update safe for concurrent publishers.
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

/// Appends one WAL record shell with zeroed CRC and LSN fields.
///
/// Time Complexity: O(k + v), where `k` is `key.len` and `v` is `value_bytes.len`.
///
/// Allocator: Uses `allocator` to grow `buf`.
///
/// Ownership: Borrows `key` and `value_bytes` for the duration of the append only.
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

/// Fills in the final LSN and CRC for one encoded record already present in `buf`.
///
/// Time Complexity: O(r), where `r` is the encoded record length.
///
/// Allocator: Does not allocate.
fn patch_encoded_record(buf: []u8, record: EncodedAppendRecord, lsn: u64) void {
    std.mem.writeInt(u64, buf[record.lsn_offset .. record.lsn_offset + 8], lsn, .little);
    const crc = std.hash.crc.Crc32.hash(buf[record.start_offset + 4 .. record.end_offset]);
    std.mem.writeInt(u32, buf[record.start_offset .. record.start_offset + 4], crc, .little);
}

/// Appends one little-endian `u16` to the target buffer.
///
/// Time Complexity: O(1) amortized, excluding buffer growth.
///
/// Allocator: Uses `allocator` only if `buf` must grow.
fn write_u16_le(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), value: u16) !void {
    var tmp: [2]u8 = undefined;
    std.mem.writeInt(u16, &tmp, value, .little);
    try buf.appendSlice(allocator, &tmp);
}

/// Appends one little-endian `u32` to the target buffer.
///
/// Time Complexity: O(1) amortized, excluding buffer growth.
///
/// Allocator: Uses `allocator` only if `buf` must grow.
fn write_u32_le(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), value: u32) !void {
    var tmp: [4]u8 = undefined;
    std.mem.writeInt(u32, &tmp, value, .little);
    try buf.appendSlice(allocator, &tmp);
}

/// Appends one little-endian `u64` to the target buffer.
///
/// Time Complexity: O(1) amortized, excluding buffer growth.
///
/// Allocator: Uses `allocator` only if `buf` must grow.
fn write_u64_le(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), value: u64) !void {
    var tmp: [8]u8 = undefined;
    std.mem.writeInt(u64, &tmp, value, .little);
    try buf.appendSlice(allocator, &tmp);
}

var g_fail_next_write = std.atomic.Value(bool).init(false);
var g_fail_next_fsync = std.atomic.Value(bool).init(false);

/// Writes all bytes to the WAL file, honoring test fault injection when enabled.
///
/// Time Complexity: O(n), where `n` is `bytes.len`, plus filesystem write latency.
///
/// Allocator: Does not allocate.
///
/// Ownership: Borrows `bytes` for the duration of the write only.
fn write_all_tracked(file: std.fs.File, bytes: []const u8) !void {
    if (builtin.is_test and g_fail_next_write.swap(false, .acq_rel)) {
        return error.SimulatedWriteFailure;
    }
    try file.writeAll(bytes);
}

/// Forces one file descriptor to stable storage, honoring test fault injection when enabled.
///
/// Time Complexity: O(1) CPU work plus filesystem-dependent fsync latency.
///
/// Allocator: Does not allocate.
fn perform_fsync(fd: std.posix.fd_t) !void {
    if (builtin.is_test and g_fail_next_fsync.swap(false, .acq_rel)) {
        return error.SimulatedFsyncFailure;
    }
    try std.posix.fsync(fd);
}

/// Exposes test-only fault injection hooks for WAL append and fsync paths.
///
/// Time Complexity: O(1) per hook arm.
///
/// Allocator: Does not allocate.
///
/// Thread Safety: Uses atomics only; intended for single-test coordination in test builds.
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

/// Reads one whole test WAL file into owned memory.
///
/// Time Complexity: O(n), where `n` is the file size.
///
/// Allocator: Allocates the returned byte slice with `allocator`.
///
/// Ownership: Caller owns the returned slice.
fn read_all_test(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    return try file.readToEndAlloc(allocator, 1 << 20);
}

/// Extracts WAL record tags from a test byte stream.
///
/// Time Complexity: O(n) over `bytes.len`.
///
/// Allocator: Allocates the returned tag slice with `allocator`.
///
/// Ownership: Caller owns the returned slice.
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

/// Ignores one replayed `PUT` during WAL tests.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
fn noop_put(ctx: *anyopaque, key: []const u8, value: *const Value) !void {
    _ = ctx;
    _ = key;
    _ = value;
}

/// Ignores one replayed `DELETE` during WAL tests.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
fn noop_delete(ctx: *anyopaque, key: []const u8) !void {
    _ = ctx;
    _ = key;
}

/// Ignores one replayed `EXPIRE` during WAL tests.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
fn noop_expire(ctx: *anyopaque, key: []const u8, expire_at_sec: i64) !void {
    _ = ctx;
    _ = key;
    _ = expire_at_sec;
}

/// Builds a replay applier that discards every recovered mutation.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
///
/// Ownership: Returns borrowed callback pointers only.
fn noop_applier() ReplayApplier {
    return .{
        .ctx = undefined,
        .put = noop_put,
        .delete = noop_delete,
        .expire = noop_expire,
    };
}

/// Classifies one replay callback observed by `ReplayCollector`.
const ReplayEventKind = enum { put, delete, expire };

/// Stores one replay callback observed during WAL tests.
const ReplayEvent = struct {
    kind: ReplayEventKind,
    key: []u8,
    expire_at: i64 = 0,
};

/// Captures replay callbacks into owned test events.
const ReplayCollector = struct {
    allocator: std.mem.Allocator,
    events: std.ArrayList(ReplayEvent),

    /// Initializes one empty replay collector for WAL tests.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Stores `allocator` for future event ownership.
    fn init(allocator: std.mem.Allocator) ReplayCollector {
        return .{
            .allocator = allocator,
            .events = std.ArrayList(ReplayEvent).init(allocator),
        };
    }

    /// Releases all owned replay-event keys and event storage.
    ///
    /// Time Complexity: O(n), where `n` is `events.items.len`.
    ///
    /// Allocator: Frees event-owned memory through `self.allocator`.
    fn deinit(self: *ReplayCollector) void {
        for (self.events.items) |event| {
            self.allocator.free(event.key);
        }
        self.events.deinit();
        self.* = undefined;
    }

    /// Exposes this collector as a `ReplayApplier`.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Returns borrowed callback pointers tied to `self`.
    fn applier(self: *ReplayCollector) ReplayApplier {
        return .{
            .ctx = self,
            .put = replay_collector_put,
            .delete = replay_collector_delete,
            .expire = replay_collector_expire,
        };
    }
};

/// Records one replayed `PUT` callback into the collector.
///
/// Time Complexity: O(k), where `k` is `key.len`.
///
/// Allocator: Duplicates `key` with the collector allocator.
fn replay_collector_put(ctx: *anyopaque, key: []const u8, value: *const Value) !void {
    _ = value;
    const collector: *ReplayCollector = @ptrCast(@alignCast(ctx));
    try collector.events.append(.{
        .kind = .put,
        .key = try collector.allocator.dupe(u8, key),
    });
}

/// Records one replayed `DELETE` callback into the collector.
///
/// Time Complexity: O(k), where `k` is `key.len`.
///
/// Allocator: Duplicates `key` with the collector allocator.
fn replay_collector_delete(ctx: *anyopaque, key: []const u8) !void {
    const collector: *ReplayCollector = @ptrCast(@alignCast(ctx));
    try collector.events.append(.{
        .kind = .delete,
        .key = try collector.allocator.dupe(u8, key),
    });
}

/// Records one replayed `EXPIRE` callback into the collector.
///
/// Time Complexity: O(k), where `k` is `key.len`.
///
/// Allocator: Duplicates `key` with the collector allocator.
fn replay_collector_expire(ctx: *anyopaque, key: []const u8, expire_at_sec: i64) !void {
    const collector: *ReplayCollector = @ptrCast(@alignCast(ctx));
    try collector.events.append(.{
        .kind = .expire,
        .key = try collector.allocator.dupe(u8, key),
        .expire_at = expire_at_sec,
    });
}

/// Writes one handcrafted WAL record for replay-focused tests.
///
/// Time Complexity: O(k + v), where `k` is `key.len` and `v` is `value_bytes.len`.
///
/// Allocator: Uses `allocator` for the temporary encoded record buffer.
///
/// Ownership: Borrows `key` and `value_bytes` for the duration of the call only.
fn write_crafted_record_test(
    allocator: std.mem.Allocator,
    file: std.fs.File,
    lsn: u64,
    tag: u8,
    key: []const u8,
    value_bytes: []const u8,
) !void {
    var buf = std.ArrayList(u8).init(allocator);
    defer buf.deinit();

    try buf.appendNTimes(allocator, 0, 4);
    try write_u64_le(allocator, &buf, lsn);
    try buf.append(allocator, tag);
    try write_u16_le(allocator, &buf, @intCast(key.len));
    try buf.appendSlice(allocator, key);
    try write_u32_le(allocator, &buf, @intCast(value_bytes.len));
    try buf.appendSlice(allocator, value_bytes);

    const crc = std.hash.crc.Crc32.hash(buf.items[4..]);
    std.mem.writeInt(u32, buf.items[0..4], crc, .little);
    try file.writeAll(buf.items);
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

test "replay restores standalone put delete and expire records" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step7-replay-standalone.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    {
        var wal = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
        defer wal.close();

        const value = Value{ .string = "hello" };
        try wal.append_put("alpha", &value);
        try wal.append_delete("beta");
        try wal.append_expire("gamma", 123);
    }

    const replay_file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer replay_file.close();

    var collector = ReplayCollector.init(testing.allocator);
    defer collector.deinit();

    const result = try storage_replay.replay(collector.applier(), testing.allocator, replay_file, 0);
    try testing.expectEqual(@as(u64, 3), result.last_lsn);
    try testing.expectEqual(@as(usize, 3), result.records_applied);
    try testing.expectEqual(@as(usize, 3), collector.events.items.len);
    try testing.expectEqual(ReplayEventKind.put, collector.events.items[0].kind);
    try testing.expectEqualStrings("alpha", collector.events.items[0].key);
    try testing.expectEqual(ReplayEventKind.delete, collector.events.items[1].kind);
    try testing.expectEqualStrings("beta", collector.events.items[1].key);
    try testing.expectEqual(ReplayEventKind.expire, collector.events.items[2].kind);
    try testing.expectEqualStrings("gamma", collector.events.items[2].key);
    try testing.expectEqual(@as(i64, 123), collector.events.items[2].expire_at);
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

test "replay restores committed put batch atomically and in order" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step7-batch-replay.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    {
        var wal = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
        defer wal.close();

        const one = Value{ .integer = 1 };
        const two = Value{ .integer = 2 };
        _ = try wal.append_put_batch(&.{
            .{ .key = "alpha", .value = &one },
            .{ .key = "beta", .value = &two },
        });
    }

    const replay_file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer replay_file.close();

    var collector = ReplayCollector.init(testing.allocator);
    defer collector.deinit();

    const result = try storage_replay.replay(collector.applier(), testing.allocator, replay_file, 0);
    try testing.expectEqual(@as(u64, 4), result.last_lsn);
    try testing.expectEqual(@as(usize, 2), result.records_applied);
    try testing.expectEqual(@as(usize, 2), collector.events.items.len);
    try testing.expectEqualStrings("alpha", collector.events.items[0].key);
    try testing.expectEqualStrings("beta", collector.events.items[1].key);
}

test "replay truncates incomplete batch tails safely" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step7-incomplete-tail.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    {
        var wal = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
        defer wal.close();

        const one = Value{ .integer = 1 };
        const two = Value{ .integer = 2 };
        _ = try wal.append_put_batch(&.{
            .{ .key = "alpha", .value = &one },
            .{ .key = "beta", .value = &two },
        });
    }

    {
        const file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer file.close();
        const size = try file.getEndPos();
        try file.setEndPos(size - 3);
    }

    const replay_file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer replay_file.close();

    var collector = ReplayCollector.init(testing.allocator);
    defer collector.deinit();

    const result = try storage_replay.replay(collector.applier(), testing.allocator, replay_file, 0);
    try testing.expectEqual(@as(u64, 2), result.last_lsn);
    try testing.expectEqual(@as(usize, 0), result.records_applied);
    try testing.expectEqual(@as(usize, 0), collector.events.items.len);
    try testing.expectEqual(@as(u64, 0), try replay_file.getEndPos());
}

test "replay truncates corrupt crc tails safely" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step7-corrupt-tail.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    var second_start: u64 = 0;
    {
        var wal = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
        defer wal.close();

        const value = Value{ .string = "hello" };
        try wal.append_put("alpha", &value);
        second_start = try wal.state.file.getEndPos();
        try wal.append_delete("beta");
    }

    {
        const file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer file.close();
        try file.seekTo(second_start);
        var byte: [1]u8 = undefined;
        try file.readNoEof(&byte);
        try file.seekTo(second_start);
        byte[0] ^= 0xff;
        try file.writeAll(&byte);
    }

    const replay_file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer replay_file.close();

    var collector = ReplayCollector.init(testing.allocator);
    defer collector.deinit();

    const result = try storage_replay.replay(collector.applier(), testing.allocator, replay_file, 0);
    try testing.expectEqual(@as(u64, 1), result.last_lsn);
    try testing.expectEqual(@as(usize, 1), result.records_applied);
    try testing.expectEqual(@as(usize, 1), collector.events.items.len);
    try testing.expectEqualStrings("alpha", collector.events.items[0].key);
    try testing.expectEqual(second_start, try replay_file.getEndPos());
}

test "replay truncates invalid batch structures from the matching begin" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step7-invalid-batch.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    const file = try std.fs.cwd().createFile(path, .{ .read = true, .truncate = true });
    defer file.close();

    var put_buf = std.ArrayList(u8).init(testing.allocator);
    defer put_buf.deinit();
    try codec.serialize_value(testing.allocator, &Value{ .integer = 1 }, &put_buf, 0);

    var begin_payload: [batch_begin_payload_len]u8 = undefined;
    std.mem.writeInt(u32, &begin_payload, 1, .little);

    var commit_payload: [batch_commit_payload_len]u8 = undefined;
    std.mem.writeInt(u64, commit_payload[0..8], 999, .little);
    std.mem.writeInt(u32, commit_payload[8..12], 1, .little);

    try write_crafted_record_test(testing.allocator, file, 1, tag_put, "seed", put_buf.items);
    const batch_begin_offset = try file.getEndPos();
    try write_crafted_record_test(testing.allocator, file, 2, tag_batch_begin, "", &begin_payload);
    try write_crafted_record_test(testing.allocator, file, 3, tag_put, "alpha", put_buf.items);
    try write_crafted_record_test(testing.allocator, file, 4, tag_batch_commit, "", &commit_payload);

    const replay_file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer replay_file.close();

    var collector = ReplayCollector.init(testing.allocator);
    defer collector.deinit();

    const result = try storage_replay.replay(collector.applier(), testing.allocator, replay_file, 0);
    try testing.expectEqual(@as(u64, 1), result.last_lsn);
    try testing.expectEqual(@as(usize, 1), result.records_applied);
    try testing.expectEqual(@as(usize, 1), collector.events.items.len);
    try testing.expectEqualStrings("seed", collector.events.items[0].key);
    try testing.expectEqual(batch_begin_offset, try replay_file.getEndPos());
}

test "replay honors min_lsn for standalone records and whole batches" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step7-min-lsn.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    {
        var wal = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
        defer wal.close();

        const one = Value{ .integer = 1 };
        const two = Value{ .integer = 2 };
        const three = Value{ .integer = 3 };
        try wal.append_put("alpha", &one);
        _ = try wal.append_put_batch(&.{
            .{ .key = "beta", .value = &two },
            .{ .key = "gamma", .value = &three },
        });
    }

    {
        const replay_file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer replay_file.close();

        var collector = ReplayCollector.init(testing.allocator);
        defer collector.deinit();

        const result = try storage_replay.replay(collector.applier(), testing.allocator, replay_file, 1);
        try testing.expectEqual(@as(u64, 4), result.last_lsn);
        try testing.expectEqual(@as(usize, 2), result.records_applied);
        try testing.expectEqual(@as(usize, 2), collector.events.items.len);
        try testing.expectEqualStrings("beta", collector.events.items[0].key);
        try testing.expectEqualStrings("gamma", collector.events.items[1].key);
    }

    {
        const replay_file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer replay_file.close();

        var collector = ReplayCollector.init(testing.allocator);
        defer collector.deinit();

        const result = try storage_replay.replay(collector.applier(), testing.allocator, replay_file, 2);
        try testing.expectEqual(@as(u64, 4), result.last_lsn);
        try testing.expectEqual(@as(usize, 0), result.records_applied);
        try testing.expectEqual(@as(usize, 0), collector.events.items.len);
    }
}

test "open on a non-empty wal replays and advances next_lsn" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step7-open-replay.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    {
        var wal = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
        defer wal.close();

        const value = Value{ .integer = 1 };
        try wal.append_put("alpha", &value);
    }

    var collector = ReplayCollector.init(testing.allocator);
    defer collector.deinit();

    var wal = try open(path, .{ .fsync_mode = .none }, collector.applier(), testing.allocator);
    defer wal.close();

    try testing.expectEqual(@as(usize, 1), collector.events.items.len);
    try testing.expectEqualStrings("alpha", collector.events.items[0].key);
    try testing.expectEqual(@as(u64, 2), wal.next_lsn);
}

test "fsync failure propagates from always mode append" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step7-fsync-failure.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    var wal = try open(path, .{ .fsync_mode = .always }, noop_applier(), testing.allocator);
    defer wal.close();

    const value = Value{ .integer = 1 };
    test_hooks.failNextFsync();
    try testing.expectError(error.SimulatedFsyncFailure, wal.append_put("alpha", &value));
}

test "truncate_up_to_lsn drops standalone records at or below the cutoff" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step7-compact-standalone.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    {
        var wal = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
        defer wal.close();

        const one = Value{ .integer = 1 };
        const two = Value{ .integer = 2 };
        try wal.append_put("alpha", &one);
        try wal.append_put("beta", &two);
        try wal.truncate_up_to_lsn(1);
    }

    const replay_file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer replay_file.close();

    var collector = ReplayCollector.init(testing.allocator);
    defer collector.deinit();

    const result = try storage_replay.replay(collector.applier(), testing.allocator, replay_file, 0);
    try testing.expectEqual(@as(u64, 2), result.last_lsn);
    try testing.expectEqual(@as(usize, 1), result.records_applied);
    try testing.expectEqual(@as(usize, 1), collector.events.items.len);
    try testing.expectEqualStrings("beta", collector.events.items[0].key);
}

test "truncate_up_to_lsn keeps committed batches by commit lsn" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step7-compact-batch.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    {
        var wal = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
        defer wal.close();

        const one = Value{ .integer = 1 };
        const two = Value{ .integer = 2 };
        const three = Value{ .integer = 3 };
        _ = try wal.append_put_batch(&.{
            .{ .key = "alpha", .value = &one },
            .{ .key = "beta", .value = &two },
        });
        try wal.append_put("gamma", &three);
        try wal.truncate_up_to_lsn(4);
    }

    const replay_file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer replay_file.close();

    var collector = ReplayCollector.init(testing.allocator);
    defer collector.deinit();

    const result = try storage_replay.replay(collector.applier(), testing.allocator, replay_file, 0);
    try testing.expectEqual(@as(u64, 5), result.last_lsn);
    try testing.expectEqual(@as(usize, 1), result.records_applied);
    try testing.expectEqual(@as(usize, 1), collector.events.items.len);
    try testing.expectEqualStrings("gamma", collector.events.items[0].key);

    var reopened = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
    defer reopened.close();
    try testing.expectEqual(@as(u64, 6), reopened.next_lsn);
}

test "truncate_up_to_lsn drops incomplete trailing batches instead of retaining fragments" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(testing.allocator, "{s}/step7-compact-invalid-tail.wal", .{tmp.sub_path});
    defer testing.allocator.free(path);

    {
        var wal = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
        defer wal.close();

        const one = Value{ .integer = 1 };
        const two = Value{ .integer = 2 };
        _ = try wal.append_put_batch(&.{
            .{ .key = "alpha", .value = &one },
            .{ .key = "beta", .value = &two },
        });
    }

    {
        const file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer file.close();
        const size = try file.getEndPos();
        try file.setEndPos(size - 2);
    }

    {
        var wal = try open(path, .{ .fsync_mode = .none }, noop_applier(), testing.allocator);
        defer wal.close();
        try wal.truncate_up_to_lsn(0);
    }

    const replay_file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer replay_file.close();

    var collector = ReplayCollector.init(testing.allocator);
    defer collector.deinit();

    const result = try storage_replay.replay(collector.applier(), testing.allocator, replay_file, 0);
    try testing.expectEqual(@as(u64, 0), result.last_lsn);
    try testing.expectEqual(@as(usize, 0), result.records_applied);
    try testing.expectEqual(@as(usize, 0), collector.events.items.len);
}
