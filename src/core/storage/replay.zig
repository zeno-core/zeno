//! Storage-owned WAL replay mechanics for physical recovery.
//! Time Complexity: O(n) over WAL bytes scanned, plus decoded payload work for applied `PUT` records.
//! Allocator: Uses explicit allocators for scan scratch, decoded values, and temporary committed-batch buffering.
//! Thread Safety: Not safe to race with WAL appenders; replay mutates and may truncate the underlying file during recovery.

const std = @import("std");
const codec = @import("../internal/codec.zig");
const Value = @import("../types/value.zig").Value;

const tag_put: u8 = 0x01;
const tag_delete: u8 = 0x02;
const tag_expire: u8 = 0x03;
const tag_batch_begin: u8 = 0x05;
const tag_batch_commit: u8 = 0x06;

const batch_begin_payload_len: usize = 4;
const batch_commit_payload_len: usize = 12;

const replay_retain_key_limit: usize = 64 * 1024;
const replay_retain_val_limit: usize = 1 * 1024 * 1024;

/// Reports the replay prefix accepted during WAL open.
///
/// Ownership: Pure value metadata; does not own external memory.
pub const ReplayResult = struct {
    /// Highest accepted LSN from the surviving WAL prefix after truncation.
    last_lsn: u64,
    /// Number of physical mutations actually applied to the replay target.
    records_applied: usize,
};

/// Classifies one scan outcome while walking a WAL file.
const RecordClass = enum {
    valid,
    eof,
    partial_eof,
    corrupt,
};

/// Describes the validated payload carried by one scanned WAL record.
const RecordPayload = union(enum) {
    put,
    delete,
    expire: i64,
    batch_begin: u32,
    batch_commit: struct {
        begin_lsn: u64,
        member_record_count: u32,
    },
};

/// Stores one validated WAL record borrowed from the current scan scratch buffers.
const ScannedRecord = struct {
    start_offset: u64,
    end_offset: u64,
    lsn: u64,
    tag: u8,
    key: []const u8,
    value: []const u8,
    payload: RecordPayload,
};

/// Returns either one scanned record or one terminal scan classification.
const RecordScan = struct {
    class: RecordClass,
    record: ?ScannedRecord,
};

/// Holds one owned batch member while replay waits for a matching commit marker.
const BufferedBatchMutation = struct {
    tag: u8,
    key: []u8,
    value: []u8,
};

/// Selects whether a pending batch should be buffered or skipped past `min_lsn`.
const ReplayBatchMode = enum {
    buffer_batch,
    skip_batch,
};

/// Tracks one pending committed-batch candidate during replay.
const ReplayBatchState = struct {
    begin_offset: u64,
    begin_lsn: u64,
    member_record_count: u32,
    seen_mutations: u32,
    mode: ReplayBatchMode,
    mutations: std.ArrayListUnmanaged(BufferedBatchMutation),
    arena: std.heap.ArenaAllocator,

    /// Initializes one pending-batch state rooted at one `BEGIN` record.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Initializes one arena and empty mutation list from `allocator`.
    fn init(
        allocator: std.mem.Allocator,
        begin_offset: u64,
        begin_lsn: u64,
        member_record_count: u32,
        mode: ReplayBatchMode,
    ) ReplayBatchState {
        return .{
            .begin_offset = begin_offset,
            .begin_lsn = begin_lsn,
            .member_record_count = member_record_count,
            .seen_mutations = 0,
            .mode = mode,
            .mutations = .{},
            .arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    /// Releases all owned buffered-batch memory.
    ///
    /// Time Complexity: O(n), where `n` is `mutations.items.len`.
    ///
    /// Allocator: Frees arena-owned keys, values, and list storage.
    fn deinit(self: *ReplayBatchState) void {
        self.mutations.deinit(self.arena.allocator());
        self.arena.deinit();
        self.* = undefined;
    }

    /// Copies one scanned mutation into the pending batch arena.
    ///
    /// Time Complexity: O(k + v), where `k` is `mutation.key.len` and `v` is `mutation.value.len`.
    ///
    /// Allocator: Duplicates key and value bytes into the batch arena.
    fn append_mutation(self: *ReplayBatchState, mutation: *const ScannedRecord) !void {
        const arena_allocator = self.arena.allocator();
        try self.mutations.append(arena_allocator, .{
            .tag = mutation.tag,
            .key = try arena_allocator.dupe(u8, mutation.key),
            .value = try arena_allocator.dupe(u8, mutation.value),
        });
    }
};

/// Tracks one pending committed batch while compaction decides whether to retain or drop it as one unit.
const CompactBatchState = struct {
    begin_offset: u64,
    begin_lsn: u64,
    member_record_count: u32,
    seen_mutations: u32,
};

/// Replays one WAL file into the provided physical applier.
///
/// Records with `lsn <= min_lsn` are skipped after validation. Committed batches
/// whose `BEGIN` LSN is at or below the floor are skipped as a whole. Partial or
/// corrupt tails are truncated in place before returning.
///
/// Time Complexity: O(n) over WAL bytes plus decoded payload work for applied records.
///
/// Allocator: Uses `allocator` for reusable scan scratch, decoded PUT values, and temporary committed-batch buffering.
///
/// Ownership: Borrows `file` and the `applier` callbacks for the duration of the call only.
///
/// Thread Safety: Not safe to call concurrently with WAL appends or other replay passes against the same file.
pub fn replay(applier: anytype, allocator: std.mem.Allocator, file: std.fs.File, min_lsn: u64) !ReplayResult {
    try file.seekTo(0);

    var result = ReplayResult{
        .last_lsn = 0,
        .records_applied = 0,
    };
    var decode_arena = std.heap.ArenaAllocator.init(allocator);
    defer decode_arena.deinit();

    var key_buf = std.ArrayList(u8).empty;
    defer key_buf.deinit(allocator);
    var val_buf = std.ArrayList(u8).empty;
    defer val_buf.deinit(allocator);

    var pending_batch: ?ReplayBatchState = null;
    defer if (pending_batch) |*batch| batch.deinit();

    while (true) {
        const scanned = try scan_next_record(file, allocator, &key_buf, &val_buf);
        switch (scanned.class) {
            .eof => break,
            .partial_eof => {
                if (pending_batch) |batch| {
                    try truncate_at(file, batch.begin_offset);
                } else {
                    try truncate_at(file, scanned.record.?.start_offset);
                }
                return result;
            },
            .corrupt => {
                const truncate_offset = if (pending_batch) |batch| batch.begin_offset else scanned.record.?.start_offset;
                try truncate_at(file, truncate_offset);
                return result;
            },
            .valid => {},
        }
        const record = scanned.record.?;

        if (pending_batch != null and record.tag == tag_batch_begin) {
            try truncate_at(file, pending_batch.?.begin_offset);
            return result;
        }
        if (pending_batch == null and record.tag == tag_batch_commit) {
            try truncate_at(file, record.start_offset);
            return result;
        }

        defer maybe_drop_replay_scratch(allocator, &key_buf, &val_buf, record.key.len, record.value.len);

        switch (record.payload) {
            .batch_begin => |member_record_count| {
                const mode: ReplayBatchMode = if (record.lsn > min_lsn) .buffer_batch else .skip_batch;
                pending_batch = ReplayBatchState.init(allocator, record.start_offset, record.lsn, member_record_count, mode);
                result.last_lsn = @max(result.last_lsn, record.lsn);
            },
            .batch_commit => |commit| {
                if (pending_batch == null) {
                    try truncate_at(file, record.start_offset);
                    return result;
                }

                const batch = &pending_batch.?;
                if (commit.begin_lsn != batch.begin_lsn or
                    commit.member_record_count != batch.member_record_count or
                    batch.seen_mutations != batch.member_record_count)
                {
                    try truncate_at(file, batch.begin_offset);
                    return result;
                }

                if (batch.mode == .buffer_batch) {
                    const completed = try apply_buffered_batch(applier, decode_arena.allocator(), &result, batch, file);
                    if (!completed) {
                        batch.deinit();
                        pending_batch = null;
                        return result;
                    }
                }

                result.last_lsn = @max(result.last_lsn, record.lsn);
                batch.deinit();
                pending_batch = null;
            },
            else => {
                if (pending_batch) |*batch| {
                    batch.seen_mutations += 1;
                    if (batch.seen_mutations > batch.member_record_count) {
                        try truncate_at(file, batch.begin_offset);
                        return result;
                    }
                    if (batch.mode == .buffer_batch) {
                        try batch.append_mutation(&record);
                    }
                    continue;
                }

                if (record.lsn <= min_lsn) {
                    result.last_lsn = @max(result.last_lsn, record.lsn);
                    continue;
                }

                const applied = try apply_replay_mutation(applier, &decode_arena, &result, &record, file, record.start_offset);
                if (!applied) return result;
            },
        }
    }

    if (pending_batch) |*batch| {
        try truncate_at(file, batch.begin_offset);
        batch.deinit();
        pending_batch = null;
    }

    return result;
}

/// Rewrites the accepted WAL prefix into `dst_file`, dropping records with `lsn <= max_lsn_inclusive`.
///
/// Standalone records newer than the cutoff are preserved. Committed batches are
/// retained or dropped as a whole based on the `COMMIT` record LSN. Partial or
/// structurally invalid trailing batches stop the copy at the corresponding
/// `BEGIN` record so compaction never emits partial batch fragments.
///
/// Time Complexity: O(n + m), where `n` is WAL bytes scanned and `m` is retained bytes rewritten.
///
/// Allocator: Uses `allocator` for reusable scan scratch and temporary copied-record buffers.
///
/// Ownership: Borrows `src_file` and `dst_file` for the duration of the rewrite only.
pub fn compact_up_to_lsn(
    allocator: std.mem.Allocator,
    src_file: std.fs.File,
    dst_file: std.fs.File,
    max_lsn_inclusive: u64,
) !void {
    try src_file.seekTo(0);

    var record = std.ArrayList(u8).empty;
    defer record.deinit(allocator);
    var key_buf = std.ArrayList(u8).empty;
    defer key_buf.deinit(allocator);
    var val_buf = std.ArrayList(u8).empty;
    defer val_buf.deinit(allocator);
    var pending_batch: ?CompactBatchState = null;
    var pending_bytes = std.ArrayList(u8).empty;
    defer pending_bytes.deinit(allocator);

    while (true) {
        const scanned = try scan_next_record(src_file, allocator, &key_buf, &val_buf);
        switch (scanned.class) {
            .eof => break,
            .partial_eof, .corrupt => break,
            .valid => {},
        }
        const record_item = scanned.record.?;
        const batch_is_pending = pending_batch != null;

        if (batch_is_pending and record_item.tag == tag_batch_begin) break;
        if (!batch_is_pending and record_item.tag == tag_batch_commit) break;
        if (batch_is_pending and !is_mutation_tag(record_item.tag) and record_item.tag != tag_batch_commit) break;

        try load_record_bytes(src_file, allocator, &record, record_item.start_offset, record_item.end_offset);
        defer maybe_drop_replay_scratch(allocator, &key_buf, &val_buf, record_item.key.len, record_item.value.len);

        switch (record_item.payload) {
            .batch_begin => |member_record_count| {
                std.debug.assert(!batch_is_pending);
                pending_batch = .{
                    .begin_offset = record_item.start_offset,
                    .begin_lsn = record_item.lsn,
                    .member_record_count = member_record_count,
                    .seen_mutations = 0,
                };
                pending_bytes.clearRetainingCapacity();
                try pending_bytes.appendSlice(allocator, record.items);
            },
            .batch_commit => |commit| {
                if (!batch_is_pending) break;
                const batch = pending_batch.?;
                if (commit.begin_lsn != batch.begin_lsn or
                    commit.member_record_count != batch.member_record_count or
                    batch.seen_mutations != batch.member_record_count)
                {
                    break;
                }

                try pending_bytes.appendSlice(allocator, record.items);
                if (record_item.lsn > max_lsn_inclusive) {
                    try dst_file.writeAll(pending_bytes.items);
                }
                pending_batch = null;
                pending_bytes.clearRetainingCapacity();
            },
            else => {
                if (batch_is_pending) {
                    pending_batch.?.seen_mutations += 1;
                    if (pending_batch.?.seen_mutations > pending_batch.?.member_record_count) break;
                    try pending_bytes.appendSlice(allocator, record.items);
                } else if (record_item.lsn > max_lsn_inclusive) {
                    try dst_file.writeAll(record.items);
                }
            },
        }
    }
}

/// Truncates the WAL file to `offset` and seeks to the new end.
///
/// Time Complexity: O(1) CPU work plus filesystem truncate latency.
///
/// Allocator: Does not allocate.
fn truncate_at(file: std.fs.File, offset: u64) !void {
    try file.setEndPos(offset);
    try file.seekTo(offset);
}

/// Releases oversized scan scratch buffers instead of retaining their capacity.
///
/// Time Complexity: O(1).
///
/// Allocator: May free and recreate the `ArrayList` backing storage through `allocator`.
fn maybe_drop_replay_scratch(
    allocator: std.mem.Allocator,
    key_buf: *std.ArrayList(u8),
    val_buf: *std.ArrayList(u8),
    key_len: usize,
    val_len: usize,
) void {
    if (key_len > replay_retain_key_limit) {
        key_buf.deinit(allocator);
        key_buf.* = .empty;
    }
    if (val_len > replay_retain_val_limit) {
        val_buf.deinit(allocator);
        val_buf.* = .empty;
    }
}

/// Copies one validated WAL record byte range into `buf` so compaction can rewrite it without borrowing scan scratch.
///
/// Time Complexity: O(n), where `n` is `end_offset - start_offset`.
///
/// Allocator: Resizes `buf` through its owned allocator; caller retains ownership of the copied bytes.
fn load_record_bytes(
    file: std.fs.File,
    allocator: std.mem.Allocator,
    buf: *std.ArrayList(u8),
    start_offset: u64,
    end_offset: u64,
) !void {
    const restore_offset = try file.getPos();
    defer file.seekTo(restore_offset) catch {};

    const record_len: usize = @intCast(end_offset - start_offset);
    try buf.resize(allocator, record_len);
    try file.seekTo(start_offset);
    if ((try file.readAll(buf.items)) != record_len) return error.EndOfStream;
}

/// Applies one validated physical replay mutation to the target applier.
///
/// Time Complexity: O(k + v), where `k` is `record.key.len` and `v` is `record.value.len` for `PUT`.
///
/// Allocator: Uses `decode_arena` only for decoded `PUT` values.
///
/// Ownership: Borrows record slices for the duration of the callback only.
fn apply_replay_mutation(
    applier: anytype,
    decode_arena: *std.heap.ArenaAllocator,
    result: *ReplayResult,
    record: *const ScannedRecord,
    file: std.fs.File,
    truncate_offset: u64,
) !bool {
    switch (record.payload) {
        .put => {
            var fbs = std.io.fixedBufferStream(record.value);
            const decode_allocator = decode_arena.allocator();
            const value = codec.deserialize_value(fbs.reader(), decode_allocator, 0) catch |err| switch (err) {
                error.MaxDepthExceeded, error.UnknownValueTag => {
                    try truncate_at(file, truncate_offset);
                    return false;
                },
                else => return err,
            };
            try applier.put(applier.ctx, record.key, &value);
            _ = decode_arena.reset(.retain_capacity);
        },
        .delete => try applier.delete(applier.ctx, record.key),
        .expire => |expire_at| try applier.expire(applier.ctx, record.key, expire_at),
        .batch_begin, .batch_commit => unreachable,
    }

    result.last_lsn = @max(result.last_lsn, record.lsn);
    result.records_applied += 1;
    return true;
}

/// Applies one fully buffered committed batch after its `COMMIT` record validates.
///
/// Time Complexity: O(n * (k + v)), where `n` is `batch.mutations.items.len`.
///
/// Allocator: Uses `allocator` for the temporary decode arena only.
fn apply_buffered_batch(
    applier: anytype,
    allocator: std.mem.Allocator,
    result: *ReplayResult,
    batch: *const ReplayBatchState,
    file: std.fs.File,
) !bool {
    var decode_arena = std.heap.ArenaAllocator.init(allocator);
    defer decode_arena.deinit();

    for (batch.mutations.items) |mutation| {
        const payload = decode_record_payload(mutation.tag, mutation.key, mutation.value) orelse {
            try truncate_at(file, batch.begin_offset);
            return false;
        };
        const record = ScannedRecord{
            .start_offset = batch.begin_offset,
            .end_offset = batch.begin_offset,
            .lsn = batch.begin_lsn,
            .tag = mutation.tag,
            .key = mutation.key,
            .value = mutation.value,
            .payload = payload,
        };
        const applied = try apply_replay_mutation(applier, &decode_arena, result, &record, file, batch.begin_offset);
        if (!applied) return false;
    }

    return true;
}

/// Scans, validates, and classifies the next WAL record from `file`.
///
/// Time Complexity: O(k + v), where `k` is the decoded key length and `v` is the payload length.
///
/// Allocator: Reuses `key_buf` and `val_buf`; may grow them to fit the next record.
///
/// Ownership: Returned `key` and `value` slices borrow `key_buf` and `val_buf` storage until the next scan.
fn scan_next_record(
    file: std.fs.File,
    allocator: std.mem.Allocator,
    key_buf: *std.ArrayList(u8),
    val_buf: *std.ArrayList(u8),
) !RecordScan {
    const record_start = try file.getPos();

    var crc_buf: [4]u8 = undefined;
    switch (try read_exact_classified_file(file, &crc_buf)) {
        .ok => {},
        .eof => return .{ .class = .eof, .record = null },
        .partial => return .{ .class = .partial_eof, .record = .{
            .start_offset = record_start,
            .end_offset = try file.getPos(),
            .lsn = 0,
            .tag = 0,
            .key = "",
            .value = "",
            .payload = .delete,
        } },
    }
    const stored_crc = std.mem.readInt(u32, &crc_buf, .little);

    var lsn_buf: [8]u8 = undefined;
    switch (try read_exact_classified_file(file, &lsn_buf)) {
        .ok => {},
        .eof, .partial => return .{ .class = .partial_eof, .record = .{
            .start_offset = record_start,
            .end_offset = try file.getPos(),
            .lsn = 0,
            .tag = 0,
            .key = "",
            .value = "",
            .payload = .delete,
        } },
    }
    const lsn = std.mem.readInt(u64, &lsn_buf, .little);

    var tag_buf: [1]u8 = undefined;
    switch (try read_exact_classified_file(file, &tag_buf)) {
        .ok => {},
        .eof, .partial => return .{ .class = .partial_eof, .record = .{
            .start_offset = record_start,
            .end_offset = try file.getPos(),
            .lsn = lsn,
            .tag = 0,
            .key = "",
            .value = "",
            .payload = .delete,
        } },
    }
    const tag = tag_buf[0];
    if (!is_known_tag(tag)) {
        return .{ .class = .corrupt, .record = .{
            .start_offset = record_start,
            .end_offset = try file.getPos(),
            .lsn = lsn,
            .tag = tag,
            .key = "",
            .value = "",
            .payload = .delete,
        } };
    }

    var key_len_buf: [2]u8 = undefined;
    switch (try read_exact_classified_file(file, &key_len_buf)) {
        .ok => {},
        .eof, .partial => return .{ .class = .partial_eof, .record = .{
            .start_offset = record_start,
            .end_offset = try file.getPos(),
            .lsn = lsn,
            .tag = tag,
            .key = "",
            .value = "",
            .payload = .delete,
        } },
    }
    const key_len = std.mem.readInt(u16, &key_len_buf, .little);
    if (key_len > codec.MAX_KEY_LEN) {
        return .{ .class = .corrupt, .record = .{
            .start_offset = record_start,
            .end_offset = try file.getPos(),
            .lsn = lsn,
            .tag = tag,
            .key = "",
            .value = "",
            .payload = .delete,
        } };
    }

    try key_buf.resize(allocator, key_len);
    switch (try read_exact_classified_file(file, key_buf.items)) {
        .ok => {},
        .eof, .partial => return .{ .class = .partial_eof, .record = .{
            .start_offset = record_start,
            .end_offset = try file.getPos(),
            .lsn = lsn,
            .tag = tag,
            .key = key_buf.items,
            .value = "",
            .payload = .delete,
        } },
    }

    var val_len_buf: [4]u8 = undefined;
    switch (try read_exact_classified_file(file, &val_len_buf)) {
        .ok => {},
        .eof, .partial => return .{ .class = .partial_eof, .record = .{
            .start_offset = record_start,
            .end_offset = try file.getPos(),
            .lsn = lsn,
            .tag = tag,
            .key = key_buf.items,
            .value = "",
            .payload = .delete,
        } },
    }
    const val_len = std.mem.readInt(u32, &val_len_buf, .little);
    if (val_len > codec.MAX_VAL_LEN) {
        return .{ .class = .corrupt, .record = .{
            .start_offset = record_start,
            .end_offset = try file.getPos(),
            .lsn = lsn,
            .tag = tag,
            .key = key_buf.items,
            .value = "",
            .payload = .delete,
        } };
    }

    try val_buf.resize(allocator, val_len);
    switch (try read_exact_classified_file(file, val_buf.items)) {
        .ok => {},
        .eof, .partial => return .{ .class = .partial_eof, .record = .{
            .start_offset = record_start,
            .end_offset = try file.getPos(),
            .lsn = lsn,
            .tag = tag,
            .key = key_buf.items,
            .value = val_buf.items,
            .payload = .delete,
        } },
    }

    var hasher = std.hash.crc.Crc32.init();
    hasher.update(&lsn_buf);
    hasher.update(&tag_buf);
    hasher.update(&key_len_buf);
    hasher.update(key_buf.items);
    hasher.update(&val_len_buf);
    hasher.update(val_buf.items);
    if (hasher.final() != stored_crc) {
        return .{ .class = .corrupt, .record = .{
            .start_offset = record_start,
            .end_offset = try file.getPos(),
            .lsn = lsn,
            .tag = tag,
            .key = key_buf.items,
            .value = val_buf.items,
            .payload = .delete,
        } };
    }

    const payload = decode_record_payload(tag, key_buf.items, val_buf.items) orelse return .{ .class = .corrupt, .record = .{
        .start_offset = record_start,
        .end_offset = try file.getPos(),
        .lsn = lsn,
        .tag = tag,
        .key = key_buf.items,
        .value = val_buf.items,
        .payload = .delete,
    } };

    return .{ .class = .valid, .record = .{
        .start_offset = record_start,
        .end_offset = try file.getPos(),
        .lsn = lsn,
        .tag = tag,
        .key = key_buf.items,
        .value = val_buf.items,
        .payload = payload,
    } };
}

/// Returns whether `tag` is a physical mutation record.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
fn is_mutation_tag(tag: u8) bool {
    return tag == tag_put or tag == tag_delete or tag == tag_expire;
}

/// Returns whether `tag` belongs to the WAL grammar accepted by this replay pass.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
fn is_known_tag(tag: u8) bool {
    return is_mutation_tag(tag) or tag == tag_batch_begin or tag == tag_batch_commit;
}

/// Validates one scanned key/value payload against the expected record shape.
///
/// Time Complexity: O(1), excluding borrowed-slice length inspection.
///
/// Allocator: Does not allocate.
///
/// Ownership: Borrows `key` and `value`; does not retain them.
fn decode_record_payload(tag: u8, key: []const u8, value: []const u8) ?RecordPayload {
    return switch (tag) {
        tag_put => .put,
        tag_delete => if (value.len == 0) .delete else null,
        tag_expire => if (value.len == 8) .{ .expire = std.mem.readInt(i64, value[0..8], .little) } else null,
        tag_batch_begin => if (key.len == 0 and value.len == batch_begin_payload_len) .{ .batch_begin = std.mem.readInt(u32, value[0..batch_begin_payload_len], .little) } else null,
        tag_batch_commit => if (key.len == 0 and value.len == batch_commit_payload_len) .{
            .batch_commit = .{
                .begin_lsn = std.mem.readInt(u64, value[0..8], .little),
                .member_record_count = std.mem.readInt(u32, value[8..12], .little),
            },
        } else null,
        else => null,
    };
}

const ReadClass = enum {
    ok,
    eof,
    partial,
};

/// Reads exactly `buf.len` bytes and classifies EOF versus partial tail cases.
///
/// Time Complexity: O(n), where `n` is `buf.len`.
///
/// Allocator: Does not allocate.
///
/// Ownership: Fills the caller-owned `buf` in place.
fn read_exact_classified_file(file: std.fs.File, buf: []u8) !ReadClass {
    if (buf.len == 0) return .ok;
    const read_count = try file.readAll(buf);
    if (read_count == 0) return .eof;
    if (read_count < buf.len) return .partial;
    return .ok;
}
