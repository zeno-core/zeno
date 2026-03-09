//! Engine coordination center for the zeno-core facade.
//! Cost: O(1) dispatch plus downstream runtime and storage work.
//! Allocator: Uses explicit allocators to own the engine handle, runtime state, and caller-visible cloned values.

const std = @import("std");
const batch_ops = @import("batch.zig");
const error_mod = @import("error.zig");
const expiration = @import("expiration.zig");
const internal_codec = @import("../internal/codec.zig");
const internal_mutate = @import("../internal/mutate.zig");
const internal_ttl_index = @import("../internal/ttl_index.zig");
const lifecycle = @import("lifecycle.zig");
const metrics = @import("metrics.zig");
const read = @import("read.zig");
const runtime_shard = @import("../runtime/shard.zig");
const scan_ops = @import("scan.zig");
const runtime_state = @import("../runtime/state.zig");
const storage_snapshot = @import("../storage/snapshot.zig");
const storage_wal = @import("../storage/wal.zig");
const types = @import("../types.zig");
const write = @import("write.zig");

/// Shared error set for engine contract operations.
pub const EngineError = error_mod.EngineError;
pub const MergePageProfileStats = scan_ops.MergePageProfileStats;
pub const ProfiledScanPageResult = scan_ops.ProfiledScanPageResult;

/// Central engine handle for the finalized `zeno-core` contract surface.
pub const Database = struct {
    allocator: std.mem.Allocator,
    state: runtime_state.DatabaseState,

    /// Flushes and closes engine-owned resources.
    ///
    /// Time Complexity: O(s), where `s` is the runtime shard count.
    ///
    /// Ownership: Returns `error.ActiveReadViews` when any `ReadView` handles are still active.
    ///
    /// Thread Safety: Not thread-safe; caller must ensure exclusive ownership of the engine handle.
    pub fn close(self: *Database) EngineError!void {
        return lifecycle.close(self);
    }

    /// Writes a consistent checkpoint of engine-owned state.
    ///
    /// Time Complexity: O(s + n + m), where:
    ///  - `s` is the runtime shard count,
    ///  - `n` is snapshot serialization work
    ///  - `m` is WAL compaction scan and rewrite work when durability is enabled
    ///
    /// Allocator: Uses explicit allocator paths for snapshot serialization scratch and optional WAL compaction scratch.
    ///
    /// Ownership: Returns `error.NoSnapshotPath` when `snapshot_path` was not configured for this engine handle.
    ///
    /// Thread Safety: Not safe to call concurrently with other mutation of the same engine handle.
    /// Active `ReadView` handles may delay the brief checkpoint barrier before it begins, then
    /// writers and new readers are blocked globally during that barrier and writers are blocked
    /// globally again during each shard-serialization window.
    pub fn checkpoint(self: *Database) EngineError!void {
        return metrics.call_with_latency(&self.state, lifecycle.checkpoint, .{self});
    }

    /// Reads one key from the engine contract surface.
    ///
    /// Time Complexity: O(n + k + v), where `n` is `key.len` for shard routing, `k` is ART lookup work, and `v` is cloned value size when the key exists.
    ///
    /// Allocator: Allocates the returned cloned value through `allocator` when the key exists.
    ///
    /// Ownership: Returns a caller-owned cloned value when non-null. The caller must later call `deinit` with `allocator`.
    pub fn get(self: *const Database, allocator: std.mem.Allocator, key: []const u8) EngineError!?types.Value {
        return metrics.call_with_latency(&self.state, read.get, .{ &self.state, allocator, key });
    }

    /// Writes one plain key/value pair through the engine contract surface.
    ///
    /// Time Complexity: O(n + k + v), where `n` is `key.len` for shard routing, `k` is ART lookup or insert work, and `v` is cloned value size.
    ///
    /// Allocator: Clones owned key and value storage through the engine base allocator.
    ///
    /// Ownership: Clones `value` into engine-owned storage before the call returns.
    ///
    /// Thread Safety: Safe for concurrent use with other point operations; acquires the global visibility gate exclusively before taking one shard-exclusive lock.
    pub fn put(self: *Database, key: []const u8, value: *const types.Value) EngineError!void {
        return metrics.call_with_latency(&self.state, write.put, .{ &self.state, key, value });
    }

    /// Deletes one plain key from the engine contract surface.
    ///
    /// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is ART lookup and removal work.
    ///
    /// Allocator: Frees engine-owned key and value storage when the key exists and may allocate delegated WAL record scratch when durability is enabled.
    ///
    /// Thread Safety: Safe for concurrent use with other point operations; acquires the global visibility gate exclusively before taking one shard-exclusive lock and appends the live DELETE record inside that same visibility window.
    pub fn delete(self: *Database, key: []const u8) EngineError!bool {
        return metrics.call_with_latency(&self.state, write.delete, .{ &self.state, key });
    }

    /// Sets or clears key expiration at an absolute unix-second timestamp.
    ///
    /// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is shard-local lookup plus optional TTL metadata update work.
    ///
    /// Allocator: Uses the engine base allocator when inserting a new TTL entry and may allocate delegated WAL record scratch for durable live mutations.
    ///
    /// Thread Safety: Safe for concurrent use with reads and scans; acquires the global visibility gate exclusively before taking one shard-exclusive lock.
    pub fn expire_at(self: *Database, key: []const u8, unix_seconds: ?i64) EngineError!bool {
        return metrics.call_with_latency(&self.state, expire_at_boundary, .{ self, key, unix_seconds });
    }

    /// Returns Redis-style TTL for one plain key.
    ///
    /// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is shard-local lookup plus optional expired-key cleanup work.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Acquires the shared side of the global visibility gate before taking one shard shared lock for TTL reads and only attempts lazy cleanup afterward if the exclusive visibility gate can be acquired immediately.
    pub fn ttl(self: *const Database, key: []const u8) EngineError!i64 {
        return metrics.call_with_latency(&self.state, expiration.ttl, .{ @constCast(&self.state), key });
    }

    /// Performs a full prefix scan over the current visible state.
    ///
    /// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
    ///
    /// Allocator: Allocates owned entry keys and values plus result storage through `allocator`.
    ///
    /// Ownership: Returns a result that owns all returned keys and values until `deinit`.
    ///
    /// Thread Safety: Acquires the shared side of the global visibility gate before taking shard shared locks to collect entries.
    pub fn scan_prefix(
        self: *const Database,
        allocator: std.mem.Allocator,
        prefix: []const u8,
    ) EngineError!types.ScanResult {
        return metrics.call_with_latency(&self.state, scan_ops.scan_prefix, .{ &self.state, allocator, prefix });
    }

    /// Performs a full range scan over the current visible state.
    ///
    /// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
    ///
    /// Allocator: Allocates owned entry keys and values plus result storage through `allocator`.
    ///
    /// Ownership: Returns a result that owns all returned keys and values until `deinit`.
    ///
    /// Thread Safety: Acquires the shared side of the global visibility gate before taking shard shared locks to collect entries.
    pub fn scan_range(
        self: *const Database,
        allocator: std.mem.Allocator,
        range: types.KeyRange,
    ) EngineError!types.ScanResult {
        return metrics.call_with_latency(&self.state, scan_ops.scan_range, .{ &self.state, allocator, range });
    }

    /// Applies one plain atomic batch.
    ///
    /// Time Complexity: O(n + b + v), where `n` is `writes.len`, `b` is total serialized value bytes measured during planning, and `v` is total cloned value size for prepared writes.
    ///
    /// Allocator: Uses the engine base allocator for committed values and temporary planner scratch plus temporary WAL batch-view storage while validating and preparing the batch.
    ///
    /// Ownership: Clones all surviving write values into engine-owned storage before making the batch visible.
    ///
    /// Thread Safety: Safe for concurrent use with point operations and read views; acquires the global visibility gate exclusively for the full apply window.
    pub fn apply_batch(self: *Database, writes: []const types.PutWrite) EngineError!void {
        return metrics.call_with_latency(&self.state, batch_ops.apply_batch, .{ &self.state, self.allocator, writes });
    }

    /// Opens one consistent read view.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Returns a `ReadView` that keeps one registry-backed visibility hold alive until `deinit` is called.
    ///
    /// Thread Safety: Acquires the shared side of the global visibility gate and keeps it held for the lifetime of the returned `ReadView`.
    pub fn read_view(self: *Database) EngineError!types.ReadView {
        return metrics.call_with_latency(&self.state, read.read_view, .{&self.state});
    }
};

/// Creates an in-memory engine handle.
///
/// Time Complexity: O(s), where `s` is the runtime shard count.
///
/// Allocator: Allocates the engine handle and runtime state from `allocator`.
pub fn create(allocator: std.mem.Allocator) EngineError!*Database {
    return lifecycle.create(allocator);
}

/// Opens an engine handle from the provided runtime options.
///
/// Time Complexity: O(s + n + r + e), where `s` is the runtime shard count, `n` is snapshot load work, `r` is replayed WAL work, and `e` is post-recovery expired-key purge work when persistence is configured.
///
/// Allocator: Allocates the engine handle from `allocator` and uses explicit allocator paths for snapshot load and WAL replay scratch when persistence is configured.
pub fn open(allocator: std.mem.Allocator, options: types.DatabaseOptions) EngineError!*Database {
    return lifecycle.open(allocator, options);
}

/// Scans the next prefix page inside a consistent read view.
///
/// Time Complexity: O(s log s + p * (k + log s + v)), where `s` is shard count, `p` is emitted page size, `k` is ART seek work for one shard refill, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page exposes any continuation cursor through `borrow_next_cursor` and may transfer it into `OwnedScanCursor` through `take_next_cursor`.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while fetching or refilling shard-local ART heads.
pub fn scan_prefix_from_in_view(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) EngineError!types.ScanPageResult {
    const state = runtime_state_from_view_for_latency(view);
    return metrics.call_with_optional_latency(state, scan_ops.scan_prefix_from_in_view, .{ view, allocator, prefix, cursor, limit });
}

/// Scans the next prefix page inside a consistent read view while reporting merged-executor refill counters.
///
/// Time Complexity: O(s log s + p * (k + log s + v)), where `s` is shard count, `p` is emitted page size, `k` is ART seek work for one shard refill, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page owns its entries, may own one continuation cursor, and carries caller-owned profiling counters by value.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while fetching or refilling shard-local ART heads.
pub fn scan_prefix_from_in_view_profiled(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) EngineError!ProfiledScanPageResult {
    const state = runtime_state_from_view_for_latency(view);
    return metrics.call_with_optional_latency(state, scan_ops.scan_prefix_from_in_view_profiled, .{ view, allocator, prefix, cursor, limit });
}

/// Scans the next range page inside a consistent read view.
///
/// Time Complexity: O(s log s + p * (k + log s + v)), where `s` is shard count, `p` is emitted page size, `k` is ART seek work for one shard refill, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page exposes any continuation cursor through `borrow_next_cursor` and may transfer it into `OwnedScanCursor` through `take_next_cursor`.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while fetching or refilling shard-local ART heads.
pub fn scan_range_from_in_view(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    range: types.KeyRange,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) EngineError!types.ScanPageResult {
    const state = runtime_state_from_view_for_latency(view);
    return metrics.call_with_optional_latency(state, scan_ops.scan_range_from_in_view, .{ view, allocator, range, cursor, limit });
}

/// Applies one checked batch under the official advanced contract.
///
/// Time Complexity: O(g + n + b + v), where `g` is `batch.guards.len`, `n` is surviving write count, `b` is total serialized value bytes measured during planning, and `v` is total cloned value size for prepared writes.
///
/// Allocator: Uses the engine base allocator for committed values and temporary planner scratch while validating guards and preparing the batch.
///
/// Ownership: Clones all surviving write values into engine-owned storage before making the batch visible.
///
/// Thread Safety: Safe for concurrent use with point operations and read views; acquires the global visibility gate exclusively for the full guard-check and apply window.
pub fn apply_checked_batch(db: *Database, batch: types.CheckedBatch) EngineError!void {
    return metrics.call_with_latency(&db.state, batch_ops.apply_checked_batch, .{ &db.state, db.allocator, batch });
}

fn set_ttl_for_test(db: *Database, key: []const u8, expire_at_seconds: i64) !void {
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();
    try internal_ttl_index.set_ttl_entry(shard, key, expire_at_seconds);
}

fn has_ttl_for_test(db: *Database, key: []const u8) bool {
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lockShared();
    defer shard.lock.unlockShared();
    return internal_ttl_index.get_expire_at(shard, key) != null;
}

fn has_stored_key_for_test(db: *Database, key: []const u8) bool {
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lockShared();
    defer shard.lock.unlockShared();
    return shard.tree.lookup(key) != null;
}

fn value_is_heap_owned_for_test(db: *Database, key: []const u8) bool {
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lockShared();
    defer shard.lock.unlockShared();
    return internal_mutate.value_is_heap_owned_unlocked(shard, key);
}

fn total_committed_arenas_for_test(db: *Database) usize {
    var total: usize = 0;
    for (&db.state.shards) |*shard| {
        shard.lock.lockShared();
        total += internal_mutate.count_committed_arenas_unlocked(shard);
        shard.lock.unlockShared();
    }
    return total;
}

fn expire_at_boundary(db: *Database, key: []const u8, unix_seconds: ?i64) EngineError!bool {
    const updated = try expiration.expire_at(&db.state, key, unix_seconds);
    db.state.record_operation(.expire, 1);
    return updated;
}

fn runtime_state_from_view_for_latency(view: *const types.ReadView) ?*const runtime_state.DatabaseState {
    const opaque_state = view.resolve_runtime_state() orelse return null;
    return @ptrCast(@alignCast(opaque_state));
}

fn latency_samples_for_test(db: *const Database) u64 {
    return db.state.stats_snapshot().latency_samples_total;
}

fn current_checkpoint_lsn_for_test(db: *Database) u64 {
    if (db.state.wal) |wal| {
        if (wal.next_lsn > 0) return wal.next_lsn - 1;
    }
    return 0;
}

fn corrupt_file_byte_for_test(path: []const u8, offset: u64, mask: u8) !void {
    const file = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
    defer file.close();

    try file.seekTo(offset);
    var byte: [1]u8 = undefined;
    if ((try file.readAll(&byte)) != byte.len) return error.EndOfStream;
    byte[0] ^= mask;
    try file.seekTo(offset);
    try file.writeAll(&byte);
}

fn alloc_tmp_path_test(allocator: std.mem.Allocator, tmp: std.testing.TmpDir, basename: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, basename });
}

var noop_replay_ctx: u8 = 0;

fn noop_replay_put(ctx: *anyopaque, key: []const u8, value: *const types.Value) !void {
    _ = ctx;
    _ = key;
    _ = value;
}

fn noop_replay_delete(ctx: *anyopaque, key: []const u8) !void {
    _ = ctx;
    _ = key;
}

fn noop_replay_expire(ctx: *anyopaque, key: []const u8, expire_at_sec: i64) !void {
    _ = ctx;
    _ = key;
    _ = expire_at_sec;
}

fn attach_test_wal(db: *Database, path: []const u8, fsync_mode: types.FsyncMode) !void {
    db.state.wal = try storage_wal.open(path, .{ .fsync_mode = fsync_mode }, .{
        .ctx = &noop_replay_ctx,
        .put = noop_replay_put,
        .delete = noop_replay_delete,
        .expire = noop_replay_expire,
    }, db.allocator);
}

const CheckpointThreadState = struct {
    db: *Database,
    barrier_attempted: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    barrier_acquired: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    finished: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    checkpoint_error: ?EngineError = null,
};

fn run_checkpoint_in_thread(state: *CheckpointThreadState) void {
    state.db.checkpoint() catch |err| {
        state.checkpoint_error = err;
        state.finished.store(true, .release);
        return;
    };
    state.finished.store(true, .release);
}

test "create initializes runtime-owned database state" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    try testing.expectEqual(@as(usize, runtime_state.NUM_SHARDS), db.state.shards.len);
    try testing.expect(db.state.snapshot_path == null);
}

test "snapshot-only open restores values and ttl metadata without incrementing counters" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try alloc_tmp_path_test(testing.allocator, tmp, "step9-snapshot-only.snapshot");
    defer testing.allocator.free(snapshot_path);

    {
        const db = try create(testing.allocator);
        defer db.close() catch unreachable;

        const alpha = types.Value{ .string = "alive" };
        const beta = types.Value{ .integer = 7 };
        try db.put("alpha", &alpha);
        try db.put("beta", &beta);
        try set_ttl_for_test(db, "alpha", runtime_shard.unix_now() + 60);

        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, 33);
    }

    const reopened = try open(testing.allocator, .{
        .snapshot_path = snapshot_path,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    var beta = (try reopened.get(testing.allocator, "beta")).?;
    defer beta.deinit(testing.allocator);

    try testing.expectEqualStrings("alive", alpha.string);
    try testing.expectEqual(@as(i64, 7), beta.integer);
    try testing.expect((try reopened.ttl("alpha")) >= 0);
    try testing.expectEqualStrings(snapshot_path, reopened.state.snapshot_path.?);
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_put_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_delete_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_expire_total.load(.monotonic));
}

test "wal-only open replays recovered keys deletes and ttl metadata without incrementing counters" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "step7-open-replay.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const alpha = types.Value{ .string = "alive" };
        const beta = types.Value{ .string = "gone" };
        const gamma = types.Value{ .integer = 9 };
        try db.put("alpha", &alpha);
        try db.put("beta", &beta);
        try db.put("gamma", &gamma);
        try testing.expect(try db.delete("beta"));
        try testing.expect(try db.expire_at("gamma", runtime_shard.unix_now() + 60));
    }

    const reopened = try open(testing.allocator, .{
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    try testing.expectEqualStrings("alive", alpha.string);
    try testing.expect((try reopened.get(testing.allocator, "beta")) == null);
    try testing.expect((try reopened.ttl("gamma")) >= 0);

    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_put_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_delete_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_expire_total.load(.monotonic));
}

test "snapshot-backed open replays wal delta after the snapshot lsn" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "step9-delta.wal");
    defer testing.allocator.free(wal_path);
    const snapshot_path = try alloc_tmp_path_test(testing.allocator, tmp, "step9-delta.snapshot");
    defer testing.allocator.free(snapshot_path);

    {
        const db = try open(testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const one = types.Value{ .integer = 1 };
        const two = types.Value{ .integer = 2 };
        try db.put("alpha", &one);
        try db.put("beta", &two);

        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, current_checkpoint_lsn_for_test(db));

        const replacement = types.Value{ .integer = 9 };
        const gamma = types.Value{ .string = "delta" };
        try db.put("alpha", &replacement);
        try testing.expect(try db.delete("beta"));
        try db.put("gamma", &gamma);
        try testing.expect(try db.expire_at("gamma", runtime_shard.unix_now() + 60));
    }

    const reopened = try open(testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    var gamma = (try reopened.get(testing.allocator, "gamma")).?;
    defer gamma.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, 9), alpha.integer);
    try testing.expect((try reopened.get(testing.allocator, "beta")) == null);
    try testing.expectEqualStrings("delta", gamma.string);
    try testing.expect((try reopened.ttl("gamma")) >= 0);
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_put_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_delete_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_expire_total.load(.monotonic));
}

test "corrupted snapshot falls back to full wal replay when wal is non-empty" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "step9-fallback.wal");
    defer testing.allocator.free(wal_path);
    const snapshot_path = try alloc_tmp_path_test(testing.allocator, tmp, "step9-fallback.snapshot");
    defer testing.allocator.free(snapshot_path);

    {
        const db = try open(testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const alpha = types.Value{ .integer = 1 };
        const beta = types.Value{ .integer = 2 };
        try db.put("alpha", &alpha);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, current_checkpoint_lsn_for_test(db));
        try db.put("beta", &beta);
    }

    try corrupt_file_byte_for_test(snapshot_path, 0, 0xff);

    const reopened = try open(testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    var beta = (try reopened.get(testing.allocator, "beta")).?;
    defer beta.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, 1), alpha.integer);
    try testing.expectEqual(@as(i64, 2), beta.integer);
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_put_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_delete_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_expire_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 1), reopened.state.stats_snapshot().snapshot_corruption_fallback_total);
}

test "corrupted snapshot with missing wal returns snapshot corrupted" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try alloc_tmp_path_test(testing.allocator, tmp, "step9-missing-wal.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "missing.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try create(testing.allocator);
        defer db.close() catch unreachable;

        const alpha = types.Value{ .integer = 1 };
        try db.put("alpha", &alpha);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, 0);
    }

    try corrupt_file_byte_for_test(snapshot_path, 0, 0xff);

    try testing.expectError(error.SnapshotCorrupted, open(testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    }));
}

test "corrupted snapshot with empty wal returns snapshot corrupted" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try alloc_tmp_path_test(testing.allocator, tmp, "step9-empty-wal.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "empty.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try create(testing.allocator);
        defer db.close() catch unreachable;

        const alpha = types.Value{ .integer = 1 };
        try db.put("alpha", &alpha);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, 0);
    }

    {
        const file = try std.fs.cwd().createFile(wal_path, .{ .truncate = true });
        file.close();
    }
    try corrupt_file_byte_for_test(snapshot_path, 0, 0xff);

    try testing.expectError(error.SnapshotCorrupted, open(testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    }));
}

test "open purges expired keys recovered from snapshot and wal replay" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "step9-purge.wal");
    defer testing.allocator.free(wal_path);
    const snapshot_path = try alloc_tmp_path_test(testing.allocator, tmp, "step9-purge.snapshot");
    defer testing.allocator.free(snapshot_path);

    {
        const db = try open(testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const snapshot_value = types.Value{ .integer = 1 };
        const wal_value = types.Value{ .integer = 2 };
        try db.put("snapshot:expired", &snapshot_value);
        try set_ttl_for_test(db, "snapshot:expired", runtime_shard.unix_now() - 10);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, current_checkpoint_lsn_for_test(db));

        try db.state.wal.?.append_put("wal:expired", &wal_value);
        try db.state.wal.?.append_expire("wal:expired", runtime_shard.unix_now() - 10);
    }

    const reopened = try open(testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    try testing.expect((try reopened.get(testing.allocator, "snapshot:expired")) == null);
    try testing.expect((try reopened.get(testing.allocator, "wal:expired")) == null);
    try testing.expectEqual(@as(i64, -2), try reopened.ttl("snapshot:expired"));
    try testing.expectEqual(@as(i64, -2), try reopened.ttl("wal:expired"));
    try testing.expect(!has_ttl_for_test(reopened, "snapshot:expired"));
    try testing.expect(!has_ttl_for_test(reopened, "wal:expired"));
    try testing.expect(!has_stored_key_for_test(reopened, "snapshot:expired"));
    try testing.expect(!has_stored_key_for_test(reopened, "wal:expired"));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_put_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_delete_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_expire_total.load(.monotonic));
}

test "checkpoint without a configured snapshot path returns no snapshot path" {
    const testing = std.testing;

    const db = try open(testing.allocator, .{
        .metrics = .{ .mode = .full },
    });
    defer db.close() catch unreachable;

    try testing.expectError(error.NoSnapshotPath, db.checkpoint());
    const stats = db.state.stats_snapshot();
    try testing.expectEqual(@as(u64, 1), stats.latency_samples_total);
    try testing.expectEqual(@as(u64, 0), stats.checkpoint_count_total);
    try testing.expectEqual(@as(u64, 0), stats.checkpoint_duration_last_ms);
    try testing.expectEqual(@as(u64, 0), stats.checkpoint_lsn_last);
}

test "checkpoint writes a snapshot and reopens the same visible state" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try alloc_tmp_path_test(testing.allocator, tmp, "step7-checkpoint.snapshot");
    defer testing.allocator.free(snapshot_path);

    {
        const db = try open(testing.allocator, .{
            .snapshot_path = snapshot_path,
        });
        defer db.close() catch unreachable;

        const alpha_value = types.Value{ .string = "hello" };
        const beta_value = types.Value{ .integer = 7 };
        try db.put("alpha", &alpha_value);
        try db.put("beta", &beta_value);
        try testing.expect(try db.expire_at("beta", runtime_shard.unix_now() + 60));
        try db.checkpoint();
        const stats = db.state.stats_snapshot();
        try testing.expectEqual(@as(u64, 1), stats.checkpoint_count_total);
        try testing.expectEqual(@as(u64, 0), stats.checkpoint_lsn_last);
    }

    const reopened = try open(testing.allocator, .{
        .snapshot_path = snapshot_path,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", alpha.string);

    var beta = (try reopened.get(testing.allocator, "beta")).?;
    defer beta.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 7), beta.integer);
    try testing.expect((try reopened.ttl("beta")) > 0);
}

test "checkpoint preserves post-snapshot wal delta on reopen" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try alloc_tmp_path_test(testing.allocator, tmp, "step7-delta.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "step7-delta.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(testing.allocator, .{
            .snapshot_path = snapshot_path,
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const alpha_value = types.Value{ .integer = 1 };
        const beta_value = types.Value{ .integer = 2 };
        try db.put("alpha", &alpha_value);
        try db.checkpoint();
        try db.put("beta", &beta_value);
        try testing.expect(try db.delete("alpha"));
    }

    const reopened = try open(testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    try testing.expect((try reopened.get(testing.allocator, "alpha")) == null);

    var beta = (try reopened.get(testing.allocator, "beta")).?;
    defer beta.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 2), beta.integer);
}

test "engine boundary latency sampling records one sample per call including errors" {
    const testing = std.testing;

    const db = try open(testing.allocator, .{
        .metrics = .{ .mode = .full },
    });
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    var expected = latency_samples_for_test(db);

    try db.put("alpha", &value);
    expected += 1;
    try testing.expectEqual(expected, latency_samples_for_test(db));

    var alpha = (try db.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    expected += 1;
    try testing.expectEqual(expected, latency_samples_for_test(db));

    try testing.expect(!try db.expire_at("missing", runtime_shard.unix_now() + 30));
    expected += 1;
    try testing.expectEqual(expected, latency_samples_for_test(db));

    _ = try db.ttl("alpha");
    expected += 1;
    try testing.expectEqual(expected, latency_samples_for_test(db));

    var prefix = try db.scan_prefix(testing.allocator, "a");
    defer prefix.deinit();
    expected += 1;
    try testing.expectEqual(expected, latency_samples_for_test(db));

    var range = try db.scan_range(testing.allocator, .{
        .start = "a",
        .end = "z",
    });
    defer range.deinit();
    expected += 1;
    try testing.expectEqual(expected, latency_samples_for_test(db));

    try db.apply_batch(&.{
        .{ .key = "beta", .value = &value },
    });
    expected += 1;
    try testing.expectEqual(expected, latency_samples_for_test(db));

    var view = try db.read_view();
    expected += 1;
    try testing.expectEqual(expected, latency_samples_for_test(db));

    var in_view_prefix = try scan_prefix_from_in_view(&view, testing.allocator, "", null, 10);
    expected += 1;
    try testing.expectEqual(expected, latency_samples_for_test(db));

    var in_view_range = try scan_range_from_in_view(&view, testing.allocator, .{
        .start = "a",
        .end = "z",
    }, null, 10);
    expected += 1;
    try testing.expectEqual(expected, latency_samples_for_test(db));

    in_view_range.deinit();
    in_view_prefix.deinit();
    view.deinit();

    try apply_checked_batch(db, .{
        .writes = &.{
            .{ .key = "gamma", .value = &value },
        },
        .guards = &.{
            .{ .key_exists = "alpha" },
        },
    });
    expected += 1;
    try testing.expectEqual(expected, latency_samples_for_test(db));

    try testing.expectError(error.KeyTooLarge, db.put("", &value));
    expected += 1;
    try testing.expectEqual(expected, latency_samples_for_test(db));

    try testing.expectError(error.KeyTooLarge, apply_checked_batch(db, .{
        .writes = &.{
            .{ .key = "", .value = &value },
        },
        .guards = &.{},
    }));
    expected += 1;
    try testing.expectEqual(expected, latency_samples_for_test(db));
}

test "wal-only restart preserves committed batch semantics" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "step7-batch-restart.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const one = types.Value{ .integer = 1 };
        const two = types.Value{ .integer = 2 };
        const three = types.Value{ .integer = 3 };
        try db.apply_batch(&.{
            .{ .key = "alpha", .value = &one },
            .{ .key = "beta", .value = &two },
            .{ .key = "alpha", .value = &three },
        });
    }

    const reopened = try open(testing.allocator, .{
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    var beta = (try reopened.get(testing.allocator, "beta")).?;
    defer beta.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, 3), alpha.integer);
    try testing.expectEqual(@as(i64, 2), beta.integer);
}

test "recovered expired ttl metadata remains invisible after wal-only restart" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "step7-expired-restart.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const value = types.Value{ .integer = 7 };
        try db.put("alpha", &value);
        try testing.expect(try db.expire_at("alpha", runtime_shard.unix_now() + 1));
    }

    std.Thread.sleep(1100 * std.time.ns_per_ms);

    const reopened = try open(testing.allocator, .{
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    try testing.expect((try reopened.get(testing.allocator, "alpha")) == null);
    try testing.expectEqual(@as(i64, -2), try reopened.ttl("alpha"));

    var scan = try reopened.scan_prefix(testing.allocator, "alpha");
    defer scan.deinit();
    try testing.expectEqual(@as(usize, 0), scan.entries.items.len);
}

test "truncated batch tail does not become visible after wal-only reopen" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "step7-truncated-batch.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const one = types.Value{ .integer = 1 };
        const two = types.Value{ .integer = 2 };
        try db.apply_batch(&.{
            .{ .key = "alpha", .value = &one },
            .{ .key = "beta", .value = &two },
        });
    }

    {
        const file = try std.fs.cwd().openFile(wal_path, .{ .mode = .read_write });
        defer file.close();
        const size = try file.getEndPos();
        try file.setEndPos(size - 1);
    }

    const reopened = try open(testing.allocator, .{
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    try testing.expect((try reopened.get(testing.allocator, "alpha")) == null);
    try testing.expect((try reopened.get(testing.allocator, "beta")) == null);
}

test "plain point operations store clone and delete values" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const original = types.Value{ .string = "hello" };
    try db.put("alpha", &original);

    {
        var first_read = (try db.get(testing.allocator, "alpha")).?;
        defer first_read.deinit(testing.allocator);

        try testing.expectEqualStrings("hello", first_read.string);
    }

    var second_read = (try db.get(testing.allocator, "alpha")).?;
    defer second_read.deinit(testing.allocator);

    try testing.expectEqualStrings("hello", second_read.string);

    try testing.expect(try db.delete("alpha"));
    try testing.expect(!try db.delete("alpha"));
    try testing.expect((try db.get(testing.allocator, "alpha")) == null);
}

test "put overwrites existing plain value" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const first = types.Value{ .integer = 7 };
    try db.put("counter", &first);

    const second = types.Value{ .string = "updated" };
    try db.put("counter", &second);

    var stored = (try db.get(testing.allocator, "counter")).?;
    defer stored.deinit(testing.allocator);

    try testing.expectEqualStrings("updated", stored.string);
}

test "put leaves state unchanged when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try alloc_tmp_path_test(testing.allocator, tmp, "step6-put-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;
    try attach_test_wal(db, path, .none);

    const value = types.Value{ .string = "value" };
    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, db.put("wal:put", &value));
    try testing.expect((try db.get(testing.allocator, "wal:put")) == null);
}

test "delete returns a durability error and keeps the key when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try alloc_tmp_path_test(testing.allocator, tmp, "step6-delete-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("wal:delete", &value);
    try attach_test_wal(db, path, .none);

    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, db.delete("wal:delete"));

    var stored = (try db.get(testing.allocator, "wal:delete")).?;
    defer stored.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 1), stored.integer);
}

test "expire_at returns false for missing keys and increments the expire counter" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    try testing.expect(!try db.expire_at("missing", runtime_shard.unix_now() + 30));
    try testing.expectEqual(@as(u64, 1), db.state.counters.ops_expire_total.load(.monotonic));
}

test "expire_at null clears existing ttl while keeping the stored value" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .string = "value" };
    try db.put("ttl:key", &value);

    const expire_at_seconds = runtime_shard.unix_now() + 30;
    try testing.expect(try db.expire_at("ttl:key", expire_at_seconds));
    const ttl_before_clear = try db.ttl("ttl:key");
    try testing.expect(ttl_before_clear >= 0);
    try testing.expect(ttl_before_clear <= 30);

    try testing.expect(try db.expire_at("ttl:key", null));
    try testing.expectEqual(@as(i64, -1), try db.ttl("ttl:key"));

    var stored = (try db.get(testing.allocator, "ttl:key")).?;
    defer stored.deinit(testing.allocator);
    try testing.expectEqualStrings("value", stored.string);
}

test "expire_at at or before now deletes the key immediately" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 9 };
    try db.put("gone", &value);

    try testing.expect(try db.expire_at("gone", runtime_shard.unix_now()));
    try testing.expect((try db.get(testing.allocator, "gone")) == null);
    try testing.expectEqual(@as(i64, -2), try db.ttl("gone"));
    try testing.expect(!has_ttl_for_test(db, "gone"));
}

test "expire_at future leaves ttl unchanged when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try alloc_tmp_path_test(testing.allocator, tmp, "step6-expire-future-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("wal:expire-future", &value);
    try set_ttl_for_test(db, "wal:expire-future", runtime_shard.unix_now() + 30);
    try attach_test_wal(db, path, .none);

    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, db.expire_at("wal:expire-future", runtime_shard.unix_now() + 90));
    try testing.expect(has_ttl_for_test(db, "wal:expire-future"));
    try testing.expect((try db.ttl("wal:expire-future")) <= 30);
}

test "expire_at immediate delete leaves key visible when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try alloc_tmp_path_test(testing.allocator, tmp, "step6-expire-delete-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 9 };
    try db.put("wal:expire-delete", &value);
    try attach_test_wal(db, path, .none);

    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, db.expire_at("wal:expire-delete", runtime_shard.unix_now()));

    var stored = (try db.get(testing.allocator, "wal:expire-delete")).?;
    defer stored.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 9), stored.integer);
    try testing.expectEqual(@as(i64, -1), try db.ttl("wal:expire-delete"));
}

test "expire_at null leaves ttl intact when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try alloc_tmp_path_test(testing.allocator, tmp, "step6-expire-clear-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("wal:expire-clear", &value);
    try set_ttl_for_test(db, "wal:expire-clear", runtime_shard.unix_now() + 30);
    try attach_test_wal(db, path, .none);

    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, db.expire_at("wal:expire-clear", null));
    try testing.expect(has_ttl_for_test(db, "wal:expire-clear"));
    try testing.expect((try db.ttl("wal:expire-clear")) >= 0);
}

test "ttl eagerly cleans up expired keys while get remains lazily invisible" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .string = "stale" };
    try db.put("stale:key", &value);
    try set_ttl_for_test(db, "stale:key", runtime_shard.unix_now() - 1);

    try testing.expect(has_ttl_for_test(db, "stale:key"));
    try testing.expect((try db.get(testing.allocator, "stale:key")) == null);
    try testing.expect(has_ttl_for_test(db, "stale:key"));

    try testing.expectEqual(@as(i64, -2), try db.ttl("stale:key"));
    try testing.expect(!has_ttl_for_test(db, "stale:key"));
    try testing.expect((try db.get(testing.allocator, "stale:key")) == null);
}

test "read view holds the visibility gate until released" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    var view = try db.read_view();
    defer if (view.token_id != 0) view.deinit();

    try testing.expect(!db.state.visibility_gate.try_lock_exclusive());

    view.deinit();
    try testing.expect(db.state.visibility_gate.try_lock_exclusive());
    db.state.visibility_gate.unlock_exclusive();
}

test "read view copies release the visibility gate only once" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    var view = try db.read_view();
    var copied = view;
    defer copied.deinit();
    defer view.deinit();

    try testing.expectEqual(@as(usize, 1), db.state.active_read_views.load(.monotonic));
    try testing.expect(!db.state.visibility_gate.try_lock_exclusive());

    view.deinit();

    try testing.expectEqual(@as(usize, 0), db.state.active_read_views.load(.monotonic));
    try testing.expect(db.state.visibility_gate.try_lock_exclusive());
    db.state.visibility_gate.unlock_exclusive();
}

test "in-view scans reject stale read view copies" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("alpha", &value);

    var view = try db.read_view();
    var copied = view;
    defer copied.deinit();

    view.deinit();

    try testing.expectError(error.InvalidReadView, scan_prefix_from_in_view(&copied, testing.allocator, "alpha", null, 1));
}

test "close fails while a read view is still active" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    var view = try db.read_view();
    defer view.deinit();

    try testing.expectError(error.ActiveReadViews, db.close());
}

test "close surfaces final wal flush failure for batched async mode" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "step7-close-fsync.wal");
    defer testing.allocator.free(wal_path);

    const db = try open(testing.allocator, .{
        .wal_path = wal_path,
        .fsync_mode = .batched_async,
        .fsync_interval_ms = 1,
    });
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("alpha", &value);

    storage_wal.test_hooks.failNextFsync();
    try testing.expectError(error.WalFlushFailed, db.close());
}

test "apply_batch keeps the final value in declared key order" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };

    try db.apply_batch(&.{
        .{ .key = "alpha", .value = &one },
        .{ .key = "beta", .value = &two },
        .{ .key = "alpha", .value = &three },
    });

    var alpha = (try db.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    var beta = (try db.get(testing.allocator, "beta")).?;
    defer beta.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, 3), alpha.integer);
    try testing.expectEqual(@as(i64, 2), beta.integer);
}

test "apply_batch leaves survivor writes unapplied when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try alloc_tmp_path_test(testing.allocator, tmp, "step6-batch-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;
    try attach_test_wal(db, path, .none);

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, db.apply_batch(&.{
        .{ .key = "wal:batch:one", .value = &one },
        .{ .key = "wal:batch:two", .value = &two },
    }));

    try testing.expect((try db.get(testing.allocator, "wal:batch:one")) == null);
    try testing.expect((try db.get(testing.allocator, "wal:batch:two")) == null);
}

test "apply_checked_batch keeps state unchanged when a guard fails" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const original = types.Value{ .string = "original" };
    try db.put("guarded", &original);

    const replacement = types.Value{ .string = "replacement" };
    const other = types.Value{ .integer = 9 };

    try testing.expectError(error.GuardFailed, apply_checked_batch(db, .{
        .writes = &.{
            .{ .key = "guarded", .value = &replacement },
            .{ .key = "other", .value = &other },
        },
        .guards = &.{
            .{ .key_not_exists = "guarded" },
        },
    }));

    var guarded = (try db.get(testing.allocator, "guarded")).?;
    defer guarded.deinit(testing.allocator);

    try testing.expectEqualStrings("original", guarded.string);
    try testing.expect((try db.get(testing.allocator, "other")) == null);
}

test "apply_checked_batch leaves survivor writes unapplied when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try alloc_tmp_path_test(testing.allocator, tmp, "step6-checked-batch-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const existing = types.Value{ .integer = 1 };
    try db.put("wal:guard", &existing);
    try attach_test_wal(db, path, .none);

    const target = types.Value{ .integer = 2 };
    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, apply_checked_batch(db, .{
        .writes = &.{
            .{ .key = "wal:checked-target", .value = &target },
        },
        .guards = &.{
            .{ .key_exists = "wal:guard" },
        },
    }));

    try testing.expect((try db.get(testing.allocator, "wal:checked-target")) == null);

    var guarded = (try db.get(testing.allocator, "wal:guard")).?;
    defer guarded.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 1), guarded.integer);
}

test "apply_checked_batch validates guard keys and expected values" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    try testing.expectError(error.KeyTooLarge, apply_checked_batch(db, .{
        .writes = &.{},
        .guards = &.{
            .{ .key_exists = "" },
        },
    }));

    const oversized_bytes = try allocator.alloc(u8, @as(usize, @intCast(internal_codec.MAX_VAL_LEN)) + 1);
    defer allocator.free(oversized_bytes);
    @memset(oversized_bytes, 'x');
    const oversized_value = types.Value{ .string = oversized_bytes };

    try testing.expectError(error.ValueTooLarge, apply_checked_batch(db, .{
        .writes = &.{},
        .guards = &.{
            .{ .key_value_equals = .{
                .key = "guarded",
                .value = &oversized_value,
            } },
        },
    }));
}

test "put and delete clear prior ttl metadata" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const first = types.Value{ .integer = 1 };
    try db.put("ttl:put", &first);
    try set_ttl_for_test(db, "ttl:put", runtime_shard.unix_now() + 30);

    const replacement = types.Value{ .integer = 2 };
    try db.put("ttl:put", &replacement);
    try testing.expectEqual(@as(i64, -1), try db.ttl("ttl:put"));
    try testing.expect(!has_ttl_for_test(db, "ttl:put"));

    try set_ttl_for_test(db, "ttl:put", runtime_shard.unix_now() + 30);
    try testing.expect(try db.delete("ttl:put"));
    try testing.expect(!has_ttl_for_test(db, "ttl:put"));
    try testing.expectEqual(@as(i64, -2), try db.ttl("ttl:put"));
}

test "delete returns false for expired keys that are already invisible" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("ttl:expired-delete", &value);
    try set_ttl_for_test(db, "ttl:expired-delete", runtime_shard.unix_now() - 1);

    try testing.expect(!try db.delete("ttl:expired-delete"));
    try testing.expect((try db.get(testing.allocator, "ttl:expired-delete")) == null);
    try testing.expect(!has_ttl_for_test(db, "ttl:expired-delete"));
}

test "batch writes clear prior ttl metadata" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const original = types.Value{ .integer = 1 };
    try db.put("ttl:batch", &original);
    try db.put("ttl:checked", &original);
    try set_ttl_for_test(db, "ttl:batch", runtime_shard.unix_now() + 30);
    try set_ttl_for_test(db, "ttl:checked", runtime_shard.unix_now() + 30);

    const batch_value = types.Value{ .integer = 2 };
    try db.apply_batch(&.{
        .{ .key = "ttl:batch", .value = &batch_value },
    });
    try testing.expectEqual(@as(i64, -1), try db.ttl("ttl:batch"));
    try testing.expect(!has_ttl_for_test(db, "ttl:batch"));

    const checked_value = types.Value{ .integer = 3 };
    try apply_checked_batch(db, .{
        .writes = &.{
            .{ .key = "ttl:checked", .value = &checked_value },
        },
        .guards = &.{
            .{ .key_exists = "ttl:checked" },
        },
    });
    try testing.expectEqual(@as(i64, -1), try db.ttl("ttl:checked"));
    try testing.expect(!has_ttl_for_test(db, "ttl:checked"));
}

test "checked batch guards treat expired keys as absent" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const stale = types.Value{ .integer = 1 };
    try db.put("ttl:guarded", &stale);
    try set_ttl_for_test(db, "ttl:guarded", runtime_shard.unix_now() - 1);

    const fresh = types.Value{ .integer = 2 };
    try apply_checked_batch(db, .{
        .writes = &.{
            .{ .key = "ttl:target", .value = &fresh },
        },
        .guards = &.{
            .{ .key_not_exists = "ttl:guarded" },
        },
    });

    var target = (try db.get(testing.allocator, "ttl:target")).?;
    defer target.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 2), target.integer);

    try testing.expectError(error.GuardFailed, apply_checked_batch(db, .{
        .writes = &.{
            .{ .key = "ttl:another", .value = &fresh },
        },
        .guards = &.{
            .{ .key_exists = "ttl:guarded" },
        },
    }));

    try testing.expectError(error.GuardFailed, apply_checked_batch(db, .{
        .writes = &.{
            .{ .key = "ttl:another", .value = &fresh },
        },
        .guards = &.{
            .{ .key_value_equals = .{
                .key = "ttl:guarded",
                .value = &stale,
            } },
        },
    }));
}

test "checked batch uses one expiration timestamp across all guards" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const pathological_key = try testing.allocator.alloc(u8, write.MAX_KEY_LEN);
    defer testing.allocator.free(pathological_key);
    @memset(pathological_key, '{');

    const original = types.Value{ .integer = 1 };
    try db.put(pathological_key, &original);
    try testing.expect(try db.expire_at(pathological_key, runtime_shard.unix_now() + 2));

    const guards = try testing.allocator.alloc(types.CheckedBatchGuard, 128);
    defer testing.allocator.free(guards);
    for (guards) |*guard| {
        guard.* = .{ .key_exists = pathological_key };
    }

    const replacement = types.Value{ .integer = 2 };
    try apply_checked_batch(db, .{
        .writes = &.{
            .{ .key = "ttl:guard-window", .value = &replacement },
        },
        .guards = guards,
    });

    var stored = (try db.get(testing.allocator, "ttl:guard-window")).?;
    defer stored.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 2), stored.integer);
}

test "scan_prefix returns lexicographically ordered owned entries" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const alpha = types.Value{ .integer = 1 };
    const alpha_one = types.Value{ .integer = 2 };
    const beta = types.Value{ .integer = 3 };
    try db.put("alpha", &alpha);
    try db.put("alpha:1", &alpha_one);
    try db.put("beta", &beta);

    var result = try db.scan_prefix(testing.allocator, "alpha");
    defer result.deinit();

    try testing.expectEqual(@as(usize, 2), result.entries.items.len);
    try testing.expectEqualStrings("alpha", result.entries.items[0].key);
    try testing.expectEqualStrings("alpha:1", result.entries.items[1].key);
    try testing.expectEqual(@as(i64, 1), result.entries.items[0].value.integer);
    try testing.expectEqual(@as(i64, 2), result.entries.items[1].value.integer);
}

test "scan operations omit expired keys while preserving lexicographic order" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const a = types.Value{ .integer = 1 };
    const b = types.Value{ .integer = 2 };
    const c = types.Value{ .integer = 3 };
    try db.put("alpha", &a);
    try db.put("beta", &b);
    try db.put("gamma", &c);
    try set_ttl_for_test(db, "beta", runtime_shard.unix_now() - 1);

    var prefix_result = try db.scan_prefix(testing.allocator, "");
    defer prefix_result.deinit();
    try testing.expectEqual(@as(usize, 2), prefix_result.entries.items.len);
    try testing.expectEqualStrings("alpha", prefix_result.entries.items[0].key);
    try testing.expectEqualStrings("gamma", prefix_result.entries.items[1].key);

    var range_result = try db.scan_range(testing.allocator, .{
        .start = "a",
        .end = "z",
    });
    defer range_result.deinit();
    try testing.expectEqual(@as(usize, 2), range_result.entries.items.len);
    try testing.expectEqualStrings("alpha", range_result.entries.items[0].key);
    try testing.expectEqualStrings("gamma", range_result.entries.items[1].key);
}

test "scan_range uses inclusive start and exclusive end" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const a = types.Value{ .integer = 1 };
    const b = types.Value{ .integer = 2 };
    const c = types.Value{ .integer = 3 };
    try db.put("a", &a);
    try db.put("b", &b);
    try db.put("c", &c);

    var result = try db.scan_range(testing.allocator, .{
        .start = "a",
        .end = "c",
    });
    defer result.deinit();

    try testing.expectEqual(@as(usize, 2), result.entries.items.len);
    try testing.expectEqualStrings("a", result.entries.items[0].key);
    try testing.expectEqualStrings("b", result.entries.items[1].key);
}

test "scan_prefix_from_in_view paginates in key order" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };
    try db.put("alpha", &one);
    try db.put("alpha:1", &two);
    try db.put("alpha:2", &three);

    var view = try db.read_view();
    defer view.deinit();

    var first_page = try scan_prefix_from_in_view(&view, testing.allocator, "alpha", null, 2);
    defer first_page.deinit();

    try testing.expectEqual(@as(usize, 2), first_page.entries.items.len);
    try testing.expect(first_page.borrow_next_cursor() != null);
    try testing.expectEqualStrings("alpha", first_page.entries.items[0].key);
    try testing.expectEqualStrings("alpha:1", first_page.entries.items[1].key);

    var cursor = first_page.take_next_cursor().?;
    defer cursor.deinit();
    const cursor_view = cursor.as_cursor().?;
    var second_page = try scan_prefix_from_in_view(&view, testing.allocator, "alpha", &cursor_view, 2);
    defer second_page.deinit();

    try testing.expectEqual(@as(usize, 1), second_page.entries.items.len);
    try testing.expect(second_page.borrow_next_cursor() == null);
    try testing.expectEqualStrings("alpha:2", second_page.entries.items[0].key);
}

test "scan_prefix_from_in_view merges cross-shard heads in global key order" {
    const testing = std.testing;

    var candidate_storage: [256][16]u8 = undefined;
    var chosen: [3][]const u8 = undefined;
    var chosen_count: usize = 0;
    var seen_shards = [_]bool{false} ** runtime_shard.NUM_SHARDS;

    for (0..candidate_storage.len) |index| {
        const candidate = try std.fmt.bufPrint(&candidate_storage[index], "merge:{d:0>3}", .{index});
        const shard_idx = runtime_shard.get_shard_index(candidate);
        if (seen_shards[shard_idx]) continue;
        seen_shards[shard_idx] = true;
        chosen[chosen_count] = candidate;
        chosen_count += 1;
        if (chosen_count == chosen.len) break;
    }

    try testing.expectEqual(@as(usize, chosen.len), chosen_count);

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };
    try db.put(chosen[0], &one);
    try db.put(chosen[1], &two);
    try db.put(chosen[2], &three);

    var view = try db.read_view();
    defer view.deinit();

    var first_page = try scan_prefix_from_in_view(&view, testing.allocator, "merge:", null, 2);
    defer first_page.deinit();

    try testing.expectEqual(@as(usize, 2), first_page.entries.items.len);
    try testing.expectEqualStrings(chosen[0], first_page.entries.items[0].key);
    try testing.expectEqualStrings(chosen[1], first_page.entries.items[1].key);
    try testing.expect(first_page.borrow_next_cursor() != null);

    var cursor = first_page.take_next_cursor().?;
    defer cursor.deinit();
    const cursor_view = cursor.as_cursor().?;
    var second_page = try scan_prefix_from_in_view(&view, testing.allocator, "merge:", &cursor_view, 2);
    defer second_page.deinit();

    try testing.expectEqual(@as(usize, 1), second_page.entries.items.len);
    try testing.expectEqualStrings(chosen[2], second_page.entries.items[0].key);
    try testing.expect(second_page.borrow_next_cursor() == null);
}

test "scan_prefix_from_in_view cursor records the last emitted key" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    try db.put("alpha", &one);
    try db.put("zz-after", &two);

    var view = try db.read_view();
    defer view.deinit();

    var page = try scan_prefix_from_in_view(&view, testing.allocator, "", null, 1);
    defer page.deinit();

    const cursor = page.borrow_next_cursor().?;
    try testing.expectEqualStrings(page.entries.items[0].key, cursor.resume_key);
}

test "scan_prefix_from_in_view omits keys expired before the view opens" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };
    try db.put("alpha", &one);
    try db.put("alpha:1", &two);
    try db.put("alpha:2", &three);
    try set_ttl_for_test(db, "alpha", runtime_shard.unix_now() - 1);

    var view = try db.read_view();
    defer view.deinit();

    var first_page = try scan_prefix_from_in_view(&view, testing.allocator, "alpha", null, 1);
    defer first_page.deinit();
    try testing.expectEqual(@as(usize, 1), first_page.entries.items.len);
    try testing.expectEqualStrings("alpha:1", first_page.entries.items[0].key);

    var cursor = first_page.take_next_cursor().?;
    defer cursor.deinit();
    const cursor_view = cursor.as_cursor().?;
    var second_page = try scan_prefix_from_in_view(&view, testing.allocator, "alpha", &cursor_view, 1);
    defer second_page.deinit();
    try testing.expectEqual(@as(usize, 1), second_page.entries.items.len);
    try testing.expectEqualStrings("alpha:2", second_page.entries.items[0].key);
}

test "read view freezes expiration time at open" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 7 };
    try db.put("alpha", &value);
    try testing.expect(try db.expire_at("alpha", runtime_shard.unix_now() + 1));

    var view = try db.read_view();
    defer view.deinit();

    std.Thread.sleep(1100 * std.time.ns_per_ms);

    var in_view = try scan_prefix_from_in_view(&view, testing.allocator, "alpha", null, 10);
    defer in_view.deinit();
    try testing.expectEqual(@as(usize, 1), in_view.entries.items.len);
    try testing.expectEqualStrings("alpha", in_view.entries.items[0].key);

    var plain = try db.scan_prefix(testing.allocator, "alpha");
    defer plain.deinit();
    try testing.expectEqual(@as(usize, 0), plain.entries.items.len);
}

test "ttl does not deadlock under an active read view and defers cleanup" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 7 };
    try db.put("ttl:view", &value);
    try testing.expect(try db.expire_at("ttl:view", runtime_shard.unix_now() + 1));

    var view = try db.read_view();
    defer view.deinit();

    std.Thread.sleep(1100 * std.time.ns_per_ms);

    try testing.expectEqual(@as(i64, -2), try db.ttl("ttl:view"));
    try testing.expect(has_ttl_for_test(db, "ttl:view"));

    var in_view = try scan_prefix_from_in_view(&view, testing.allocator, "ttl:view", null, 10);
    defer in_view.deinit();
    try testing.expectEqual(@as(usize, 1), in_view.entries.items.len);

    view.deinit();
    try testing.expectEqual(@as(i64, -2), try db.ttl("ttl:view"));
    try testing.expect(!has_ttl_for_test(db, "ttl:view"));
    try testing.expect((try db.get(testing.allocator, "ttl:view")) == null);
}

test "scan page can promote one borrowed continuation cursor into owned storage" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    try db.put("alpha", &one);
    try db.put("alpha:1", &two);

    var view = try db.read_view();
    defer view.deinit();

    var page = try scan_prefix_from_in_view(&view, testing.allocator, "alpha", null, 1);

    const borrowed_cursor = page.borrow_next_cursor().?;
    var owned_cursor = try borrowed_cursor.clone(testing.allocator);
    defer owned_cursor.deinit();

    page.deinit();

    const cursor_view = owned_cursor.as_cursor().?;
    var second_page = try scan_prefix_from_in_view(&view, testing.allocator, "alpha", &cursor_view, 1);
    defer second_page.deinit();

    try testing.expectEqual(@as(usize, 1), second_page.entries.items.len);
    try testing.expectEqualStrings("alpha:1", second_page.entries.items[0].key);
}

test "owned scan cursor clone makes an independent continuation owner" {
    const testing = std.testing;

    var cursor = try types.OwnedScanCursor.init(testing.allocator, "alpha");
    defer cursor.deinit();

    var cloned = (try cursor.clone(testing.allocator)).?;
    defer cloned.deinit();

    try testing.expect(cursor.as_cursor() != null);
    try testing.expect(cloned.as_cursor() != null);

    cursor.deinit();

    try testing.expect(cloned.as_cursor() != null);
    try testing.expectEqualStrings("alpha", cloned.as_cursor().?.resume_key);
}

test "point operation boundaries reject empty keys" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };

    try testing.expectError(error.KeyTooLarge, db.put("", &value));
    try testing.expectError(error.KeyTooLarge, db.get(testing.allocator, ""));
    try testing.expectError(error.KeyTooLarge, db.delete(""));
    try testing.expectError(error.KeyTooLarge, db.expire_at("", runtime_shard.unix_now() + 60));
    try testing.expectError(error.KeyTooLarge, db.ttl(""));
}

test "apply_batch rejects invalid keys and oversized values before changing state" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const sentinel = types.Value{ .integer = 7 };
    try db.put("sentinel", &sentinel);

    const replacement = types.Value{ .integer = 9 };
    try testing.expectError(error.KeyTooLarge, db.apply_batch(&.{
        .{ .key = "", .value = &replacement },
    }));

    const oversized_bytes = try allocator.alloc(u8, @as(usize, @intCast(internal_codec.MAX_VAL_LEN)) + 1);
    defer allocator.free(oversized_bytes);
    @memset(oversized_bytes, 'x');
    const oversized_value = types.Value{ .string = oversized_bytes };

    try testing.expectError(error.ValueTooLarge, db.apply_batch(&.{
        .{ .key = "too:large", .value = &oversized_value },
    }));

    try testing.expect((try db.get(testing.allocator, "too:large")) == null);
    var stored = (try db.get(testing.allocator, "sentinel")).?;
    defer stored.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 7), stored.integer);
}

test "empty batches are no-ops and checked empty batches still validate guards" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("guarded", &value);

    try db.apply_batch(&.{});
    try testing.expectError(error.GuardFailed, apply_checked_batch(db, .{
        .writes = &.{},
        .guards = &.{
            .{ .key_not_exists = "guarded" },
        },
    }));

    var guarded = (try db.get(testing.allocator, "guarded")).?;
    defer guarded.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 1), guarded.integer);
}

test "shard reset reclaims committed batch arenas" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    try db.apply_batch(&.{
        .{ .key = "arena:one", .value = &one },
        .{ .key = "arena:two", .value = &two },
    });

    const shard_idx = runtime_shard.get_shard_index("arena:one");
    const shard = &db.state.shards[shard_idx];
    shard.lock.lock();
    defer shard.lock.unlock();

    try testing.expect(shard.committed_arenas_head != null);
    shard.reset_unlocked();
    try testing.expect(shard.committed_arenas_head == null);
    try testing.expect(shard.committed_arenas_tail == null);
    try testing.expect(shard.tree.lookup("arena:one") == null);
    try testing.expect(shard.tree.lookup("arena:two") == null);
}

test "overwrite-only batches avoid retaining new committed arenas and remain reclaimable" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    var values: [64]types.Value = undefined;
    var writes: [64]types.PutWrite = undefined;
    var key_storage: [64][16]u8 = undefined;

    for (0..writes.len) |index| {
        values[index] = .{ .integer = @intCast(index) };
        const key = try std.fmt.bufPrint(&key_storage[index], "batch:{d:0>4}", .{index});
        writes[index] = .{
            .key = key,
            .value = &values[index],
        };
    }

    try db.apply_batch(&writes);
    try testing.expectEqual(@as(usize, 41), total_committed_arenas_for_test(db));

    for (0..writes.len) |index| {
        values[index] = .{ .integer = @intCast(index + 1_000) };
    }

    try db.apply_batch(&writes);
    try testing.expectEqual(@as(usize, 41), total_committed_arenas_for_test(db));
    try testing.expect(value_is_heap_owned_for_test(db, "batch:0000"));

    const point_value = types.Value{ .integer = 9_999 };
    try db.put("batch:0000", &point_value);
    try testing.expect(!value_is_heap_owned_for_test(db, "batch:0000"));

    const replacement = types.Value{ .integer = 8_888 };
    _ = try db.delete("batch:0000");
    try db.put("batch:0000", &replacement);
    try testing.expect(!value_is_heap_owned_for_test(db, "batch:0000"));
}

test "scan_prefix includes the exact key and its subkeys" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };
    const four = types.Value{ .integer = 4 };
    try db.put("alpha", &one);
    try db.put("alpha:1", &two);
    try db.put("alpha:2", &three);
    try db.put("alphabet", &four);

    var result = try db.scan_prefix(testing.allocator, "alpha");
    defer result.deinit();

    try testing.expectEqual(@as(usize, 4), result.entries.items.len);
    try testing.expectEqualStrings("alpha", result.entries.items[0].key);
    try testing.expectEqualStrings("alpha:1", result.entries.items[1].key);
    try testing.expectEqualStrings("alpha:2", result.entries.items[2].key);
    try testing.expectEqualStrings("alphabet", result.entries.items[3].key);
}

test "scan_range_from_in_view paginates binary keys in global order and terminates cleanly" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };
    try db.put("\x00a", &one);
    try db.put("\x00b", &two);
    try db.put("\x01a", &three);

    var view = try db.read_view();
    defer view.deinit();

    var first = try scan_range_from_in_view(&view, testing.allocator, .{
        .start = "\x00a",
        .end = "\x02",
    }, null, 2);
    defer first.deinit();
    try testing.expectEqual(@as(usize, 2), first.entries.items.len);
    try testing.expectEqualStrings("\x00a", first.entries.items[0].key);
    try testing.expectEqualStrings("\x00b", first.entries.items[1].key);
    try testing.expect(first.borrow_next_cursor() != null);

    var cursor = first.take_next_cursor().?;
    defer cursor.deinit();
    const cursor_view = cursor.as_cursor().?;
    var second = try scan_range_from_in_view(&view, testing.allocator, .{
        .start = "\x00a",
        .end = "\x02",
    }, &cursor_view, 2);
    defer second.deinit();
    try testing.expectEqual(@as(usize, 1), second.entries.items.len);
    try testing.expectEqualStrings("\x01a", second.entries.items[0].key);
    try testing.expect(second.borrow_next_cursor() == null);
}

test "batch visibility-gate pause hook blocks completion until resumed" {
    const testing = std.testing;

    const db = try create(std.heap.page_allocator);
    defer db.close() catch unreachable;

    const initial = types.Value{ .integer = 1 };
    try db.put("alpha", &initial);

    const BatchThread = struct {
        db: *Database,
        finished: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        err: ?EngineError = null,

        fn run(state: *@This()) void {
            const next = types.Value{ .integer = 2 };
            state.db.apply_batch(&.{
                .{ .key = "alpha", .value = &next },
                .{ .key = "beta", .value = &next },
            }) catch |err| {
                state.err = err;
            };
            state.finished.store(true, .release);
        }
    };

    batch_ops.test_hooks.pause_next_batch_after_visibility_gate();

    var batch_state = BatchThread{ .db = db };
    const batch_thread = try std.Thread.spawn(.{}, BatchThread.run, .{&batch_state});
    var pause_seen = false;
    var pause_attempts: usize = 0;
    while (pause_attempts < 5_000) : (pause_attempts += 1) {
        if (batch_ops.test_hooks.is_batch_paused_after_visibility_gate()) {
            pause_seen = true;
            break;
        }
        std.Thread.sleep(std.time.ns_per_ms);
    }
    try testing.expect(pause_seen);
    std.Thread.sleep(20 * std.time.ns_per_ms);
    try testing.expect(!batch_state.finished.load(.acquire));

    batch_ops.test_hooks.resume_batch_after_visibility_gate();
    var completed = false;
    var completion_attempts: usize = 0;
    while (completion_attempts < 5_000) : (completion_attempts += 1) {
        if (batch_state.finished.load(.acquire)) {
            completed = true;
            break;
        }
        std.Thread.sleep(std.time.ns_per_ms);
    }
    batch_thread.join();

    try testing.expect(completed);
    try testing.expect(batch_state.err == null);
}

test "concurrent unique-key writers keep all committed values visible" {
    const testing = std.testing;

    const db = try create(std.heap.page_allocator);
    defer db.close() catch unreachable;

    const Writer = struct {
        db: *Database,
        key: []const u8,
        value: i64,
        err: ?EngineError = null,

        fn run(state: *@This()) void {
            const payload = types.Value{ .integer = state.value };
            state.db.put(state.key, &payload) catch |err| {
                state.err = err;
            };
        }
    };

    var writers = [_]Writer{
        .{ .db = db, .key = "writer:0", .value = 10 },
        .{ .db = db, .key = "writer:1", .value = 11 },
        .{ .db = db, .key = "writer:2", .value = 12 },
        .{ .db = db, .key = "writer:3", .value = 13 },
    };
    var threads: [writers.len]std.Thread = undefined;
    for (&writers, 0..) |*writer, index| {
        threads[index] = try std.Thread.spawn(.{}, Writer.run, .{writer});
    }
    for (&threads) |thread| thread.join();

    for (writers) |writer| {
        try testing.expect(writer.err == null);
        var stored = (try db.get(testing.allocator, writer.key)).?;
        defer stored.deinit(testing.allocator);
        try testing.expectEqual(writer.value, stored.integer);
    }
}

test "concurrent put and delete on one key leave the key in a well-formed state" {
    const testing = std.testing;

    const db = try create(std.heap.page_allocator);
    defer db.close() catch unreachable;

    const Mutator = struct {
        db: *Database,
        err: ?EngineError = null,

        fn run_put(state: *@This()) void {
            var index: usize = 0;
            while (index < 100) : (index += 1) {
                const payload = types.Value{ .integer = 1 };
                state.db.put("race:key", &payload) catch |err| {
                    state.err = err;
                    return;
                };
            }
        }

        fn run_delete(state: *@This()) void {
            var index: usize = 0;
            while (index < 100) : (index += 1) {
                _ = state.db.delete("race:key") catch |err| {
                    state.err = err;
                    return;
                };
            }
        }
    };

    var putter = Mutator{ .db = db };
    var deleter = Mutator{ .db = db };
    const put_thread = try std.Thread.spawn(.{}, Mutator.run_put, .{&putter});
    const delete_thread = try std.Thread.spawn(.{}, Mutator.run_delete, .{&deleter});
    put_thread.join();
    delete_thread.join();

    try testing.expect(putter.err == null);
    try testing.expect(deleter.err == null);

    const result = try db.get(testing.allocator, "race:key");
    if (result) |value| {
        var owned = value;
        defer owned.deinit(testing.allocator);
        try testing.expectEqual(@as(i64, 1), owned.integer);
    }
}

test "snapshot floor at a batch commit skips already-checkpointed batch records on reopen" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try alloc_tmp_path_test(testing.allocator, tmp, "step12-batch-floor.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "step12-batch-floor.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(testing.allocator, .{
            .snapshot_path = snapshot_path,
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const one = types.Value{ .integer = 1 };
        const two = types.Value{ .integer = 2 };
        try db.apply_batch(&.{
            .{ .key = "batch:alpha", .value = &one },
            .{ .key = "batch:beta", .value = &two },
        });

        const checkpoint_lsn = current_checkpoint_lsn_for_test(db);
        try testing.expectEqual(@as(u64, 4), checkpoint_lsn);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, checkpoint_lsn);
    }

    const reopened = try open(testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "batch:alpha")).?;
    defer alpha.deinit(testing.allocator);
    var beta = (try reopened.get(testing.allocator, "batch:beta")).?;
    defer beta.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 1), alpha.integer);
    try testing.expectEqual(@as(i64, 2), beta.integer);
    try testing.expectEqual(@as(u64, 5), reopened.state.wal.?.next_lsn);
}

test "checkpointed restart keeps wal next_lsn monotonic for later writes" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try alloc_tmp_path_test(testing.allocator, tmp, "step12-next-lsn.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "step12-next-lsn.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(testing.allocator, .{
            .snapshot_path = snapshot_path,
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const value = types.Value{ .integer = 1 };
        try db.put("lsn:alpha", &value);
        try db.checkpoint();
    }

    {
        const reopened = try open(testing.allocator, .{
            .snapshot_path = snapshot_path,
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer reopened.close() catch unreachable;

        try testing.expectEqual(@as(u64, 2), reopened.state.wal.?.next_lsn);
        const value = types.Value{ .integer = 2 };
        try reopened.put("lsn:beta", &value);
        try testing.expectEqual(@as(u64, 3), reopened.state.wal.?.next_lsn);
    }

    const final = try open(testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer final.close() catch unreachable;

    var alpha = (try final.get(testing.allocator, "lsn:alpha")).?;
    defer alpha.deinit(testing.allocator);
    var beta = (try final.get(testing.allocator, "lsn:beta")).?;
    defer beta.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 1), alpha.integer);
    try testing.expectEqual(@as(i64, 2), beta.integer);
}
