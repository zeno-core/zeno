//! Lifecycle ownership boundary for engine open, create, close, and checkpoint work.
//! Time Complexity: O(s + n + r + e) for persistent open, where `s` is the shard count, `n` is snapshot load work, `r` is replayed WAL work, and `e` is recovery-time expired-key purge work.
//! Allocator: Uses explicit allocators for engine-handle ownership plus snapshot-load and WAL-replay scratch.

const builtin = @import("builtin");
const std = @import("std");
const engine_db = @import("db.zig");
const error_mod = @import("error.zig");
const internal_mutate = @import("../internal/mutate.zig");
const internal_ttl_index = @import("../internal/ttl_index.zig");
const runtime_state = @import("../runtime/state.zig");
const storage_snapshot = @import("../storage/snapshot.zig");
const storage_wal = @import("../storage/wal.zig");
const runtime_shard = @import("../runtime/shard.zig");
const types = @import("../types.zig");

const Allocator = std.mem.Allocator;
const EngineError = error_mod.EngineError;
const Database = engine_db.Database;
const DatabaseOptions = types.DatabaseOptions;
const DatabaseState = runtime_state.DatabaseState;
const Value = @import("../types/value.zig").Value;

/// Test-only probe for observing checkpoint barrier progress without scheduler timing assumptions.
pub const CheckpointBarrierTestProbe = struct {
    attempted: *std.atomic.Value(bool),
    acquired: *std.atomic.Value(bool),
};

var checkpoint_barrier_test_probe: ?*CheckpointBarrierTestProbe = null;

/// Creates an in-memory engine handle without persistence.
///
/// Time Complexity: O(s), where `s` is the runtime shard count.
///
/// Allocator: Allocates the engine handle from `allocator`.
///
/// Ownership: Borrows `io`; its provider must outlive the database, read views, and background workers.
pub fn create(io: std.Io, allocator: Allocator) EngineError!*Database {
    return createWithSnapshotPath(io, allocator, null, types.defaultMetricsConfig());
}

/// Opens an engine handle and routes persistence work through storage-owned modules.
///
/// Time Complexity: O(s + n + r + e), where:
///  - `s` is the runtime shard count
///  - `n` is snapshot load work
///  - `r` is replayed WAL record work
///  - `e` is post-recovery expired-key purge work
///
/// Allocator: Allocates the engine handle from `allocator` and uses explicit allocator
/// paths for snapshot load and WAL replay scratch when persistence is configured.
///
/// Thread Safety: Not thread-safe during open; recovery mutates runtime state before the database handle is published to callers.
///
/// Ownership: Borrows `io`; its provider must outlive the database, read views, and background workers.
pub fn open(io: std.Io, allocator: Allocator, options: DatabaseOptions) EngineError!*Database {
    var db = try createWithSnapshotPath(io, allocator, options.snapshot_path, options.metrics);
    errdefer db.close() catch unreachable;

    db.auto_compaction.every = if (options.heavy_overwrite_compact_every) |n|
        if (n == 0) null else n
    else
        null;
    db.auto_wal_checkpoint.max_bytes = options.max_wal_bytes;

    const snapshot_lsn = try loadSnapshotForOpen(db, allocator, options.snapshot_path, options.wal_path);

    if (options.wal_path) |wal_path| {
        const replay_applier = storage_wal.ReplayApplier{
            .ctx = db,
            .put = replayPut,
            .delete = replayDelete,
            .expire = replayExpire,
        };
        db.state.wal = storage_wal.open(db.state.io, wal_path, .{
            .fsync_mode = options.fsync_mode,
            .fsync_interval_ms = options.fsync_interval_ms,
            .min_lsn = snapshot_lsn,
        }, replay_applier, allocator) catch |err| return error_mod.mapPersistenceError(err);
    }

    try purgeExpiredAfterRecovery(&db.state);
    if (options.ttl_sweep_interval_ms) |interval_ms| {
        db.startTtlSweeper(interval_ms) catch |err| return error_mod.mapPersistenceError(err);
    }
    return db;
}

/// Allocates one engine handle and initializes its runtime state with the borrowed `snapshot_path`.
///
/// Time Complexity: O(s), where `s` is the runtime shard count.
///
/// Allocator: Allocates the engine handle from `allocator` and stores the borrowed `snapshot_path` inside the initialized runtime state.
///
/// Ownership: Borrows `snapshot_path`; the caller must keep those bytes valid for the lifetime of the returned engine handle.
fn createWithSnapshotPath(
    io: std.Io,
    allocator: Allocator,
    snapshot_path: ?[]const u8,
    metrics_config: types.MetricsConfig,
) EngineError!*Database {
    const db = allocator.create(Database) catch return error.OutOfMemory;
    errdefer allocator.destroy(db);

    db.* = .{
        .allocator = allocator,
        .state = DatabaseState.initWithMetrics(io, allocator, snapshot_path, metrics_config),
    };
    db.state.rebindShardAllocators();
    return db;
}

/// Flushes and closes persistence handles, then releases runtime state and engine ownership.
///
/// Time Complexity: O(s), where `s` is the runtime shard count.
///
/// Allocator: Does not allocate.
///
/// Ownership: Returns `error.ActiveReadViews` when any `ReadView` handles still borrow this database.
///
/// Thread Safety: Not thread-safe; caller must ensure exclusive ownership of the engine handle.
pub fn close(db: *Database) EngineError!void {
    if (db.state.active_read_views.load(.monotonic) != 0) return error.ActiveReadViews;
    // Stop the sweep thread before tearing down shard state. The thread accesses
    // shard memory; it must be joined before `db.state.deinit()` frees it.
    db.stopTtlSweeper();
    if (db.state.wal) |*wal| {
        if (wal.needsCloseFsync()) {
            wal.fsync() catch return error.WalFlushFailed;
        }
    }
    db.state.deinit();
    db.allocator.destroy(db);
}

/// Writes one consistent checkpoint through the storage-owned snapshot boundary.
///
/// The checkpoint runs in three phases:
/// 1. Acquire a brief checkpoint barrier by taking the exclusive visibility gate and every shard-exclusive lock, then capture `checkpoint_lsn`.
/// 2. Write the snapshot shard by shard under the shared visibility gate.
/// 3. Compact the WAL by removing records with `lsn <= checkpoint_lsn`.
///
/// Time Complexity: O(s + n + m), where:
///  - `s` is the runtime shard count
///  - `n` is snapshot serialization work
///  - `m` is WAL compaction scan and rewrite work when durability is enabled.
///
/// Allocator: Uses `db.allocator` for snapshot serialization scratch and optional WAL compaction scratch.
///
/// Ownership: Returns `error.NoSnapshotPath` when `snapshot_path` was not configured for this engine handle.
///
/// Thread Safety: Not thread-safe; caller must ensure exclusive ownership of the engine handle. Writers and new readers are blocked globally during the brief checkpoint barrier, active `ReadView` handles holding the shared visibility gate cause one fail-fast `error.CheckpointBusy` response instead of waiting, and writers are blocked globally again during each shard-serialization window.
pub fn checkpoint(db: *Database) EngineError!void {
    const checkpoint_started = std.Io.Clock.awake.now(db.state.io);
    const snapshot_path = db.state.snapshot_path orelse return error.NoSnapshotPath;

    notifyCheckpointBarrierAttemptForTest();
    if (!db.state.tryLockAllShardsExclusive()) {
        db.state.recordBusyCheckpoint();
        return error.CheckpointBusy;
    }
    notifyCheckpointBarrierAcquiredForTest();

    for (&db.state.shards) |*shard| shard.lock.lockUncancelable(shard.io);
    const checkpoint_lsn: u64 = if (db.state.wal) |wal|
        if (wal.next_lsn > 0) wal.next_lsn - 1 else 0
    else
        0;
    for (&db.state.shards) |*shard| shard.lock.unlock(shard.io);

    db.state.unlockAllShardsExclusive();

    _ = storage_snapshot.write(&db.state, db.allocator, snapshot_path, checkpoint_lsn) catch |err| {
        return error_mod.mapPersistenceError(err);
    };

    if (db.state.wal) |*wal| {
        wal.truncateUpToLsn(checkpoint_lsn) catch |err| return error_mod.mapPersistenceError(err);
    }

    db.state.recordSuccessfulCheckpoint(@intCast(checkpoint_started.durationTo(std.Io.Clock.awake.now(db.state.io)).toNanoseconds()), checkpoint_lsn);
}

/// Rebuilds runtime-owned shard storage from a fresh checkpoint snapshot to reclaim arena-retained churn.
///
/// Time Complexity: O(s + n + m), where:
///  - `s` is the runtime shard count
///  - `n` is snapshot serialization plus load work
///  - `m` is WAL compaction scan and rewrite work when durability is enabled.
///
/// Allocator: Uses `db.allocator` for snapshot write and load scratch.
///
/// Ownership: Returns `error.NoSnapshotPath` when `snapshot_path` was not configured for this engine handle.
///
/// Thread Safety: Not thread-safe; caller must ensure exclusive ownership of the engine handle. Follows the same checkpoint barrier semantics and may return `error.CheckpointBusy` when active `ReadView` handles block the barrier.
pub fn compactMemory(db: *Database) EngineError!void {
    const snapshot_path = db.state.snapshot_path orelse return error.NoSnapshotPath;
    try checkpoint(db);

    _ = storage_snapshot.load(&db.state, db.allocator, snapshot_path) catch |err| {
        return error_mod.mapPersistenceError(err);
    };
}

/// Rebuilds one shard in place to reclaim retained arena churn.
///
/// Time Complexity: O(n + t), where:
///  - `n` is keys in the shard tree
///  - `t` is keys in shard TTL metadata.
///
/// Allocator: Uses `db.allocator` for temporary clones during rebuild.
///
/// Thread Safety: Synchronous maintenance operation. Blocks the selected shard by
/// taking its exclusive visibility gate and shard-exclusive lock.
pub fn compactShard(db: *Database, shard_idx: usize) EngineError!void {
    if (shard_idx >= runtime_shard.NUM_SHARDS) return error.InvalidShardIndex;

    const shard = &db.state.shards[shard_idx];
    shard.visibility_gate.lockExclusive();
    defer shard.visibility_gate.unlockExclusive();

    shard.lock.lockUncancelable(shard.io);
    defer shard.lock.unlock(shard.io);

    const seq0 = shard.seq.load(.monotonic);
    shard.seq.store(seq0 + 1, .release);
    defer shard.seq.store(seq0 + 2, .release);

    const RetainedEntry = struct {
        key: []u8,
        value: Value,
    };
    const RetainedTtl = struct {
        key: []u8,
        expireAt: i64,
    };

    var retained_entries = std.ArrayList(RetainedEntry).empty;
    defer {
        for (retained_entries.items) |*entry| {
            db.allocator.free(entry.key);
            entry.value.deinit(db.allocator);
        }
        retained_entries.deinit(db.allocator);
    }

    var retained_ttls = std.ArrayList(RetainedTtl).empty;
    defer {
        for (retained_ttls.items) |entry| db.allocator.free(entry.key);
        retained_ttls.deinit(db.allocator);
    }

    const EntryContext = struct {
        allocator: std.mem.Allocator,
        entries: *std.ArrayList(RetainedEntry),

        fn visit(ctx_ptr: *anyopaque, key: []const u8, value: *const Value) !void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            try ctx.entries.append(ctx.allocator, .{
                .key = try ctx.allocator.dupe(u8, key),
                .value = try value.clone(ctx.allocator),
            });
        }
    };

    var entry_ctx = EntryContext{
        .allocator = db.allocator,
        .entries = &retained_entries,
    };
    _ = shard.tree.forEach(&entry_ctx, EntryContext.visit) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => unreachable,
    };

    var ttl_iterator = shard.ttl_index.iterator();
    while (ttl_iterator.next()) |entry| {
        try retained_ttls.append(db.allocator, .{
            .key = try db.allocator.dupe(u8, entry.key_ptr.*),
            .expireAt = entry.value_ptr.*,
        });
    }

    var replacement = runtime_shard.Shard.init(db.state.io, db.allocator);
    var replacement_transferred = false;
    defer if (!replacement_transferred) replacement.deinit();
    replacement.rebindTreeAllocator();

    for (retained_entries.items) |*entry| {
        _ = try internal_mutate.upsertValueUnlocked(&replacement, entry.key, &entry.value);
    }
    for (retained_ttls.items) |entry| {
        try internal_ttl_index.setTtlEntry(&replacement, entry.key, entry.expireAt);
    }

    shard.replaceStorageUnlocked(replacement.arena, replacement.ttl_index, replacement.tree);
    replacement_transferred = true;

    db.state.clearRetainedHeavyBytesEstimateForShard(shard_idx);
}

/// Rebuilds every shard in place to reclaim retained arena churn without snapshot/WAL round-trips.
///
/// Time Complexity: O(s + n + t), where:
///  - `s` is shard count
///  - `n` is total key count across shard trees
///  - `t` is total TTL entries across shards.
///
/// Allocator: Uses `db.allocator` for temporary clones during each shard rebuild.
///
/// Thread Safety: Synchronous maintenance operation. Blocks one shard at a time.
pub fn compactAll(db: *Database) EngineError!void {
    for (0..runtime_shard.NUM_SHARDS) |shard_idx| {
        try compactShard(db, shard_idx);
    }
}

/// Installs one test-only probe for the checkpoint barrier.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn setCheckpointBarrierTestProbeForTest(probe: ?*CheckpointBarrierTestProbe) void {
    if (!builtin.is_test) return;
    checkpoint_barrier_test_probe = probe;
}

fn notifyCheckpointBarrierAttemptForTest() void {
    if (!builtin.is_test) return;
    const probe = checkpoint_barrier_test_probe orelse return;
    probe.attempted.store(true, .release);
}

fn notifyCheckpointBarrierAcquiredForTest() void {
    if (!builtin.is_test) return;
    const probe = checkpoint_barrier_test_probe orelse return;
    probe.acquired.store(true, .release);
}

/// Returns whether `wal_path` currently exists and contains at least one byte.
///
/// Time Complexity: O(1), excluding filesystem metadata access.
///
/// Allocator: Does not allocate.
fn walFileHasContent(io: std.Io, wal_path: ?[]const u8) bool {
    const path = wal_path orelse return false;
    const file = std.Io.Dir.cwd().openFile(io, path, .{}) catch return false;
    defer file.close(io);

    const size = file.length(io) catch return false;
    return size > 0;
}

/// Loads snapshot state for one open call and decides whether snapshot corruption can fall back to full WAL replay.
///
/// Time Complexity: O(n + e), where `n` is snapshot load work and `e` is retained-shard reset work when corruption falls back to replay.
///
/// Allocator: Uses `allocator` only through delegated snapshot load paths.
///
/// Thread Safety: Not thread-safe; open owns the runtime state exclusively before the database handle is published.
fn loadSnapshotForOpen(
    db: *Database,
    allocator: Allocator,
    snapshot_path: ?[]const u8,
    wal_path: ?[]const u8,
) EngineError!u64 {
    if (snapshot_path) |path| {
        if (storage_snapshot.load(&db.state, allocator, path)) |result| {
            return result.checkpoint_lsn;
        } else |err| switch (err) {
            error.FileNotFound => return 0,
            error.SnapshotCorrupted => {
                const wal_has_content = walFileHasContent(db.state.io, wal_path);
                if (!wal_has_content) return error.SnapshotCorrupted;

                db.state.recordSnapshotCorruptionFallback();
                resetRuntimeShardsForRecovery(&db.state);
                return 0;
            },
            else => return error_mod.mapPersistenceError(err),
        }
    }
    return 0;
}

/// Reinitializes every shard after a corrupted snapshot forces fallback to full WAL replay.
///
/// Time Complexity: O(s + n), where `s` is the runtime shard count and `n` is total retained shard teardown work.
///
/// Allocator: Does not allocate; releases current shard-owned storage while retaining shard arena capacity for the reconstructed ART-empty state.
///
/// Thread Safety: Not thread-safe; recovery owns the runtime state exclusively before the engine handle is published.
fn resetRuntimeShardsForRecovery(state: *DatabaseState) void {
    for (&state.shards) |*shard| {
        shard.resetUnlocked();
    }
}

/// Removes expired keys that survived snapshot load or WAL replay before the engine handle becomes visible.
///
/// Time Complexity: O(s + t + k), where `s` is the runtime shard count, `t` is total TTL entry count scanned, and `k` is total bytes cloned for temporary expired-key buffers.
///
/// Allocator: Uses `state.base_allocator` for temporary cloned expired keys and frees that scratch before return.
///
/// Thread Safety: Not thread-safe; recovery owns the runtime state exclusively before the engine handle is published.
fn purgeExpiredAfterRecovery(state: *DatabaseState) !void {
    const now = runtime_shard.unixNow(state.io);

    for (&state.shards) |*shard| {
        shard.lock.lockUncancelable(shard.io);
        defer shard.lock.unlock(shard.io);

        var expired_keys = std.ArrayList([]u8).empty;
        defer {
            for (expired_keys.items) |key| state.base_allocator.free(key);
            expired_keys.deinit(state.base_allocator);
        }

        var iterator = shard.ttl_index.iterator();
        while (iterator.next()) |entry| {
            if (!internal_ttl_index.isExpired(entry.value_ptr.*, now)) continue;
            try expired_keys.append(state.base_allocator, try state.base_allocator.dupe(u8, entry.key_ptr.*));
        }

        for (expired_keys.items) |key| {
            _ = try internal_mutate.removeStoredValueUnlocked(shard, key);
            internal_ttl_index.clearTtlEntry(shard, key);
        }
    }
}

/// Applies one replayed `PUT` mutation into runtime state while WAL open still owns recovery.
///
/// Time Complexity: O(n + k + v), where `n` is `key.len` for shard routing, `k` is ART insert or overwrite work, and `v` is cloned value size.
///
/// Allocator: Clones owned key and value storage through the target shard arena.
///
/// Ownership: Clones `value` into runtime-owned storage before returning.
///
/// Thread Safety: Replay runs before the WAL handle is shared; this helper acquires one shard-exclusive lock for the targeted key.
fn replayPut(ctx: *anyopaque, key: []const u8, value: *const Value) !void {
    const db: *Database = @ptrCast(@alignCast(ctx));
    const shard_idx = runtime_shard.getShardIndex(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lockUncancelable(shard.io);
    defer shard.lock.unlock(shard.io);

    _ = try internal_mutate.upsertValueUnlocked(shard, key, value);
    internal_ttl_index.clearTtlEntry(shard, key);
}

/// Applies one replayed `DELETE` mutation into runtime state while WAL open still owns recovery.
///
/// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is ART delete work.
///
/// Allocator: Does not allocate directly; removal retains replaced storage inside the shard arena model.
///
/// Thread Safety: Replay runs before the WAL handle is shared; this helper acquires one shard-exclusive lock for the targeted key.
fn replayDelete(ctx: *anyopaque, key: []const u8) !void {
    const db: *Database = @ptrCast(@alignCast(ctx));
    const shard_idx = runtime_shard.getShardIndex(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lockUncancelable(shard.io);
    defer shard.lock.unlock(shard.io);

    _ = try internal_mutate.removeStoredValueUnlocked(shard, key);
    internal_ttl_index.clearTtlEntry(shard, key);
}

/// Applies one replayed `EXPIRE` mutation into runtime state while WAL open still owns recovery.
///
/// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is shard-local ART lookup plus optional TTL metadata update work.
///
/// Allocator: Uses `db.state.base_allocator` only when inserting a new TTL entry.
///
/// Thread Safety: Replay runs before the WAL handle is shared; this helper acquires one shard-exclusive lock for the targeted key.
fn replayExpire(ctx: *anyopaque, key: []const u8, expire_at_sec: i64) !void {
    const db: *Database = @ptrCast(@alignCast(ctx));
    const shard_idx = runtime_shard.getShardIndex(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lockUncancelable(shard.io);
    defer shard.lock.unlock(shard.io);

    if (!internal_mutate.keyExistsUnlocked(shard, key)) {
        internal_ttl_index.clearTtlEntry(shard, key);
        return;
    }

    try internal_ttl_index.setTtlEntry(shard, key, expire_at_sec);
}

fn allocTmpPathTest(allocator: std.mem.Allocator, tmp: std.testing.TmpDir, basename: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, basename });
}

test "create initializes runtime state without storage handles" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer close(db) catch unreachable;

    try testing.expect(db.state.wal == null);
    try testing.expect(db.state.snapshot_path == null);
}

test "load_snapshot_for_open records corruption fallback when replayable wal exists" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "fallback.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "fallback.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try create(std.testing.io, testing.allocator);
        defer close(db) catch unreachable;

        const value = Value{ .integer = 1 };
        try db.put("alpha", &value);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, 0);
    }

    {
        const wal_file = try std.Io.Dir.cwd().createFile(std.testing.io, wal_path, .{ .truncate = true });
        defer wal_file.close(std.testing.io);
        try wal_file.writeStreamingAll(std.testing.io, "wal");
    }

    const file = try std.Io.Dir.cwd().openFile(std.testing.io, snapshot_path, .{ .mode = .read_write });
    defer file.close(std.testing.io);
    try file.writeStreamingAll(std.testing.io, "bad!");

    const db = try createWithSnapshotPath(std.testing.io, testing.allocator, snapshot_path, types.defaultMetricsConfig());
    defer close(db) catch unreachable;

    try testing.expectEqual(@as(u64, 0), try loadSnapshotForOpen(db, testing.allocator, snapshot_path, wal_path));
    try testing.expectEqual(@as(u64, 1), db.state.statsSnapshot().snapshot_corruption_fallback_total);
}

test "load_snapshot_for_open leaves fallback metric unchanged when corruption cannot replay from wal" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "no-fallback.snapshot");
    defer testing.allocator.free(snapshot_path);
    const missing_wal_path = try allocTmpPathTest(testing.allocator, tmp, "missing.wal");
    defer testing.allocator.free(missing_wal_path);
    const empty_wal_path = try allocTmpPathTest(testing.allocator, tmp, "empty.wal");
    defer testing.allocator.free(empty_wal_path);

    {
        const db = try create(std.testing.io, testing.allocator);
        defer close(db) catch unreachable;

        const value = Value{ .integer = 1 };
        try db.put("alpha", &value);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, 0);
    }

    {
        const wal_file = try std.Io.Dir.cwd().createFile(std.testing.io, empty_wal_path, .{ .truncate = true });
        wal_file.close(std.testing.io);
    }

    {
        const file = try std.Io.Dir.cwd().openFile(std.testing.io, snapshot_path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        try file.writeStreamingAll(std.testing.io, "bad!");
    }

    {
        const db = try createWithSnapshotPath(std.testing.io, testing.allocator, snapshot_path, types.defaultMetricsConfig());
        defer close(db) catch unreachable;

        try testing.expectError(error.SnapshotCorrupted, loadSnapshotForOpen(db, testing.allocator, snapshot_path, missing_wal_path));
        try testing.expectEqual(@as(u64, 0), db.state.statsSnapshot().snapshot_corruption_fallback_total);
    }

    {
        const db = try createWithSnapshotPath(std.testing.io, testing.allocator, snapshot_path, types.defaultMetricsConfig());
        defer close(db) catch unreachable;

        try testing.expectError(error.SnapshotCorrupted, loadSnapshotForOpen(db, testing.allocator, snapshot_path, empty_wal_path));
        try testing.expectEqual(@as(u64, 0), db.state.statsSnapshot().snapshot_corruption_fallback_total);
    }
}
