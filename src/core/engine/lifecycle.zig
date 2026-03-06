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
pub fn create(allocator: Allocator) EngineError!*Database {
    return create_with_snapshot_path(allocator, null);
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
pub fn open(allocator: Allocator, options: DatabaseOptions) EngineError!*Database {
    var db = try create_with_snapshot_path(allocator, options.snapshot_path);
    errdefer db.close() catch unreachable;

    const snapshot_lsn = try load_snapshot_for_open(db, allocator, options.snapshot_path, options.wal_path);

    if (options.wal_path) |wal_path| {
        const replay_applier = storage_wal.ReplayApplier{
            .ctx = db,
            .put = replay_put,
            .delete = replay_delete,
            .expire = replay_expire,
        };
        db.state.wal = storage_wal.open(wal_path, .{
            .fsync_mode = options.fsync_mode,
            .fsync_interval_ms = options.fsync_interval_ms,
            .min_lsn = snapshot_lsn,
        }, replay_applier, allocator) catch |err| return error_mod.map_persistence_error(err);
    }

    try purge_expired_after_recovery(&db.state);
    return db;
}

/// Allocates one engine handle and initializes its runtime state with the borrowed `snapshot_path`.
///
/// Time Complexity: O(s), where `s` is the runtime shard count.
///
/// Allocator: Allocates the engine handle from `allocator` and stores the borrowed `snapshot_path` inside the initialized runtime state.
///
/// Ownership: Borrows `snapshot_path`; the caller must keep those bytes valid for the lifetime of the returned engine handle.
fn create_with_snapshot_path(
    allocator: Allocator,
    snapshot_path: ?[]const u8,
) EngineError!*Database {
    const db = allocator.create(Database) catch return error.OutOfMemory;
    errdefer allocator.destroy(db);

    db.* = .{
        .allocator = allocator,
        .state = DatabaseState.init(allocator, snapshot_path),
    };
    db.state.rebind_shard_allocators();
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
    if (db.state.wal) |*wal| {
        if (wal.needs_close_fsync()) {
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
/// Thread Safety: Not thread-safe; caller must ensure exclusive ownership of the engine handle. Writers and new readers are blocked globally during the brief checkpoint barrier, active `ReadView` handles holding the shared visibility gate may delay that barrier, and writers are blocked globally again during each shard-serialization window.
pub fn checkpoint(db: *Database) EngineError!void {
    var checkpoint_timer = std.time.Timer.start() catch unreachable;
    const snapshot_path = db.state.snapshot_path orelse return error.NoSnapshotPath;

    notify_checkpoint_barrier_attempt_for_test();
    db.state.visibility_gate.lock_exclusive();
    notify_checkpoint_barrier_acquired_for_test();
    for (&db.state.shards) |*shard| shard.lock.lock();
    const checkpoint_lsn: u64 = if (db.state.wal) |wal|
        if (wal.next_lsn > 0) wal.next_lsn - 1 else 0
    else
        0;
    for (&db.state.shards) |*shard| shard.lock.unlock();
    db.state.visibility_gate.unlock_exclusive();

    _ = storage_snapshot.write(&db.state, db.allocator, snapshot_path, checkpoint_lsn) catch |err| {
        return error_mod.map_persistence_error(err);
    };

    if (db.state.wal) |*wal| {
        wal.truncate_up_to_lsn(checkpoint_lsn) catch |err| return error_mod.map_persistence_error(err);
    }

    db.state.record_successful_checkpoint(checkpoint_timer.read(), checkpoint_lsn);
}

/// Installs one test-only probe for the checkpoint barrier.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn set_checkpoint_barrier_test_probe_for_test(probe: ?*CheckpointBarrierTestProbe) void {
    if (!builtin.is_test) return;
    checkpoint_barrier_test_probe = probe;
}

fn notify_checkpoint_barrier_attempt_for_test() void {
    if (!builtin.is_test) return;
    const probe = checkpoint_barrier_test_probe orelse return;
    probe.attempted.store(true, .release);
}

fn notify_checkpoint_barrier_acquired_for_test() void {
    if (!builtin.is_test) return;
    const probe = checkpoint_barrier_test_probe orelse return;
    probe.acquired.store(true, .release);
}

/// Returns whether `wal_path` currently exists and contains at least one byte.
///
/// Time Complexity: O(1), excluding filesystem metadata access.
///
/// Allocator: Does not allocate.
fn wal_file_has_content(wal_path: ?[]const u8) bool {
    const path = wal_path orelse return false;
    const file = std.fs.cwd().openFile(path, .{}) catch return false;
    defer file.close();

    const size = file.getEndPos() catch return false;
    return size > 0;
}

/// Loads snapshot state for one open call and decides whether snapshot corruption can fall back to full WAL replay.
///
/// Time Complexity: O(n + e), where `n` is snapshot load work and `e` is retained-shard reset work when corruption falls back to replay.
///
/// Allocator: Uses `allocator` only through delegated snapshot load paths.
///
/// Thread Safety: Not thread-safe; open owns the runtime state exclusively before the database handle is published.
fn load_snapshot_for_open(
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
                const wal_has_content = wal_file_has_content(wal_path);
                if (!wal_has_content) return error.SnapshotCorrupted;

                db.state.record_snapshot_corruption_fallback();
                reset_runtime_shards_for_recovery(&db.state);
                return 0;
            },
            else => return error_mod.map_persistence_error(err),
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
fn reset_runtime_shards_for_recovery(state: *DatabaseState) void {
    for (&state.shards) |*shard| {
        shard.reset_unlocked();
    }
}

/// Removes expired keys that survived snapshot load or WAL replay before the engine handle becomes visible.
///
/// Time Complexity: O(s + t + k), where `s` is the runtime shard count, `t` is total TTL entry count scanned, and `k` is total bytes cloned for temporary expired-key buffers.
///
/// Allocator: Uses `state.base_allocator` for temporary cloned expired keys and frees that scratch before return.
///
/// Thread Safety: Not thread-safe; recovery owns the runtime state exclusively before the engine handle is published.
fn purge_expired_after_recovery(state: *DatabaseState) !void {
    const now = runtime_shard.unix_now();

    for (&state.shards) |*shard| {
        shard.lock.lock();
        defer shard.lock.unlock();

        var expired_keys = std.ArrayList([]u8).empty;
        defer {
            for (expired_keys.items) |key| state.base_allocator.free(key);
            expired_keys.deinit(state.base_allocator);
        }

        var iterator = shard.ttl_index.iterator();
        while (iterator.next()) |entry| {
            if (!internal_ttl_index.is_expired(entry.value_ptr.*, now)) continue;
            try expired_keys.append(state.base_allocator, try state.base_allocator.dupe(u8, entry.key_ptr.*));
        }

        for (expired_keys.items) |key| {
            _ = try internal_mutate.remove_stored_value_unlocked(shard, key);
            internal_ttl_index.clear_ttl_entry(shard, key);
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
fn replay_put(ctx: *anyopaque, key: []const u8, value: *const Value) !void {
    const db: *Database = @ptrCast(@alignCast(ctx));
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();

    try internal_mutate.upsert_value_unlocked(shard, key, value);
    internal_ttl_index.clear_ttl_entry(shard, key);
}

/// Applies one replayed `DELETE` mutation into runtime state while WAL open still owns recovery.
///
/// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is ART delete work.
///
/// Allocator: Does not allocate directly; removal retains replaced storage inside the shard arena model.
///
/// Thread Safety: Replay runs before the WAL handle is shared; this helper acquires one shard-exclusive lock for the targeted key.
fn replay_delete(ctx: *anyopaque, key: []const u8) !void {
    const db: *Database = @ptrCast(@alignCast(ctx));
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();

    _ = try internal_mutate.remove_stored_value_unlocked(shard, key);
    internal_ttl_index.clear_ttl_entry(shard, key);
}

/// Applies one replayed `EXPIRE` mutation into runtime state while WAL open still owns recovery.
///
/// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is shard-local ART lookup plus optional TTL metadata update work.
///
/// Allocator: Uses `db.state.base_allocator` only when inserting a new TTL entry.
///
/// Thread Safety: Replay runs before the WAL handle is shared; this helper acquires one shard-exclusive lock for the targeted key.
fn replay_expire(ctx: *anyopaque, key: []const u8, expire_at_sec: i64) !void {
    const db: *Database = @ptrCast(@alignCast(ctx));
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();

    if (!internal_mutate.key_exists_unlocked(shard, key)) {
        internal_ttl_index.clear_ttl_entry(shard, key);
        return;
    }

    try internal_ttl_index.set_ttl_entry(shard, key, expire_at_sec);
}

fn alloc_tmp_path_test(allocator: std.mem.Allocator, tmp: std.testing.TmpDir, basename: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, basename });
}

test "create initializes runtime state without storage handles" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer close(db) catch unreachable;

    try testing.expect(db.state.wal == null);
    try testing.expect(db.state.snapshot_path == null);
}

test "load_snapshot_for_open records corruption fallback when replayable wal exists" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try alloc_tmp_path_test(testing.allocator, tmp, "fallback.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "fallback.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try create(testing.allocator);
        defer close(db) catch unreachable;

        const value = Value{ .integer = 1 };
        try db.put("alpha", &value);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, 0);
    }

    {
        const wal_file = try std.fs.cwd().createFile(wal_path, .{ .truncate = true });
        defer wal_file.close();
        try wal_file.writeAll("wal");
    }

    const file = try std.fs.cwd().openFile(snapshot_path, .{ .mode = .read_write });
    defer file.close();
    try file.writeAll("bad!");

    const db = try create_with_snapshot_path(testing.allocator, snapshot_path);
    defer close(db) catch unreachable;

    try testing.expectEqual(@as(u64, 0), try load_snapshot_for_open(db, testing.allocator, snapshot_path, wal_path));
    try testing.expectEqual(@as(u64, 1), db.state.stats_snapshot().snapshot_corruption_fallback_total);
}

test "load_snapshot_for_open leaves fallback metric unchanged when corruption cannot replay from wal" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try alloc_tmp_path_test(testing.allocator, tmp, "no-fallback.snapshot");
    defer testing.allocator.free(snapshot_path);
    const missing_wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "missing.wal");
    defer testing.allocator.free(missing_wal_path);
    const empty_wal_path = try alloc_tmp_path_test(testing.allocator, tmp, "empty.wal");
    defer testing.allocator.free(empty_wal_path);

    {
        const db = try create(testing.allocator);
        defer close(db) catch unreachable;

        const value = Value{ .integer = 1 };
        try db.put("alpha", &value);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, 0);
    }

    {
        const wal_file = try std.fs.cwd().createFile(empty_wal_path, .{ .truncate = true });
        wal_file.close();
    }

    {
        const file = try std.fs.cwd().openFile(snapshot_path, .{ .mode = .read_write });
        defer file.close();
        try file.writeAll("bad!");
    }

    {
        const db = try create_with_snapshot_path(testing.allocator, snapshot_path);
        defer close(db) catch unreachable;

        try testing.expectError(error.SnapshotCorrupted, load_snapshot_for_open(db, testing.allocator, snapshot_path, missing_wal_path));
        try testing.expectEqual(@as(u64, 0), db.state.stats_snapshot().snapshot_corruption_fallback_total);
    }

    {
        const db = try create_with_snapshot_path(testing.allocator, snapshot_path);
        defer close(db) catch unreachable;

        try testing.expectError(error.SnapshotCorrupted, load_snapshot_for_open(db, testing.allocator, snapshot_path, empty_wal_path));
        try testing.expectEqual(@as(u64, 0), db.state.stats_snapshot().snapshot_corruption_fallback_total);
    }
}
