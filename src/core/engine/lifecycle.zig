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

    var snapshot_lsn: u64 = 0;
    if (options.snapshot_path) |snapshot_path| {
        if (storage_snapshot.load(&db.state, allocator, snapshot_path)) |result| {
            snapshot_lsn = result.checkpoint_lsn;
        } else |err| switch (err) {
            error.FileNotFound => {},
            error.SnapshotCorrupted => {
                const wal_has_content = wal_file_has_content(options.wal_path);
                if (!wal_has_content) return error.SnapshotCorrupted;

                reset_runtime_shards_for_recovery(&db.state);
            },
            else => return error_mod.map_persistence_error(err),
        }
    }

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

    storage_snapshot.write(&db.state, db.allocator, snapshot_path, checkpoint_lsn) catch |err| {
        return error_mod.map_persistence_error(err);
    };

    if (db.state.wal) |*wal| {
        wal.truncate_up_to_lsn(checkpoint_lsn) catch |err| return error_mod.map_persistence_error(err);
    }
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

/// Reinitializes every shard after a corrupted snapshot forces fallback to full WAL replay.
///
/// Time Complexity: O(s + n), where `s` is the runtime shard count and `n` is total retained shard teardown work.
///
/// Allocator: Does not allocate; releases current shard-owned storage, then reconstructs empty shard state using `state.base_allocator`.
///
/// Thread Safety: Not thread-safe; recovery owns the runtime state exclusively before the engine handle is published.
fn reset_runtime_shards_for_recovery(state: *DatabaseState) void {
    for (&state.shards) |*shard| {
        shard.deinit();
        shard.* = runtime_shard.Shard.init(state.base_allocator);
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

        var expired_keys = std.ArrayList([]u8).init(state.base_allocator);
        defer {
            for (expired_keys.items) |key| state.base_allocator.free(key);
            expired_keys.deinit();
        }

        var iterator = shard.ttl_index.iterator();
        while (iterator.next()) |entry| {
            if (!internal_ttl_index.is_expired(entry.value_ptr.*, now)) continue;
            try expired_keys.append(try state.base_allocator.dupe(u8, entry.key_ptr.*));
        }

        for (expired_keys.items) |key| {
            _ = internal_mutate.remove_stored_value_unlocked(shard, state.base_allocator, key);
            internal_ttl_index.clear_ttl_entry(shard, key);
        }
    }
}

/// Applies one replayed `PUT` mutation into runtime state while WAL open still owns recovery.
///
/// Time Complexity: O(n^2 + k + v), where `n` is `key.len` for shard routing, `k` is hash-map lookup or insert work, and `v` is cloned value size.
///
/// Allocator: Clones owned key and value storage through `db.state.base_allocator`.
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

    const allocator = db.state.base_allocator;
    if (shard.values.getPtr(key)) |stored| {
        const cloned = try value.clone(allocator);
        errdefer {
            var owned_value = cloned;
            owned_value.deinit(allocator);
        }

        stored.deinit(allocator);
        stored.* = cloned;
    } else {
        try shard.values.ensureUnusedCapacity(allocator, 1);

        const owned_key = try allocator.dupe(u8, key);
        errdefer allocator.free(owned_key);

        const cloned = try value.clone(allocator);
        errdefer {
            var owned_value = cloned;
            owned_value.deinit(allocator);
        }

        shard.values.putAssumeCapacityNoClobber(owned_key, cloned);
    }

    internal_ttl_index.clear_ttl_entry(shard, key);
}

/// Applies one replayed `DELETE` mutation into runtime state while WAL open still owns recovery.
///
/// Time Complexity: O(n^2 + k), where `n` is `key.len` for shard routing and `k` is hash-map removal work.
///
/// Allocator: Does not allocate; frees runtime-owned key and value storage when the key exists.
///
/// Thread Safety: Replay runs before the WAL handle is shared; this helper acquires one shard-exclusive lock for the targeted key.
fn replay_delete(ctx: *anyopaque, key: []const u8) !void {
    const db: *Database = @ptrCast(@alignCast(ctx));
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();

    _ = internal_mutate.remove_stored_value_unlocked(shard, db.state.base_allocator, key);
    internal_ttl_index.clear_ttl_entry(shard, key);
}

/// Applies one replayed `EXPIRE` mutation into runtime state while WAL open still owns recovery.
///
/// Time Complexity: O(n^2 + k), where `n` is `key.len` for shard routing and `k` is shard-local lookup plus optional TTL metadata update work.
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

    if (!shard.values.contains(key)) {
        internal_ttl_index.clear_ttl_entry(shard, key);
        return;
    }

    try internal_ttl_index.set_ttl_entry(shard, key, expire_at_sec);
}

test "create initializes runtime state without storage handles" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer close(db) catch unreachable;

    try testing.expect(db.state.wal == null);
    try testing.expect(db.state.snapshot_path == null);
}
