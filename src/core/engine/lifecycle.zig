//! Lifecycle ownership boundary for engine open, create, close, and checkpoint work.
//! Cost: O(s) for runtime-state setup and teardown, where `s` is the shard count.
//! Allocator: Uses explicit allocators for engine-handle ownership; storage I/O remains partially unimplemented.

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

/// Creates an in-memory engine handle without persistence.
///
/// Time Complexity: O(s), where `s` is the runtime shard count.
///
/// Allocator: Allocates the engine handle from `allocator`.
pub fn create(allocator: std.mem.Allocator) error_mod.EngineError!*engine_db.Database {
    const db = allocator.create(engine_db.Database) catch return error.OutOfMemory;
    errdefer allocator.destroy(db);

    db.* = .{
        .allocator = allocator,
        .state = runtime_state.DatabaseState.init(allocator, null),
    };
    return db;
}

/// Opens an engine handle and routes persistence work through storage-owned modules.
///
/// Time Complexity: O(s + r), where `s` is the runtime shard count and `r` is replayed WAL record work when `wal_path` is set.
///
/// Allocator: Allocates the engine handle from `allocator` and uses explicit allocator paths for WAL replay scratch when `wal_path` is set.
pub fn open(allocator: std.mem.Allocator, options: types.DatabaseOptions) error_mod.EngineError!*engine_db.Database {
    if (options.snapshot_path != null) return error.NotImplemented;

    var db = try create(allocator);
    errdefer db.close() catch unreachable;

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
            .min_lsn = 0,
        }, replay_applier, allocator) catch return error.NotImplemented;
    }

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
pub fn close(db: *engine_db.Database) error_mod.EngineError!void {
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
/// Time Complexity: O(1) until snapshot persistence is implemented.
///
/// Allocator: Does not allocate; returns `error.NotImplemented` when snapshot persistence is requested.
pub fn checkpoint(db: *engine_db.Database) error_mod.EngineError!void {
    const snapshot_path = db.state.snapshot_path orelse return error.NotImplemented;
    _ = storage_snapshot.write(&db.state, db.allocator, snapshot_path, 0) catch return error.NotImplemented;
}

fn replay_put(ctx: *anyopaque, key: []const u8, value: *const @import("../types/value.zig").Value) !void {
    const db: *engine_db.Database = @ptrCast(@alignCast(ctx));
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

fn replay_delete(ctx: *anyopaque, key: []const u8) !void {
    const db: *engine_db.Database = @ptrCast(@alignCast(ctx));
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();

    _ = internal_mutate.remove_stored_value_unlocked(shard, db.state.base_allocator, key);
    internal_ttl_index.clear_ttl_entry(shard, key);
}

fn replay_expire(ctx: *anyopaque, key: []const u8, expire_at_sec: i64) !void {
    const db: *engine_db.Database = @ptrCast(@alignCast(ctx));
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
