//! Lifecycle ownership boundary for engine open, create, close, and checkpoint work.
//! Cost: O(s) for runtime-state setup and teardown, where `s` is the shard count.
//! Allocator: Uses explicit allocators for engine-handle ownership; storage I/O remains partially unimplemented.

const std = @import("std");
const engine_db = @import("db.zig");
const error_mod = @import("error.zig");
const runtime_state = @import("../runtime/state.zig");
const storage_replay = @import("../storage/replay.zig");
const storage_snapshot = @import("../storage/snapshot.zig");
const storage_wal = @import("../storage/wal.zig");
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
/// Time Complexity: O(s) when no persistence is requested, where `s` is the runtime shard count.
///
/// Allocator: Allocates the engine handle from `allocator`; storage-owned persistence remains partially unimplemented.
pub fn open(allocator: std.mem.Allocator, options: types.DatabaseOptions) error_mod.EngineError!*engine_db.Database {
    var db = try create(allocator);
    errdefer db.close() catch unreachable;
    db.state.snapshot_path = options.snapshot_path;

    if (options.snapshot_path) |snapshot_path| {
        _ = storage_snapshot.load(&db.state, allocator, snapshot_path) catch return error.NotImplemented;
    }

    if (options.wal_path) |wal_path| {
        const replay_applier = storage_replay.build_replay_applier(db, .{
            .put = replay_put,
            .delete = replay_delete,
            .expire = replay_expire,
        });
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
    _ = ctx;
    _ = key;
    _ = value;
    return error.NotImplemented;
}

fn replay_delete(ctx: *anyopaque, key: []const u8) !void {
    _ = ctx;
    _ = key;
    return error.NotImplemented;
}

fn replay_expire(ctx: *anyopaque, key: []const u8, expire_at_sec: i64) !void {
    _ = ctx;
    _ = key;
    _ = expire_at_sec;
    return error.NotImplemented;
}

test "create initializes runtime state without storage handles" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer close(db) catch unreachable;

    try testing.expect(db.state.wal == null);
    try testing.expect(db.state.snapshot_path == null);
}
