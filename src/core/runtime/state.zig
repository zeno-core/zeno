//! Runtime-owned database state for shards, visibility coordination, and local counters.
//! Cost: O(s) initialization and teardown over the shard count.
//! Allocator: Uses the engine base allocator for shard-owned key/value storage and engine-owned resources.

const std = @import("std");
const runtime_shard = @import("shard.zig");
const runtime_visibility = @import("visibility.zig");
const storage_wal = @import("../storage/wal.zig");

/// Number of shards in the runtime execution state.
pub const NUM_SHARDS: usize = runtime_shard.NUM_SHARDS;

/// Runtime counters kept local to engine state.
pub const RuntimeCounters = struct {
    ops_put_total: std.atomic.Value(u64),
    ops_get_total: std.atomic.Value(u64),
    ops_delete_total: std.atomic.Value(u64),
    ops_scan_total: std.atomic.Value(u64),
    ops_expire_total: std.atomic.Value(u64),

    fn init() RuntimeCounters {
        return .{
            .ops_put_total = std.atomic.Value(u64).init(0),
            .ops_get_total = std.atomic.Value(u64).init(0),
            .ops_delete_total = std.atomic.Value(u64).init(0),
            .ops_scan_total = std.atomic.Value(u64).init(0),
            .ops_expire_total = std.atomic.Value(u64).init(0),
        };
    }
};

/// Full database runtime state, including shards, visibility coordination, and local counters.
pub const DatabaseState = struct {
    base_allocator: std.mem.Allocator,
    visibility_gate: runtime_visibility.VisibilityGate,
    wal: ?storage_wal.Wal = null,
    snapshot_path: ?[]const u8 = null,
    shards: [NUM_SHARDS]runtime_shard.Shard,
    counters: RuntimeCounters,
    active_read_views: std.atomic.Value(usize),

    /// Initializes runtime state for one engine handle.
    ///
    /// Time Complexity: O(s), where `s` is `NUM_SHARDS`.
    ///
    /// Allocator: Does not allocate during state construction; stores `base_allocator` so shards can later allocate owned key/value data.
    ///
    /// Thread Safety: Must be called before the state is shared across threads.
    pub fn init(base_allocator: std.mem.Allocator, snapshot_path: ?[]const u8) DatabaseState {
        var state = DatabaseState{
            .base_allocator = base_allocator,
            .visibility_gate = .{},
            .wal = null,
            .snapshot_path = snapshot_path,
            .shards = undefined,
            .counters = RuntimeCounters.init(),
            .active_read_views = std.atomic.Value(usize).init(0),
        };
        for (&state.shards) |*shard| {
            shard.* = runtime_shard.Shard.init(base_allocator);
        }
        return state;
    }

    /// Releases runtime state owned by one engine handle.
    ///
    /// Time Complexity: O(s), where `s` is `NUM_SHARDS`.
    ///
    /// Allocator: Does not allocate; frees shard-owned key/value storage and closes the optional WAL.
    ///
    /// Thread Safety: Not thread-safe; caller must ensure exclusive ownership of the enclosing engine handle.
    pub fn deinit(self: *DatabaseState) void {
        if (self.wal) |*wal| wal.close();
        for (&self.shards) |*shard| {
            shard.deinit();
        }
        self.* = undefined;
    }
};
