//! Runtime-owned database state for shards, visibility coordination, and local counters.
//! Cost: O(s) initialization and teardown over the shard count.
//! Allocator: Uses the engine base allocator for shard-owned key/value storage and engine-owned resources.

const std = @import("std");
const runtime_shard = @import("shard.zig");
const runtime_visibility = @import("visibility.zig");
const storage_wal = @import("../storage/wal.zig");
const types = @import("../types.zig");

/// Number of shards in the runtime execution state.
pub const NUM_SHARDS: usize = runtime_shard.NUM_SHARDS;

/// Immutable snapshot of runtime-local metrics counters.
pub const StatsCounters = struct {
    ops_put_total: u64,
    ops_get_total: u64,
    ops_delete_total: u64,
    ops_scan_total: u64,
    ops_expire_total: u64,
    latency_lt_1us_total: u64,
    latency_lt_10us_total: u64,
    latency_lt_100us_total: u64,
    latency_lt_1ms_total: u64,
    latency_ge_1ms_total: u64,
    latency_samples_total: u64,
    checkpoint_count_total: u64,
    checkpoint_duration_last_ms: u64,
    checkpoint_lsn_last: u64,
    snapshot_corruption_fallback_total: u64,
};

/// Runtime counters kept local to engine state.
pub const RuntimeCounters = struct {
    ops_put_total: std.atomic.Value(u64),
    ops_get_total: std.atomic.Value(u64),
    ops_delete_total: std.atomic.Value(u64),
    ops_scan_total: std.atomic.Value(u64),
    ops_expire_total: std.atomic.Value(u64),
    latency_lt_1us_total: std.atomic.Value(u64),
    latency_lt_10us_total: std.atomic.Value(u64),
    latency_lt_100us_total: std.atomic.Value(u64),
    latency_lt_1ms_total: std.atomic.Value(u64),
    latency_ge_1ms_total: std.atomic.Value(u64),
    latency_samples_total: std.atomic.Value(u64),
    checkpoint_count_total: std.atomic.Value(u64),
    checkpoint_duration_last_ms: std.atomic.Value(u64),
    checkpoint_lsn_last: std.atomic.Value(u64),
    snapshot_corruption_fallback_total: std.atomic.Value(u64),

    fn init() RuntimeCounters {
        return .{
            .ops_put_total = std.atomic.Value(u64).init(0),
            .ops_get_total = std.atomic.Value(u64).init(0),
            .ops_delete_total = std.atomic.Value(u64).init(0),
            .ops_scan_total = std.atomic.Value(u64).init(0),
            .ops_expire_total = std.atomic.Value(u64).init(0),
            .latency_lt_1us_total = std.atomic.Value(u64).init(0),
            .latency_lt_10us_total = std.atomic.Value(u64).init(0),
            .latency_lt_100us_total = std.atomic.Value(u64).init(0),
            .latency_lt_1ms_total = std.atomic.Value(u64).init(0),
            .latency_ge_1ms_total = std.atomic.Value(u64).init(0),
            .latency_samples_total = std.atomic.Value(u64).init(0),
            .checkpoint_count_total = std.atomic.Value(u64).init(0),
            .checkpoint_duration_last_ms = std.atomic.Value(u64).init(0),
            .checkpoint_lsn_last = std.atomic.Value(u64).init(0),
            .snapshot_corruption_fallback_total = std.atomic.Value(u64).init(0),
        };
    }
};

/// Operation counters tracked at the engine boundary.
pub const OperationKind = enum {
    put,
    get,
    delete,
    scan,
    expire,
};

/// Full database runtime state, including shards, visibility coordination, and local counters.
pub const DatabaseState = struct {
    base_allocator: std.mem.Allocator,
    visibility_gate: runtime_visibility.VisibilityGate,
    wal: ?storage_wal.Wal = null,
    snapshot_path: ?[]const u8 = null,
    shards: [NUM_SHARDS]runtime_shard.Shard,
    metrics_config: types.MetricsConfig,
    counters: RuntimeCounters,
    latency_sample_clock: std.atomic.Value(u64),
    active_read_views: std.atomic.Value(usize),

    /// Initializes runtime state for one engine handle.
    ///
    /// Time Complexity: O(s), where `s` is `NUM_SHARDS`.
    ///
    /// Allocator: Does not allocate during state construction; stores `base_allocator` so shards can later allocate owned key/value data.
    ///
    /// Thread Safety: Must be called before the state is shared across threads.
    pub fn init(base_allocator: std.mem.Allocator, snapshot_path: ?[]const u8) DatabaseState {
        return init_with_metrics(base_allocator, snapshot_path, types.default_metrics_config());
    }

    /// Initializes runtime state for one engine handle with explicit metrics behavior.
    ///
    /// Time Complexity: O(s), where `s` is `NUM_SHARDS`.
    ///
    /// Allocator: Does not allocate during state construction; stores `base_allocator` so shards can later allocate owned key/value data.
    ///
    /// Thread Safety: Must be called before the state is shared across threads.
    pub fn init_with_metrics(
        base_allocator: std.mem.Allocator,
        snapshot_path: ?[]const u8,
        metrics_config: types.MetricsConfig,
    ) DatabaseState {
        var state = DatabaseState{
            .base_allocator = base_allocator,
            .visibility_gate = .{},
            .wal = null,
            .snapshot_path = snapshot_path,
            .shards = undefined,
            .metrics_config = metrics_config,
            .counters = RuntimeCounters.init(),
            .latency_sample_clock = std.atomic.Value(u64).init(0),
            .active_read_views = std.atomic.Value(usize).init(0),
        };
        for (&state.shards) |*shard| {
            shard.* = runtime_shard.Shard.init(base_allocator);
            shard.rebind_tree_allocator();
        }
        return state;
    }

    /// Rebinds every shard tree allocator to the current in-struct shard storage.
    ///
    /// Time Complexity: O(s), where `s` is `NUM_SHARDS`.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Must run before the state is shared across threads or whenever a by-value move may have invalidated shard-local allocator interfaces.
    pub fn rebind_shard_allocators(self: *DatabaseState) void {
        for (&self.shards) |*shard| shard.rebind_tree_allocator();
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

    /// Captures one point-in-time snapshot of all runtime-local metrics counters.
    ///
    /// Time Complexity: O(1), performs a bounded sequence of atomic loads.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Reads shared counter state through atomic loads with monotonic ordering.
    pub fn stats_snapshot(self: *const DatabaseState) StatsCounters {
        return .{
            .ops_put_total = self.counters.ops_put_total.load(.monotonic),
            .ops_get_total = self.counters.ops_get_total.load(.monotonic),
            .ops_delete_total = self.counters.ops_delete_total.load(.monotonic),
            .ops_scan_total = self.counters.ops_scan_total.load(.monotonic),
            .ops_expire_total = self.counters.ops_expire_total.load(.monotonic),
            .latency_lt_1us_total = self.counters.latency_lt_1us_total.load(.monotonic),
            .latency_lt_10us_total = self.counters.latency_lt_10us_total.load(.monotonic),
            .latency_lt_100us_total = self.counters.latency_lt_100us_total.load(.monotonic),
            .latency_lt_1ms_total = self.counters.latency_lt_1ms_total.load(.monotonic),
            .latency_ge_1ms_total = self.counters.latency_ge_1ms_total.load(.monotonic),
            .latency_samples_total = self.counters.latency_samples_total.load(.monotonic),
            .checkpoint_count_total = self.counters.checkpoint_count_total.load(.monotonic),
            .checkpoint_duration_last_ms = self.counters.checkpoint_duration_last_ms.load(.monotonic),
            .checkpoint_lsn_last = self.counters.checkpoint_lsn_last.load(.monotonic),
            .snapshot_corruption_fallback_total = self.counters.snapshot_corruption_fallback_total.load(.monotonic),
        };
    }

    /// Records one completed engine operation when the selected metrics mode enables counters.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Uses atomic increments only; safe to call from concurrent engine entrypoints.
    pub fn record_operation(self: *const DatabaseState, kind: OperationKind, count: u64) void {
        if (count == 0) return;
        if (self.metrics_config.mode == .disabled) return;

        switch (kind) {
            .put => _ = @constCast(&self.counters.ops_put_total).fetchAdd(count, .monotonic),
            .get => _ = @constCast(&self.counters.ops_get_total).fetchAdd(count, .monotonic),
            .delete => _ = @constCast(&self.counters.ops_delete_total).fetchAdd(count, .monotonic),
            .scan => _ = @constCast(&self.counters.ops_scan_total).fetchAdd(count, .monotonic),
            .expire => _ = @constCast(&self.counters.ops_expire_total).fetchAdd(count, .monotonic),
        }
    }

    /// Returns whether the current operation should pay latency instrumentation cost.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Uses atomic sampling state only; safe to call from concurrent engine entrypoints.
    pub fn should_record_latency(self: *const DatabaseState) bool {
        return switch (self.metrics_config.mode) {
            .disabled, .counters_only => false,
            .full => true,
            .sampled_latency => blk: {
                const sample_index = @constCast(&self.latency_sample_clock).fetchAdd(1, .monotonic);
                break :blk (sample_index & latency_sample_mask(self.metrics_config.latency_sample_shift)) == 0;
            },
        };
    }

    /// Buckets one sampled engine-boundary latency into runtime-local histogram counters.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Uses atomic increments only; safe to call from concurrent engine entrypoints.
    pub fn record_latency_sample(self: *const DatabaseState, latency_ns: u64) void {
        switch (self.metrics_config.mode) {
            .disabled, .counters_only => return,
            .sampled_latency, .full => {},
        }

        if (latency_ns < std.time.ns_per_us) {
            _ = @constCast(&self.counters.latency_lt_1us_total).fetchAdd(1, .monotonic);
        } else if (latency_ns < 10 * std.time.ns_per_us) {
            _ = @constCast(&self.counters.latency_lt_10us_total).fetchAdd(1, .monotonic);
        } else if (latency_ns < 100 * std.time.ns_per_us) {
            _ = @constCast(&self.counters.latency_lt_100us_total).fetchAdd(1, .monotonic);
        } else if (latency_ns < std.time.ns_per_ms) {
            _ = @constCast(&self.counters.latency_lt_1ms_total).fetchAdd(1, .monotonic);
        } else {
            _ = @constCast(&self.counters.latency_ge_1ms_total).fetchAdd(1, .monotonic);
        }
        _ = @constCast(&self.counters.latency_samples_total).fetchAdd(1, .monotonic);
    }

    /// Records one successful checkpoint after snapshot write and WAL compaction complete.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Uses atomic updates only; safe to call after a successful checkpoint under normal engine synchronization.
    pub fn record_successful_checkpoint(self: *const DatabaseState, duration_ns: u64, checkpoint_lsn: u64) void {
        _ = @constCast(&self.counters.checkpoint_count_total).fetchAdd(1, .monotonic);
        @constCast(&self.counters.checkpoint_duration_last_ms).store(duration_ns / std.time.ns_per_ms, .monotonic);
        @constCast(&self.counters.checkpoint_lsn_last).store(checkpoint_lsn, .monotonic);
    }

    /// Increments the counter tracking snapshot-corruption fallbacks to full WAL replay.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Uses atomic increments only; safe to call from the single-threaded open path before publication.
    pub fn record_snapshot_corruption_fallback(self: *const DatabaseState) void {
        _ = @constCast(&self.counters.snapshot_corruption_fallback_total).fetchAdd(1, .monotonic);
    }
};

fn latency_sample_mask(shift: u8) u64 {
    if (shift >= 63) return std.math.maxInt(u64);
    return (@as(u64, 1) << @as(u6, @intCast(shift))) - 1;
}

test "database state metrics start at zero" {
    const testing = std.testing;

    var state = DatabaseState.init(testing.allocator, null);
    defer state.deinit();

    const snapshot = state.stats_snapshot();
    try testing.expectEqual(@as(u64, 0), snapshot.ops_put_total);
    try testing.expectEqual(@as(u64, 0), snapshot.ops_get_total);
    try testing.expectEqual(@as(u64, 0), snapshot.ops_delete_total);
    try testing.expectEqual(@as(u64, 0), snapshot.ops_scan_total);
    try testing.expectEqual(@as(u64, 0), snapshot.ops_expire_total);
    try testing.expectEqual(@as(u64, 0), snapshot.latency_samples_total);
    try testing.expectEqual(@as(u64, 0), snapshot.checkpoint_count_total);
    try testing.expectEqual(@as(u64, 0), snapshot.checkpoint_duration_last_ms);
    try testing.expectEqual(@as(u64, 0), snapshot.checkpoint_lsn_last);
    try testing.expectEqual(@as(u64, 0), snapshot.snapshot_corruption_fallback_total);
}

test "database state metrics disabled mode skips counters and latency" {
    const testing = std.testing;

    var state = DatabaseState.init_with_metrics(testing.allocator, null, .{
        .mode = .disabled,
    });
    defer state.deinit();

    state.record_operation(.put, 4);
    state.record_latency_sample(500);

    const snapshot = state.stats_snapshot();
    try testing.expectEqual(@as(u64, 0), snapshot.ops_put_total);
    try testing.expectEqual(@as(u64, 0), snapshot.latency_samples_total);
}

test "database state metrics counters only mode updates ops without latency" {
    const testing = std.testing;

    var state = DatabaseState.init_with_metrics(testing.allocator, null, .{
        .mode = .counters_only,
    });
    defer state.deinit();

    state.record_operation(.put, 2);
    state.record_operation(.scan, 1);
    state.record_latency_sample(2 * std.time.ns_per_ms);

    const snapshot = state.stats_snapshot();
    try testing.expectEqual(@as(u64, 2), snapshot.ops_put_total);
    try testing.expectEqual(@as(u64, 1), snapshot.ops_scan_total);
    try testing.expectEqual(@as(u64, 0), snapshot.latency_samples_total);
}

test "database state sampled latency mode records only selected calls" {
    const testing = std.testing;

    var state = DatabaseState.init_with_metrics(testing.allocator, null, .{
        .mode = .sampled_latency,
        .latency_sample_shift = 2,
    });
    defer state.deinit();

    var samples_taken: u64 = 0;
    for (0..8) |_| {
        if (state.should_record_latency()) {
            samples_taken += 1;
            state.record_latency_sample(500);
        }
    }

    const snapshot = state.stats_snapshot();
    try testing.expectEqual(@as(u64, 2), samples_taken);
    try testing.expectEqual(@as(u64, 2), snapshot.latency_lt_1us_total);
    try testing.expectEqual(@as(u64, 2), snapshot.latency_samples_total);
}

test "database state full metrics snapshot reflects latency checkpoint and fallback updates" {
    const testing = std.testing;

    var state = DatabaseState.init_with_metrics(testing.allocator, null, .{
        .mode = .full,
    });
    defer state.deinit();

    state.record_latency_sample(500);
    state.record_latency_sample(5 * std.time.ns_per_us);
    state.record_latency_sample(50 * std.time.ns_per_us);
    state.record_latency_sample(500 * std.time.ns_per_us);
    state.record_latency_sample(2 * std.time.ns_per_ms);
    state.record_successful_checkpoint(12 * std.time.ns_per_ms, 33);
    state.record_snapshot_corruption_fallback();

    const snapshot = state.stats_snapshot();
    try testing.expectEqual(@as(u64, 1), snapshot.latency_lt_1us_total);
    try testing.expectEqual(@as(u64, 1), snapshot.latency_lt_10us_total);
    try testing.expectEqual(@as(u64, 1), snapshot.latency_lt_100us_total);
    try testing.expectEqual(@as(u64, 1), snapshot.latency_lt_1ms_total);
    try testing.expectEqual(@as(u64, 1), snapshot.latency_ge_1ms_total);
    try testing.expectEqual(@as(u64, 5), snapshot.latency_samples_total);
    try testing.expectEqual(@as(u64, 1), snapshot.checkpoint_count_total);
    try testing.expectEqual(@as(u64, 12), snapshot.checkpoint_duration_last_ms);
    try testing.expectEqual(@as(u64, 33), snapshot.checkpoint_lsn_last);
    try testing.expectEqual(@as(u64, 1), snapshot.snapshot_corruption_fallback_total);
}
