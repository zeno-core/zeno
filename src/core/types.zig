//! Public access point for zeno-core contract types.
//! Cost: O(1) module reexports plus declared type metadata.
//! Allocator: Does not allocate.

const builtin = @import("builtin");
const batch = @import("types/batch.zig");
const checked_batch = @import("types/checked_batch.zig");
const read_view = @import("types/read_view.zig");
const scan = @import("types/scan.zig");
const storage_wal = @import("storage/wal.zig");
const value_mod = @import("types/value.zig");

/// Durability policy for WAL fsync behavior.
pub const FsyncMode = storage_wal.FsyncMode;

/// Runtime instrumentation mode for engine hot paths.
pub const MetricsMode = enum {
    disabled,
    counters_only,
    sampled_latency,
    full,
};

/// Runtime metrics configuration carried into one engine handle.
pub const MetricsConfig = struct {
    mode: MetricsMode = default_metrics_mode(),
    latency_sample_shift: u8 = default_latency_sample_shift(),
};

/// Options for opening a database with WAL and optional snapshot support.
pub const DatabaseOptions = struct {
    /// Path to the WAL file. `null` keeps the engine in-memory only.
    wal_path: ?[]const u8 = null,
    /// Path to the snapshot file. `null` disables snapshot load and checkpoint writes.
    snapshot_path: ?[]const u8 = null,
    fsync_mode: FsyncMode = .always,
    fsync_interval_ms: u32 = 2,
    metrics: MetricsConfig = default_metrics_config(),
    heavy_overwrite_compact_every: ?u32 = null,
    /// Triggers an automatic checkpoint when the WAL grows beyond this many bytes since
    /// the last `truncate_up_to_lsn`. `null` keeps checkpoint scheduling fully manual.
    ///
    /// Requires `snapshot_path` to be configured; if `snapshot_path` is absent, the
    /// threshold is never armed and WAL growth remains unbounded regardless of this value.
    ///
    /// Operational guidance: a value around 64 MiB (67_108_864) works well for most
    /// workloads. Lower values cause more frequent checkpoints; values below ~1 MiB may
    /// impose noticeable checkpoint overhead on write-heavy workloads.
    max_wal_bytes: ?u64 = null,
    /// Interval in milliseconds between background TTL sweep passes.
    /// When set, a background thread wakes every `ttl_sweep_interval_ms` milliseconds
    /// and removes expired entries from the TTL index and ART of each shard.
    /// `null` leaves TTL cleanup fully lazy (only happens when expired keys are accessed).
    ///
    /// Operational guidance: 1_000 ms (1 second) is a safe starting point. Lower values
    /// reclaim memory faster at the cost of more frequent shard locking.
    ttl_sweep_interval_ms: ?u32 = null,

    // Maintenance note:
    // Heavy-overwrite reclaim is currently caller-managed through explicit
    // `Database.compact_shard(shard_idx)` and `Database.compact_all()` calls.
    //
    // Optional automatic cadence:
    // `heavy_overwrite_compact_every: ?u32` where:
    // - `null` keeps maintenance fully manual
    // - positive values trigger one maintenance cycle every N heavy overwrites
    // - `0` is treated as manual (`null`) for safety
};

/// Public engine value model.
pub const Value = value_mod.Value;

/// Public request type for plain-key batch writes.
pub const PutWrite = batch.PutWrite;

/// Public guarded batch request for official advanced atomic writes.
pub const CheckedBatch = checked_batch.CheckedBatch;

/// Public physical guard type for checked-batch execution.
pub const CheckedBatchGuard = checked_batch.CheckedBatchGuard;

/// Prefix or range bounds for ordered key scans.
pub const KeyRange = scan.KeyRange;

/// One owned key/value pair yielded by scan operations.
pub const ScanEntry = scan.ScanEntry;

/// Owned result container for full scan responses.
pub const ScanResult = scan.ScanResult;

/// Borrowed continuation view for paginated scans.
pub const ScanCursor = scan.ScanCursor;

/// Owned continuation state retained independently of any page result.
pub const OwnedScanCursor = scan.OwnedScanCursor;

/// Owned result container for one paginated scan page.
pub const ScanPageResult = scan.ScanPageResult;

/// Consistent read window handle for official advanced reads.
pub const ReadView = read_view.ReadView;

fn default_metrics_mode() MetricsMode {
    return if (builtin.mode == .ReleaseFast) .disabled else .sampled_latency;
}

fn default_latency_sample_shift() u8 {
    return if (builtin.mode == .ReleaseFast) 0 else 10;
}

/// Returns the default runtime metrics configuration for the current build mode.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn default_metrics_config() MetricsConfig {
    return .{
        .mode = default_metrics_mode(),
        .latency_sample_shift = default_latency_sample_shift(),
    };
}

test "default metrics config uses sampled latency outside ReleaseFast" {
    const std = @import("std");
    const testing = std.testing;

    const config = default_metrics_config();
    const expected: MetricsMode = if (builtin.mode == .ReleaseFast) .disabled else .sampled_latency;
    const expected_shift: u8 = if (builtin.mode == .ReleaseFast) 0 else 10;
    try testing.expectEqual(expected, config.mode);
    try testing.expectEqual(expected_shift, config.latency_sample_shift);
}
