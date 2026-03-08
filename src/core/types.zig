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
