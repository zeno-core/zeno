//! Thin official facade for advanced in-repo engine access.
//! Cost: O(1) facade delegation plus downstream engine work.
//! Allocator: Delegates allocation behavior to engine entry points.

const std = @import("std");
const engine_db = @import("engine/db.zig");
const types = @import("types.zig");

/// Official advanced database handle.
pub const Database = engine_db.Database;

/// Consistent read window handle.
pub const ReadView = types.ReadView;

/// Guarded atomic write request.
pub const CheckedBatch = types.CheckedBatch;

/// Physical guard evaluated before a checked batch is applied.
pub const CheckedBatchGuard = types.CheckedBatchGuard;

/// Public error set used by the step 3 official facade skeleton.
pub const Error = engine_db.EngineError;

/// Scans the next prefix page inside a consistent read view.
///
/// Time Complexity: O(1) in the finalized skeleton.
///
/// Allocator: Does not allocate in the finalized skeleton; returns `error.NotImplemented`.
pub fn scan_prefix_from_in_view(
    view: *const ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    cursor: ?types.ScanCursor,
    limit: usize,
) Error!types.ScanPageResult {
    return engine_db.scan_prefix_from_in_view(view, allocator, prefix, cursor, limit);
}

/// Scans the next range page inside a consistent read view.
///
/// Time Complexity: O(1) in the finalized skeleton.
///
/// Allocator: Does not allocate in the finalized skeleton; returns `error.NotImplemented`.
pub fn scan_range_from_in_view(
    view: *const ReadView,
    allocator: std.mem.Allocator,
    range: types.KeyRange,
    cursor: ?types.ScanCursor,
    limit: usize,
) Error!types.ScanPageResult {
    return engine_db.scan_range_from_in_view(view, allocator, range, cursor, limit);
}

/// Applies one checked batch under the official advanced contract.
///
/// Time Complexity: O(1) in the finalized skeleton.
///
/// Allocator: Does not allocate in the finalized skeleton; returns `error.NotImplemented`.
pub fn apply_checked_batch(db: *Database, batch: CheckedBatch) Error!void {
    return engine_db.apply_checked_batch(db, batch);
}
