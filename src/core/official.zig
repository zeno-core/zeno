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

/// Public error set used by the official engine contract.
pub const Error = engine_db.EngineError;

/// Scans the next prefix page inside a consistent read view.
///
/// Time Complexity: O(1) until in-view scan behavior is implemented.
///
/// Allocator: Does not allocate; returns `error.NotImplemented` until in-view scan behavior is implemented.
///
/// Ownership: `cursor` is borrowed when present and is never consumed by this call.
pub fn scan_prefix_from_in_view(
    view: *const ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) Error!types.ScanPageResult {
    return engine_db.scan_prefix_from_in_view(view, allocator, prefix, cursor, limit);
}

/// Scans the next range page inside a consistent read view.
///
/// Time Complexity: O(1) until in-view scan behavior is implemented.
///
/// Allocator: Does not allocate; returns `error.NotImplemented` until in-view scan behavior is implemented.
///
/// Ownership: `cursor` is borrowed when present and is never consumed by this call.
pub fn scan_range_from_in_view(
    view: *const ReadView,
    allocator: std.mem.Allocator,
    range: types.KeyRange,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) Error!types.ScanPageResult {
    return engine_db.scan_range_from_in_view(view, allocator, range, cursor, limit);
}

/// Applies one checked batch under the official advanced contract.
///
/// Time Complexity: O(1) until checked-batch execution is implemented.
///
/// Allocator: Does not allocate; returns `error.NotImplemented` until checked-batch execution is implemented.
pub fn apply_checked_batch(db: *Database, batch: CheckedBatch) Error!void {
    return engine_db.apply_checked_batch(db, batch);
}
