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

/// Owned continuation cursor retained independently of any page result.
pub const OwnedScanCursor = types.OwnedScanCursor;

/// Guarded atomic write request.
pub const CheckedBatch = types.CheckedBatch;

/// Physical guard evaluated before a checked batch is applied.
pub const CheckedBatchGuard = types.CheckedBatchGuard;

/// Public error set used by the official engine contract.
pub const Error = engine_db.EngineError;
pub const MergePageProfileStats = engine_db.MergePageProfileStats;
pub const ProfiledScanPageResult = engine_db.ProfiledScanPageResult;
pub const ProfiledScanResult = engine_db.ProfiledScanResult;
pub const ProfiledPagedScanResult = engine_db.ProfiledPagedScanResult;

/// Scans the next prefix page inside a consistent read view.
///
/// Time Complexity: O(s log s + p * (k + log s + v)), where `s` is shard count, `p` is emitted page size, `k` is ART seek work for one shard refill, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page exposes any continuation cursor through `borrow_next_cursor` and may transfer it into `OwnedScanCursor` through `take_next_cursor`.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while fetching or refilling shard-local ART heads.
pub fn scan_prefix_from_in_view(
    view: *const ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) Error!types.ScanPageResult {
    return engine_db.scan_prefix_from_in_view(view, allocator, prefix, cursor, limit);
}

/// Scans the next prefix page inside a consistent read view while reporting merged-executor refill counters.
///
/// Time Complexity: O(s log s + p * (k + log s + v)), where `s` is shard count, `p` is emitted page size, `k` is ART seek work for one shard refill, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page owns its entries, may own one continuation cursor, and carries caller-owned profiling counters by value.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while fetching or refilling shard-local ART heads.
pub fn scan_prefix_from_in_view_profiled(
    view: *const ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) Error!ProfiledScanPageResult {
    return engine_db.scan_prefix_from_in_view_profiled(view, allocator, prefix, cursor, limit);
}

/// Materializes one full prefix scan inside a consistent read view while reporting chunked merged-executor counters.
///
/// Time Complexity: O(s log s + r * (k + log s + v)), where `s` is shard count, `r` is emitted result size, `k` is ART seek work for one chunk refill, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus result storage and bounded per-shard chunk scratch through `allocator`.
///
/// Ownership: Returns a caller-owned `ScanResult` plus profiling counters by value. The caller must later call `deinit` on the returned `ScanResult`.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while seeding or refilling shard-local ART chunks.
pub fn scan_prefix_materialized_from_in_view_profiled(
    view: *const ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    shard_chunk_size: usize,
) Error!ProfiledScanResult {
    return engine_db.scan_prefix_materialized_from_in_view_profiled(view, allocator, prefix, shard_chunk_size);
}

/// Materializes one full prefix scan inside a consistent read view by consuming a persistent merged shard-buffer state page-by-page.
///
/// Time Complexity: O(s log s + r * (k + log s + v)), where `s` is shard count, `r` is emitted result size, `k` is ART seek work for one chunk refill, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus result storage, per-page cursor storage, and bounded per-shard chunk scratch through `allocator`.
///
/// Ownership: Returns a caller-owned `ScanResult` plus profiling counters by value. The caller must later call `deinit` on the returned `ScanResult`.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while seeding or refilling shard-local ART chunks.
pub fn scan_prefix_materialized_from_in_view_paged_profiled(
    view: *const ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    page_limit: usize,
    shard_chunk_size: usize,
) Error!ProfiledPagedScanResult {
    return engine_db.scan_prefix_materialized_from_in_view_paged_profiled(view, allocator, prefix, page_limit, shard_chunk_size);
}

/// Scans the next range page inside a consistent read view.
///
/// Time Complexity: O(s log s + p * (k + log s + v)), where `s` is shard count, `p` is emitted page size, `k` is ART seek work for one shard refill, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page exposes any continuation cursor through `borrow_next_cursor` and may transfer it into `OwnedScanCursor` through `take_next_cursor`.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while fetching or refilling shard-local ART heads.
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
/// Time Complexity: O(g + n + b + v), where `g` is `batch.guards.len`, `n` is surviving write count, `b` is total serialized value bytes measured during planning, and `v` is total cloned value size for prepared writes.
///
/// Allocator: Uses the engine base allocator for committed values and temporary planner scratch while validating guards and preparing the batch.
///
/// Ownership: Clones all surviving write values into engine-owned storage before making the batch visible.
pub fn apply_checked_batch(db: *Database, batch: CheckedBatch) Error!void {
    return engine_db.apply_checked_batch(db, batch);
}
