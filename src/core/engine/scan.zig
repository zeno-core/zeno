//! Scan-semantics ownership boundary for prefix and range reads.
//! Cost: Full scans still materialize O(s + m log m + v), while in-view paginated scans use a merged incremental path bounded by shard fan-out and emitted page size.
//! Allocator: Uses explicit allocators for owned scan entries, merge scratch, sorting buffers, and continuation cursors.

const std = @import("std");
const expiration = @import("expiration.zig");
const error_mod = @import("error.zig");
const art = @import("../index/art/tree.zig");
const read_view_mod = @import("../types/read_view.zig");
const runtime_shard = @import("../runtime/shard.zig");
const runtime_state = @import("../runtime/state.zig");
const types = @import("../types.zig");

const ScanQuery = union(enum) {
    prefix: []const u8,
    range: types.KeyRange,
};

const CollectedEntry = struct {
    entry: types.ScanEntry,
};

const ShardHead = struct {
    shard_idx: u8,
    entry: types.ScanEntry,
};

fn entry_less_than(_: void, left: CollectedEntry, right: CollectedEntry) bool {
    return std.mem.lessThan(u8, left.entry.key, right.entry.key);
}

fn shard_head_order(_: void, left: ShardHead, right: ShardHead) std.math.Order {
    return std.mem.order(u8, left.entry.key, right.entry.key);
}

fn free_entry(allocator: std.mem.Allocator, entry: types.ScanEntry) void {
    allocator.free(entry.key);
    const owned_value: *types.Value = @constCast(entry.value);
    owned_value.deinit(allocator);
    allocator.destroy(owned_value);
}

fn free_collected_entries(allocator: std.mem.Allocator, entries: []const CollectedEntry) void {
    for (entries) |entry| free_entry(allocator, entry.entry);
}

fn clone_entry(
    allocator: std.mem.Allocator,
    key: []const u8,
    value: *const types.Value,
) error_mod.EngineError!types.ScanEntry {
    const owned_key = try allocator.dupe(u8, key);
    errdefer allocator.free(owned_key);

    const owned_value = try allocator.create(types.Value);
    errdefer allocator.destroy(owned_value);

    owned_value.* = try value.clone(allocator);
    return .{
        .key = owned_key,
        .value = owned_value,
    };
}

fn key_matches_query(key: []const u8, query: ScanQuery) bool {
    return switch (query) {
        .prefix => |prefix| std.mem.startsWith(u8, key, prefix),
        .range => |range| blk: {
            if (range.start) |start| {
                if (std.mem.lessThan(u8, key, start)) break :blk false;
            }
            if (range.end) |end| {
                if (!std.mem.lessThan(u8, key, end)) break :blk false;
            }
            break :blk true;
        },
    };
}

const CollectCtx = struct {
    allocator: std.mem.Allocator,
    entries: *std.ArrayList(CollectedEntry),
    query: ScanQuery,
    now: i64,
    shard: *const runtime_shard.Shard,
};

fn collect_visit(ctx_ptr: *anyopaque, key: []const u8, value: *const types.Value) error_mod.EngineError!void {
    const ctx: *CollectCtx = @ptrCast(@alignCast(ctx_ptr));
    if (!key_matches_query(key, ctx.query)) return;
    if (!expiration.key_is_visible_unlocked(ctx.shard, key, ctx.now)) return;
    try ctx.entries.append(ctx.allocator, .{
        .entry = try clone_entry(ctx.allocator, key, value),
    });
}

fn collect_entries_no_visibility(
    state: *const runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    query: ScanQuery,
    now: i64,
) error_mod.EngineError!std.ArrayList(CollectedEntry) {
    var entries = std.ArrayList(CollectedEntry).empty;
    errdefer {
        free_collected_entries(allocator, entries.items);
        entries.deinit(allocator);
    }

    for (&state.shards) |*const_shard| {
        const shard = @constCast(const_shard);
        shard.lock.lockShared();
        errdefer shard.lock.unlockShared();
        var ctx = CollectCtx{
            .allocator = allocator,
            .entries = &entries,
            .query = query,
            .now = now,
            .shard = shard,
        };
        _ = shard.tree.for_each(&ctx, collect_visit) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => unreachable,
        };
        shard.lock.unlockShared();
    }

    std.mem.sort(CollectedEntry, entries.items, {}, entry_less_than);
    return entries;
}

fn collect_page_from_entries(
    allocator: std.mem.Allocator,
    all_entries: *std.ArrayList(CollectedEntry),
    cursor: ?*const types.ScanCursor,
    limit: usize,
) error_mod.EngineError!types.ScanPageResult {
    var page = types.ScanPageResult{
        .entries = std.ArrayList(types.ScanEntry).empty,
        .allocator = allocator,
        ._next_cursor = null,
    };
    errdefer page.deinit();
    errdefer {
        free_collected_entries(allocator, all_entries.items);
        all_entries.deinit(allocator);
    }

    if (limit == 0) {
        free_collected_entries(allocator, all_entries.items);
        all_entries.deinit(allocator);
        return page;
    }

    const start_index: usize = blk: {
        if (cursor) |resume_cursor| {
            var index: usize = 0;
            while (index < all_entries.items.len) : (index += 1) {
                if (std.mem.lessThan(u8, resume_cursor.resume_key, all_entries.items[index].entry.key)) break;
            }
            break :blk index;
        }
        break :blk 0;
    };

    const end_index = @min(start_index + limit, all_entries.items.len);
    try page.entries.ensureTotalCapacity(allocator, end_index - start_index);

    if (end_index < all_entries.items.len) {
        const last_emitted = all_entries.items[end_index - 1];
        page._next_cursor = try types.OwnedScanCursor.init(allocator, last_emitted.entry.key);
    }

    for (all_entries.items, 0..) |collected, index| {
        if (index >= start_index and index < end_index) {
            page.entries.appendAssumeCapacity(collected.entry);
            continue;
        }
        free_entry(allocator, collected.entry);
    }
    all_entries.deinit(allocator);
    return page;
}

fn runtime_state_from_view(view: *const types.ReadView) error_mod.EngineError!*const runtime_state.DatabaseState {
    const opaque_state = view.resolve_runtime_state() orelse return error.InvalidReadView;
    return @ptrCast(@alignCast(opaque_state));
}

fn range_is_empty(range: types.KeyRange) bool {
    const start = range.start orelse return false;
    const end = range.end orelse return false;
    return !std.mem.lessThan(u8, start, end);
}

const RangeVisitCtx = struct {
    entry: ?art.ScanEntry = null,

    fn visit(ctx_ptr: *anyopaque, key: []const u8, value: *const types.Value) error_mod.EngineError!void {
        const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
        ctx.entry = .{
            .key = key,
            .value = value,
        };
    }
};

fn fetch_one_prefix_entry(
    allocator: std.mem.Allocator,
    scratch: *std.ArrayList(art.ScanEntry),
    shard: *const runtime_shard.Shard,
    prefix: []const u8,
    start_after_key: ?[]const u8,
) error_mod.EngineError!?art.ScanEntry {
    scratch.clearRetainingCapacity();
    _ = shard.tree.scan_from(prefix, start_after_key, allocator, scratch, 1) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => unreachable,
    };
    if (scratch.items.len == 0) return null;
    return scratch.items[0];
}

fn fetch_one_range_entry(
    shard: *const runtime_shard.Shard,
    range: types.KeyRange,
    start_after_key: ?[]const u8,
) error_mod.EngineError!?art.ScanEntry {
    if (range_is_empty(range)) return null;

    var visit_ctx = RangeVisitCtx{};
    _ = shard.tree.scan_range_visit_from(.{
        .start = range.start,
        .end = range.end,
    }, start_after_key, &visit_ctx, RangeVisitCtx.visit, 1) catch |err| switch (err) {
        error.InvalidRangeBounds => return null,
        else => unreachable,
    };
    return visit_ctx.entry;
}

fn fetch_next_visible_head(
    allocator: std.mem.Allocator,
    prefix_scratch: *std.ArrayList(art.ScanEntry),
    shard: *const runtime_shard.Shard,
    shard_idx: u8,
    query: ScanQuery,
    start_after_key: ?[]const u8,
    now: i64,
) error_mod.EngineError!?ShardHead {
    const mutable_shard = @constCast(shard);
    var resume_after = start_after_key;

    mutable_shard.lock.lockShared();
    defer mutable_shard.lock.unlockShared();

    while (true) {
        const borrowed_entry = switch (query) {
            .prefix => |prefix| try fetch_one_prefix_entry(allocator, prefix_scratch, shard, prefix, resume_after),
            .range => |range| try fetch_one_range_entry(shard, range, resume_after),
        } orelse return null;

        if (!expiration.key_is_visible_unlocked(shard, borrowed_entry.key, now)) {
            resume_after = borrowed_entry.key;
            continue;
        }

        return .{
            .shard_idx = shard_idx,
            .entry = try clone_entry(allocator, borrowed_entry.key, borrowed_entry.value),
        };
    }
}

fn add_head_or_free(
    allocator: std.mem.Allocator,
    heap: *std.PriorityQueue(ShardHead, void, shard_head_order),
    head: ShardHead,
) error_mod.EngineError!void {
    heap.add(head) catch |err| {
        free_entry(allocator, head.entry);
        switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
        }
    };
}

fn merge_page_from_shards(
    state: *const runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    query: ScanQuery,
    cursor: ?*const types.ScanCursor,
    limit: usize,
    now: i64,
) error_mod.EngineError!types.ScanPageResult {
    var page = types.ScanPageResult{
        .entries = std.ArrayList(types.ScanEntry).empty,
        .allocator = allocator,
        ._next_cursor = null,
    };
    errdefer page.deinit();

    if (limit == 0) return page;
    if (query == .range and range_is_empty(query.range)) return page;

    try page.entries.ensureTotalCapacity(allocator, limit);

    var heap = std.PriorityQueue(ShardHead, void, shard_head_order).init(allocator, {});
    defer {
        while (heap.removeOrNull()) |head| free_entry(allocator, head.entry);
        heap.deinit();
    }

    var prefix_scratch = std.ArrayList(art.ScanEntry).empty;
    defer prefix_scratch.deinit(allocator);

    const resume_after_key = if (cursor) |resume_cursor| resume_cursor.resume_key else null;
    for (&state.shards, 0..) |*shard, shard_idx| {
        const maybe_head = try fetch_next_visible_head(
            allocator,
            &prefix_scratch,
            shard,
            @intCast(shard_idx),
            query,
            resume_after_key,
            now,
        );
        if (maybe_head) |head| try add_head_or_free(allocator, &heap, head);
    }

    var last_emitted_key: ?[]const u8 = null;

    while (page.entries.items.len < limit) {
        const head = heap.removeOrNull() orelse break;
        page.entries.appendAssumeCapacity(head.entry);
        last_emitted_key = head.entry.key;

        const maybe_refill = try fetch_next_visible_head(
            allocator,
            &prefix_scratch,
            &state.shards[head.shard_idx],
            head.shard_idx,
            query,
            head.entry.key,
            now,
        );
        if (maybe_refill) |refill| try add_head_or_free(allocator, &heap, refill);
    }

    if (last_emitted_key) |resume_key| {
        if (heap.count() != 0) {
            page._next_cursor = try types.OwnedScanCursor.init(allocator, resume_key);
        }
    }

    return page;
}

/// Scans all currently visible keys with the requested prefix.
///
/// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
///
/// Allocator: Allocates owned scan-entry keys and values through `allocator`.
///
/// Ownership: Returns a caller-owned `ScanResult`. The caller must later call `deinit` with the same allocator.
///
/// Thread Safety: Acquires the shared side of the global visibility gate, then takes one shard-shared lock at a time while collecting visible ART entries.
pub fn scan_prefix(
    state: *const runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    prefix: []const u8,
) error_mod.EngineError!types.ScanResult {
    const visibility_gate = @constCast(&state.visibility_gate);
    visibility_gate.lock_shared();
    defer visibility_gate.unlock_shared();
    const now = runtime_shard.unix_now();
    state.record_operation(.scan, 1);

    var collected = try collect_entries_no_visibility(state, allocator, .{ .prefix = prefix }, now);
    return .{
        .entries = blk: {
            var entries = std.ArrayList(types.ScanEntry).empty;
            errdefer entries.deinit(allocator);
            try entries.ensureTotalCapacity(allocator, collected.items.len);
            for (collected.items) |entry| entries.appendAssumeCapacity(entry.entry);
            collected.deinit(allocator);
            break :blk entries;
        },
        .allocator = allocator,
    };
}

/// Scans all currently visible keys inside one inclusive-start, exclusive-end range.
///
/// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
///
/// Allocator: Allocates owned scan-entry keys and values through `allocator`.
///
/// Ownership: Returns a caller-owned `ScanResult`. The caller must later call `deinit` with the same allocator.
///
/// Thread Safety: Acquires the shared side of the global visibility gate, then takes one shard-shared lock at a time while collecting visible ART entries.
pub fn scan_range(
    state: *const runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    range: types.KeyRange,
) error_mod.EngineError!types.ScanResult {
    const visibility_gate = @constCast(&state.visibility_gate);
    visibility_gate.lock_shared();
    defer visibility_gate.unlock_shared();
    const now = runtime_shard.unix_now();
    state.record_operation(.scan, 1);

    var collected = try collect_entries_no_visibility(state, allocator, .{ .range = range }, now);
    return .{
        .entries = blk: {
            var entries = std.ArrayList(types.ScanEntry).empty;
            errdefer entries.deinit(allocator);
            try entries.ensureTotalCapacity(allocator, collected.items.len);
            for (collected.items) |entry| entries.appendAssumeCapacity(entry.entry);
            collected.deinit(allocator);
            break :blk entries;
        },
        .allocator = allocator,
    };
}

/// Scans one prefix page inside a consistent read view.
///
/// Time Complexity: O(s log s + p * (k + log s + v)), where `s` is shard count, `p` is emitted page size, `k` is ART seek work for one shard refill, and `v` is total cloned value size in the returned page.
///
/// Allocator: Allocates owned scan-entry keys, values, and any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page owns its entries and may own one continuation cursor.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes one shard-shared lock at a time while fetching or refilling shard-local ART heads.
pub fn scan_prefix_from_in_view(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) error_mod.EngineError!types.ScanPageResult {
    const state = try runtime_state_from_view(view);
    const opened_at_unix_seconds = read_view_mod.resolve_opened_at_unix_seconds(view) orelse return error.InvalidReadView;
    state.record_operation(.scan, 1);

    return merge_page_from_shards(state, allocator, .{ .prefix = prefix }, cursor, limit, opened_at_unix_seconds);
}

/// Scans one range page inside a consistent read view.
///
/// Time Complexity: O(s log s + p * (k + log s + v)), where `s` is shard count, `p` is emitted page size, `k` is ART seek work for one shard refill, and `v` is total cloned value size in the returned page.
///
/// Allocator: Allocates owned scan-entry keys, values, and any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page owns its entries and may own one continuation cursor.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes one shard-shared lock at a time while fetching or refilling shard-local ART heads.
pub fn scan_range_from_in_view(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    range: types.KeyRange,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) error_mod.EngineError!types.ScanPageResult {
    const state = try runtime_state_from_view(view);
    const opened_at_unix_seconds = read_view_mod.resolve_opened_at_unix_seconds(view) orelse return error.InvalidReadView;
    state.record_operation(.scan, 1);

    return merge_page_from_shards(state, allocator, .{ .range = range }, cursor, limit, opened_at_unix_seconds);
}
