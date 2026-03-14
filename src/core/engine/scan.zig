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

/// Internal handle for one shard's current scan position.
const ShardHead = struct {
    shard_idx: u8,
    entry: types.ScanEntry,
};

/// Internal handle for one shard's borrowed ART entry.
const BorrowedShardHead = struct {
    shard_idx: u8,
    entry: art.ScanEntry,
};

/// Fixed-capacity min-heap for merging shard heads.
const FixedBorrowedShardHeap = struct {
    items: [runtime_shard.NUM_SHARDS]BorrowedShardHead = undefined,
    len: usize = 0,

    fn add(self: *@This(), head: BorrowedShardHead) void {
        std.debug.assert(self.len < runtime_shard.NUM_SHARDS);
        self.items[self.len] = head;
        self.sift_up(self.len);
        self.len += 1;
    }

    fn remove_min(self: *@This()) ?BorrowedShardHead {
        if (self.len == 0) return null;
        const min = self.items[0];
        self.len -= 1;
        if (self.len > 0) {
            self.items[0] = self.items[self.len];
            self.sift_down(0);
        }
        return min;
    }

    fn sift_up(self: *@This(), start: usize) void {
        var idx = start;
        while (idx > 0) {
            const parent = (idx - 1) / 2;
            if (!std.mem.lessThan(u8, self.items[idx].entry.key, self.items[parent].entry.key)) break;
            const tmp = self.items[idx];
            self.items[idx] = self.items[parent];
            self.items[parent] = tmp;
            idx = parent;
        }
    }

    fn sift_down(self: *@This(), start: usize) void {
        var idx = start;
        while (true) {
            const left = 2 * idx + 1;
            if (left >= self.len) break;
            var min_child = left;
            const right = left + 1;
            if (right < self.len and std.mem.lessThan(u8, self.items[right].entry.key, self.items[left].entry.key)) {
                min_child = right;
            }
            if (!std.mem.lessThan(u8, self.items[min_child].entry.key, self.items[idx].entry.key)) break;
            const tmp = self.items[idx];
            self.items[idx] = self.items[min_child];
            self.items[min_child] = tmp;
            idx = min_child;
        }
    }
};

/// Orders shard heads by key for the priority queue.
///
/// Time Complexity: O(k), where k is the common prefix length of the keys.
fn shard_head_order(_: void, left: ShardHead, right: ShardHead) std.math.Order {
    return std.mem.order(u8, left.entry.key, right.entry.key);
}

/// Frees one engine-owned scan entry.
///
/// Time Complexity: O(1) plus deallocation cost.
///
/// Allocator: Frees key and value storage through `allocator`.
fn free_entry(allocator: std.mem.Allocator, entry: types.ScanEntry) void {
    allocator.free(entry.key);
    const owned_value: *types.Value = @constCast(entry.value);
    owned_value.deinit(allocator);
    allocator.destroy(owned_value);
}

/// Clones one borrowed entry into owned storage.
///
/// Time Complexity: O(k + v), where k is key length and v is value size.
///
/// Allocator: Allocates new key and value storage through `allocator`.
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

/// Returns true if the range start is not less than the range end.
///
/// Time Complexity: O(k).
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

/// Fetches at most one visible entry with the prefix from one shard.
///
/// Time Complexity: O(k), where k is key length.
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

/// Fetches at most one visible entry inside the range from one shard.
///
/// Time Complexity: O(k), where k is key length.
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

/// Fetches the next head from one shard that is visible at the requested time.
///
/// Time Complexity: O(v * k), where v is the number of version skips and k is key length.
///
/// Allocator: Clones the matched entry into owned storage through `allocator`.
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

/// Transfers ownership of a shard head to the priority queue or frees it on failure.
///
/// Time Complexity: O(log s), where s is shard count.
fn add_head_or_free(
    allocator: std.mem.Allocator,
    heap: *std.PriorityQueue(ShardHead, void, shard_head_order),
    head: ShardHead,
) error_mod.EngineError!void {
    heap.add(head) catch |err| {
        free_entry(allocator, head.entry);
        return err;
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

fn collect_entries_paginated(
    state: *const runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    query: ScanQuery,
    now: i64,
) error_mod.EngineError!std.ArrayList(types.ScanEntry) {
    var entries = std.ArrayList(types.ScanEntry).empty;
    errdefer {
        for (entries.items) |entry| free_entry(allocator, entry);
        entries.deinit(allocator);
    }

    var cursor: ?types.OwnedScanCursor = null;
    defer if (cursor) |*c| c.deinit();

    const PAGE_LIMIT = 4096;

    while (true) {
        var borrow_val = if (cursor) |c| c.as_cursor() else null;
        const borrow_cursor = if (borrow_val) |*c| c else null;
        var page = try merge_page_from_shards(state, allocator, query, borrow_cursor, PAGE_LIMIT, now);
        defer page.deinit();

        try entries.ensureUnusedCapacity(allocator, page.entries.items.len);
        for (page.entries.items) |entry| {
            entries.appendAssumeCapacity(try clone_entry(allocator, entry.key, entry.value));
        }

        if (cursor) |*c| c.deinit();
        cursor = page.take_next_cursor();

        if (page.entries.items.len < PAGE_LIMIT or cursor == null) break;
    }

    return entries;
}

/// Advances the iterator until a visible entry is found or the end of the prefix is reached.
///
/// Time Complexity: O(v * k), where v is version skips and k is key length.
fn next_visible_from_iterator(
    it: *art.Iterator,
    shard: *const runtime_shard.Shard,
    now: i64,
) error_mod.EngineError!?art.ScanEntry {
    while (true) {
        const entry = (try it.next()) orelse return null;
        if (!expiration.key_is_visible_unlocked(shard, entry.key, now)) continue;
        return entry;
    }
}

const PrefixShardIteratorMergeState = struct {
    heap: FixedBorrowedShardHeap,
    iterators: [runtime_shard.NUM_SHARDS]art.Iterator,
    now: i64,
    state: *const runtime_state.DatabaseState,

    fn init(
        allocator: std.mem.Allocator,
        state: *const runtime_state.DatabaseState,
        prefix: []const u8,
        now: i64,
    ) error_mod.EngineError!@This() {
        var self = @This(){
            .heap = .{},
            .iterators = undefined,
            .now = now,
            .state = state,
        };
        errdefer self.deinit(allocator);

        for (&state.shards) |*shard| {
            @constCast(shard).lock.lockShared();
        }

        for (&state.shards, 0..) |*shard, shard_idx| {
            self.iterators[shard_idx] = try art.Iterator.init(allocator, &shard.tree, prefix, null);
            if (try next_visible_from_iterator(&self.iterators[shard_idx], shard, now)) |entry| {
                self.heap.add(.{ .shard_idx = @intCast(shard_idx), .entry = entry });
            }
        }

        return self;
    }

    fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        for (&self.iterators) |*it| {
            it.deinit();
        }
        for (&self.state.shards) |*shard| {
            @constCast(shard).lock.unlockShared();
        }
        self.* = undefined;
    }

    /// Materializes the next owned entry from the merged iterator stream.
    ///
    /// Time Complexity: O(k + log s), where k is cloned entry size and s is shard count.
    ///
    /// Allocator: Allocates the returned owned entry through `allocator`.
    fn next_owned_entry(
        self: *@This(),
        allocator: std.mem.Allocator,
    ) error_mod.EngineError!?types.ScanEntry {
        if (self.heap.len == 0) return null;
        const head = self.heap.items[0];
        const owned_entry = try clone_entry(allocator, head.entry.key, head.entry.value);

        const shard_idx = head.shard_idx;
        if (try next_visible_from_iterator(&self.iterators[shard_idx], &self.state.shards[shard_idx], self.now)) |next_entry| {
            self.heap.items[0] = .{ .shard_idx = shard_idx, .entry = next_entry };
            self.heap.sift_down(0);
        } else {
            _ = self.heap.remove_min();
        }

        return owned_entry;
    }
};

/// Materializes one full prefix scan by merging multiple shard-local iterators.
///
/// Time Complexity: O(s + m log s + v), where s is shard count, m is matched entries, and v is total cloned size.
///
/// Allocator: Allocates merge state and result entries through `allocator`.
fn materialize_prefix_from_shard_iterator(
    state: *const runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    now: i64,
) error_mod.EngineError!types.ScanResult {
    var result = types.ScanResult{
        .entries = std.ArrayList(types.ScanEntry).empty,
        .allocator = allocator,
    };
    errdefer result.deinit();

    var merge_state = try PrefixShardIteratorMergeState.init(
        allocator,
        state,
        prefix,
        now,
    );
    defer merge_state.deinit(allocator);

    while (try merge_state.next_owned_entry(allocator)) |entry| {
        try result.entries.append(allocator, entry);
    }

    return result;
}

/// Resolves the database runtime state from a generic ReadView.
///
/// Time Complexity: O(1).
fn runtime_state_from_view(view: *const types.ReadView) error_mod.EngineError!*const runtime_state.DatabaseState {
    const opaque_state = view.resolve_runtime_state() orelse return error.InvalidReadView;
    return @ptrCast(@alignCast(opaque_state));
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
    state.lock_all_shards_shared();
    
    defer state.unlock_all_shards_shared();
    const now = runtime_shard.unix_now();
    state.record_operation(.scan, 1);

    return materialize_prefix_from_shard_iterator(state, allocator, prefix, now);
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
    state.lock_all_shards_shared();
    
    defer state.unlock_all_shards_shared();
    const now = runtime_shard.unix_now();
    state.record_operation(.scan, 1);

    const collected = try collect_entries_paginated(state, allocator, .{ .range = range }, now);
    return .{
        .entries = collected,
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
