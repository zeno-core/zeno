//! Scan-semantics ownership boundary for prefix and range reads.
//! Cost: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
//! Allocator: Uses explicit allocators for owned scan entries, sorting buffers, and continuation cursors.

const std = @import("std");
const expiration = @import("expiration.zig");
const error_mod = @import("error.zig");
const read_view_mod = @import("../types/read_view.zig");
const runtime_shard = @import("../runtime/shard.zig");
const runtime_state = @import("../runtime/state.zig");
const types = @import("../types.zig");

const ScanQuery = union(enum) {
    prefix: []const u8,
    range: types.KeyRange,
};

/// Orders scan entries lexicographically by key bytes.
///
/// Time Complexity: O(min(a, b)), where `a` and `b` are the compared key lengths.
///
/// Allocator: Does not allocate.
fn entry_less_than(_: void, left: types.ScanEntry, right: types.ScanEntry) bool {
    return std.mem.lessThan(u8, left.key, right.key);
}

/// Releases one owned scan entry produced by this module.
///
/// Time Complexity: O(k + v), where `k` is `entry.key.len` and `v` is total teardown work for the stored value.
///
/// Allocator: Does not allocate; frees the owned key and value through `allocator`.
///
/// Ownership: Releases only entry storage previously cloned by this module.
fn free_entry(allocator: std.mem.Allocator, entry: types.ScanEntry) void {
    allocator.free(entry.key);
    const owned_value: *types.Value = @constCast(entry.value);
    owned_value.deinit(allocator);
    allocator.destroy(owned_value);
}

/// Releases a slice of owned scan entries produced by this module.
///
/// Time Complexity: O(n + b), where `n` is `entries.len` and `b` is total teardown work for all stored values.
///
/// Allocator: Does not allocate; frees each owned key and value through `allocator`.
///
/// Ownership: Releases only entry storage previously cloned by this module.
fn free_entries(allocator: std.mem.Allocator, entries: []const types.ScanEntry) void {
    for (entries) |entry| free_entry(allocator, entry);
}

/// Clones one shard-owned key/value pair into scan-result-owned storage.
///
/// Time Complexity: O(k + v), where `k` is `key.len` and `v` is total clone work for `value`.
///
/// Allocator: Allocates one owned key buffer and one owned value object through `allocator`.
///
/// Ownership: Returns an entry whose key and value are owned by the eventual scan result container.
fn clone_entry(allocator: std.mem.Allocator, key: []const u8, value: *const types.Value) !types.ScanEntry {
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

/// Returns whether `key` satisfies one prefix or range scan query.
///
/// Time Complexity: O(k + q), where `k` is `key.len` and `q` is the compared bound or prefix length.
///
/// Allocator: Does not allocate.
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

/// Collects and globally sorts all visible entries matching one scan query.
///
/// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys, values, and result storage through `allocator`.
///
/// Ownership: Returns an array list that owns all collected entries and must later be torn down by the caller.
///
/// Thread Safety: Requires a surrounding visibility window and acquires each shard's shared lock while cloning entries.
fn collect_entries_no_visibility(
    state: *const runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    query: ScanQuery,
    now: i64,
) !std.ArrayList(types.ScanEntry) {
    var entries = std.ArrayList(types.ScanEntry).empty;
    errdefer {
        for (entries.items) |entry| free_entry(allocator, entry);
        entries.deinit(allocator);
    }

    for (&state.shards) |*shard| {
        shard.lock.lockShared();
        defer shard.lock.unlockShared();

        var iterator = shard.values.iterator();
        while (iterator.next()) |entry| {
            if (!key_matches_query(entry.key_ptr.*, query)) continue;
            if (!expiration.key_is_visible_unlocked(shard, entry.key_ptr.*, now)) continue;
            try entries.append(allocator, try clone_entry(allocator, entry.key_ptr.*, entry.value_ptr));
        }
    }

    std.mem.sort(types.ScanEntry, entries.items, {}, entry_less_than);
    return entries;
}

/// Builds one paginated page result from a globally sorted owned entry set.
///
/// Time Complexity: O(n + k), where `n` is `all_entries.items.len` and `k` is the continuation resume key length when a next cursor is produced.
///
/// Allocator: Reuses ownership from `all_entries`, allocates page entry storage, and may allocate one owned continuation cursor through `allocator`.
///
/// Ownership: Transfers selected entries and any continuation cursor into the returned page, and releases all non-page entries before returning.
fn collect_page_from_entries(
    allocator: std.mem.Allocator,
    all_entries: *std.ArrayList(types.ScanEntry),
    cursor: ?*const types.ScanCursor,
    limit: usize,
) error_mod.EngineError!types.ScanPageResult {
    var page = types.ScanPageResult{
        .entries = std.ArrayList(types.ScanEntry).empty,
        .allocator = allocator,
        ._cursor_slot = .invalid,
    };
    errdefer page.deinit();
    errdefer {
        free_entries(allocator, all_entries.items);
        all_entries.deinit(allocator);
    }

    if (limit == 0) {
        free_entries(allocator, all_entries.items);
        all_entries.deinit(allocator);
        return page;
    }

    const start_index: usize = blk: {
        if (cursor) |resume_cursor| {
            var index: usize = 0;
            while (index < all_entries.items.len) : (index += 1) {
                if (std.mem.lessThan(u8, resume_cursor.resume_key, all_entries.items[index].key)) break;
            }
            break :blk index;
        }
        break :blk 0;
    };

    const end_index = @min(start_index + limit, all_entries.items.len);
    try page.entries.ensureTotalCapacity(allocator, end_index - start_index);

    if (end_index < all_entries.items.len) {
        page._cursor_slot = @enumFromInt(@intFromEnum(try types.OwnedScanCursor.init(allocator, 0, all_entries.items[end_index - 1].key)));
    }

    for (all_entries.items, 0..) |entry, index| {
        if (index >= start_index and index < end_index) {
            page.entries.appendAssumeCapacity(entry);
            continue;
        }
        free_entry(allocator, entry);
    }
    all_entries.deinit(allocator);
    return page;
}

/// Resolves the runtime state borrowed by one active read view.
///
/// Time Complexity: O(1) expected.
///
/// Allocator: Does not allocate.
///
/// Ownership: Returns a borrowed runtime-state pointer only while the read-view token remains active.
fn runtime_state_from_view(view: *const types.ReadView) error_mod.EngineError!*const runtime_state.DatabaseState {
    const opaque_state = view.resolve_runtime_state() orelse return error.InvalidReadView;
    return @ptrCast(@alignCast(opaque_state));
}

/// Performs a full prefix scan over the current visible state.
///
/// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus result storage through `allocator`.
///
/// Ownership: Returns a result that owns all returned keys and values until `deinit`.
///
/// Thread Safety: Acquires the shared side of the global visibility gate before taking shard shared locks to collect entries.
pub fn scan_prefix(
    state: *const runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    prefix: []const u8,
) error_mod.EngineError!types.ScanResult {
    const visibility_gate = @constCast(&state.visibility_gate);
    visibility_gate.lock_shared();
    defer visibility_gate.unlock_shared();
    const now = runtime_shard.unix_now();
    _ = state.counters.ops_scan_total.fetchAdd(1, .monotonic);

    const entries = try collect_entries_no_visibility(state, allocator, .{ .prefix = prefix }, now);
    return .{
        .entries = entries,
        .allocator = allocator,
    };
}

/// Performs a full range scan over the current visible state.
///
/// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus result storage through `allocator`.
///
/// Ownership: Returns a result that owns all returned keys and values until `deinit`.
///
/// Thread Safety: Acquires the shared side of the global visibility gate before taking shard shared locks to collect entries.
pub fn scan_range(
    state: *const runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    range: types.KeyRange,
) error_mod.EngineError!types.ScanResult {
    const visibility_gate = @constCast(&state.visibility_gate);
    visibility_gate.lock_shared();
    defer visibility_gate.unlock_shared();
    const now = runtime_shard.unix_now();
    _ = state.counters.ops_scan_total.fetchAdd(1, .monotonic);

    const entries = try collect_entries_no_visibility(state, allocator, .{ .range = range }, now);
    return .{
        .entries = entries,
        .allocator = allocator,
    };
}

/// Scans the next prefix page inside one consistent read view.
///
/// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus any continuation cursor through `allocator`.
///
/// Ownership: Returns a page that owns all returned keys and values until `deinit`, and exposes any continuation cursor through `borrow_next_cursor` until `take_next_cursor` or `deinit`.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while collecting entries.
pub fn scan_prefix_from_in_view(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) error_mod.EngineError!types.ScanPageResult {
    const state = try runtime_state_from_view(view);
    const opened_at_unix_seconds = read_view_mod.resolve_opened_at_unix_seconds(view) orelse return error.InvalidReadView;
    _ = state.counters.ops_scan_total.fetchAdd(1, .monotonic);

    var entries = try collect_entries_no_visibility(state, allocator, .{ .prefix = prefix }, opened_at_unix_seconds);
    return collect_page_from_entries(allocator, &entries, cursor, limit);
}

/// Scans the next range page inside one consistent read view.
///
/// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus any continuation cursor through `allocator`.
///
/// Ownership: Returns a page that owns all returned keys and values until `deinit`, and exposes any continuation cursor through `borrow_next_cursor` until `take_next_cursor` or `deinit`.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while collecting entries.
pub fn scan_range_from_in_view(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    range: types.KeyRange,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) error_mod.EngineError!types.ScanPageResult {
    const state = try runtime_state_from_view(view);
    const opened_at_unix_seconds = read_view_mod.resolve_opened_at_unix_seconds(view) orelse return error.InvalidReadView;
    _ = state.counters.ops_scan_total.fetchAdd(1, .monotonic);

    var entries = try collect_entries_no_visibility(state, allocator, .{ .range = range }, opened_at_unix_seconds);
    return collect_page_from_entries(allocator, &entries, cursor, limit);
}
