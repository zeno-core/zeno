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

const CollectedEntry = struct {
    shard_idx: u8,
    entry: types.ScanEntry,
};

fn entry_less_than(_: void, left: CollectedEntry, right: CollectedEntry) bool {
    return std.mem.lessThan(u8, left.entry.key, right.entry.key);
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
    shard_idx: u8,
    now: i64,
    shard: *const runtime_shard.Shard,
};

fn collect_visit(ctx_ptr: *anyopaque, key: []const u8, value: *const types.Value) error_mod.EngineError!void {
    const ctx: *CollectCtx = @ptrCast(@alignCast(ctx_ptr));
    if (!key_matches_query(key, ctx.query)) return;
    if (!expiration.key_is_visible_unlocked(ctx.shard, key, ctx.now)) return;
    try ctx.entries.append(ctx.allocator, .{
        .shard_idx = ctx.shard_idx,
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

    for (&state.shards, 0..) |*const_shard, shard_idx| {
        const shard = @constCast(const_shard);
        shard.lock.lockShared();
        errdefer shard.lock.unlockShared();
        var ctx = CollectCtx{
            .allocator = allocator,
            .entries = &entries,
            .query = query,
            .shard_idx = @intCast(shard_idx),
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
        ._cursor_slot = .invalid,
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
        page._cursor_slot = @enumFromInt(@intFromEnum(try types.OwnedScanCursor.init(
            allocator,
            last_emitted.shard_idx,
            last_emitted.entry.key,
        )));
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
    _ = @constCast(&state.counters.ops_scan_total).fetchAdd(1, .monotonic);

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
    _ = @constCast(&state.counters.ops_scan_total).fetchAdd(1, .monotonic);

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
/// Time Complexity: O(s + m log m + p + v), where `s` is shard count, `m` is matched entry count, `p` is pagination resume work over the collected sorted entries, and `v` is total cloned value size in the returned page.
///
/// Allocator: Allocates owned scan-entry keys, values, and any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page owns its entries and may own one continuation cursor.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes one shard-shared lock at a time while collecting ART-backed entries.
pub fn scan_prefix_from_in_view(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) error_mod.EngineError!types.ScanPageResult {
    const state = try runtime_state_from_view(view);
    const opened_at_unix_seconds = read_view_mod.resolve_opened_at_unix_seconds(view) orelse return error.InvalidReadView;
    _ = @constCast(&state.counters.ops_scan_total).fetchAdd(1, .monotonic);

    var entries = try collect_entries_no_visibility(state, allocator, .{ .prefix = prefix }, opened_at_unix_seconds);
    return collect_page_from_entries(allocator, &entries, cursor, limit);
}

/// Scans one range page inside a consistent read view.
///
/// Time Complexity: O(s + m log m + p + v), where `s` is shard count, `m` is matched entry count, `p` is pagination resume work over the collected sorted entries, and `v` is total cloned value size in the returned page.
///
/// Allocator: Allocates owned scan-entry keys, values, and any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page owns its entries and may own one continuation cursor.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes one shard-shared lock at a time while collecting ART-backed entries.
pub fn scan_range_from_in_view(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    range: types.KeyRange,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) error_mod.EngineError!types.ScanPageResult {
    const state = try runtime_state_from_view(view);
    const opened_at_unix_seconds = read_view_mod.resolve_opened_at_unix_seconds(view) orelse return error.InvalidReadView;
    _ = @constCast(&state.counters.ops_scan_total).fetchAdd(1, .monotonic);

    var entries = try collect_entries_no_visibility(state, allocator, .{ .range = range }, opened_at_unix_seconds);
    return collect_page_from_entries(allocator, &entries, cursor, limit);
}
