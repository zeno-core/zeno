//! Batch-semantics ownership boundary for atomic plain and guarded writes.
//! Cost: O(g + s + n * (k + v)), where `g` is touched shard-group count, `s` is shadow-planning work, `n` is survivor write count, `k` is average key length, and `v` is value clone cost.
//! Allocator: Uses explicit allocators for planning scratch, committed delta arenas, and temporary WAL batch views.

const std = @import("std");
const builtin = @import("builtin");
const art = @import("../index/art/tree.zig");
const art_node = @import("../index/art/node.zig");
const durability = @import("durability.zig");
const expiration = @import("expiration.zig");
const error_mod = @import("error.zig");
const internal_batch_plan = @import("../internal/batch_plan.zig");
const internal_codec = @import("../internal/codec.zig");
const internal_mutate = @import("../internal/mutate.zig");
const internal_ttl_index = @import("../internal/ttl_index.zig");
const runtime_shard = @import("../runtime/shard.zig");
const runtime_state = @import("../runtime/state.zig");
const types = @import("../types.zig");

const PUT_BATCH_APPLY_STACK_BYTES = 32 * 1024;

var pause_after_visibility_gate = std.atomic.Value(bool).init(false);
var paused_at_visibility_gate = std.atomic.Value(bool).init(false);
var resume_after_visibility_gate = std.atomic.Value(bool).init(false);

const ReservedBatchWrite = struct {
    write: internal_batch_plan.PlannedWrite,
    target: union(enum) {
        overwrite_leaf: struct {
            leaf: *art_node.Leaf,
            value: *types.Value,
        },
        prepared: struct {
            prepared: art.PreparedInsert,
            reserved: art.ReservedInsert,
        },
    },
};

const ReservedShard = struct {
    committed: ?*runtime_shard.CommittedArena,
    writes: []ReservedBatchWrite,

    fn deinit(self: *ReservedShard, allocator: std.mem.Allocator) void {
        if (self.committed == null) {
            for (self.writes) |reserved_write| {
                switch (reserved_write.target) {
                    .overwrite_leaf => |overwrite| internal_mutate.free_heap_value(allocator, overwrite.value),
                    .prepared => {},
                }
            }
        }
        if (self.committed) |committed| {
            committed.arena.deinit();
            allocator.destroy(committed);
            self.committed = null;
        }
        self.* = undefined;
    }
};

fn translate_plan_error(err: anyerror) error_mod.EngineError {
    return switch (err) {
        error.EmptyKey, error.KeyTooLarge => error.KeyTooLarge,
        error.ValueTooLarge => error.ValueTooLarge,
        error.MaxDepthExceeded => error.ValueTooDeep,
        error.OutOfMemory => error.OutOfMemory,
        else => unreachable,
    };
}

fn validate_guard(
    allocator: std.mem.Allocator,
    scratch: *std.ArrayList(u8),
    guard: types.CheckedBatchGuard,
) internal_batch_plan.BatchPlanError!void {
    switch (guard) {
        .key_exists => |key| try internal_mutate.validate_key(key),
        .key_not_exists => |key| try internal_mutate.validate_key(key),
        .key_value_equals => |guarded| {
            try internal_mutate.validate_key(guarded.key);
            scratch.clearRetainingCapacity();
            try internal_codec.serialize_value(allocator, guarded.value, scratch, 0);
            if (scratch.items.len > internal_codec.MAX_VAL_LEN) return error.ValueTooLarge;
        },
    }
}

fn validate_guards(
    allocator: std.mem.Allocator,
    guards: []const types.CheckedBatchGuard,
) internal_batch_plan.BatchPlanError!void {
    var scratch = std.ArrayList(u8).empty;
    defer scratch.deinit(allocator);

    for (guards) |guard| {
        try validate_guard(allocator, &scratch, guard);
    }
}

fn guard_holds(state: *runtime_state.DatabaseState, guard: types.CheckedBatchGuard, now: i64) bool {
    return switch (guard) {
        .key_exists => |key| blk: {
            const shard = &state.shards[runtime_shard.get_shard_index(key)];
            if (!internal_mutate.key_exists_unlocked(shard, key)) break :blk false;
            break :blk expiration.key_is_visible_unlocked(shard, key, now);
        },
        .key_not_exists => |key| blk: {
            const shard = &state.shards[runtime_shard.get_shard_index(key)];
            if (!internal_mutate.key_exists_unlocked(shard, key)) break :blk true;
            break :blk !expiration.key_is_visible_unlocked(shard, key, now);
        },
        .key_value_equals => |guarded| blk: {
            const shard = &state.shards[runtime_shard.get_shard_index(guarded.key)];
            if (!internal_mutate.key_exists_unlocked(shard, guarded.key)) break :blk false;
            if (!expiration.key_is_visible_unlocked(shard, guarded.key, now)) break :blk false;
            break :blk internal_mutate.stored_value_equals_unlocked(shard, guarded.key, guarded.value);
        },
    };
}

fn reserve_internal_node(allocator: std.mem.Allocator, node_type: art_node.NodeType) !art_node.ReservedInternalNode {
    return switch (node_type) {
        .node4 => .{ .node4 = try allocator.create(art_node.Node4) },
        .node16 => .{ .node16 = try allocator.create(art_node.Node16) },
        .node48 => .{ .node48 = try allocator.create(art_node.Node48) },
        .node256 => .{ .node256 = try allocator.create(art_node.Node256) },
    };
}

fn reserve_write(
    allocator: std.mem.Allocator,
    write: internal_batch_plan.PlannedWrite,
    prepared: art.PreparedInsert,
) !art.ReservedInsert {
    const cloned_value = try allocator.create(types.Value);
    cloned_value.* = try write.value.clone(allocator);

    var reserved: art.ReservedInsert = .{
        .value = cloned_value,
        .value_owner = .committed_arena,
    };

    if (prepared.reservation.needs_stored_key) {
        reserved.stored_key = try allocator.dupe(u8, write.key);
    }
    if (prepared.reservation.needs_leaf) {
        const leaf = try allocator.create(art_node.Leaf);
        leaf.* = .{
            .key = reserved.stored_key.?,
            .value = cloned_value,
            .value_owner = .committed_arena,
        };
        reserved.leaf = leaf;
    }
    if (prepared.reservation.split_node_type) |node_type| {
        reserved.split_node = try reserve_internal_node(allocator, node_type);
    }
    if (prepared.reservation.promoted_node_type) |node_type| {
        reserved.promoted_node = try reserve_internal_node(allocator, node_type);
    }

    return reserved;
}

fn clone_value_to_heap(allocator: std.mem.Allocator, value: *const types.Value) !*types.Value {
    const cloned_value = try allocator.create(types.Value);
    errdefer allocator.destroy(cloned_value);
    cloned_value.* = try value.clone(allocator);
    return cloned_value;
}

fn try_reserve_overwrite_group(
    base_allocator: std.mem.Allocator,
    shard: *runtime_shard.Shard,
    group: internal_batch_plan.ShardWriteGroup,
    scratch_allocator: std.mem.Allocator,
    writes: []ReservedBatchWrite,
) !bool {
    std.debug.assert(writes.len == group.writes.len);
    const leaves = try scratch_allocator.alloc(*art_node.Leaf, group.writes.len);
    for (group.writes, 0..) |write, index| {
        leaves[index] = shard.tree.find_leaf_for_exact_key(write.key) orelse return false;
    }
    for (group.writes, 0..) |write, index| {
        const cloned_value = try clone_value_to_heap(base_allocator, write.value);
        writes[index] = .{
            .write = write,
            .target = .{
                .overwrite_leaf = .{
                    .leaf = leaves[index],
                    .value = cloned_value,
                },
            },
        };
    }
    return true;
}

fn apply_reserved_write_or_panic(shard: *runtime_shard.Shard, reserved_write: *const ReservedBatchWrite) void {
    switch (reserved_write.target) {
        .overwrite_leaf => |overwrite| {
            const previous_value = overwrite.leaf.value;
            const previous_owner = overwrite.leaf.value_owner;
            overwrite.leaf.value = overwrite.value;
            overwrite.leaf.value_owner = .heap_allocation;
            if (previous_owner == .heap_allocation) {
                internal_mutate.free_heap_value(shard.base_allocator, previous_value);
            }
        },
        .prepared => |prepared_write| {
            const old_heap_value: ?*types.Value = switch (prepared_write.prepared.kind) {
                .overwrite_leaf, .overwrite_leaf_value => blk: {
                    const live_leaf = shard.tree.find_leaf_for_exact_key(reserved_write.write.key) orelse break :blk null;
                    break :blk if (live_leaf.value_owner == .heap_allocation) live_leaf.value else null;
                },
                else => null,
            };
            shard.tree.apply_prepared_insert(&prepared_write.prepared, &prepared_write.reserved) catch |err| {
                std.debug.panic("apply_batch prepared apply invariant failed: {s}", .{@errorName(err)});
            };
            switch (prepared_write.prepared.kind) {
                .overwrite_leaf, .overwrite_leaf_value => {
                    const live_leaf = shard.tree.find_leaf_for_exact_key(reserved_write.write.key) orelse {
                        std.debug.panic("apply_batch prepared overwrite lost target leaf", .{});
                    };
                    live_leaf.value_owner = prepared_write.reserved.value_owner;
                    if (old_heap_value) |value| {
                        internal_mutate.free_heap_value(shard.base_allocator, value);
                    }
                },
                else => {},
            }
        },
    }
}

fn append_plan_to_wal(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    plan: *const internal_batch_plan.BatchPlan,
) error_mod.EngineError!void {
    if (plan.writes.len == 0) return;

    const writes = try allocator.alloc(durability.PutBatchWrite, plan.writes.len);
    defer allocator.free(writes);

    var write_index: usize = 0;
    for (plan.groups) |group| {
        for (group.writes) |write| {
            writes[write_index] = .{
                .key = write.key,
                .value = write.value,
            };
            write_index += 1;
        }
    }

    try durability.append_put_batch_if_enabled(state, allocator, writes);
}

fn maybe_pause_after_visibility_gate() void {
    if (!builtin.is_test) return;
    if (!pause_after_visibility_gate.swap(false, .acq_rel)) return;

    paused_at_visibility_gate.store(true, .release);
    defer {
        paused_at_visibility_gate.store(false, .release);
        resume_after_visibility_gate.store(false, .release);
    }

    while (!resume_after_visibility_gate.load(.acquire)) {
        std.Thread.sleep(100 * std.time.ns_per_us);
    }
}

fn apply_plan(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    plan: *const internal_batch_plan.BatchPlan,
    guards: []const types.CheckedBatchGuard,
) error_mod.EngineError!void {
    state.visibility_gate.lock_exclusive();
    defer state.visibility_gate.unlock_exclusive();
    maybe_pause_after_visibility_gate();

    const now = runtime_shard.unix_now();
    for (guards) |guard| {
        if (!guard_holds(state, guard, now)) return error.GuardFailed;
    }
    if (plan.writes.len == 0) return;

    var acquired: usize = 0;
    var applied_count: u64 = 0;
    errdefer state.record_operation(.put, applied_count);
    defer {
        while (acquired > 0) {
            acquired -= 1;
            state.shards[plan.groups[acquired].shard_idx].lock.unlock();
        }
    }

    for (plan.groups) |group| {
        state.shards[group.shard_idx].lock.lock();
        acquired += 1;
    }

    var scratch_arena = std.heap.ArenaAllocator.init(allocator);
    defer scratch_arena.deinit();
    var scratch_fallback = std.heap.stackFallback(PUT_BATCH_APPLY_STACK_BYTES, scratch_arena.allocator());
    const scratch_allocator = scratch_fallback.get();
    const reservations = try scratch_allocator.alloc(ReservedShard, plan.groups.len);

    var initialized: usize = 0;
    errdefer {
        for (reservations[0..initialized]) |*reservation| reservation.deinit(state.base_allocator);
    }

    for (plan.groups, 0..) |group, index| {
        reservations[index] = .{
            .committed = null,
            .writes = try scratch_allocator.alloc(ReservedBatchWrite, group.writes.len),
        };
        initialized += 1;

        const shard = &state.shards[group.shard_idx];

        if (try try_reserve_overwrite_group(state.base_allocator, shard, group, scratch_allocator, reservations[index].writes)) {
            continue;
        }

        const committed = try state.base_allocator.create(runtime_shard.CommittedArena);
        errdefer state.base_allocator.destroy(committed);
        committed.* = .{
            .arena = std.heap.ArenaAllocator.init(state.base_allocator),
            .next = null,
        };
        errdefer committed.arena.deinit();
        reservations[index].committed = committed;
        const reservation_allocator = committed.arena.allocator();

        var shadow = try shard.tree.build_shadow_tree(scratch_allocator);
        var last_input_index: ?usize = null;
        for (group.writes, 0..) |write, write_index| {
            if (last_input_index) |prev_index| {
                std.debug.assert(prev_index < write.input_index);
            }
            const prepared = try shard.tree.plan_prepared_insert(&shadow, scratch_allocator, write.key);
            const reserved = try reserve_write(reservation_allocator, write, prepared);
            reservations[index].writes[write_index] = .{
                .write = write,
                .target = .{
                    .prepared = .{
                        .prepared = prepared,
                        .reserved = reserved,
                    },
                },
            };
            last_input_index = write.input_index;
        }
    }

    try append_plan_to_wal(state, scratch_allocator, plan);

    for (plan.groups, 0..) |group, index| {
        const shard = &state.shards[group.shard_idx];
        for (reservations[index].writes) |*reserved_write| {
            apply_reserved_write_or_panic(shard, reserved_write);
            internal_ttl_index.clear_ttl_entry(shard, reserved_write.write.key);
        }
        if (reservations[index].committed) |committed| {
            shard.append_committed_arena(committed);
            reservations[index].committed = null;
        }
        applied_count += group.writes.len;
    }

    state.record_operation(.put, applied_count);
}

/// Applies one plain atomic batch.
///
/// Time Complexity: O(s + n * (k + v)), where `s` is shard grouping and shadow-planning work, `n` is survivor write count, `k` is average key length, and `v` is value clone cost.
///
/// Allocator: Uses `allocator` for planner scratch, committed delta arenas, and temporary WAL batch views.
pub fn apply_batch(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    writes: []const types.PutWrite,
) error_mod.EngineError!void {
    var plan = internal_batch_plan.plan_put_batch(allocator, writes) catch |err| return translate_plan_error(err);
    defer plan.deinit();
    return apply_plan(state, allocator, &plan, &.{});
}

/// Applies one checked batch after verifying all guards against the pre-batch state.
///
/// Time Complexity: O(g + s + n * (k + v)), where `g` is guard count, `s` is shard grouping and shadow-planning work, `n` is survivor write count, `k` is average key length, and `v` is value clone cost.
///
/// Allocator: Uses `allocator` for validation scratch, planner scratch, committed delta arenas, and temporary WAL batch views.
pub fn apply_checked_batch(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    batch: types.CheckedBatch,
) error_mod.EngineError!void {
    validate_guards(allocator, batch.guards) catch |err| return translate_plan_error(err);
    var plan = internal_batch_plan.plan_put_batch(allocator, batch.writes) catch |err| return translate_plan_error(err);
    defer plan.deinit();
    return apply_plan(state, allocator, &plan, batch.guards);
}

pub const test_hooks = if (builtin.is_test) struct {
    /// Arms a one-shot pause after the next batch apply acquires the global visibility gate.
    ///
    /// Time Complexity: O(1).
    pub fn pause_next_batch_after_visibility_gate() void {
        paused_at_visibility_gate.store(false, .release);
        resume_after_visibility_gate.store(false, .release);
        pause_after_visibility_gate.store(true, .release);
    }

    /// Returns whether a batch apply is currently paused behind the visibility gate hook.
    ///
    /// Time Complexity: O(1).
    pub fn is_batch_paused_after_visibility_gate() bool {
        return paused_at_visibility_gate.load(.acquire);
    }

    /// Releases a paused batch apply visibility-gate hook.
    ///
    /// Time Complexity: O(1).
    pub fn resume_batch_after_visibility_gate() void {
        resume_after_visibility_gate.store(true, .release);
    }
} else struct {};
