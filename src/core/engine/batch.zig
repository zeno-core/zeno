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
                    .overwrite_leaf => |overwrite| internal_mutate.freeHeapValue(allocator, overwrite.value),
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

fn translatePlanError(err: anyerror) error_mod.EngineError {
    return switch (err) {
        error.EmptyKey, error.KeyTooLarge => error.KeyTooLarge,
        error.ValueTooLarge => error.ValueTooLarge,
        error.MaxDepthExceeded => error.ValueTooDeep,
        error.OutOfMemory => error.OutOfMemory,
        else => unreachable,
    };
}

fn validateGuard(
    allocator: std.mem.Allocator,
    scratch: *std.ArrayList(u8),
    guard: types.CheckedBatchGuard,
) internal_batch_plan.BatchPlanError!void {
    switch (guard) {
        .key_exists => |key| try internal_mutate.validateKey(key),
        .key_not_exists => |key| try internal_mutate.validateKey(key),
        .key_value_equals => |guarded| {
            try internal_mutate.validateKey(guarded.key);
            scratch.clearRetainingCapacity();
            try internal_codec.serializeValue(allocator, guarded.value, scratch, 0);
            if (scratch.items.len > internal_codec.MAX_VAL_LEN) return error.ValueTooLarge;
        },
    }
}

fn validateGuards(
    allocator: std.mem.Allocator,
    guards: []const types.CheckedBatchGuard,
) internal_batch_plan.BatchPlanError!void {
    var scratch = std.ArrayList(u8).empty;
    defer scratch.deinit(allocator);

    for (guards) |guard| {
        try validateGuard(allocator, &scratch, guard);
    }
}

fn guardHolds(state: *runtime_state.DatabaseState, guard: types.CheckedBatchGuard, now: i64) bool {
    return switch (guard) {
        .key_exists => |key| blk: {
            const shard = &state.shards[runtime_shard.getShardIndex(key)];
            if (!internal_mutate.keyExistsUnlocked(shard, key)) break :blk false;
            break :blk expiration.keyIsVisibleUnlocked(shard, key, now);
        },
        .key_not_exists => |key| blk: {
            const shard = &state.shards[runtime_shard.getShardIndex(key)];
            if (!internal_mutate.keyExistsUnlocked(shard, key)) break :blk true;
            break :blk !expiration.keyIsVisibleUnlocked(shard, key, now);
        },
        .key_value_equals => |guarded| blk: {
            const shard = &state.shards[runtime_shard.getShardIndex(guarded.key)];
            if (!internal_mutate.keyExistsUnlocked(shard, guarded.key)) break :blk false;
            if (!expiration.keyIsVisibleUnlocked(shard, guarded.key, now)) break :blk false;
            break :blk internal_mutate.storedValueEqualsUnlocked(shard, guarded.key, guarded.value);
        },
    };
}

fn reserveInternalNode(allocator: std.mem.Allocator, nodeType: art_node.NodeType) !art_node.ReservedInternalNode {
    return switch (nodeType) {
        .node4 => .{ .node4 = try allocator.create(art_node.Node4) },
        .node16 => .{ .node16 = try allocator.create(art_node.Node16) },
        .node48 => .{ .node48 = try allocator.create(art_node.Node48) },
        .node256 => .{ .node256 = try allocator.create(art_node.Node256) },
    };
}

fn reserveWrite(
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
    if (prepared.reservation.split_node_type) |nodeType| {
        reserved.split_node = try reserveInternalNode(allocator, nodeType);
    }
    if (prepared.reservation.promoted_node_type) |nodeType| {
        reserved.promoted_node = try reserveInternalNode(allocator, nodeType);
    }

    return reserved;
}

fn cloneValueToHeap(allocator: std.mem.Allocator, value: *const types.Value) !*types.Value {
    const cloned_value = try allocator.create(types.Value);
    errdefer allocator.destroy(cloned_value);
    cloned_value.* = try value.clone(allocator);
    return cloned_value;
}

fn tryReserveOverwriteGroup(
    base_allocator: std.mem.Allocator,
    shard: *runtime_shard.Shard,
    group: internal_batch_plan.ShardWriteGroup,
    scratch_allocator: std.mem.Allocator,
    writes: []ReservedBatchWrite,
) !bool {
    std.debug.assert(writes.len == group.writes.len);
    const leaves = try scratch_allocator.alloc(*art_node.Leaf, group.writes.len);
    for (group.writes, 0..) |write, index| {
        leaves[index] = shard.tree.findLeafForExactKey(write.key) orelse return false;
    }
    for (group.writes, 0..) |write, index| {
        const cloned_value = try cloneValueToHeap(base_allocator, write.value);
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

fn applyReservedWriteOrPanic(shard: *runtime_shard.Shard, reserved_write: *const ReservedBatchWrite) void {
    switch (reserved_write.target) {
        .overwrite_leaf => |overwrite| {
            const previous_value = overwrite.leaf.value;
            const previous_owner = overwrite.leaf.value_owner;
            overwrite.leaf.value = overwrite.value;
            overwrite.leaf.value_owner = .heap_allocation;
            if (previous_owner == .heap_allocation) {
                internal_mutate.freeHeapValue(shard.base_allocator, previous_value);
            }
        },
        .prepared => |prepared_write| {
            const old_heap_value: ?*types.Value = switch (prepared_write.prepared.kind) {
                .overwrite_leaf, .overwrite_leaf_value => blk: {
                    const live_leaf = shard.tree.findLeafForExactKey(reserved_write.write.key) orelse break :blk null;
                    break :blk if (live_leaf.value_owner == .heap_allocation) live_leaf.value else null;
                },
                else => null,
            };
            shard.tree.applyPreparedInsert(&prepared_write.prepared, &prepared_write.reserved) catch |err| {
                std.debug.panic("apply_batch prepared apply invariant failed: {s}", .{@errorName(err)});
            };
            switch (prepared_write.prepared.kind) {
                .overwrite_leaf, .overwrite_leaf_value => {
                    const live_leaf = shard.tree.findLeafForExactKey(reserved_write.write.key) orelse {
                        std.debug.panic("apply_batch prepared overwrite lost target leaf", .{});
                    };
                    live_leaf.value_owner = prepared_write.reserved.value_owner;
                    if (old_heap_value) |value| {
                        internal_mutate.freeHeapValue(shard.base_allocator, value);
                    }
                },
                else => {},
            }
        },
    }
}

fn appendPlanToWal(
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

    try durability.appendPutBatchIfEnabled(state, allocator, writes);
}

fn maybePauseAfterVisibilityGate(io: std.Io) void {
    if (!builtin.is_test) return;
    if (!pause_after_visibility_gate.swap(false, .acq_rel)) return;

    paused_at_visibility_gate.store(true, .release);
    defer {
        paused_at_visibility_gate.store(false, .release);
        resume_after_visibility_gate.store(false, .release);
    }

    while (!resume_after_visibility_gate.load(.acquire)) {
        io.sleep(.fromMicroseconds(100), .awake) catch unreachable;
    }
}

fn applyPlan(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    plan: *const internal_batch_plan.BatchPlan,
    guards: []const types.CheckedBatchGuard,
) error_mod.EngineError!void {
    state.lockAllShardsExclusive();
    defer state.unlockAllShardsExclusive();
    maybePauseAfterVisibilityGate(state.io);

    const now = runtime_shard.unixNow(state.io);
    for (guards) |guard| {
        if (!guardHolds(state, guard, now)) return error.GuardFailed;
    }
    if (plan.writes.len == 0) return;

    var acquired: usize = 0;
    var applied_count: u64 = 0;
    errdefer state.recordOperation(.put, applied_count);
    defer {
        while (acquired > 0) {
            acquired -= 1;
            state.shards[plan.groups[acquired].shard_idx].lock.unlock(state.io);
        }
    }

    for (plan.groups) |group| {
        state.shards[group.shard_idx].lock.lockUncancelable(state.io);
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

        if (try tryReserveOverwriteGroup(state.base_allocator, shard, group, scratch_allocator, reservations[index].writes)) {
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

        var shadow = try shard.tree.buildShadowTree(scratch_allocator);
        var last_input_index: ?usize = null;
        for (group.writes, 0..) |write, write_index| {
            if (last_input_index) |prev_index| {
                std.debug.assert(prev_index < write.input_index);
            }
            const prepared = try shard.tree.planPreparedInsert(&shadow, scratch_allocator, write.key);
            const reserved = try reserveWrite(reservation_allocator, write, prepared);
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

    try appendPlanToWal(state, scratch_allocator, plan);

    for (plan.groups, 0..) |group, index| {
        const shard = &state.shards[group.shard_idx];
        const seq0 = shard.seq.load(.monotonic);
        shard.seq.store(seq0 + 1, .release);
        for (reservations[index].writes) |*reserved_write| {
            applyReservedWriteOrPanic(shard, reserved_write);
            internal_ttl_index.clearTtlEntry(shard, reserved_write.write.key);
        }
        shard.seq.store(seq0 + 2, .release);
        if (reservations[index].committed) |committed| {
            shard.appendCommittedArena(committed);
            reservations[index].committed = null;
        }
        applied_count += group.writes.len;
    }

    state.recordOperation(.put, applied_count);
}

/// Applies one plain atomic batch.
///
/// Time Complexity: O(s + n * (k + v)), where `s` is shard grouping and shadow-planning work, `n` is survivor write count, `k` is average key length, and `v` is value clone cost.
///
/// Allocator: Uses `allocator` for planner scratch, committed delta arenas, and temporary WAL batch views.
pub fn applyBatch(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    writes: []const types.PutWrite,
) error_mod.EngineError!void {
    var plan = internal_batch_plan.planPutBatch(allocator, writes) catch |err| return translatePlanError(err);
    defer plan.deinit();
    return applyPlan(state, allocator, &plan, &.{});
}

/// Applies one checked batch after verifying all guards against the pre-batch state.
///
/// Time Complexity: O(g + s + n * (k + v)), where `g` is guard count, `s` is shard grouping and shadow-planning work, `n` is survivor write count, `k` is average key length, and `v` is value clone cost.
///
/// Allocator: Uses `allocator` for validation scratch, planner scratch, committed delta arenas, and temporary WAL batch views.
pub fn applyCheckedBatch(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    batch: types.CheckedBatch,
) error_mod.EngineError!void {
    validateGuards(allocator, batch.guards) catch |err| return translatePlanError(err);
    var plan = internal_batch_plan.planPutBatch(allocator, batch.writes) catch |err| return translatePlanError(err);
    defer plan.deinit();
    return applyPlan(state, allocator, &plan, batch.guards);
}

pub const test_hooks = if (builtin.is_test) struct {
    /// Arms a one-shot pause after the next batch apply acquires the global visibility gate.
    ///
    /// Time Complexity: O(1).
    pub fn pauseNextBatchAfterVisibilityGate() void {
        paused_at_visibility_gate.store(false, .release);
        resume_after_visibility_gate.store(false, .release);
        pause_after_visibility_gate.store(true, .release);
    }

    /// Returns whether a batch apply is currently paused behind the visibility gate hook.
    ///
    /// Time Complexity: O(1).
    pub fn isBatchPausedAfterVisibilityGate() bool {
        return paused_at_visibility_gate.load(.acquire);
    }

    /// Releases a paused batch apply visibility-gate hook.
    ///
    /// Time Complexity: O(1).
    pub fn resumeBatchAfterVisibilityGate() void {
        resume_after_visibility_gate.store(true, .release);
    }
} else struct {};
