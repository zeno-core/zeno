//! Batch-semantics ownership boundary for atomic plain and guarded writes.
//! Cost: O(n + b + v), where `n` is batch size, `b` is total serialized value bytes measured during planning, and `v` is total cloned value size for prepared writes.
//! Allocator: Uses explicit allocators for planning scratch and prepared batch-owned value clones.

const std = @import("std");
const expiration = @import("expiration.zig");
const error_mod = @import("error.zig");
const internal_batch_plan = @import("../internal/batch_plan.zig");
const internal_mutate = @import("../internal/mutate.zig");
const internal_codec = @import("../internal/codec.zig");
const internal_ttl_index = @import("../internal/ttl_index.zig");
const runtime_shard = @import("../runtime/shard.zig");
const runtime_state = @import("../runtime/state.zig");
const types = @import("../types.zig");

/// One prepared survivor write that is ready to enter the atomic apply window.
const PreparedWrite = struct {
    shard_idx: usize,
    key: []const u8,
    owned_insert_key: ?[]u8,
    new_value: types.Value,
    transferred: bool = false,
};

/// Releases any prepared batch values and keys that were not transferred into runtime state.
///
/// Time Complexity: O(n + v), where `n` is `prepared.len` and `v` is total teardown work for any uncommitted prepared values.
///
/// Allocator: Does not allocate; frees uncommitted prepared keys and values through `allocator`.
fn cleanup_prepared_writes(prepared: []PreparedWrite, allocator: std.mem.Allocator) void {
    for (prepared) |write| {
        if (write.transferred) continue;
        if (write.owned_insert_key) |owned_key| allocator.free(owned_key);
        var owned_value = write.new_value;
        owned_value.deinit(allocator);
    }
}

/// Translates low-level planner errors into the public engine batch error surface.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
fn translate_plan_error(err: anyerror) error_mod.EngineError {
    return switch (err) {
        error.EmptyKey, error.KeyTooLarge => error.KeyTooLarge,
        error.ValueTooLarge => error.ValueTooLarge,
        error.MaxDepthExceeded => error.ValueTooDeep,
        error.OutOfMemory => error.OutOfMemory,
        else => unreachable,
    };
}

/// Validates one checked-batch guard at the same physical boundary used for writes.
///
/// Time Complexity: O(k + v), where `k` is the guarded key length and `v` is serialized expected-value size for `key_value_equals`.
///
/// Allocator: Uses `scratch` only for temporary serialization measurement.
fn validate_guard(
    allocator: std.mem.Allocator,
    scratch: *std.ArrayList(u8),
    guard: types.CheckedBatchGuard,
) (internal_batch_plan.BatchPlanError)!void {
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

/// Validates all checked-batch guards before the batch enters the apply window.
///
/// Time Complexity: O(g + b), where `g` is `guards.len` and `b` is total serialized expected-value bytes for `key_value_equals` guards.
///
/// Allocator: Uses `allocator` only for temporary serialization scratch growth.
fn validate_guards(
    allocator: std.mem.Allocator,
    guards: []const types.CheckedBatchGuard,
) (internal_batch_plan.BatchPlanError)!void {
    var scratch = std.ArrayList(u8).empty;
    defer scratch.deinit(allocator);

    for (guards) |guard| {
        try validate_guard(allocator, &scratch, guard);
    }
}

/// Returns whether one checked-batch guard holds against the current pre-batch state.
///
/// Time Complexity: O(k + v), where `k` is the guarded key length and `v` is compared value tree size for `key_value_equals`.
///
/// Allocator: Does not allocate.
///
/// Thread Safety: Requires the caller to hold the exclusive side of the visibility gate for the full guard-evaluation window.
fn guard_holds(state: *runtime_state.DatabaseState, guard: types.CheckedBatchGuard, now: i64) bool {
    return switch (guard) {
        .key_exists => |key| blk: {
            const shard_idx = runtime_shard.get_shard_index(key);
            const shard = &state.shards[shard_idx];
            if (!internal_mutate.key_exists_unlocked(shard, key)) break :blk false;
            break :blk expiration.key_is_visible_unlocked(shard, key, now);
        },
        .key_not_exists => |key| blk: {
            const shard_idx = runtime_shard.get_shard_index(key);
            const shard = &state.shards[shard_idx];
            if (!internal_mutate.key_exists_unlocked(shard, key)) break :blk true;
            break :blk !expiration.key_is_visible_unlocked(shard, key, now);
        },
        .key_value_equals => |guarded| blk: {
            const shard_idx = runtime_shard.get_shard_index(guarded.key);
            const shard = &state.shards[shard_idx];
            if (!internal_mutate.key_exists_unlocked(shard, guarded.key)) break :blk false;
            if (!expiration.key_is_visible_unlocked(shard, guarded.key, now)) break :blk false;
            break :blk internal_mutate.stored_value_equals_unlocked(shard, guarded.key, guarded.value);
        },
    };
}

/// Applies one validated survivor plan under the exclusive visibility gate.
///
/// Time Complexity: O(g + n + v), where `g` is `guards.len`, `n` is `plan.writes.len`, and `v` is total cloned value size for prepared writes.
///
/// Allocator: Uses `allocator` for temporary prepared-write metadata and uses the engine base allocator for committed keys and values.
///
/// Ownership: Clones all surviving write values before making any mutation visible, then transfers prepared ownership into runtime state on success.
///
/// Thread Safety: Acquires the exclusive side of the global visibility gate for the full guard-check, reservation, and apply window.
fn apply_plan(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    plan: *internal_batch_plan.BatchPlan,
    guards: []const types.CheckedBatchGuard,
) error_mod.EngineError!void {
    state.visibility_gate.lock_exclusive();
    defer state.visibility_gate.unlock_exclusive();

    const now = runtime_shard.unix_now();
    for (guards) |guard| {
        if (!guard_holds(state, guard, now)) return error.GuardFailed;
    }

    var prepared = try std.ArrayList(PreparedWrite).initCapacity(allocator, plan.writes.len);
    defer cleanup_prepared_writes(prepared.items, state.base_allocator);
    defer prepared.deinit(allocator);

    var new_inserts_per_shard: [runtime_state.NUM_SHARDS]usize = [_]usize{0} ** runtime_state.NUM_SHARDS;

    for (plan.writes) |write| {
        const shard = &state.shards[write.shard_idx];

        var prepared_write = PreparedWrite{
            .shard_idx = write.shard_idx,
            .key = write.key,
            .owned_insert_key = null,
            .new_value = try write.value.clone(state.base_allocator),
        };
        errdefer {
            if (prepared_write.owned_insert_key) |owned_key| state.base_allocator.free(owned_key);
            prepared_write.new_value.deinit(state.base_allocator);
        }

        if (!internal_mutate.key_exists_unlocked(shard, write.key)) {
            prepared_write.owned_insert_key = try state.base_allocator.dupe(u8, write.key);
            new_inserts_per_shard[write.shard_idx] += 1;
        }

        prepared.appendAssumeCapacity(prepared_write);
    }

    for (new_inserts_per_shard, 0..) |count, shard_idx| {
        if (count == 0) continue;
        try state.shards[shard_idx].values.ensureUnusedCapacity(state.base_allocator, count);
    }

    for (prepared.items) |*write| {
        internal_mutate.apply_owned_put_assume_capacity_unlocked(
            &state.shards[write.shard_idx],
            state.base_allocator,
            write.key,
            write.owned_insert_key,
            write.new_value,
        );
        internal_ttl_index.clear_ttl_entry(&state.shards[write.shard_idx], write.key);
        write.transferred = true;
    }

    _ = state.counters.ops_put_total.fetchAdd(plan.writes.len, .monotonic);
}

/// Applies one plain atomic batch.
///
/// Time Complexity: O(n + b + v), where `n` is `writes.len`, `b` is total serialized value bytes measured during planning, and `v` is total cloned value size for prepared writes.
///
/// Allocator: Uses `allocator` for planning scratch and temporary prepared-write metadata, and uses the engine base allocator for batch-owned values committed to runtime state.
///
/// Ownership: Clones all surviving write values into engine-owned storage before making the batch visible.
///
/// Thread Safety: Acquires the exclusive side of the global visibility gate for the full guard-check and apply window.
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
/// Time Complexity: O(g + n + b + v), where `g` is `batch.guards.len`, `n` is surviving write count, `b` is total serialized value bytes measured during planning, and `v` is total cloned value size for prepared writes.
///
/// Allocator: Uses `allocator` for planning scratch and temporary prepared-write metadata, and uses the engine base allocator for batch-owned values committed to runtime state.
///
/// Ownership: Clones all surviving write values into engine-owned storage before making the batch visible.
///
/// Thread Safety: Acquires the exclusive side of the global visibility gate for the full guard-check and apply window.
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
