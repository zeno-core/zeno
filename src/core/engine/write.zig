//! Write-semantics ownership boundary for plain writes and deletes.
//! Cost: O(n^2 + k + v) for writes, where `n` is key length for shard routing, `k` is hash-map lookup or insert work, and `v` is cloned value size.
//! Allocator: Uses the engine base allocator for owned key bytes and nested stored values.

const std = @import("std");
const durability = @import("durability.zig");
const expiration = @import("expiration.zig");
const error_mod = @import("error.zig");
const internal_mutate = @import("../internal/mutate.zig");
const internal_ttl_index = @import("../internal/ttl_index.zig");
const runtime_shard = @import("../runtime/shard.zig");
const runtime_state = @import("../runtime/state.zig");
const types = @import("../types.zig");

/// Maximum accepted plain-key length for plain point operations.
pub const MAX_KEY_LEN: usize = 4_096;

/// Inserts or replaces one plain key/value pair.
///
/// Time Complexity: O(n^2 + k + v), where `n` is `key.len` for shard routing, `k` is hash-map lookup or insert work, and `v` is cloned value size.
///
/// Allocator: Clones owned key and value storage through `state.base_allocator` and may allocate delegated WAL serialization scratch when durability is enabled.
///
/// Ownership: Clones `value` into shard-owned storage before the call returns.
///
/// Thread Safety: Acquires the exclusive side of the global visibility gate, then the selected shard's exclusive lock, and may append to the shared WAL before publishing the in-memory mutation.
pub fn put(state: *runtime_state.DatabaseState, key: []const u8, value: *const types.Value) error_mod.EngineError!void {
    if (key.len == 0 or key.len > MAX_KEY_LEN) return error.KeyTooLarge;

    state.visibility_gate.lock_exclusive();
    defer state.visibility_gate.unlock_exclusive();

    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();

    const allocator = state.base_allocator;
    if (shard.values.getPtr(key)) |stored| {
        const cloned = try value.clone(allocator);
        errdefer {
            var owned_value = cloned;
            owned_value.deinit(allocator);
        }

        try durability.append_put_if_enabled(state, key, value);

        stored.deinit(allocator);
        stored.* = cloned;
    } else {
        try shard.values.ensureUnusedCapacity(allocator, 1);

        const owned_key = try allocator.dupe(u8, key);
        errdefer allocator.free(owned_key);

        const cloned = try value.clone(allocator);
        errdefer {
            var owned_value = cloned;
            owned_value.deinit(allocator);
        }

        try durability.append_put_if_enabled(state, key, value);

        shard.values.putAssumeCapacityNoClobber(owned_key, cloned);
    }

    internal_ttl_index.clear_ttl_entry(shard, key);
    _ = state.counters.ops_put_total.fetchAdd(1, .monotonic);
}

/// Deletes one plain key/value pair when present.
///
/// Time Complexity: O(n^2 + k), where `n` is `key.len` for shard routing and `k` is hash-map lookup and removal work.
///
/// Allocator: Frees owned key and nested value storage through `state.base_allocator` when the key exists and may allocate delegated WAL record scratch when durability is enabled.
///
/// Ownership: Releases shard-owned key and value storage only after the WAL append succeeds when the key exists and is still TTL-visible.
///
/// Thread Safety: Acquires the exclusive side of the global visibility gate, then the selected shard's exclusive lock, and appends the DELETE record inside that same visibility window before publishing the removal.
pub fn delete(state: *runtime_state.DatabaseState, key: []const u8) error_mod.EngineError!bool {
    state.visibility_gate.lock_exclusive();
    defer state.visibility_gate.unlock_exclusive();

    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();

    if (!internal_mutate.key_exists_unlocked(shard, key)) {
        internal_ttl_index.clear_ttl_entry(shard, key);
        return false;
    }

    const now = runtime_shard.unix_now();
    if (!expiration.key_is_visible_unlocked(shard, key, now)) {
        _ = internal_mutate.remove_stored_value_unlocked(shard, state.base_allocator, key);
        internal_ttl_index.clear_ttl_entry(shard, key);
        return false;
    }

    try durability.append_delete_if_enabled(state, key);

    _ = internal_mutate.remove_stored_value_unlocked(shard, state.base_allocator, key);
    internal_ttl_index.clear_ttl_entry(shard, key);

    _ = state.counters.ops_delete_total.fetchAdd(1, .monotonic);
    return true;
}
