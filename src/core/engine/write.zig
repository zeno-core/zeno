//! Write-semantics ownership boundary for plain writes and deletes.
//! Cost: O(n^2 + k + v) for writes, where `n` is key length for shard routing, `k` is hash-map lookup or insert work, and `v` is cloned value size.
//! Allocator: Uses the engine base allocator for owned key bytes and nested stored values.

const std = @import("std");
const engine_db = @import("db.zig");
const runtime_shard = @import("../runtime/shard.zig");
const runtime_state = @import("../runtime/state.zig");
const types = @import("../types.zig");

/// Maximum accepted plain-key length for plain point operations.
pub const MAX_KEY_LEN: usize = 4_096;

/// Inserts or replaces one plain key/value pair.
///
/// Time Complexity: O(n^2 + k + v), where `n` is `key.len` for shard routing, `k` is hash-map lookup or insert work, and `v` is cloned value size.
///
/// Allocator: Clones owned key and value storage through `state.base_allocator` when inserting or replacing an entry.
///
/// Ownership: Clones `value` into shard-owned storage before the call returns.
///
/// Thread Safety: Acquires the exclusive side of the global visibility gate before taking the selected shard's exclusive lock.
pub fn put(state: *runtime_state.DatabaseState, key: []const u8, value: *const types.Value) engine_db.EngineError!void {
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
        stored.deinit(allocator);
        stored.* = cloned;
    } else {
        const owned_key = try allocator.dupe(u8, key);
        errdefer allocator.free(owned_key);

        const cloned = try value.clone(allocator);
        errdefer {
            var owned_value = cloned;
            owned_value.deinit(allocator);
        }

        try shard.values.put(allocator, owned_key, cloned);
    }

    _ = state.counters.ops_put_total.fetchAdd(1, .monotonic);
}

/// Deletes one plain key/value pair when present.
///
/// Time Complexity: O(n^2 + k), where `n` is `key.len` for shard routing and `k` is hash-map lookup and removal work.
///
/// Allocator: Does not allocate; frees owned key and nested value storage through `state.base_allocator` when the key exists.
///
/// Ownership: Releases shard-owned key and value storage when the key exists.
///
/// Thread Safety: Acquires the exclusive side of the global visibility gate before taking the selected shard's exclusive lock.
pub fn delete(state: *runtime_state.DatabaseState, key: []const u8) bool {
    state.visibility_gate.lock_exclusive();
    defer state.visibility_gate.unlock_exclusive();

    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();

    const removed = shard.values.fetchRemove(key) orelse return false;
    state.base_allocator.free(removed.key);

    var owned_value = removed.value;
    owned_value.deinit(state.base_allocator);

    _ = state.counters.ops_delete_total.fetchAdd(1, .monotonic);
    return true;
}
