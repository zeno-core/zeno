//! Read-semantics ownership boundary for point reads and read-view-aware reads.
//! Cost: O(n + k + v) for point reads, where `n` is key length for shard routing, `k` is ART lookup work, and `v` is cloned value size.
//! Allocator: Uses explicit allocators only for returning owned cloned values to callers.

const std = @import("std");
const expiration = @import("expiration.zig");
const error_mod = @import("error.zig");
const internal_mutate = @import("../internal/mutate.zig");
const internal_ttl_index = @import("../internal/ttl_index.zig");
const runtime_shard = @import("../runtime/shard.zig");
const runtime_state = @import("../runtime/state.zig");
const types = @import("../types.zig");

/// Clones the current plain value for `key` while relying on an already-held visibility window.
///
/// Time Complexity: O(k + v), where `k` is ART lookup work and `v` is the size of the cloned value tree.
///
/// Allocator: Allocates the returned cloned value through `allocator` when the key exists.
///
/// Ownership: Returns a value owned by the caller when non-null. The caller must later call `deinit` with the same allocator.
///
/// Thread Safety: Requires a surrounding visibility window and acquires the selected shard's shared lock while reading and cloning the stored value.
fn clone_plain_value_no_visibility(
    state: *const runtime_state.DatabaseState,
    shard: *runtime_shard.Shard,
    allocator: std.mem.Allocator,
    key: []const u8,
) error_mod.EngineError!?types.Value {
    shard.lock.lockShared();
    defer shard.lock.unlockShared();
    state.record_operation(.get, 1);

    const stored = shard.tree.lookup(key) orelse return null;
    if (shard.has_ttl_entries) {
        const stored_expire_at = internal_ttl_index.get_expire_at(shard, key) orelse return try stored.clone(allocator);
        const now = runtime_shard.unix_now();
        if (expiration.is_expired(stored_expire_at, now)) return null;
    }
    return try stored.clone(allocator);
}

/// Opens one consistent read window over the current visible engine state.
///
/// Time Complexity: O(s), where `s` is the shard count.
///
/// Allocator: May allocate through the read-view token registry.
///
/// Ownership: Returns a handle that borrows the runtime state and visibility gates until `deinit` is called.
///
/// Thread Safety: Acquires the shared side of all shard-local visibility gates and keeps them held for the lifetime of the returned `ReadView`.
pub fn read_view(state: *const runtime_state.DatabaseState) error_mod.EngineError!types.ReadView {
    state.lock_all_shards_shared();

    return types.ReadView.init(
        state,
        @constCast(&state.active_read_views),
        runtime_shard.unix_now(),
    ) catch {
        state.unlock_all_shards_shared();
        return error.OutOfMemory;
    };
}

/// Clones the current plain value for `key` under the selected shard shared lock.
///
/// Time Complexity: O(n + k + v), where `n` is `key.len` for shard routing, `k` is ART lookup work, and `v` is the size of the cloned value tree.
///
/// Allocator: Allocates the returned cloned value through `allocator` when the key exists.
///
/// Ownership: Returns a value owned by the caller when non-null. The caller must later call `deinit` with the same allocator.
///
/// Thread Safety: Acquires only the selected shard's shared lock through `clone_plain_value_no_visibility`.
pub fn get(state: *const runtime_state.DatabaseState, allocator: std.mem.Allocator, key: []const u8) error_mod.EngineError!?types.Value {
    internal_mutate.validate_key(key) catch |err| switch (err) {
        error.EmptyKey, error.KeyTooLarge => return error.KeyTooLarge,
    };

    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = @constCast(&state.shards[shard_idx]);

    return clone_plain_value_no_visibility(state, shard, allocator, key);
}
