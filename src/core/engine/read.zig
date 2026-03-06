//! Read-semantics ownership boundary for point reads and read-view-aware reads.
//! Cost: O(n + k + v) for point reads, where `n` is key length for shard routing, `k` is ART lookup work, and `v` is cloned value size.
//! Allocator: Uses explicit allocators only for returning owned cloned values to callers.

const std = @import("std");
const expiration = @import("expiration.zig");
const error_mod = @import("error.zig");
const internal_mutate = @import("../internal/mutate.zig");
const runtime_shard = @import("../runtime/shard.zig");
const runtime_state = @import("../runtime/state.zig");
const types = @import("../types.zig");

/// Clones the current plain value for `key` while relying on an already-held visibility window.
///
/// Time Complexity: O(n + k + v), where `n` is `key.len` for shard routing, `k` is ART lookup work, and `v` is the size of the cloned value tree.
///
/// Allocator: Allocates the returned cloned value through `allocator` when the key exists.
///
/// Ownership: Returns a value owned by the caller when non-null. The caller must later call `deinit` with the same allocator.
///
/// Thread Safety: Requires a surrounding visibility window and acquires the selected shard's shared lock while reading and cloning the stored value.
fn clone_plain_value_no_visibility(
    state: *const runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    key: []const u8,
    now: i64,
) error_mod.EngineError!?types.Value {
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = @constCast(&state.shards[shard_idx]);

    shard.lock.lockShared();
    defer shard.lock.unlockShared();
    _ = @constCast(&state.counters.ops_get_total).fetchAdd(1, .monotonic);

    const stored = shard.tree.lookup(key) orelse return null;
    if (!expiration.key_is_visible_unlocked(shard, key, now)) return null;
    return try stored.clone(allocator);
}

/// Opens one consistent read window over the current visible engine state.
///
/// Time Complexity: O(1).
///
/// Allocator: May allocate through the read-view token registry.
///
/// Ownership: Returns a handle that borrows the runtime state and visibility gate until `deinit` is called.
///
/// Thread Safety: Acquires the shared side of the global visibility gate and keeps it held for the lifetime of the returned `ReadView`.
pub fn read_view(state: *const runtime_state.DatabaseState) error_mod.EngineError!types.ReadView {
    const visibility_gate = @constCast(&state.visibility_gate);
    visibility_gate.lock_shared();
    return types.ReadView.init(
        state,
        visibility_gate,
        @constCast(&state.active_read_views),
        runtime_shard.unix_now(),
    ) catch {
        visibility_gate.unlock_shared();
        return error.OutOfMemory;
    };
}

/// Clones the current plain value for `key` under the shared visibility gate.
///
/// Time Complexity: O(n + k + v), where `n` is `key.len` for shard routing, `k` is ART lookup work, and `v` is the size of the cloned value tree.
///
/// Allocator: Allocates the returned cloned value through `allocator` when the key exists.
///
/// Ownership: Returns a value owned by the caller when non-null. The caller must later call `deinit` with the same allocator.
///
/// Thread Safety: Acquires the shared side of the global visibility gate before taking the selected shard's shared lock.
pub fn get(state: *const runtime_state.DatabaseState, allocator: std.mem.Allocator, key: []const u8) error_mod.EngineError!?types.Value {
    internal_mutate.validate_key(key) catch |err| switch (err) {
        error.EmptyKey, error.KeyTooLarge => return error.KeyTooLarge,
    };

    const visibility_gate = @constCast(&state.visibility_gate);
    visibility_gate.lock_shared();
    defer visibility_gate.unlock_shared();
    const now = runtime_shard.unix_now();
    return clone_plain_value_no_visibility(state, allocator, key, now);
}
