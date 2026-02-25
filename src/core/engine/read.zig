//! Read-semantics ownership boundary for point reads and read-view-aware reads.
//! Cost: O(n^2 + k + v) for point reads, where `n` is key length for shard routing, `k` is hash-map lookup work, and `v` is cloned value size.
//! Allocator: Uses explicit allocators only for returning owned cloned values to callers.

const std = @import("std");
const engine_db = @import("db.zig");
const runtime_shard = @import("../runtime/shard.zig");
const runtime_state = @import("../runtime/state.zig");
const types = @import("../types.zig");

/// Clones the current plain value for `key` when present.
///
/// Time Complexity: O(n^2 + k + v), where `n` is `key.len` for shard routing, `k` is hash-map lookup work, and `v` is the size of the cloned value tree.
///
/// Allocator: Allocates the returned cloned value through `allocator` when the key exists.
///
/// Ownership: Returns a value owned by the caller when non-null. The caller must later call `deinit` with the same allocator.
///
/// Thread Safety: Acquires the selected shard's shared lock while reading and cloning the stored value.
pub fn get(state: *const runtime_state.DatabaseState, allocator: std.mem.Allocator, key: []const u8) engine_db.EngineError!?types.Value {
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &state.shards[shard_idx];

    shard.lock.lockShared();
    defer shard.lock.unlockShared();
    _ = state.counters.ops_get_total.fetchAdd(1, .monotonic);

    const stored = shard.values.getPtr(key) orelse return null;
    return try stored.clone(allocator);
}
