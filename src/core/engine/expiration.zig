//! Expiration-semantics ownership boundary for TTL-visible behavior.
//! Cost: O(k) over one routed key, where `k` is key length for shard routing plus shard-local hash-map work.
//! Allocator: Uses the shard base allocator for TTL metadata ownership and may allocate delegated WAL serialization scratch for durable live mutations.

const durability = @import("durability.zig");
const error_mod = @import("error.zig");
const internal_mutate = @import("../internal/mutate.zig");
const internal_ttl_index = @import("../internal/ttl_index.zig");
const runtime_shard = @import("../runtime/shard.zig");
const runtime_state = @import("../runtime/state.zig");

const TtlCleanup = enum {
    none,
    missing_key,
    expired_key,
};

/// Returns whether one present stored key should remain visible at `now`.
///
/// Time Complexity: O(k), where `k` is `key.len`.
///
/// Allocator: Does not allocate.
pub fn key_is_visible_unlocked(shard: *const runtime_shard.Shard, key: []const u8, now: i64) bool {
    const stored_expire_at = internal_ttl_index.get_expire_at(shard, key) orelse return true;
    return !is_expired(stored_expire_at, now);
}

/// Returns whether one TTL timestamp should be treated as expired.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn is_expired(expire_at_seconds: i64, now: i64) bool {
    return internal_ttl_index.is_expired(expire_at_seconds, now);
}

/// Sets or clears expiration for one plain key.
///
/// Time Complexity: O(n^2 + k), where `n` is `key.len` for shard routing and `k` is hash-map lookup and update work.
///
/// Allocator: Uses the shard base allocator when preparing a new TTL entry and may allocate delegated WAL record scratch for durable live mutations.
///
/// Thread Safety: Acquires the exclusive side of the global visibility gate before taking the target shard's exclusive lock, then appends the matching live WAL record inside that same window before publishing the TTL change.
pub fn expire_at(
    state: *runtime_state.DatabaseState,
    key: []const u8,
    unix_seconds: ?i64,
) error_mod.EngineError!bool {
    state.visibility_gate.lock_exclusive();
    defer state.visibility_gate.unlock_exclusive();

    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();

    const now = runtime_shard.unix_now();
    if (!shard.values.contains(key)) {
        internal_ttl_index.clear_ttl_entry(shard, key);
        return false;
    }

    if (internal_ttl_index.get_expire_at(shard, key)) |existing_expire_at| {
        if (is_expired(existing_expire_at, now)) {
            _ = internal_mutate.remove_stored_value_unlocked(shard, state.base_allocator, key);
            internal_ttl_index.clear_ttl_entry(shard, key);
            return false;
        }
    }

    if (unix_seconds) |expire_at_seconds| {
        if (expire_at_seconds <= now) {
            try durability.append_delete_if_enabled(state, key);
            _ = internal_mutate.remove_stored_value_unlocked(shard, state.base_allocator, key);
            internal_ttl_index.clear_ttl_entry(shard, key);
            return true;
        }

        var prepared_ttl = try internal_ttl_index.prepare_set_ttl_entry(shard, key);
        errdefer prepared_ttl.deinit(state.base_allocator);

        try durability.append_expire_if_enabled(state, key, expire_at_seconds);
        internal_ttl_index.apply_prepared_set_ttl_entry_unlocked(shard, key, expire_at_seconds, prepared_ttl);
        return true;
    }

    const stored = shard.values.getPtr(key).?;
    try durability.append_put_if_enabled(state, key, stored);
    internal_ttl_index.clear_ttl_entry(shard, key);
    return true;
}

/// Tries to clean up stale TTL state without blocking behind active read views.
///
/// Time Complexity: O(n^2 + k), where `n` is `key.len` for shard routing and `k` is shard-local lookup plus optional teardown work.
///
/// Allocator: Does not allocate.
fn try_cleanup_if_possible(
    state: *runtime_state.DatabaseState,
    key: []const u8,
    cleanup: TtlCleanup,
) void {
    if (cleanup == .none) return;
    if (!state.visibility_gate.try_lock_exclusive()) return;
    defer state.visibility_gate.unlock_exclusive();

    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();

    switch (cleanup) {
        .none => {},
        .missing_key => {
            if (!shard.values.contains(key)) internal_ttl_index.clear_ttl_entry(shard, key);
        },
        .expired_key => {
            if (!shard.values.contains(key)) {
                internal_ttl_index.clear_ttl_entry(shard, key);
                return;
            }

            const now = runtime_shard.unix_now();
            const stored_expire_at = internal_ttl_index.get_expire_at(shard, key) orelse return;
            if (!is_expired(stored_expire_at, now)) return;

            _ = internal_mutate.remove_stored_value_unlocked(shard, state.base_allocator, key);
            internal_ttl_index.clear_ttl_entry(shard, key);
        },
    }
}

/// Returns Redis-style TTL for one plain key.
///
/// Time Complexity: O(n^2 + k), where `n` is `key.len` for shard routing and `k` is shard-local hash-map lookup and optional cleanup work.
///
/// Allocator: Does not allocate.
///
/// Thread Safety: Reads under the shared visibility gate and only performs lazy cleanup afterward if the exclusive gate can be acquired immediately.
pub fn ttl(state: *runtime_state.DatabaseState, key: []const u8) error_mod.EngineError!i64 {
    var cleanup: TtlCleanup = .none;
    const result = blk: {
        const visibility_gate = &state.visibility_gate;
        visibility_gate.lock_shared();
        defer visibility_gate.unlock_shared();

        const now = runtime_shard.unix_now();
        const shard_idx = runtime_shard.get_shard_index(key);
        const shard = &state.shards[shard_idx];

        shard.lock.lockShared();
        defer shard.lock.unlockShared();

        if (!shard.values.contains(key)) {
            cleanup = .missing_key;
            break :blk @as(i64, -2);
        }

        const stored_expire_at = internal_ttl_index.get_expire_at(shard, key) orelse break :blk @as(i64, -1);
        if (is_expired(stored_expire_at, now)) {
            cleanup = .expired_key;
            break :blk @as(i64, -2);
        }

        break :blk stored_expire_at - now;
    };

    try_cleanup_if_possible(state, key, cleanup);
    return result;
}
