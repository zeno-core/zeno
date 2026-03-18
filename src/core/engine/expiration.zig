//! Expiration-semantics ownership boundary for TTL-visible behavior.
//! Cost: O(k) over one routed key, where `k` is key length for shard routing plus shard-local ART and TTL work.
//! Allocator: Uses the shard base allocator for TTL metadata ownership and may allocate delegated WAL serialization scratch for durable live mutations.

const std = @import("std");
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
    if (shard.ttl_index.count() == 0) return true;
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
/// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is ART lookup and TTL update work.
///
/// Allocator: Uses the shard base allocator when preparing a new TTL entry and may allocate delegated WAL record scratch for durable live mutations.
///
/// Thread Safety: Acquires the exclusive side of the global visibility gate before taking the target shard's exclusive lock, then appends the matching live WAL record inside that same window before publishing the TTL change.
pub fn expire_at(
    state: *runtime_state.DatabaseState,
    key: []const u8,
    unix_seconds: ?i64,
) error_mod.EngineError!bool {
    internal_mutate.validate_key(key) catch |err| switch (err) {
        error.EmptyKey, error.KeyTooLarge => return error.KeyTooLarge,
    };

    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &state.shards[shard_idx];

    shard.visibility_gate.lock_exclusive();
    defer shard.visibility_gate.unlock_exclusive();

    shard.lock.lock();
    defer shard.lock.unlock();

    const now = runtime_shard.unix_now();
    if (!internal_mutate.key_exists_unlocked(shard, key)) {
        internal_ttl_index.clear_ttl_entry(shard, key);
        return false;
    }

    if (internal_ttl_index.get_expire_at(shard, key)) |existing_expire_at| {
        if (is_expired(existing_expire_at, now)) {
            _ = try internal_mutate.remove_stored_value_unlocked(shard, key);
            internal_ttl_index.clear_ttl_entry(shard, key);
            return false;
        }
    }

    if (unix_seconds) |expire_at_seconds| {
        if (expire_at_seconds <= now) {
            try durability.append_delete_if_enabled(state, key);
            _ = try internal_mutate.remove_stored_value_unlocked(shard, key);
            internal_ttl_index.clear_ttl_entry(shard, key);
            return true;
        }

        var prepared_ttl = try internal_ttl_index.prepare_set_ttl_entry(shard, key);
        errdefer prepared_ttl.deinit(state.base_allocator);

        try durability.append_expire_if_enabled(state, key, expire_at_seconds);
        internal_ttl_index.apply_prepared_set_ttl_entry_unlocked(shard, key, expire_at_seconds, prepared_ttl);
        return true;
    }

    const stored = shard.tree.lookup(key).?;
    try durability.append_put_if_enabled(state, key, stored);
    internal_ttl_index.clear_ttl_entry(shard, key);
    return true;
}

/// Tries to clean up stale TTL state without blocking behind active read views.
///
/// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is shard-local ART lookup plus optional teardown work.
///
/// Allocator: Does not allocate.
fn try_cleanup_if_possible(
    state: *runtime_state.DatabaseState,
    key: []const u8,
    cleanup: TtlCleanup,
) void {
    if (cleanup == .none) return;
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &state.shards[shard_idx];

    if (!shard.visibility_gate.try_lock_exclusive()) return;
    defer shard.visibility_gate.unlock_exclusive();

    shard.lock.lock();
    defer shard.lock.unlock();

    switch (cleanup) {
        .none => {},
        .missing_key => {
            if (!internal_mutate.key_exists_unlocked(shard, key)) internal_ttl_index.clear_ttl_entry(shard, key);
        },
        .expired_key => {
            if (!internal_mutate.key_exists_unlocked(shard, key)) {
                internal_ttl_index.clear_ttl_entry(shard, key);
                return;
            }

            const now = runtime_shard.unix_now();
            const stored_expire_at = internal_ttl_index.get_expire_at(shard, key) orelse return;
            if (!is_expired(stored_expire_at, now)) return;

            _ = internal_mutate.remove_stored_value_unlocked(shard, key) catch return;
            internal_ttl_index.clear_ttl_entry(shard, key);
        },
    }
}

/// Returns Redis-style TTL for one plain key.
///
/// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is shard-local ART lookup and optional cleanup work.
///
/// Allocator: Does not allocate.
///
/// Thread Safety: Reads under the shared visibility gate and only performs lazy cleanup afterward if the exclusive gate can be acquired immediately.
pub fn ttl(state: *runtime_state.DatabaseState, key: []const u8) error_mod.EngineError!i64 {
    internal_mutate.validate_key(key) catch |err| switch (err) {
        error.EmptyKey, error.KeyTooLarge => return error.KeyTooLarge,
    };

    var cleanup: TtlCleanup = .none;
    const result = blk: {
        const shard_idx = runtime_shard.get_shard_index(key);
        const shard = &state.shards[shard_idx];

        shard.visibility_gate.lock_shared();
        defer shard.visibility_gate.unlock_shared();

        const now = runtime_shard.unix_now();

        shard.lock.lockShared();
        defer shard.lock.unlockShared();

        if (!internal_mutate.key_exists_unlocked(shard, key)) {
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

/// Removes all expired TTL entries from one shard, including their ART values.
///
/// Time Complexity: O(t), where `t` is the number of TTL entries in the shard.
///
/// Allocator: Uses `allocator` for a temporary collection of expired keys. Does not
/// allocate when `shard.has_ttl_entries` is false or when no entries are expired.
///
/// Ownership: Does not append WAL records. Durability of expiration is encoded in
/// the original EXPIRE record; the sweep is a memory-reclaim optimization only.
///
/// Thread Safety: Uses `try_lock_exclusive` on the shard visibility gate and skips
/// the shard if the gate cannot be acquired immediately. Brackets ART modifications
/// with the shard seqlock to protect concurrent lock-free GET readers.
pub fn sweep_expired_entries_for_shard(
    state: *runtime_state.DatabaseState,
    shard_idx: usize,
    allocator: std.mem.Allocator,
) void {
    const shard = &state.shards[shard_idx];

    // Fast path: skip shards with no TTL entries
    if (!shard.has_ttl_entries) return;

    // Try to acquire exclusive visibility gate. Skip if busy, will retry next interval
    if (!shard.visibility_gate.try_lock_exclusive()) return;
    defer shard.visibility_gate.unlock_exclusive();

    shard.lock.lock();
    defer shard.lock.unlock();

    // Re-check under lock: another writer may have cleared all entries first
    if (!shard.has_ttl_entries) return;

    const now = runtime_shard.unix_now();

    // First pass: collect keys of expired entries. Do not modify the map during iteration
    var expired = std.ArrayList([]const u8).empty;
    defer expired.deinit(allocator);

    var it = shard.ttl_index.iterator();
    while (it.next()) |entry| {
        if (is_expired(entry.value_ptr.*, now)) {
            expired.append(allocator, entry.key_ptr.*) catch continue;
        }
    }

    if (expired.items.len == 0) return;

    // Second pass: remove expired entries under the seqlock bracket
    // The bracket makes concurrent lock-free GET readers spin until removal is complete,
    // preventing them from cloning values that are about to be freed
    const seq0 = shard.seq.load(.monotonic);
    shard.seq.store(seq0 + 1, .release);
    defer shard.seq.store(seq0 + 2, .release);

    for (expired.items) |key| {
        // Remove from ART first (may free heap-owned value memory)
        _ = internal_mutate.remove_stored_value_unlocked(shard, key) catch {};
        // Clear TTL index entry and free owned key bytes
        internal_ttl_index.clear_ttl_entry(shard, key);
    }
}
