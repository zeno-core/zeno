//! Write-semantics ownership boundary for plain writes and deletes.
//! Cost: O(n + k + v) for writes, where `n` is key length for shard routing, `k` is ART traversal work, and `v` is cloned value size.
//! Allocator: Uses the shard arena model for committed point-write ownership.

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
/// Time Complexity: O(n + k + v), where `n` is `key.len` for shard routing, `k` is ART lookup or insert work, and `v` is cloned value size.
///
/// Allocator: Clones owned key and value storage through the target shard arena and may allocate delegated WAL serialization scratch when durability is enabled.
///
/// Ownership: Clones `value` into shard-owned storage before the call returns.
///
/// Thread Safety: Safe for concurrent use with other point operations; acquires the shared side of the global visibility gate before taking one shard-exclusive lock.
pub fn put(state: *runtime_state.DatabaseState, key: []const u8, value: *const types.Value) error_mod.EngineError!void {
    if (key.len == 0 or key.len > MAX_KEY_LEN) return error.KeyTooLarge;

    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &state.shards[shard_idx];

    shard.visibility_gate.lock_shared();
    defer shard.visibility_gate.unlock_shared();

    shard.lock.lock();
    defer shard.lock.unlock();

    try durability.append_put_if_enabled(state, key, value);
    const overwritten = try internal_mutate.upsert_value_unlocked(shard, key, value);
    internal_ttl_index.clear_ttl_entry(shard, key);
    state.record_operation(.put, 1);
    if (overwritten) state.record_operation(.overwrite, 1);
}

/// Applies multiple plain key/value writes as independent standalone mutations.
///
/// Time Complexity: O(n * (k + v) + s * n), where `n` is `writes.len`, `k` is average key length, `v` is clone cost, and `s` is shard count.
///
/// Allocator: Clones owned key and value storage through touched shard arenas and may allocate delegated WAL serialization scratch when durability is enabled.
///
/// Ownership: Clones each write value into shard-owned storage before returning.
///
/// Thread Safety: Safe for concurrent use with reads and scans; acquires shard-local shared visibility gates and shard-exclusive locks per touched shard.
///
/// Durability Semantics: Writes are non-atomic as a group. Replays may recover any durable prefix if a failure happens mid-apply.
pub fn put_group(state: *runtime_state.DatabaseState, writes: []const types.PutWrite) error_mod.EngineError!void {
    if (writes.len == 0) return;

    var wal_writes = state.base_allocator.alloc(durability.PutBatchWrite, writes.len) catch return error.OutOfMemory;
    defer state.base_allocator.free(wal_writes);

    var touched = std.StaticBitSet(runtime_shard.NUM_SHARDS).initEmpty();
    for (writes, 0..) |write_entry, i| {
        if (write_entry.key.len == 0 or write_entry.key.len > MAX_KEY_LEN) return error.KeyTooLarge;
        wal_writes[i] = .{ .key = write_entry.key, .value = write_entry.value };
        touched.set(runtime_shard.get_shard_index(write_entry.key));
    }

    try durability.append_put_group_if_enabled(state, wal_writes);

    var applied: u64 = 0;
    var overwritten: u64 = 0;
    for (0..runtime_shard.NUM_SHARDS) |shard_idx| {
        if (!touched.isSet(shard_idx)) continue;

        const shard = &state.shards[shard_idx];
        shard.visibility_gate.lock_shared();
        shard.lock.lock();

        for (writes) |write_entry| {
            if (runtime_shard.get_shard_index(write_entry.key) != shard_idx) continue;
            if (try internal_mutate.upsert_value_unlocked(shard, write_entry.key, write_entry.value)) {
                overwritten += 1;
            }
            internal_ttl_index.clear_ttl_entry(shard, write_entry.key);
            applied += 1;
        }

        shard.lock.unlock();
        shard.visibility_gate.unlock_shared();
    }

    if (applied != @as(u64, @intCast(writes.len))) unreachable;
    state.record_operation(.put, applied);
    state.record_operation(.overwrite, overwritten);
}

/// Deletes one plain key/value pair when present.
///
/// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is ART lookup and delete work.
///
/// Allocator: Does not allocate directly; removal retains replaced storage inside the shard arena model and may allocate delegated WAL record scratch when durability is enabled.
///
/// Ownership: Releases shard-owned key and value storage only after the WAL append succeeds when the key exists and is still TTL-visible.
///
/// Thread Safety: Safe for concurrent use with other point operations; acquires the shared side of the global visibility gate before taking one shard-exclusive lock and appends the live DELETE record inside that same visibility window.
pub fn delete(state: *runtime_state.DatabaseState, key: []const u8) error_mod.EngineError!bool {
    if (key.len == 0 or key.len > MAX_KEY_LEN) return error.KeyTooLarge;

    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &state.shards[shard_idx];

    shard.visibility_gate.lock_shared();
    defer shard.visibility_gate.unlock_shared();

    shard.lock.lock();
    defer shard.lock.unlock();

    if (!internal_mutate.key_exists_unlocked(shard, key)) {
        internal_ttl_index.clear_ttl_entry(shard, key);
        return false;
    }

    const now = runtime_shard.unix_now();
    if (!expiration.key_is_visible_unlocked(shard, key, now)) {
        _ = try internal_mutate.remove_stored_value_unlocked(shard, key);
        internal_ttl_index.clear_ttl_entry(shard, key);
        return false;
    }

    try durability.append_delete_if_enabled(state, key);

    _ = try internal_mutate.remove_stored_value_unlocked(shard, key);
    internal_ttl_index.clear_ttl_entry(shard, key);

    state.record_operation(.delete, 1);
    return true;
}
