//! Internal TTL bookkeeping helpers shared by runtime and expiration paths.
//! Cost: O(1) expected TTL map operations plus O(k) key duplication when inserting one new entry.
//! Allocator: Uses the shard base allocator for owned TTL-key storage.

const runtime_shard = @import("../runtime/shard.zig");

/// Returns whether `expire_at` is at or before `now`.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn is_expired(expire_at: i64, now: i64) bool {
    return expire_at <= now;
}

/// Returns the stored absolute-expiration timestamp for `key`, when present.
///
/// Time Complexity: O(1) expected.
///
/// Allocator: Does not allocate.
pub fn get_expire_at(shard: *const runtime_shard.Shard, key: []const u8) ?i64 {
    return shard.ttl_index.get(key);
}

/// Removes TTL metadata for `key` and frees owned key bytes when present.
///
/// Time Complexity: O(1) expected.
///
/// Allocator: Does not allocate; frees TTL-owned key bytes through `shard.base_allocator`.
pub fn clear_ttl_entry(shard: *runtime_shard.Shard, key: []const u8) void {
    if (shard.ttl_index.fetchRemove(key)) |removed| {
        shard.base_allocator.free(removed.key);
    }
}

/// Sets absolute expiration for `key`, updating an existing entry or inserting new owned key bytes.
///
/// Time Complexity: O(1) expected, plus O(k) key duplication on insert.
///
/// Allocator: Uses `shard.base_allocator` for owned key bytes and TTL map growth.
pub fn set_ttl_entry(shard: *runtime_shard.Shard, key: []const u8, expire_at: i64) @import("std").mem.Allocator.Error!void {
    if (shard.ttl_index.getEntry(key)) |entry| {
        entry.value_ptr.* = expire_at;
        return;
    }

    const owned_key = try shard.base_allocator.dupe(u8, key);
    errdefer shard.base_allocator.free(owned_key);
    try shard.ttl_index.put(shard.base_allocator, owned_key, expire_at);
}

test "set_ttl_entry inserts then updates the same key" {
    const testing = @import("std").testing;

    var shard = runtime_shard.Shard.init(testing.allocator);
    defer shard.deinit();

    try set_ttl_entry(&shard, "ttl:key", 10);
    try testing.expectEqual(@as(?i64, 10), get_expire_at(&shard, "ttl:key"));

    try set_ttl_entry(&shard, "ttl:key", 20);
    try testing.expectEqual(@as(?i64, 20), get_expire_at(&shard, "ttl:key"));
}

test "clear_ttl_entry removes metadata and frees owned key bytes" {
    const testing = @import("std").testing;

    var shard = runtime_shard.Shard.init(testing.allocator);
    defer shard.deinit();

    try set_ttl_entry(&shard, "ttl:key", 10);
    clear_ttl_entry(&shard, "ttl:key");
    try testing.expect(get_expire_at(&shard, "ttl:key") == null);
}
