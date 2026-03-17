//! Internal TTL bookkeeping helpers shared by runtime and expiration paths.
//! Cost: O(1) expected TTL map operations plus O(k) key duplication when inserting one new entry.
//! Allocator: Uses the shard base allocator for owned TTL-key storage.

const std = @import("std");
const runtime_shard = @import("../runtime/shard.zig");

/// One prepared TTL insertion that can publish without allocating after WAL append.
///
/// Ownership: Owns `owned_key` until `deinit` or `apply_prepared_set_ttl_entry_unlocked` consumes it.
pub const PreparedSetTtlEntry = struct {
    owned_key: ?[]u8 = null,

    /// Releases any prepared TTL key bytes that were not transferred into shard state.
    ///
    /// Time Complexity: O(k), where `k` is `owned_key.?.len` when present.
    ///
    /// Allocator: Does not allocate; frees through `allocator`.
    pub fn deinit(self: *PreparedSetTtlEntry, allocator: std.mem.Allocator) void {
        if (self.owned_key) |owned_key| allocator.free(owned_key);
        self.* = undefined;
    }
};

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
        shard.has_ttl_entries = shard.ttl_index.count() != 0;
    }
}

/// Prepares shard-local ownership for one future TTL insert without publishing it yet.
///
/// Time Complexity: O(1) expected, plus O(k) key duplication when the key is not already present in the TTL map.
///
/// Allocator: Uses `shard.base_allocator` for TTL-map growth and owned key bytes when insertion is required.
pub fn prepare_set_ttl_entry(shard: *runtime_shard.Shard, key: []const u8) std.mem.Allocator.Error!PreparedSetTtlEntry {
    if (shard.ttl_index.contains(key)) return .{};

    try shard.ttl_index.ensureUnusedCapacity(shard.base_allocator, 1);
    return .{
        .owned_key = try shard.base_allocator.dupe(u8, key),
    };
}

/// Publishes one prepared TTL update without allocating after WAL append.
///
/// Time Complexity: O(1) expected.
///
/// Allocator: Does not allocate; assumes any required TTL-map capacity and owned key bytes were prepared earlier.
pub fn apply_prepared_set_ttl_entry_unlocked(
    shard: *runtime_shard.Shard,
    key: []const u8,
    expire_at: i64,
    prepared: PreparedSetTtlEntry,
) void {
    if (shard.ttl_index.getEntry(key)) |entry| {
        if (prepared.owned_key) |owned_key| shard.base_allocator.free(owned_key);
        entry.value_ptr.* = expire_at;
        shard.has_ttl_entries = true;
        return;
    }

    std.debug.assert(prepared.owned_key != null);
    shard.ttl_index.putAssumeCapacityNoClobber(prepared.owned_key.?, expire_at);
    shard.has_ttl_entries = true;
}

/// Sets absolute expiration for `key`, updating an existing entry or inserting new owned key bytes.
///
/// Time Complexity: O(1) expected, plus O(k) key duplication on insert.
///
/// Allocator: Uses `shard.base_allocator` for owned key bytes and TTL map growth.
pub fn set_ttl_entry(shard: *runtime_shard.Shard, key: []const u8, expire_at: i64) std.mem.Allocator.Error!void {
    if (shard.ttl_index.getEntry(key)) |entry| {
        entry.value_ptr.* = expire_at;
        shard.has_ttl_entries = true;
        return;
    }

    const owned_key = try shard.base_allocator.dupe(u8, key);
    errdefer shard.base_allocator.free(owned_key);
    try shard.ttl_index.put(shard.base_allocator, owned_key, expire_at);
    shard.has_ttl_entries = true;
}

test "set_ttl_entry inserts then updates the same key" {
    const testing = std.testing;

    var shard = runtime_shard.Shard.init(testing.allocator);
    defer shard.deinit();

    try set_ttl_entry(&shard, "ttl:key", 10);
    try testing.expectEqual(@as(?i64, 10), get_expire_at(&shard, "ttl:key"));

    try set_ttl_entry(&shard, "ttl:key", 20);
    try testing.expectEqual(@as(?i64, 20), get_expire_at(&shard, "ttl:key"));
}

test "clear_ttl_entry removes metadata and frees owned key bytes" {
    const testing = std.testing;

    var shard = runtime_shard.Shard.init(testing.allocator);
    defer shard.deinit();

    try set_ttl_entry(&shard, "ttl:key", 10);
    clear_ttl_entry(&shard, "ttl:key");
    try testing.expect(get_expire_at(&shard, "ttl:key") == null);
}

test "prepared ttl entry publishes without allocating" {
    const testing = std.testing;

    var shard = runtime_shard.Shard.init(testing.allocator);
    defer shard.deinit();

    const prepared = try prepare_set_ttl_entry(&shard, "ttl:key");
    apply_prepared_set_ttl_entry_unlocked(&shard, "ttl:key", 10, prepared);

    try testing.expectEqual(@as(?i64, 10), get_expire_at(&shard, "ttl:key"));
}
