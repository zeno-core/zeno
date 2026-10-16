//! Internal TTL bookkeeping helpers shared by runtime and expiration paths.
//! Cost: O(1) expected TTL map operations plus O(k) key duplication when inserting one new entry.
//! Allocator: Uses the shard base allocator for owned TTL-key storage.

const std = @import("std");
const runtime_shard = @import("../runtime/shard.zig");

/// One prepared TTL insertion that can publish without allocating after WAL append.
///
/// Ownership: Owns `owned_key` until `deinit` or `applyPreparedSetTtlEntryUnlocked` consumes it.
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

/// Returns whether `expireAt` is at or before `now`.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn isExpired(expireAt: i64, now: i64) bool {
    return expireAt <= now;
}

/// Returns the stored absolute-expiration timestamp for `key`, when present.
///
/// Time Complexity: O(1) expected.
///
/// Allocator: Does not allocate.
pub fn getExpireAt(shard: *const runtime_shard.Shard, key: []const u8) ?i64 {
    return shard.ttl_index.get(key);
}

/// Removes TTL metadata for `key` and frees owned key bytes when present.
///
/// Time Complexity: O(1) expected.
///
/// Allocator: Does not allocate; frees TTL-owned key bytes through `shard.base_allocator`.
pub fn clearTtlEntry(shard: *runtime_shard.Shard, key: []const u8) void {
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
pub fn prepareSetTtlEntry(shard: *runtime_shard.Shard, key: []const u8) std.mem.Allocator.Error!PreparedSetTtlEntry {
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
pub fn applyPreparedSetTtlEntryUnlocked(
    shard: *runtime_shard.Shard,
    key: []const u8,
    expireAt: i64,
    prepared: PreparedSetTtlEntry,
) void {
    if (shard.ttl_index.getEntry(key)) |entry| {
        if (prepared.owned_key) |owned_key| shard.base_allocator.free(owned_key);
        entry.value_ptr.* = expireAt;
        shard.has_ttl_entries = true;
        return;
    }

    std.debug.assert(prepared.owned_key != null);
    shard.ttl_index.putAssumeCapacityNoClobber(prepared.owned_key.?, expireAt);
    shard.has_ttl_entries = true;
}

/// Sets absolute expiration for `key`, updating an existing entry or inserting new owned key bytes.
///
/// Time Complexity: O(1) expected, plus O(k) key duplication on insert.
///
/// Allocator: Uses `shard.base_allocator` for owned key bytes and TTL map growth.
pub fn setTtlEntry(shard: *runtime_shard.Shard, key: []const u8, expireAt: i64) std.mem.Allocator.Error!void {
    if (shard.ttl_index.getEntry(key)) |entry| {
        entry.value_ptr.* = expireAt;
        shard.has_ttl_entries = true;
        return;
    }

    const owned_key = try shard.base_allocator.dupe(u8, key);
    errdefer shard.base_allocator.free(owned_key);
    try shard.ttl_index.put(shard.base_allocator, owned_key, expireAt);
    shard.has_ttl_entries = true;
}

test "set_ttl_entry inserts then updates the same key" {
    const testing = std.testing;

    var shard = runtime_shard.Shard.init(std.testing.io, testing.allocator);
    defer shard.deinit();

    try setTtlEntry(&shard, "ttl:key", 10);
    try testing.expectEqual(@as(?i64, 10), getExpireAt(&shard, "ttl:key"));

    try setTtlEntry(&shard, "ttl:key", 20);
    try testing.expectEqual(@as(?i64, 20), getExpireAt(&shard, "ttl:key"));
}

test "clear_ttl_entry removes metadata and frees owned key bytes" {
    const testing = std.testing;

    var shard = runtime_shard.Shard.init(std.testing.io, testing.allocator);
    defer shard.deinit();

    try setTtlEntry(&shard, "ttl:key", 10);
    clearTtlEntry(&shard, "ttl:key");
    try testing.expect(getExpireAt(&shard, "ttl:key") == null);
}

test "prepared ttl entry publishes without allocating" {
    const testing = std.testing;

    var shard = runtime_shard.Shard.init(std.testing.io, testing.allocator);
    defer shard.deinit();

    const prepared = try prepareSetTtlEntry(&shard, "ttl:key");
    applyPreparedSetTtlEntryUnlocked(&shard, "ttl:key", 10, prepared);

    try testing.expectEqual(@as(?i64, 10), getExpireAt(&shard, "ttl:key"));
}
