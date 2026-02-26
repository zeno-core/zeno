//! Runtime-owned shard state for locks and long-lived plain key-value storage.
//! Cost: O(1) initialization plus O(n) teardown over retained keys and nested values.
//! Allocator: Uses the engine base allocator for owned key bytes, nested value storage, and shard-local TTL metadata.

const std = @import("std");
const types = @import("../types.zig");

/// Number of shards in the runtime execution state.
pub const NUM_SHARDS: usize = 64;

/// Extracts the first non-empty hash tag from `key` using `{tag}` syntax.
///
/// Time Complexity: O(n^2), where `n` is `key.len` in the worst case because unmatched braces continue the scan.
///
/// Allocator: Does not allocate.
pub fn extract_hash_tag(key: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i < key.len) : (i += 1) {
        if (key[i] != '{') continue;

        var j: usize = i + 1;
        while (j < key.len) : (j += 1) {
            if (key[j] == '}') {
                if (j > i + 1) return key[i + 1 .. j];
                break;
            }
        }
    }
    return null;
}

/// Computes the runtime shard index for one key.
///
/// Time Complexity: O(n^2), where `n` is `key.len`, because hash-tag extraction may rescan bytes before hashing.
///
/// Allocator: Does not allocate.
pub fn get_shard_index(key: []const u8) usize {
    const hash_bytes = extract_hash_tag(key) orelse key;
    const hash = std.hash.Wyhash.hash(0, hash_bytes);
    return @as(usize, @intCast(hash & (NUM_SHARDS - 1)));
}

/// Returns the current unix timestamp in seconds.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn unix_now() i64 {
    return std.time.timestamp();
}

/// One shard of runtime state owned by the engine.
pub const Shard = struct {
    lock: std.Thread.RwLock = .{},
    base_allocator: std.mem.Allocator,
    values: std.StringHashMapUnmanaged(types.Value) = .{},
    ttl_index: std.StringHashMapUnmanaged(i64) = .{},
    committed_batch_count: usize = 0,

    /// Initializes one shard-local runtime state container.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate during initialization; stores `base_allocator` for future owned key and value storage.
    ///
    /// Thread Safety: Must be called before the shard is shared across threads.
    pub fn init(base_allocator: std.mem.Allocator) Shard {
        return .{
            .base_allocator = base_allocator,
        };
    }

    /// Releases shard-local runtime resources.
    ///
    /// Time Complexity: O(n + t + b), where `n` is retained key count, `t` is retained TTL entry count, and `b` is total bytes owned by nested values.
    ///
    /// Allocator: Does not allocate; frees owned keys, nested values, and TTL metadata through `base_allocator`.
    ///
    /// Thread Safety: Not thread-safe; caller must ensure exclusive ownership.
    pub fn deinit(self: *Shard) void {
        var ttl_iterator = self.ttl_index.iterator();
        while (ttl_iterator.next()) |entry| {
            self.base_allocator.free(entry.key_ptr.*);
        }
        self.ttl_index.deinit(self.base_allocator);

        var iterator = self.values.iterator();
        while (iterator.next()) |entry| {
            self.base_allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.base_allocator);
        }
        self.values.deinit(self.base_allocator);
        self.* = undefined;
    }
};

test "get_shard_index is stable for the same hash tag" {
    const testing = std.testing;

    const a = get_shard_index("{user:1}\x00name");
    const b = get_shard_index("{user:1}\x00email");

    try testing.expectEqual(a, b);
    try testing.expect(a < NUM_SHARDS);
}

test "unix_now returns a recent unix timestamp" {
    const testing = std.testing;

    const now = unix_now();
    try testing.expect(now > 0);
}
