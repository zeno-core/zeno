//! Runtime-owned shard state for locks and ART-backed plain key-value storage.
//! Cost: O(1) initialization plus O(t + a) teardown over retained TTL keys and committed delta arenas.
//! Allocator: Uses the engine base allocator for shard-owned TTL keys, one long-lived ART arena, and committed batch delta arenas.

const std = @import("std");
const art = @import("../index/art/tree.zig");

/// Number of shards in the runtime execution state.
pub const NUM_SHARDS: usize = 64;

/// Extracts the first non-empty hash tag from `key` using `{tag}` syntax.
///
/// Time Complexity: O(n), where `n` is `key.len`.
///
/// Allocator: Does not allocate.
pub fn extract_hash_tag(key: []const u8) ?[]const u8 {
    var tag_start: ?usize = null;
    for (key, 0..) |byte, index| {
        switch (byte) {
            '{' => {
                if (tag_start == null) tag_start = index + 1;
            },
            '}' => if (tag_start) |start| {
                if (index > start) return key[start..index];
                tag_start = null;
            },
            else => {},
        }
    }
    return null;
}

/// Computes the runtime shard index for one key.
///
/// Time Complexity: O(n), where `n` is `key.len`, including hash-tag extraction and hashing.
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

/// One committed batch delta arena linked into shard lifetime after a successful apply.
pub const CommittedArena = struct {
    arena: std.heap.ArenaAllocator,
    next: ?*CommittedArena = null,
};

/// One shard of runtime state owned by the engine.
pub const Shard = struct {
    lock: std.Thread.RwLock = .{},
    base_allocator: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    committed_arenas_head: ?*CommittedArena = null,
    committed_arenas_tail: ?*CommittedArena = null,
    ttl_index: std.StringHashMapUnmanaged(i64) = .{},
    tree: art.Tree,

    /// Rebinds the tree allocator interface to the shard's current arena owner.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Not thread-safe; caller must already have exclusive access to the shard.
    pub fn rebind_tree_allocator(self: *Shard) void {
        self.tree.allocator = self.arena.allocator();
    }

    /// Initializes one shard-local runtime state container.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Initializes one shard-local arena and empty TTL index while storing `base_allocator` for future committed-delta ownership.
    ///
    /// Thread Safety: Must be called before the shard is shared across threads.
    pub fn init(base_allocator: std.mem.Allocator) Shard {
        const arena = std.heap.ArenaAllocator.init(base_allocator);
        var shard: Shard = .{
            .base_allocator = base_allocator,
            .arena = arena,
            .committed_arenas_head = null,
            .committed_arenas_tail = null,
            .ttl_index = .{},
            .tree = undefined,
        };
        shard.tree = art.Tree.init(shard.arena.allocator());
        return shard;
    }

    /// Links one successfully applied batch delta arena into shard lifetime.
    ///
    /// Time Complexity: O(1).
    ///
    /// Thread Safety: Not thread-safe; caller must already hold exclusive shard ownership.
    pub fn append_committed_arena(self: *Shard, committed: *CommittedArena) void {
        committed.next = null;
        if (self.committed_arenas_tail) |tail| {
            tail.next = committed;
        } else {
            self.committed_arenas_head = committed;
        }
        self.committed_arenas_tail = committed;
    }

    fn release_committed_arenas(self: *Shard) void {
        var current = self.committed_arenas_head;
        while (current) |node| {
            const next = node.next;
            node.arena.deinit();
            self.base_allocator.destroy(node);
            current = next;
        }
        self.committed_arenas_head = null;
        self.committed_arenas_tail = null;
    }

    fn release_storage_unlocked(self: *Shard) void {
        var ttl_iterator = self.ttl_index.iterator();
        while (ttl_iterator.next()) |entry| {
            self.base_allocator.free(entry.key_ptr.*);
        }
        self.ttl_index.deinit(self.base_allocator);
        self.release_committed_arenas();
        self.arena.deinit();
    }

    /// Releases shard-local runtime resources.
    ///
    /// Time Complexity: O(t + a), where `t` is retained TTL entry count and `a` is committed delta-arena chain length.
    ///
    /// Allocator: Does not allocate; frees TTL metadata and tears down arena-owned ART storage through `base_allocator`.
    ///
    /// Thread Safety: Not thread-safe; caller must ensure exclusive ownership.
    pub fn deinit(self: *Shard) void {
        self.release_storage_unlocked();
        self.* = undefined;
    }

    /// Clears all shard contents while retaining arena capacity for reuse.
    ///
    /// Time Complexity: O(t + a), where `t` is retained TTL entry count and `a` is committed delta-arena chain length.
    ///
    /// Allocator: Does not allocate; frees TTL metadata, releases committed arenas, and resets the primary shard arena with retained capacity.
    ///
    /// Thread Safety: Not thread-safe; caller must already hold exclusive shard ownership or otherwise prove no concurrent access.
    pub fn reset_unlocked(self: *Shard) void {
        var ttl_iterator = self.ttl_index.iterator();
        while (ttl_iterator.next()) |entry| {
            self.base_allocator.free(entry.key_ptr.*);
        }
        self.ttl_index.clearRetainingCapacity();
        self.release_committed_arenas();
        _ = self.arena.reset(.retain_capacity);
        self.tree.root = .{ .empty = {} };
        self.tree.allocator = self.arena.allocator();
    }

    /// Replaces one shard's storage ownership while preserving the existing lock instance.
    ///
    /// Time Complexity: O(t + a), where `t` is retained TTL entry count and `a` is committed delta-arena chain length in the previous shard contents.
    ///
    /// Allocator: Does not allocate; releases old storage and takes ownership of `arena`, `ttl_index`, and `tree`.
    ///
    /// Thread Safety: Not thread-safe; caller must already hold exclusive shard ownership.
    pub fn replace_storage_unlocked(
        self: *Shard,
        arena: std.heap.ArenaAllocator,
        ttl_index: std.StringHashMapUnmanaged(i64),
        tree: art.Tree,
    ) void {
        self.release_storage_unlocked();
        self.arena = arena;
        self.committed_arenas_head = null;
        self.committed_arenas_tail = null;
        self.ttl_index = ttl_index;
        self.tree = tree;
        self.tree.allocator = self.arena.allocator();
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

test "reset_unlocked leaves the shard empty and reusable" {
    const testing = std.testing;
    const Value = @import("../types/value.zig").Value;

    var shard = Shard.init(testing.allocator);
    defer shard.deinit();
    shard.rebind_tree_allocator();

    const allocator = shard.arena.allocator();
    const value = try allocator.create(Value);
    value.* = .{ .integer = 1 };
    try shard.tree.insert(try allocator.dupe(u8, "alpha"), value);
    try shard.ttl_index.put(testing.allocator, try testing.allocator.dupe(u8, "alpha"), 10);

    shard.reset_unlocked();

    try testing.expect(shard.tree.lookup("alpha") == null);
    try testing.expectEqual(@as(usize, 0), shard.ttl_index.count());

    const reused_value = try shard.arena.allocator().create(Value);
    reused_value.* = .{ .integer = 2 };
    try shard.tree.insert(try shard.arena.allocator().dupe(u8, "beta"), reused_value);
    try testing.expectEqual(@as(i64, 2), shard.tree.lookup("beta").?.integer);
}
