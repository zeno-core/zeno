//! Runtime-owned shard state for locks and ART-backed plain key-value storage.
//! Cost: O(1) initialization plus O(t + a) teardown over retained TTL keys and committed delta arenas.
//! Allocator: Uses the engine base allocator for shard-owned TTL keys, one long-lived ART arena, and committed batch delta arenas.

const std = @import("std");
const art = @import("../index/art/tree.zig");
const art_node = @import("../index/art/node.zig");
const runtime_visibility = @import("visibility.zig");

/// Number of shards in the runtime execution state.
pub const NUM_SHARDS: usize = 256;

/// Extracts the first non-empty hash tag from `key` using `{tag}` syntax.
///
/// Time Complexity: O(n), where `n` is `key.len`.
///
/// Allocator: Does not allocate.
pub fn extractHashTag(key: []const u8) ?[]const u8 {
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
pub fn getShardIndex(key: []const u8) usize {
    const hash_bytes = extractHashTag(key) orelse key;
    const hash = std.hash.Wyhash.hash(0, hash_bytes);
    return @as(usize, @intCast(hash & (NUM_SHARDS - 1)));
}

/// Returns the current unix timestamp in seconds.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn unixNow(io: std.Io) i64 {
    return @intCast(@divFloor(std.Io.Clock.real.now(io).nanoseconds, std.time.ns_per_s));
}

/// One committed batch delta arena linked into shard lifetime after a successful apply.
pub const CommittedArena = struct {
    arena: std.heap.ArenaAllocator,
    next: ?*CommittedArena = null,
};

/// One shard of runtime state owned by the engine.
pub const Shard = struct {
    io: std.Io,
    lock: std.Io.RwLock align(64) = .init,
    visibility_gate: runtime_visibility.VisibilityGate align(64),
    seq: std.atomic.Value(u64) align(64) = .init(0),
    base_allocator: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    committed_arenas_head: ?*CommittedArena = null,
    committed_arenas_tail: ?*CommittedArena = null,
    ttl_index: std.StringHashMapUnmanaged(i64) = .{},
    has_ttl_entries: bool = false,
    tree: art.Tree,

    /// Rebinds the tree allocator interface to the shard's current arena owner.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Not thread-safe; caller must already have exclusive access to the shard.
    pub fn rebindTreeAllocator(self: *Shard) void {
        self.tree.allocator = self.arena.allocator();
    }

    /// Initializes one shard-local runtime state container.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Initializes one shard-local arena and empty TTL index while storing `base_allocator` for future committed-delta ownership.
    ///
    /// Ownership: Borrows `io`; its provider must outlive this shard.
    ///
    /// Thread Safety: Must be called before the shard is shared across threads.
    pub fn init(io: std.Io, base_allocator: std.mem.Allocator) Shard {
        const arena = std.heap.ArenaAllocator.init(base_allocator);
        var shard: Shard = .{
            .io = io,
            .visibility_gate = .{ .io = io },
            .base_allocator = base_allocator,
            .arena = arena,
            .committed_arenas_head = null,
            .committed_arenas_tail = null,
            .ttl_index = .{},
            .has_ttl_entries = false,
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
    pub fn appendCommittedArena(self: *Shard, committed: *CommittedArena) void {
        committed.next = null;
        if (self.committed_arenas_tail) |tail| {
            tail.next = committed;
        } else {
            self.committed_arenas_head = committed;
        }
        self.committed_arenas_tail = committed;
    }

    fn releaseCommittedArenas(self: *Shard) void {
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

    fn releaseHeapOwnedValuesUnlocked(self: *Shard) void {
        const Context = struct {
            shard: *Shard,

            fn visit(ctx_ptr: *anyopaque, key: []const u8, _: *const @import("../types/value.zig").Value) !void {
                const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
                const leaf = ctx.shard.tree.findLeafForExactKey(key) orelse return;
                if (leaf.value_owner != art_node.ValueOwner.heap_allocation) return;
                leaf.value.deinit(ctx.shard.base_allocator);
                ctx.shard.base_allocator.destroy(leaf.value);
                leaf.value_owner = .tree_allocator;
            }
        };

        var ctx = Context{ .shard = self };
        _ = self.tree.forEach(&ctx, Context.visit) catch unreachable;
    }

    fn releaseStorageUnlocked(self: *Shard) void {
        self.releaseHeapOwnedValuesUnlocked();
        var ttl_iterator = self.ttl_index.iterator();
        while (ttl_iterator.next()) |entry| {
            self.base_allocator.free(entry.key_ptr.*);
        }
        self.ttl_index.deinit(self.base_allocator);
        self.has_ttl_entries = false;
        self.releaseCommittedArenas();
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
        self.releaseStorageUnlocked();
        self.* = undefined;
    }

    /// Clears all shard contents while retaining arena capacity for reuse.
    ///
    /// Time Complexity: O(t + a), where `t` is retained TTL entry count and `a` is committed delta-arena chain length.
    ///
    /// Allocator: Does not allocate; frees TTL metadata, releases committed arenas, and resets the primary shard arena with retained capacity.
    ///
    /// Thread Safety: Not thread-safe; caller must already hold exclusive shard ownership or otherwise prove no concurrent access.
    pub fn resetUnlocked(self: *Shard) void {
        self.releaseHeapOwnedValuesUnlocked();
        var ttl_iterator = self.ttl_index.iterator();
        while (ttl_iterator.next()) |entry| {
            self.base_allocator.free(entry.key_ptr.*);
        }
        self.ttl_index.clearRetainingCapacity();
        self.has_ttl_entries = false;
        self.releaseCommittedArenas();
        _ = self.arena.reset(.retain_capacity);
        @atomicStore(usize, &self.tree.root, art_node.nodeEmpty(), .release);
        self.tree.allocator = self.arena.allocator();
    }

    /// Replaces one shard's storage ownership while preserving the existing lock instance.
    ///
    /// Time Complexity: O(t + a), where `t` is retained TTL entry count and `a` is committed delta-arena chain length in the previous shard contents.
    ///
    /// Allocator: Does not allocate; releases old storage and takes ownership of `arena`, `ttl_index`, and `tree`.
    ///
    /// Thread Safety: Not thread-safe; caller must already hold exclusive shard ownership.
    pub fn replaceStorageUnlocked(
        self: *Shard,
        arena: std.heap.ArenaAllocator,
        ttl_index: std.StringHashMapUnmanaged(i64),
        tree: art.Tree,
    ) void {
        self.releaseStorageUnlocked();
        self.arena = arena;
        self.committed_arenas_head = null;
        self.committed_arenas_tail = null;
        self.ttl_index = ttl_index;
        self.has_ttl_entries = self.ttl_index.count() != 0;
        self.tree = tree;
        self.tree.allocator = self.arena.allocator();
    }
};

test "get_shard_index is stable for the same hash tag" {
    const testing = std.testing;

    const a = getShardIndex("{user:1}\x00name");
    const b = getShardIndex("{user:1}\x00email");

    try testing.expectEqual(a, b);
    try testing.expect(a < NUM_SHARDS);
}

test "unix_now returns a recent unix timestamp" {
    const testing = std.testing;

    const now = unixNow(std.testing.io);
    try testing.expect(now > 0);
}

test "unix_now preserves floor rounding on both sides of the epoch" {
    const testing = std.testing;
    var nanoseconds: i96 = -1;
    var vtable = std.Io.failing.vtable.*;
    vtable.now = struct {
        fn now(userdata: ?*anyopaque, clock: std.Io.Clock) std.Io.Timestamp {
            std.debug.assert(clock == .real);
            const value: *const i96 = @ptrCast(@alignCast(userdata.?));
            return .fromNanoseconds(value.*);
        }
    }.now;
    const io = std.Io{ .userdata = &nanoseconds, .vtable = &vtable };
    try testing.expectEqual(@as(i64, -1), unixNow(io));
    nanoseconds = -std.time.ns_per_s - 1;
    try testing.expectEqual(@as(i64, -2), unixNow(io));
    nanoseconds = std.time.ns_per_s - 1;
    try testing.expectEqual(@as(i64, 0), unixNow(io));
    nanoseconds = std.time.ns_per_s;
    try testing.expectEqual(@as(i64, 1), unixNow(io));
}

test "reset_unlocked leaves the shard empty and reusable" {
    const testing = std.testing;
    const Value = @import("../types/value.zig").Value;

    var shard = Shard.init(std.testing.io, testing.allocator);
    defer shard.deinit();
    shard.rebindTreeAllocator();

    const allocator = shard.arena.allocator();
    const value = try allocator.create(Value);
    value.* = .{ .integer = 1 };
    try shard.tree.insert(try allocator.dupe(u8, "alpha"), value);
    try shard.ttl_index.put(testing.allocator, try testing.allocator.dupe(u8, "alpha"), 10);
    shard.has_ttl_entries = true;

    shard.resetUnlocked();

    try testing.expect(shard.tree.lookup("alpha") == null);
    try testing.expectEqual(@as(usize, 0), shard.ttl_index.count());
    try testing.expect(!shard.has_ttl_entries);

    const reused_value = try shard.arena.allocator().create(Value);
    reused_value.* = .{ .integer = 2 };
    try shard.tree.insert(try shard.arena.allocator().dupe(u8, "beta"), reused_value);
    try testing.expectEqual(@as(i64, 2), shard.tree.lookup("beta").?.integer);
}
