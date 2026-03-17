//! ART internal node layouts and bounded grow/shrink transitions.
//! Cost: Node-local queries are O(1); grow and shrink copy at most one bounded node payload.
//! Allocator: Uses explicit allocators only for node transitions that materialize a replacement node.

const std = @import("std");
const Value = @import("../../types/value.zig").Value;

/// Node types in the Adaptive Radix Tree.
pub const NodeType = enum(u8) {
    node4,
    node16,
    node48,
    node256,
};

/// Inline prefix-byte capacity stored in every ART internal-node header.
pub const MAX_PREFIX_LEN = 11;

/// Shared metadata stored at the front of every ART internal-node layout.
pub const NodeHeader = extern struct {
    num_children: u16 = 0,
    prefix_len: u16 = 0,
    node_type: NodeType,
    prefix: [MAX_PREFIX_LEN]u8 = undefined,
    leaf_value: ?*Leaf = null,

    /// Initializes one empty internal-node header for the requested node class.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    pub fn init(n_type: NodeType) NodeHeader {
        return .{
            .node_type = n_type,
            .leaf_value = null,
        };
    }

    /// Copies shared metadata (child count, compressed prefix, leaf value) from another header.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    pub fn copy_meta_from(self: *NodeHeader, src: *const NodeHeader) void {
        self.num_children = src.num_children;
        self.prefix_len = src.prefix_len;
        self.prefix = src.prefix;
        self.store_leaf_value(src.load_leaf_value());
    }

    pub inline fn load_leaf_value(self: *const NodeHeader) ?*Leaf {
        const raw = @atomicLoad(usize, @as(*const usize, @ptrCast(&self.leaf_value)), .acquire);
        if (raw == 0) return null;
        return @ptrFromInt(raw);
    }

    pub inline fn store_leaf_value(self: *NodeHeader, leaf: ?*Leaf) void {
        const raw: usize = if (leaf) |l| @intFromPtr(l) else 0;
        @atomicStore(usize, @as(*usize, @ptrCast(&self.leaf_value)), raw, .monotonic);
    }

    /// Finds a child pointer by transition byte for any internal-node class.
    ///
    /// Time Complexity: O(1) bounded by node class implementation.
    ///
    /// Allocator: Does not allocate.
    pub fn find_child(self: *const NodeHeader, key_byte: u8) ?*Node {
        return switch (self.node_type) {
            .node4 => @as(*const Node4, @alignCast(@fieldParentPtr("header", self))).find_child(key_byte),
            .node16 => @as(*const Node16, @alignCast(@fieldParentPtr("header", self))).find_child(key_byte),
            .node48 => @as(*const Node48, @alignCast(@fieldParentPtr("header", self))).find_child(key_byte),
            .node256 => @as(*const Node256, @alignCast(@fieldParentPtr("header", self))).find_child(key_byte),
        };
    }

    /// Returns the first present child in lexicographic edge-byte order.
    ///
    /// Time Complexity: O(1) for Node4/Node16 and bounded O(256) for Node48/Node256.
    ///
    /// Allocator: Does not allocate.
    pub fn first_child(self: *const NodeHeader) *const Node {
        switch (self.node_type) {
            .node4 => {
                const n4 = @as(*const Node4, @alignCast(@fieldParentPtr("header", self)));
                std.debug.assert(n4.header.num_children > 0);
                return &n4.children[0];
            },
            .node16 => {
                const n16 = @as(*const Node16, @alignCast(@fieldParentPtr("header", self)));
                std.debug.assert(n16.header.num_children > 0);
                return &n16.children[0];
            },
            .node48 => {
                const n48 = @as(*const Node48, @alignCast(@fieldParentPtr("header", self)));
                for (0..256) |i| {
                    if (n48.child_index[i] != Node48.EMPTY_INDEX) {
                        return &n48.children[n48.child_index[i]];
                    }
                }
                unreachable;
            },
            .node256 => {
                const n256 = @as(*const Node256, @alignCast(@fieldParentPtr("header", self)));
                for (0..256) |i| {
                    if (!node_is_empty(n256.children[i])) {
                        return &n256.children[i];
                    }
                }
                unreachable;
            },
        }
    }

    /// Visits each present child in lexicographic edge-byte order.
    ///
    /// The callback returns `true` to continue iteration and `false` to stop early.
    ///
    /// Time Complexity: O(children) for Node4/Node16 and bounded O(256) for Node48/Node256.
    ///
    /// Allocator: Does not allocate.
    pub fn for_each_child(
        self: *const NodeHeader,
        comptime Ctx: type,
        ctx: Ctx,
        comptime callback: fn (Ctx, u8, *const Node) anyerror!bool,
    ) !void {
        switch (self.node_type) {
            .node4 => {
                const n4 = @as(*const Node4, @alignCast(@fieldParentPtr("header", self)));
                for (0..n4.header.num_children) |i| {
                    if (!try callback(ctx, n4.keys[i], &n4.children[i])) return;
                }
            },
            .node16 => {
                const n16 = @as(*const Node16, @alignCast(@fieldParentPtr("header", self)));
                for (0..n16.header.num_children) |i| {
                    if (!try callback(ctx, n16.keys[i], &n16.children[i])) return;
                }
            },
            .node48 => {
                const n48 = @as(*const Node48, @alignCast(@fieldParentPtr("header", self)));
                for (0..256) |b| {
                    const idx = n48.child_index[b];
                    if (idx != Node48.EMPTY_INDEX) {
                        if (!try callback(ctx, @intCast(b), &n48.children[idx])) return;
                    }
                }
            },
            .node256 => {
                const n256 = @as(*const Node256, @alignCast(@fieldParentPtr("header", self)));
                for (0..256) |b| {
                    if (!node_is_empty(n256.children[b])) {
                        if (!try callback(ctx, @intCast(b), &n256.children[b])) return;
                    }
                }
            },
        }
    }
};

/// Ownership class for one leaf's current value pointer.
pub const ValueOwner = enum(u8) {
    tree_allocator,
    committed_arena,
    heap_allocation,
};

/// A Leaf node stores the full key and the value pointer.
/// Storing the key is essential for splitting leaves during path collisions.
pub const Leaf = struct {
    key: []const u8,
    value: *Value,
    value_owner: ValueOwner = .tree_allocator,

    pub inline fn load_value(self: *const Leaf) *Value {
        return @atomicLoad(*Value, &self.value, .acquire);
    }

    pub inline fn store_value(self: *Leaf, v: *Value) void {
        @atomicStore(*Value, &self.value, v, .monotonic);
    }
};

/// ART node encoded as a tagged pointer (usize).
/// Bottom 2 bits encode the node type:
///   0 = empty, 1 = internal (*NodeHeader), 2 = leaf (*Leaf)
pub const Node = usize;

pub const TAG_EMPTY: usize = 0;
pub const TAG_INTERNAL: usize = 1;
pub const TAG_LEAF: usize = 2;
pub const TAG_MASK: usize = 3;
pub const PTR_MASK: usize = ~@as(usize, 3);

pub const DecodedNode = union(enum) {
    empty: void,
    internal: *NodeHeader,
    leaf: *Leaf,
};

pub inline fn node_empty() Node {
    return 0;
}

pub inline fn node_internal(h: *NodeHeader) Node {
    return @intFromPtr(h) | TAG_INTERNAL;
}

pub inline fn node_leaf(l: *Leaf) Node {
    return @intFromPtr(l) | TAG_LEAF;
}

pub inline fn node_tag(n: Node) usize {
    return n & TAG_MASK;
}

pub inline fn node_is_empty(n: Node) bool {
    return n == 0;
}

pub inline fn node_is_leaf(n: Node) bool {
    return (n & TAG_MASK) == TAG_LEAF;
}

pub inline fn node_is_internal(n: Node) bool {
    return (n & TAG_MASK) == TAG_INTERNAL;
}

pub inline fn node_to_internal(n: Node) *NodeHeader {
    return @ptrFromInt(n & PTR_MASK);
}

pub inline fn node_to_leaf(n: Node) *Leaf {
    return @ptrFromInt(n & PTR_MASK);
}

pub inline fn node_decode(n: Node) DecodedNode {
    return switch (node_tag(n)) {
        TAG_EMPTY => .{ .empty = {} },
        TAG_INTERNAL => .{ .internal = node_to_internal(n) },
        TAG_LEAF => .{ .leaf = node_to_leaf(n) },
        else => unreachable,
    };
}

/// The maximum child capacity for one ART node class.
/// Time Complexity: O(1).
/// Allocator: Does not allocate.
pub fn capacity_for(node_type: NodeType) u16 {
    return switch (node_type) {
        .node4 => 4,
        .node16 => 16,
        .node48 => 48,
        .node256 => 256,
    };
}

/// Returns the next node class after a grow transition.
/// Time Complexity: O(1).
/// Allocator: Does not allocate.
pub fn next_type(node_type: NodeType) ?NodeType {
    return switch (node_type) {
        .node4 => .node16,
        .node16 => .node48,
        .node48 => .node256,
        .node256 => null,
    };
}

/// Grows an ART node to the next size capacity.
/// This occurs when a node reaches its maximum child capacity (e.g., Node4 reaching 4 children).
/// The function allocates a larger node (e.g., Node16), migrates the keys and children
/// from the current node maintaining their order, and carefully redirects the internal
/// pointer without needing to alert the parent node, completing the mutation safely.
/// Time Complexity: O(1) overhead, copying up to 256 pointers.
/// Allocator: Allocates exactly one new node using the provided `allocator`. The old node becomes unreachable and is reclaimed with its arena.
pub fn grow(self: *Node, allocator: std.mem.Allocator) !void {
    if (!node_is_internal(self.*)) return error.InvalidNodeGrowth;
    const header = node_to_internal(self.*);
    switch (header.node_type) {
        .node4 => {
            const n4: *Node4 = @alignCast(@fieldParentPtr("header", header));
            const n16 = try allocator.create(Node16);
            n16.* = Node16.init();
            n16.header.copy_meta_from(&n4.header);

            for (0..n4.header.num_children) |i| {
                n16.keys[i] = n4.keys[i];
                n16.children[i] = n4.children[i];
            }
            @atomicStore(usize, self, node_internal(&n16.header), .monotonic);
        },
        .node16 => {
            const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", header)));
            const n48 = try allocator.create(Node48);
            n48.* = Node48.init();
            n48.header.copy_meta_from(&n16.header);

            for (0..n16.header.num_children) |i| {
                const k = n16.keys[i];
                n48.child_index[k] = @intCast(i);
                n48.children[i] = n16.children[i];
                n48.present |= (@as(u64, 1) << @intCast(i));
            }
            @atomicStore(usize, self, node_internal(&n48.header), .monotonic);
        },
        .node48 => {
            const n48 = @as(*Node48, @alignCast(@fieldParentPtr("header", header)));
            const n256 = try allocator.create(Node256);
            n256.* = Node256.init();
            n256.header.copy_meta_from(&n48.header);

            for (0..256) |i| {
                const idx = n48.child_index[i];
                if (idx != Node48.EMPTY_INDEX) {
                    n256.children[i] = n48.children[idx];
                }
            }
            @atomicStore(usize, self, node_internal(&n256.header), .monotonic);
        },
        .node256 => return error.TreeFull,
    }
}

/// Maps a new child into the current node at the given transition byte.
/// This method is responsible for keeping node arrays sorted `keys` in Node4/Node16
/// or manipulating the `child_index` bitmap in Node48. If the node is completely full,
/// it invokes `grow()` first and attempts insertion on the expanded architecture.
/// Time Complexity: O(1), max 16 byte comparisons or 1 constant-time bitwise ctz operation.
/// Allocator: May allocate symmetrically by calling `grow(allocator)`.
pub fn add_child(self: *Node, allocator: std.mem.Allocator, key_byte: u8, child: Node) !void {
    if (!node_is_internal(self.*)) return error.InvalidNodeType;
    const header = node_to_internal(self.*);
    switch (header.node_type) {
        .node4 => {
            const n4 = @as(*Node4, @alignCast(@fieldParentPtr("header", header)));
            const count = n4.header.num_children;
            if (count == 4) {
                try grow(self, allocator);
                return try add_child(self, allocator, key_byte, child);
            }
            var i: usize = 0;
            while (i < count) : (i += 1) {
                if (key_byte < n4.keys[i]) break;
            }
            var j: usize = count;
            while (j > i) : (j -= 1) {
                n4.keys[j] = n4.keys[j - 1];
                n4.children[j] = n4.children[j - 1];
            }
            n4.keys[i] = key_byte;
            n4.children[i] = child;
            @atomicStore(u16, &n4.header.num_children, count + 1, .monotonic);
        },
        .node16 => {
            const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", header)));
            const count = n16.header.num_children;
            if (count == 16) {
                try grow(self, allocator);
                return try add_child(self, allocator, key_byte, child);
            }
            var i: usize = 0;
            while (i < count) : (i += 1) {
                if (key_byte < n16.keys[i]) break;
            }
            var j: usize = count;
            while (j > i) : (j -= 1) {
                n16.keys[j] = n16.keys[j - 1];
                n16.children[j] = n16.children[j - 1];
            }
            n16.keys[i] = key_byte;
            n16.children[i] = child;
            @atomicStore(u16, &n16.header.num_children, count + 1, .monotonic);
        },
        .node48 => {
            const n48 = @as(*Node48, @alignCast(@fieldParentPtr("header", header)));
            if (n48.header.num_children == 48) {
                try grow(self, allocator);
                return try add_child(self, allocator, key_byte, child);
            }
            const pos = @ctz(~n48.present);
            std.debug.assert(pos < 48);
            n48.present |= (@as(u64, 1) << @intCast(pos));

            n48.children[pos] = child;
            @atomicStore(u8, &n48.child_index[key_byte], @intCast(pos), .monotonic);
            n48.header.num_children += 1;
        },
        .node256 => {
            const n256 = @as(*Node256, @alignCast(@fieldParentPtr("header", header)));
            @atomicStore(usize, &n256.children[key_byte], child, .monotonic);
            n256.header.num_children += 1;
        },
    }
}

/// Shrinks an ART node to a smaller size capacity or merges paths.
/// Triggered when a node loses children below critical thresholds (e.g., Node48 dropping <= 16).
/// If a `Node4` drops to just 1 child, we perform path compression merge.
/// Time Complexity: O(1), mapping or compressing up to 256 children arrays.
/// Allocator: Allocates exactly one smaller node using `allocator`. The old node becomes unreachable and is reclaimed with its arena.
pub fn shrink(self: *Node, allocator: std.mem.Allocator) !void {
    if (!node_is_internal(self.*)) return error.InvalidNodeShrink;
    const header = node_to_internal(self.*);
    switch (header.node_type) {
        .node4 => {
            const n4 = @as(*Node4, @alignCast(@fieldParentPtr("header", header)));
            if (n4.header.num_children == 0) {
                @atomicStore(usize, self, node_leaf(n4.header.load_leaf_value().?), .monotonic);
                return;
            }
            if (n4.header.num_children == 1 and n4.header.load_leaf_value() == null) {
                const child = n4.children[0];
                if (node_is_leaf(child)) {
                    @atomicStore(usize, self, child, .monotonic);
                } else {
                    const child_header = node_to_internal(child);
                    const combined_len = @as(usize, n4.header.prefix_len) + child_header.prefix_len + 1;

                    var new_prefix: [MAX_PREFIX_LEN]u8 = undefined;
                    var idx: usize = 0;
                    const prefix_length = @min(n4.header.prefix_len, MAX_PREFIX_LEN);
                    if (prefix_length > 0) {
                        @memcpy(new_prefix[idx .. idx + prefix_length], n4.header.prefix[0..prefix_length]);
                        idx += prefix_length;
                    }

                    if (idx < MAX_PREFIX_LEN) {
                        new_prefix[idx] = n4.keys[0];
                        idx += 1;

                        const child_prefix_length = @min(child_header.prefix_len, MAX_PREFIX_LEN - idx);
                        if (child_prefix_length > 0) {
                            @memcpy(new_prefix[idx .. idx + child_prefix_length], child_header.prefix[0..child_prefix_length]);
                        }
                    }

                    // Safety: We rewrite child prefix metadata before publishing `child` at `self`.
                    // Until the atomic store below, readers still reach this subtree via the old N4.
                    // A stale reader that already captured `child` could race on these plain stores,
                    // but the shard seqlock is odd for this entire mutation, so it must retry on
                    // v1 != v2 and cannot return a result observed during this window.
                    // Therefore plain stores for `prefix_len` and `prefix` are sufficient here;
                    // per-byte atomic stores on the prefix array would add overhead with no gain.
                    child_header.prefix_len = @intCast(combined_len);
                    child_header.prefix = new_prefix;

                    @atomicStore(usize, self, child, .monotonic);
                }
            }
        },
        .node16 => {
            const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", header)));
            if (n16.header.num_children <= 4) {
                const n4 = try allocator.create(Node4);
                n4.* = Node4.init();
                n4.header.copy_meta_from(&n16.header);

                for (0..n16.header.num_children) |i| {
                    n4.keys[i] = n16.keys[i];
                    n4.children[i] = n16.children[i];
                }
                @atomicStore(usize, self, node_internal(&n4.header), .monotonic);
            }
        },
        .node48 => {
            const n48 = @as(*Node48, @alignCast(@fieldParentPtr("header", header)));
            if (n48.header.num_children <= 16) {
                const n16 = try allocator.create(Node16);
                n16.* = Node16.init();
                n16.header.copy_meta_from(&n48.header);

                var child_idx: usize = 0;
                for (0..256) |i| {
                    const pos = n48.child_index[i];
                    if (pos != Node48.EMPTY_INDEX) {
                        n16.keys[child_idx] = @intCast(i);
                        n16.children[child_idx] = n48.children[pos];
                        child_idx += 1;
                    }
                }
                @atomicStore(usize, self, node_internal(&n16.header), .monotonic);
            }
        },
        .node256 => {
            const n256: *Node256 = @alignCast(@fieldParentPtr("header", header));
            if (n256.header.num_children <= 48) {
                const n48 = try allocator.create(Node48);
                n48.* = Node48.init();
                n48.header.copy_meta_from(&n256.header);

                var pos: usize = 0;
                for (0..256) |i| {
                    if (!node_is_empty(n256.children[i])) {
                        n48.children[pos] = n256.children[i];
                        n48.child_index[i] = @intCast(pos);
                        n48.present |= (@as(u64, 1) << @intCast(pos));
                        pos += 1;
                    }
                }
                @atomicStore(usize, self, node_internal(&n48.header), .monotonic);
            }
        },
    }
}

/// Removes a child identified by the transition byte from the current node.
/// If removing the child causes the node to fall below its minimum threshold
/// (e.g., Node16 falling to 4 children), it invokes `shrink()` to reclaim
/// memory density and enforce the compact structure of the Adaptive Radix Tree.
/// Time Complexity: O(1).
/// Allocator: May invoke allocation through `shrink(allocator)`.
pub fn remove_child(self: *Node, allocator: std.mem.Allocator, key_byte: u8) !void {
    if (!node_is_internal(self.*)) return error.InvalidNodeType;
    const header = node_to_internal(self.*);
    switch (header.node_type) {
        .node4 => {
            const n4 = @as(*Node4, @alignCast(@fieldParentPtr("header", header)));
            const count = n4.header.num_children;
            var pos: usize = 0;
            while (pos < count) : (pos += 1) {
                if (n4.keys[pos] == key_byte) break;
            }
            if (pos == count) return;

            const next_count = count - 1;
            while (pos < next_count) : (pos += 1) {
                n4.keys[pos] = n4.keys[pos + 1];
                n4.children[pos] = n4.children[pos + 1];
            }
            @atomicStore(u16, &n4.header.num_children, next_count, .monotonic);
            if (next_count <= 1) {
                try shrink(self, allocator);
            }
        },
        .node16 => {
            const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", header)));
            const count = n16.header.num_children;
            var pos: usize = 0;
            while (pos < count) : (pos += 1) {
                if (n16.keys[pos] == key_byte) break;
            }
            if (pos == count) return;

            const next_count = count - 1;
            while (pos < next_count) : (pos += 1) {
                n16.keys[pos] = n16.keys[pos + 1];
                n16.children[pos] = n16.children[pos + 1];
            }
            @atomicStore(u16, &n16.header.num_children, next_count, .monotonic);
            if (next_count <= 4) {
                try shrink(self, allocator);
            }
        },
        .node48 => {
            const n48 = @as(*Node48, @alignCast(@fieldParentPtr("header", header)));
            const pos = n48.child_index[key_byte];
            if (pos == Node48.EMPTY_INDEX) return;

            @atomicStore(u8, &n48.child_index[key_byte], Node48.EMPTY_INDEX, .monotonic);
            n48.present &= ~(@as(u64, 1) << @intCast(pos));
            n48.header.num_children -= 1;

            if (n48.header.num_children <= 16) {
                try shrink(self, allocator);
            }
        },
        .node256 => {
            const n256 = @as(*Node256, @alignCast(@fieldParentPtr("header", header)));
            if (!node_is_empty(n256.children[key_byte])) {
                @atomicStore(usize, &n256.children[key_byte], node_empty(), .monotonic);
                n256.header.num_children -= 1;
                if (n256.header.num_children <= 48) {
                    try shrink(self, allocator);
                }
            }
        },
    }
}

/// One preallocated internal node reserved for batch apply.
pub const ReservedInternalNode = union(NodeType) {
    node4: *Node4,
    node16: *Node16,
    node48: *Node48,
    node256: *Node256,

    /// Returns the node class carried by this reservation.
    /// Time Complexity: O(1).
    /// Allocator: Does not allocate.
    pub fn node_type(self: ReservedInternalNode) NodeType {
        return switch (self) {
            .node4 => .node4,
            .node16 => .node16,
            .node48 => .node48,
            .node256 => .node256,
        };
    }

    /// Returns this reservation as a live `Node`.
    /// Time Complexity: O(1).
    /// Allocator: Does not allocate.
    pub fn as_node(self: ReservedInternalNode) Node {
        return node_internal(self.header());
    }

    /// Returns the reserved node header pointer.
    /// Time Complexity: O(1).
    /// Allocator: Does not allocate.
    pub fn header(self: ReservedInternalNode) *NodeHeader {
        return switch (self) {
            .node4 => |n| &n.header,
            .node16 => |n| &n.header,
            .node48 => |n| &n.header,
            .node256 => |n| &n.header,
        };
    }
};

fn promote_into_reserved(self: *Node, promoted: ReservedInternalNode) !void {
    if (!node_is_internal(self.*)) return error.InvalidNodeGrowth;
    const header = node_to_internal(self.*);

    switch (header.node_type) {
        .node4 => {
            if (promoted.node_type() != .node16) return error.InvalidNodeGrowth;
            const n4: *Node4 = @alignCast(@fieldParentPtr("header", header));
            const n16 = promoted.node16;
            n16.* = Node16.init();
            n16.header.num_children = n4.header.num_children;
            n16.header.prefix_len = n4.header.prefix_len;
            n16.header.prefix = n4.header.prefix;
            n16.header.store_leaf_value(n4.header.load_leaf_value());
            for (0..n4.header.num_children) |i| {
                n16.keys[i] = n4.keys[i];
                n16.children[i] = n4.children[i];
            }
            @atomicStore(usize, self, promoted.as_node(), .monotonic);
        },
        .node16 => {
            if (promoted.node_type() != .node48) return error.InvalidNodeGrowth;
            const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", header)));
            const n48 = promoted.node48;
            n48.* = Node48.init();
            n48.header.num_children = n16.header.num_children;
            n48.header.prefix_len = n16.header.prefix_len;
            n48.header.prefix = n16.header.prefix;
            n48.header.store_leaf_value(n16.header.load_leaf_value());
            for (0..n16.header.num_children) |i| {
                const key = n16.keys[i];
                n48.child_index[key] = @intCast(i);
                n48.children[i] = n16.children[i];
                n48.present |= (@as(u64, 1) << @intCast(i));
            }
            @atomicStore(usize, self, promoted.as_node(), .monotonic);
        },
        .node48 => {
            if (promoted.node_type() != .node256) return error.InvalidNodeGrowth;
            const n48 = @as(*Node48, @alignCast(@fieldParentPtr("header", header)));
            const n256 = promoted.node256;
            n256.* = Node256.init();
            n256.header.num_children = n48.header.num_children;
            n256.header.prefix_len = n48.header.prefix_len;
            n256.header.prefix = n48.header.prefix;
            n256.header.store_leaf_value(n48.header.load_leaf_value());
            for (0..256) |i| {
                const idx = n48.child_index[i];
                if (idx != Node48.EMPTY_INDEX) {
                    n256.children[i] = n48.children[idx];
                }
            }
            @atomicStore(usize, self, promoted.as_node(), .monotonic);
        },
        .node256 => return error.TreeFull,
    }
}

/// Maps a new child into the current node without allocating.
/// If the node is full, `promoted` must provide the exact next node class reservation.
///
/// Time Complexity: O(1), bounded by local node width.
///
/// Allocator: Does not allocate.
pub fn add_child_reserved(self: *Node, key_byte: u8, child: Node, promoted: ?ReservedInternalNode) !void {
    if (!node_is_internal(self.*)) return error.InvalidNodeType;
    const header = node_to_internal(self.*);
    if (header.num_children == capacity_for(header.node_type)) {
        const reserved = promoted orelse return error.InvalidNodeGrowth;
        try promote_into_reserved(self, reserved);
        return try add_child_reserved(self, key_byte, child, null);
    }

    const current = node_to_internal(self.*);
    switch (current.node_type) {
        .node4 => {
            const n4 = @as(*Node4, @alignCast(@fieldParentPtr("header", current)));
            const count = n4.header.num_children;
            var i: usize = 0;
            while (i < count) : (i += 1) {
                if (key_byte < n4.keys[i]) break;
            }
            var j: usize = count;
            while (j > i) : (j -= 1) {
                n4.keys[j] = n4.keys[j - 1];
                n4.children[j] = n4.children[j - 1];
            }
            n4.keys[i] = key_byte;
            n4.children[i] = child;
            @atomicStore(u16, &n4.header.num_children, count + 1, .monotonic);
        },
        .node16 => {
            const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", current)));
            const count = n16.header.num_children;
            var i: usize = 0;
            while (i < count) : (i += 1) {
                if (key_byte < n16.keys[i]) break;
            }
            var j: usize = count;
            while (j > i) : (j -= 1) {
                n16.keys[j] = n16.keys[j - 1];
                n16.children[j] = n16.children[j - 1];
            }
            n16.keys[i] = key_byte;
            n16.children[i] = child;
            @atomicStore(u16, &n16.header.num_children, count + 1, .monotonic);
        },
        .node48 => {
            const n48 = @as(*Node48, @alignCast(@fieldParentPtr("header", current)));
            const pos = @ctz(~n48.present);
            std.debug.assert(pos < 48);
            n48.present |= (@as(u64, 1) << @intCast(pos));
            n48.children[pos] = child;
            @atomicStore(u8, &n48.child_index[key_byte], @intCast(pos), .monotonic);
            n48.header.num_children += 1;
        },
        .node256 => {
            const n256 = @as(*Node256, @alignCast(@fieldParentPtr("header", current)));
            std.debug.assert(node_is_empty(n256.children[key_byte]));
            @atomicStore(usize, &n256.children[key_byte], child, .monotonic);
            n256.header.num_children += 1;
        },
    }
}

/// Node4 represents an ART node containing up to 4 child pointers.
/// It uses parallel arrays for keys and children, keeping keys sorted
/// lexicographically to enable fast linear searches and ordered iteration.
/// Memory Footprint: 96 bytes (24 byte header + 4 bytes keys + 64 bytes children + padding).
pub const Node4 = struct {
    header: NodeHeader,
    keys: [4]u8 = undefined,
    children: [4]Node = undefined,

    /// Initializes a new, empty Node4 with the correct NodeType header.
    /// Time Complexity: O(1).
    /// Allocator: Does not allocate.
    pub fn init() Node4 {
        return .{
            .header = NodeHeader.init(.node4),
        };
    }

    /// Finds a child by its key byte. Linear search is fast enough for 4 elements.
    /// Time Complexity: O(1) due to fixed size.
    /// Allocator: Does not allocate.
    pub fn find_child(self: *const Node4, key_byte: u8) ?*Node {
        const n = @atomicLoad(u16, &self.header.num_children, .acquire);
        for (0..n) |i| {
            if (self.keys[i] == key_byte) {
                // Return a pointer to the child node wrapper for mutation or traversal
                return @constCast(&self.children[i]);
            }
        }
        return null;
    }
};

/// Node16 represents an ART node containing up to 16 child pointers.
/// It uses a vectorized approach for keys and children, enabling SIMD-based searches.
/// Memory Footprint: 128 bytes (24 byte header + 16 bytes keys + 128 bytes children + padding).
pub const Node16 = struct {
    header: NodeHeader,
    keys: @Vector(16, u8) = @splat(0),
    children: [16]Node = undefined,

    /// Initializes a new, empty Node16 with the correct NodeType header.
    /// Time Complexity: O(1).
    /// Allocator: Does not allocate.
    pub fn init() Node16 {
        return .{
            .header = NodeHeader.init(.node16),
        };
    }

    /// Finds a child by its key byte using SIMD.
    /// Time Complexity: O(1) due to fixed size.
    /// Allocator: Does not allocate.
    pub fn find_child(self: *const Node16, key_byte: u8) ?*Node {
        const mask: @Vector(16, u8) = @splat(key_byte);
        const match_mask = @as(u16, @bitCast(self.keys == mask));
        const n = @atomicLoad(u16, &self.header.num_children, .acquire);
        const valid_mask = @as(u16, @intCast((@as(u32, 1) << @as(u5, @intCast(n))) - 1));
        const bitmask = match_mask & valid_mask;

        if (bitmask != 0) {
            const index = @ctz(bitmask);
            if (index < n) {
                return @constCast(&self.children[index]);
            }
        }
        return null;
    }
};

/// Node48 represents an ART node containing up to 48 child pointers.
/// It uses a hybrid approach with a fixed-size array for keys and a variable-size array for children,
/// allowing for efficient searches and ordered iteration.
/// Memory Footprint: 168 bytes (24 byte header + 256 bytes child_index + 192 bytes children + padding).
pub const Node48 = struct {
    /// Sentinel stored in `child_index` for bytes that have no child edge.
    pub const EMPTY_INDEX: u8 = 255;
    header: NodeHeader,
    child_index: [256]u8 = [_]u8{EMPTY_INDEX} ** 256,
    children: [48]Node = undefined,
    present: u64 = 0,

    /// Initializes a new, empty Node48 with the correct NodeType header.
    /// Time Complexity: O(1).
    /// Allocator: Does not allocate.
    pub fn init() Node48 {
        return .{
            .header = NodeHeader.init(.node48),
            .child_index = [_]u8{EMPTY_INDEX} ** 256,
        };
    }

    /// Finds a child by its key byte using a fixed-size array.
    /// Time Complexity: O(1) due to fixed size.
    /// Allocator: Does not allocate.
    pub fn find_child(self: *const Node48, key_byte: u8) ?*Node {
        const index = @atomicLoad(u8, &self.child_index[key_byte], .acquire);
        if (index != EMPTY_INDEX) {
            return @constCast(&self.children[index]);
        }
        return null;
    }
};

/// Node256 represents an ART node containing up to 256 child pointers.
/// It uses a fixed-size array for children, allowing for efficient searches and ordered iteration.
/// Memory Footprint: 288 bytes (24 byte header + 256 bytes children + padding).
pub const Node256 = struct {
    header: NodeHeader,
    children: [256]Node = [_]Node{node_empty()} ** 256,

    /// Initializes a new, empty Node256 with the correct NodeType header.
    /// Time Complexity: O(1).
    /// Allocator: Does not allocate.
    pub fn init() Node256 {
        return .{
            .header = NodeHeader.init(.node256),
            .children = [_]Node{node_empty()} ** 256,
        };
    }

    /// Finds a child by its key byte using a fixed-size array.
    /// Time Complexity: O(1) due to fixed size.
    /// Allocator: Does not allocate.
    pub fn find_child(self: *const Node256, key_byte: u8) ?*Node {
        const slot = @atomicLoad(usize, &self.children[key_byte], .acquire);
        if (!node_is_empty(slot)) {
            return @constCast(&self.children[key_byte]);
        }
        return null;
    }
};

fn create_test_leaf(allocator: std.mem.Allocator, key: []const u8, value_int: i64) !*Leaf {
    const stored_key = try allocator.dupe(u8, key);
    const value = try allocator.create(Value);
    value.* = .{ .integer = value_int };
    const leaf = try allocator.create(Leaf);
    leaf.* = .{
        .key = stored_key,
        .value = value,
    };
    return leaf;
}

test "art node structs keep expected header and payload sizes" {
    const testing = std.testing;

    const n4 = Node4.init();
    try testing.expectEqual(NodeType.node4, n4.header.node_type);
    try testing.expectEqual(@as(usize, 24), @sizeOf(NodeHeader));
    try testing.expectEqual(@as(usize, 64), @sizeOf(Node4));

    const n16 = Node16.init();
    try testing.expectEqual(NodeType.node16, n16.header.node_type);
    try testing.expectEqual(NodeType.node48, Node48.init().header.node_type);
    try testing.expectEqual(NodeType.node256, Node256.init().header.node_type);
}

test "node4 add_child keeps child bytes sorted" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const internal = try arena.allocator().create(Node4);
    internal.* = Node4.init();
    var root = node_internal(&internal.header);

    try add_child(&root, arena.allocator(), 'm', node_leaf(try create_test_leaf(arena.allocator(), "m", 1)));
    try add_child(&root, arena.allocator(), 'a', node_leaf(try create_test_leaf(arena.allocator(), "a", 2)));
    try add_child(&root, arena.allocator(), 'z', node_leaf(try create_test_leaf(arena.allocator(), "z", 3)));
    try add_child(&root, arena.allocator(), 'b', node_leaf(try create_test_leaf(arena.allocator(), "b", 4)));

    try testing.expectEqual(NodeType.node4, node_to_internal(root).node_type);
    const n4 = @as(*Node4, @alignCast(@fieldParentPtr("header", node_to_internal(root))));
    try testing.expectEqualSlices(u8, &.{ 'a', 'b', 'm', 'z' }, n4.keys[0..4]);
}

test "add_child grows node4 into node16 and preserves children" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const internal = try arena.allocator().create(Node4);
    internal.* = Node4.init();
    var root = node_internal(&internal.header);

    for ([_]u8{ 'a', 'b', 'c', 'd', 'e' }, 0..) |byte, index| {
        const key = [_]u8{byte};
        try add_child(&root, arena.allocator(), byte, node_leaf(try create_test_leaf(arena.allocator(), &key, @intCast(index))));
    }

    try testing.expectEqual(NodeType.node16, node_to_internal(root).node_type);
    const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", node_to_internal(root))));
    try testing.expectEqual(@as(u16, 5), n16.header.num_children);
    for ([_]u8{ 'a', 'b', 'c', 'd', 'e' }) |byte| {
        try testing.expect(n16.find_child(byte) != null);
    }
}

test "add_child grows node16 into node48 and node48 into node256" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const internal = try arena.allocator().create(Node4);
    internal.* = Node4.init();
    var root = node_internal(&internal.header);

    for (0..49) |index| {
        const byte: u8 = @intCast(index);
        const key = [_]u8{byte};
        try add_child(&root, arena.allocator(), byte, node_leaf(try create_test_leaf(arena.allocator(), &key, @intCast(index))));
    }

    try testing.expectEqual(NodeType.node256, node_to_internal(root).node_type);
    const n256 = @as(*Node256, @alignCast(@fieldParentPtr("header", node_to_internal(root))));
    try testing.expectEqual(@as(u16, 49), n256.header.num_children);
    for (0..49) |index| {
        try testing.expect(n256.find_child(@intCast(index)) != null);
    }
}

test "remove_child shrinks node256 into node48 and node48 into node16 and node16 into node4" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const internal = try arena.allocator().create(Node4);
    internal.* = Node4.init();
    var root = node_internal(&internal.header);

    for (0..49) |index| {
        const byte: u8 = @intCast(index);
        const key = [_]u8{byte};
        try add_child(&root, arena.allocator(), byte, node_leaf(try create_test_leaf(arena.allocator(), &key, @intCast(index))));
    }

    try testing.expectEqual(NodeType.node256, node_to_internal(root).node_type);

    try remove_child(&root, arena.allocator(), 48);
    try testing.expectEqual(NodeType.node48, node_to_internal(root).node_type);

    for (16..48) |index| {
        try remove_child(&root, arena.allocator(), @intCast(index));
    }
    try testing.expectEqual(NodeType.node16, node_to_internal(root).node_type);

    for (4..17) |index| {
        try remove_child(&root, arena.allocator(), @intCast(index));
    }
    try testing.expectEqual(NodeType.node4, node_to_internal(root).node_type);
}

test "shrink on a one-child node4 promotes a leaf child" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const internal = try arena.allocator().create(Node4);
    internal.* = Node4.init();
    var root = node_internal(&internal.header);
    const leaf = try create_test_leaf(arena.allocator(), "alpha", 1);
    try add_child(&root, arena.allocator(), 'a', node_leaf(leaf));

    try shrink(&root, arena.allocator());

    try testing.expect(node_is_leaf(root));
    try testing.expectEqualStrings("alpha", node_to_leaf(root).key);
}

test "shrink on a one-child node4 merges the child prefix into the remaining internal node" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const parent = try arena.allocator().create(Node4);
    parent.* = Node4.init();
    parent.header.prefix_len = 2;
    parent.header.prefix[0] = 'a';
    parent.header.prefix[1] = 'b';

    const child = try arena.allocator().create(Node4);
    child.* = Node4.init();
    child.header.prefix_len = 2;
    child.header.prefix[0] = 'd';
    child.header.prefix[1] = 'e';
    var child_node = node_internal(&child.header);
    try add_child(&child_node, arena.allocator(), 'f', node_leaf(try create_test_leaf(arena.allocator(), "abxdef", 1)));

    var root = node_internal(&parent.header);
    try add_child(&root, arena.allocator(), 'x', node_internal(&child.header));
    try shrink(&root, arena.allocator());

    try testing.expect(node_is_internal(root));
    try testing.expectEqual(@as(u16, 5), node_to_internal(root).prefix_len);
    try testing.expectEqualSlices(u8, "abxde", node_to_internal(root).prefix[0..5]);
    const merged = @as(*Node4, @alignCast(@fieldParentPtr("header", node_to_internal(root))));
    try testing.expectEqual(@as(u16, 1), merged.header.num_children);
    try testing.expectEqual(@as(u8, 'f'), merged.keys[0]);
}
