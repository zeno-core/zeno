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
};

/// An ART Node can either be an internal node (storing children) or a leaf node (storing a value).
pub const Node = union(enum) {
    empty: void,
    internal: *NodeHeader,
    leaf: *Leaf,

    /// Returns whether this tagged node currently points at a leaf payload.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    pub fn is_leaf(self: *const Node) bool {
        return self.* == .leaf;
    }

    /// Returns whether this tagged node currently has no payload.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    pub fn is_empty(self: *const Node) bool {
        return self.* == .empty;
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
        switch (self.*) {
            .internal => |header| {
                switch (header.node_type) {
                    .node4 => {
                        const n4: *Node4 = @alignCast(@fieldParentPtr("header", header));
                        const n16 = try allocator.create(Node16);
                        n16.* = Node16.init();
                        n16.header.num_children = n4.header.num_children;
                        n16.header.prefix_len = n4.header.prefix_len;
                        n16.header.prefix = n4.header.prefix;
                        n16.header.leaf_value = n4.header.leaf_value;

                        for (0..n4.header.num_children) |i| {
                            n16.keys[i] = n4.keys[i];
                            n16.children[i] = n4.children[i];
                        }
                        self.* = .{ .internal = &n16.header };
                    },
                    .node16 => {
                        const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", header)));
                        const n48 = try allocator.create(Node48);
                        n48.* = Node48.init();
                        n48.header.num_children = n16.header.num_children;
                        n48.header.prefix_len = n16.header.prefix_len;
                        n48.header.prefix = n16.header.prefix;
                        n48.header.leaf_value = n16.header.leaf_value;

                        for (0..n16.header.num_children) |i| {
                            const k = n16.keys[i];
                            n48.child_index[k] = @intCast(i);
                            n48.children[i] = n16.children[i];
                            n48.present |= (@as(u64, 1) << @intCast(i));
                        }
                        self.* = .{ .internal = &n48.header };
                    },
                    .node48 => {
                        const n48 = @as(*Node48, @alignCast(@fieldParentPtr("header", header)));
                        const n256 = try allocator.create(Node256);
                        n256.* = Node256.init();
                        n256.header.num_children = n48.header.num_children;
                        n256.header.prefix_len = n48.header.prefix_len;
                        n256.header.prefix = n48.header.prefix;
                        n256.header.leaf_value = n48.header.leaf_value;

                        for (0..256) |i| {
                            const idx = n48.child_index[i];
                            if (idx != Node48.EMPTY_INDEX) {
                                n256.children[i] = n48.children[idx];
                            }
                        }
                        self.* = .{ .internal = &n256.header };
                    },
                    .node256 => return error.TreeFull,
                }
            },
            .empty => return error.InvalidNodeGrowth,
            .leaf => return error.InvalidNodeGrowth,
        }
    }

    /// Maps a new child into the current node at the given transition byte.
    /// This method is responsible for keeping node arrays sorted `keys` in Node4/Node16
    /// or manipulating the `child_index` bitmap in Node48. If the node is completely full,
    /// it invokes `self.grow()` first and attempts insertion on the expanded architecture.
    /// Time Complexity: O(1), max 16 byte comparisons or 1 constant-time bitwise ctz operation.
    /// Allocator: May allocate symmetrically by calling `grow(allocator)`.
    pub fn add_child(self: *Node, allocator: std.mem.Allocator, key_byte: u8, child: Node) !void {
        const header = switch (self.*) {
            .internal => |h| h,
            else => return error.InvalidNodeType,
        };
        switch (header.node_type) {
            .node4 => {
                const n4 = @as(*Node4, @alignCast(@fieldParentPtr("header", header)));
                if (n4.header.num_children == 4) {
                    try self.grow(allocator);
                    return try self.add_child(allocator, key_byte, child);
                }
                var i: usize = 0;
                while (i < n4.header.num_children) : (i += 1) {
                    if (key_byte < n4.keys[i]) break;
                }
                var j: usize = n4.header.num_children;
                while (j > i) : (j -= 1) {
                    n4.keys[j] = n4.keys[j - 1];
                    n4.children[j] = n4.children[j - 1];
                }
                n4.keys[i] = key_byte;
                n4.children[i] = child;
                n4.header.num_children += 1;
            },
            .node16 => {
                const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", header)));
                if (n16.header.num_children == 16) {
                    try self.grow(allocator);
                    return try self.add_child(allocator, key_byte, child);
                }
                var i: usize = 0;
                while (i < n16.header.num_children) : (i += 1) {
                    if (key_byte < n16.keys[i]) break;
                }
                var j: usize = n16.header.num_children;
                while (j > i) : (j -= 1) {
                    n16.keys[j] = n16.keys[j - 1];
                    n16.children[j] = n16.children[j - 1];
                }
                n16.keys[i] = key_byte;
                n16.children[i] = child;
                n16.header.num_children += 1;
            },
            .node48 => {
                const n48 = @as(*Node48, @alignCast(@fieldParentPtr("header", header)));
                if (n48.header.num_children == 48) {
                    try self.grow(allocator);
                    return try self.add_child(allocator, key_byte, child);
                }
                const pos = @ctz(~n48.present);
                std.debug.assert(pos < 48);
                n48.present |= (@as(u64, 1) << @intCast(pos));

                n48.children[pos] = child;
                n48.child_index[key_byte] = @intCast(pos);
                n48.header.num_children += 1;
            },
            .node256 => {
                const n256 = @as(*Node256, @alignCast(@fieldParentPtr("header", header)));
                n256.children[key_byte] = child;
                n256.header.num_children += 1;
            },
        }
    }

    /// Shrinks an ART node to a smaller size capacity or merges paths.
    /// Triggered when a node loses children below critical thresholds (e.g., Node48 dropping <= 16).
    /// If a `Node4` drops to just 1 child, we perform **Path Compression / Merging**:
    /// We collapse the node entirely. If the only child is a Leaf, the Leaf assumes its place.
    /// If the only child is another internal node, we merge their prefixes to create a compact,
    /// singular path in O(1) time without recursive reshuffling, respecting the MAX_PREFIX_LEN.
    /// Time Complexity: O(1), mapping or compressing up to 256 children arrays.
    /// Allocator: Allocates exactly one smaller node using `allocator`. The old node becomes unreachable and is reclaimed with its arena.
    pub fn shrink(self: *Node, allocator: std.mem.Allocator) !void {
        switch (self.*) {
            .internal => |header| {
                switch (header.node_type) {
                    .node4 => {
                        const n4 = @as(*Node4, @alignCast(@fieldParentPtr("header", header)));
                        if (n4.header.num_children == 0) {
                            self.* = Node{ .leaf = n4.header.leaf_value.? };
                            return;
                        }
                        if (n4.header.num_children == 1 and n4.header.leaf_value == null) {
                            const child = n4.children[0];
                            if (child == .leaf) {
                                self.* = child;
                            } else {
                                // Path compression merge
                                std.debug.assert(child == .internal);
                                const child_header = child.internal;
                                const combined_len = @as(usize, n4.header.prefix_len) + child_header.prefix_len + 1;

                                // Try to fit the new prefix into MAX_PREFIX_LEN
                                var new_prefix: [MAX_PREFIX_LEN]u8 = undefined;

                                var idx: usize = 0;
                                // Parent's prefix
                                const prefix_length = @min(n4.header.prefix_len, MAX_PREFIX_LEN);
                                if (prefix_length > 0) {
                                    @memcpy(new_prefix[idx .. idx + prefix_length], n4.header.prefix[0..prefix_length]);
                                    idx += prefix_length;
                                }

                                // Transition character
                                if (idx < MAX_PREFIX_LEN) {
                                    new_prefix[idx] = n4.keys[0];
                                    idx += 1;

                                    // Child's prefix
                                    const child_prefix_length = @min(child_header.prefix_len, MAX_PREFIX_LEN - idx);
                                    if (child_prefix_length > 0) {
                                        @memcpy(new_prefix[idx .. idx + child_prefix_length], child_header.prefix[0..child_prefix_length]);
                                    }
                                }

                                // prefix_len is intentionally allowed to exceed MAX_PREFIX_LEN (pessimistic prefix matching).
                                // Only the first MAX_PREFIX_LEN bytes are physically stored in prefix[].
                                // The full length is preserved so lookups can detect a potential mismatch and fall back
                                // @intCast is intentional: panics in Debug if combined_len overflows u16,
                                // which would indicate a corrupted or pathologically deep tree.
                                child_header.prefix_len = @intCast(combined_len);
                                child_header.prefix = new_prefix;

                                self.* = child;
                            }
                        }
                    },
                    .node16 => {
                        const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", header)));
                        if (n16.header.num_children <= 4) {
                            const n4 = try allocator.create(Node4);
                            n4.* = Node4.init();
                            n4.header.num_children = n16.header.num_children;
                            n4.header.prefix_len = n16.header.prefix_len;
                            n4.header.prefix = n16.header.prefix;
                            n4.header.leaf_value = n16.header.leaf_value;

                            for (0..n16.header.num_children) |i| {
                                n4.keys[i] = n16.keys[i];
                                n4.children[i] = n16.children[i];
                            }
                            self.* = .{ .internal = &n4.header };
                        }
                    },
                    .node48 => {
                        const n48 = @as(*Node48, @alignCast(@fieldParentPtr("header", header)));
                        if (n48.header.num_children <= 16) {
                            const n16 = try allocator.create(Node16);
                            n16.* = Node16.init();
                            n16.header.num_children = n48.header.num_children;
                            n16.header.prefix_len = n48.header.prefix_len;
                            n16.header.prefix = n48.header.prefix;
                            n16.header.leaf_value = n48.header.leaf_value;

                            var child_idx: usize = 0;
                            for (0..256) |i| {
                                const pos = n48.child_index[i];
                                if (pos != Node48.EMPTY_INDEX) {
                                    n16.keys[child_idx] = @intCast(i);
                                    n16.children[child_idx] = n48.children[pos];
                                    child_idx += 1;
                                }
                            }
                            self.* = .{ .internal = &n16.header };
                        }
                    },
                    .node256 => {
                        const n256: *Node256 = @alignCast(@fieldParentPtr("header", header));
                        if (n256.header.num_children <= 48) {
                            const n48 = try allocator.create(Node48);
                            n48.* = Node48.init();
                            n48.header.num_children = n256.header.num_children;
                            n48.header.prefix_len = n256.header.prefix_len;
                            n48.header.prefix = n256.header.prefix;
                            n48.header.leaf_value = n256.header.leaf_value;

                            var pos: usize = 0;
                            for (0..256) |i| {
                                if (!n256.children[i].is_empty()) {
                                    n48.children[pos] = n256.children[i];
                                    n48.child_index[i] = @intCast(pos);
                                    n48.present |= (@as(u64, 1) << @intCast(pos));
                                    pos += 1;
                                }
                            }
                            self.* = .{ .internal = &n48.header };
                        }
                    },
                }
            },
            .empty => return error.InvalidNodeShrink,
            .leaf => return error.InvalidNodeShrink,
        }
    }

    /// Removes a child identified by the transition byte from the current node.
    /// If removing the child causes the node to fall below its minimum threshold
    /// (e.g., Node16 falling to 4 children), it invokes `self.shrink()` to reclaim
    /// memory density and enforce the compact structure of the Adaptive Radix Tree.
    /// Time Complexity: O(1).
    /// Allocator: May invoke allocation through `shrink(allocator)`.
    pub fn remove_child(self: *Node, allocator: std.mem.Allocator, key_byte: u8) !void {
        const header = switch (self.*) {
            .internal => |h| h,
            else => return error.InvalidNodeType,
        };
        switch (header.node_type) {
            .node4 => {
                const n4 = @as(*Node4, @alignCast(@fieldParentPtr("header", header)));
                var pos: usize = 0;
                while (pos < n4.header.num_children) : (pos += 1) {
                    if (n4.keys[pos] == key_byte) break;
                }
                if (pos == n4.header.num_children) return; // not found

                n4.header.num_children -= 1;
                while (pos < n4.header.num_children) : (pos += 1) {
                    n4.keys[pos] = n4.keys[pos + 1];
                    n4.children[pos] = n4.children[pos + 1];
                }
                if (n4.header.num_children <= 1) {
                    try self.shrink(allocator);
                }
            },
            .node16 => {
                const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", header)));
                var pos: usize = 0;
                while (pos < n16.header.num_children) : (pos += 1) {
                    if (n16.keys[pos] == key_byte) break;
                }
                if (pos == n16.header.num_children) return;

                n16.header.num_children -= 1;
                while (pos < n16.header.num_children) : (pos += 1) {
                    n16.keys[pos] = n16.keys[pos + 1];
                    n16.children[pos] = n16.children[pos + 1];
                }
                if (n16.header.num_children <= 4) {
                    try self.shrink(allocator);
                }
            },
            .node48 => {
                const n48 = @as(*Node48, @alignCast(@fieldParentPtr("header", header)));
                const pos = n48.child_index[key_byte];
                if (pos == Node48.EMPTY_INDEX) return;

                n48.child_index[key_byte] = Node48.EMPTY_INDEX;
                n48.present &= ~(@as(u64, 1) << @intCast(pos));
                n48.header.num_children -= 1;

                if (n48.header.num_children <= 16) {
                    try self.shrink(allocator);
                }
            },
            .node256 => {
                const n256 = @as(*Node256, @alignCast(@fieldParentPtr("header", header)));
                if (!n256.children[key_byte].is_empty()) {
                    n256.children[key_byte] = .{ .empty = {} };
                    n256.header.num_children -= 1;
                    if (n256.header.num_children <= 48) {
                        try self.shrink(allocator);
                    }
                }
            },
        }
    }
};

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
        return .{ .internal = self.header() };
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
    const header = switch (self.*) {
        .internal => |h| h,
        else => return error.InvalidNodeGrowth,
    };

    switch (header.node_type) {
        .node4 => {
            if (promoted.node_type() != .node16) return error.InvalidNodeGrowth;
            const n4: *Node4 = @alignCast(@fieldParentPtr("header", header));
            const n16 = promoted.node16;
            n16.* = Node16.init();
            n16.header.num_children = n4.header.num_children;
            n16.header.prefix_len = n4.header.prefix_len;
            n16.header.prefix = n4.header.prefix;
            n16.header.leaf_value = n4.header.leaf_value;
            for (0..n4.header.num_children) |i| {
                n16.keys[i] = n4.keys[i];
                n16.children[i] = n4.children[i];
            }
            self.* = promoted.as_node();
        },
        .node16 => {
            if (promoted.node_type() != .node48) return error.InvalidNodeGrowth;
            const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", header)));
            const n48 = promoted.node48;
            n48.* = Node48.init();
            n48.header.num_children = n16.header.num_children;
            n48.header.prefix_len = n16.header.prefix_len;
            n48.header.prefix = n16.header.prefix;
            n48.header.leaf_value = n16.header.leaf_value;
            for (0..n16.header.num_children) |i| {
                const key = n16.keys[i];
                n48.child_index[key] = @intCast(i);
                n48.children[i] = n16.children[i];
                n48.present |= (@as(u64, 1) << @intCast(i));
            }
            self.* = promoted.as_node();
        },
        .node48 => {
            if (promoted.node_type() != .node256) return error.InvalidNodeGrowth;
            const n48 = @as(*Node48, @alignCast(@fieldParentPtr("header", header)));
            const n256 = promoted.node256;
            n256.* = Node256.init();
            n256.header.num_children = n48.header.num_children;
            n256.header.prefix_len = n48.header.prefix_len;
            n256.header.prefix = n48.header.prefix;
            n256.header.leaf_value = n48.header.leaf_value;
            for (0..256) |i| {
                const idx = n48.child_index[i];
                if (idx != Node48.EMPTY_INDEX) {
                    n256.children[i] = n48.children[idx];
                }
            }
            self.* = promoted.as_node();
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
    const header = switch (self.*) {
        .internal => |h| h,
        else => return error.InvalidNodeType,
    };
    if (header.num_children == Node.capacity_for(header.node_type)) {
        const reserved = promoted orelse return error.InvalidNodeGrowth;
        try promote_into_reserved(self, reserved);
        return try add_child_reserved(self, key_byte, child, null);
    }

    const current = switch (self.*) {
        .internal => |h| h,
        else => unreachable,
    };
    switch (current.node_type) {
        .node4 => {
            const n4 = @as(*Node4, @alignCast(@fieldParentPtr("header", current)));
            var i: usize = 0;
            while (i < n4.header.num_children) : (i += 1) {
                if (key_byte < n4.keys[i]) break;
            }
            var j: usize = n4.header.num_children;
            while (j > i) : (j -= 1) {
                n4.keys[j] = n4.keys[j - 1];
                n4.children[j] = n4.children[j - 1];
            }
            n4.keys[i] = key_byte;
            n4.children[i] = child;
            n4.header.num_children += 1;
        },
        .node16 => {
            const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", current)));
            var i: usize = 0;
            while (i < n16.header.num_children) : (i += 1) {
                if (key_byte < n16.keys[i]) break;
            }
            var j: usize = n16.header.num_children;
            while (j > i) : (j -= 1) {
                n16.keys[j] = n16.keys[j - 1];
                n16.children[j] = n16.children[j - 1];
            }
            n16.keys[i] = key_byte;
            n16.children[i] = child;
            n16.header.num_children += 1;
        },
        .node48 => {
            const n48 = @as(*Node48, @alignCast(@fieldParentPtr("header", current)));
            const pos = @ctz(~n48.present);
            std.debug.assert(pos < 48);
            n48.present |= (@as(u64, 1) << @intCast(pos));
            n48.children[pos] = child;
            n48.child_index[key_byte] = @intCast(pos);
            n48.header.num_children += 1;
        },
        .node256 => {
            const n256 = @as(*Node256, @alignCast(@fieldParentPtr("header", current)));
            std.debug.assert(n256.children[key_byte].is_empty());
            n256.children[key_byte] = child;
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
        for (0..self.header.num_children) |i| {
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
        const valid_mask = @as(u16, @intCast((@as(u32, 1) << @as(u5, @intCast(self.header.num_children))) - 1));
        const bitmask = match_mask & valid_mask;

        if (bitmask != 0) {
            const index = @ctz(bitmask);
            if (index < self.header.num_children) {
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
        const index = self.child_index[key_byte];
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
    children: [256]Node = [_]Node{.{ .empty = {} }} ** 256,

    /// Initializes a new, empty Node256 with the correct NodeType header.
    /// Time Complexity: O(1).
    /// Allocator: Does not allocate.
    pub fn init() Node256 {
        return .{
            .header = NodeHeader.init(.node256),
            .children = [_]Node{.{ .empty = {} }} ** 256,
        };
    }

    /// Finds a child by its key byte using a fixed-size array.
    /// Time Complexity: O(1) due to fixed size.
    /// Allocator: Does not allocate.
    pub fn find_child(self: *const Node256, key_byte: u8) ?*Node {
        if (!self.children[key_byte].is_empty()) {
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
    try testing.expectEqual(@as(usize, 96), @sizeOf(Node4));

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
    var root = Node{ .internal = &internal.header };

    try root.add_child(arena.allocator(), 'm', .{ .leaf = try create_test_leaf(arena.allocator(), "m", 1) });
    try root.add_child(arena.allocator(), 'a', .{ .leaf = try create_test_leaf(arena.allocator(), "a", 2) });
    try root.add_child(arena.allocator(), 'z', .{ .leaf = try create_test_leaf(arena.allocator(), "z", 3) });
    try root.add_child(arena.allocator(), 'b', .{ .leaf = try create_test_leaf(arena.allocator(), "b", 4) });

    try testing.expectEqual(NodeType.node4, root.internal.node_type);
    const n4 = @as(*Node4, @alignCast(@fieldParentPtr("header", root.internal)));
    try testing.expectEqualSlices(u8, &.{ 'a', 'b', 'm', 'z' }, n4.keys[0..4]);
}

test "add_child grows node4 into node16 and preserves children" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const internal = try arena.allocator().create(Node4);
    internal.* = Node4.init();
    var root = Node{ .internal = &internal.header };

    for ([_]u8{ 'a', 'b', 'c', 'd', 'e' }, 0..) |byte, index| {
        const key = [_]u8{byte};
        try root.add_child(arena.allocator(), byte, .{ .leaf = try create_test_leaf(arena.allocator(), &key, @intCast(index)) });
    }

    try testing.expectEqual(NodeType.node16, root.internal.node_type);
    const n16 = @as(*Node16, @alignCast(@fieldParentPtr("header", root.internal)));
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
    var root = Node{ .internal = &internal.header };

    for (0..49) |index| {
        const byte: u8 = @intCast(index);
        const key = [_]u8{byte};
        try root.add_child(arena.allocator(), byte, .{ .leaf = try create_test_leaf(arena.allocator(), &key, @intCast(index)) });
    }

    try testing.expectEqual(NodeType.node256, root.internal.node_type);
    const n256 = @as(*Node256, @alignCast(@fieldParentPtr("header", root.internal)));
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
    var root = Node{ .internal = &internal.header };

    for (0..49) |index| {
        const byte: u8 = @intCast(index);
        const key = [_]u8{byte};
        try root.add_child(arena.allocator(), byte, .{ .leaf = try create_test_leaf(arena.allocator(), &key, @intCast(index)) });
    }

    try testing.expectEqual(NodeType.node256, root.internal.node_type);

    try root.remove_child(arena.allocator(), 48);
    try testing.expectEqual(NodeType.node48, root.internal.node_type);

    for (16..48) |index| {
        try root.remove_child(arena.allocator(), @intCast(index));
    }
    try testing.expectEqual(NodeType.node16, root.internal.node_type);

    for (4..17) |index| {
        try root.remove_child(arena.allocator(), @intCast(index));
    }
    try testing.expectEqual(NodeType.node4, root.internal.node_type);
}

test "shrink on a one-child node4 promotes a leaf child" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const internal = try arena.allocator().create(Node4);
    internal.* = Node4.init();
    var root = Node{ .internal = &internal.header };
    const leaf = try create_test_leaf(arena.allocator(), "alpha", 1);
    try root.add_child(arena.allocator(), 'a', .{ .leaf = leaf });

    try root.shrink(arena.allocator());

    try testing.expect(root == .leaf);
    try testing.expectEqualStrings("alpha", root.leaf.key);
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
    var child_node = Node{ .internal = &child.header };
    try child_node.add_child(arena.allocator(), 'f', .{ .leaf = try create_test_leaf(arena.allocator(), "abxdef", 1) });

    var root = Node{ .internal = &parent.header };
    try root.add_child(arena.allocator(), 'x', .{ .internal = &child.header });
    try root.shrink(arena.allocator());

    try testing.expect(root == .internal);
    try testing.expectEqual(@as(u16, 5), root.internal.prefix_len);
    try testing.expectEqualSlices(u8, "abxde", root.internal.prefix[0..5]);
    const merged = @as(*Node4, @alignCast(@fieldParentPtr("header", root.internal)));
    try testing.expectEqual(@as(u16, 1), merged.header.num_children);
    try testing.expectEqual(@as(u8, 'f'), merged.keys[0]);
}
