//! ART-backed plain-key index storage for `zeno-core` runtime shards.
//! Cost: Lookup, insert planning, and delete paths are O(k), where `k` is key length; scans add traversal work over matched entries.
//! Allocator: Uses the tree-owned allocator for internal nodes and leaves, while lookup and traversal stay allocation-free.

const std = @import("std");
const Value = @import("../../types/value.zig").Value;
const node = @import("node.zig");
const prepared_insert = @import("prepared_insert.zig");
const Node = node.Node;
const NodeHeader = node.NodeHeader;
const NodeType = node.NodeType;
const Node4 = node.Node4;
const Node16 = node.Node16;
const Node48 = node.Node48;
const Node256 = node.Node256;
const Leaf = node.Leaf;
const MAX_PREFIX_LEN = node.MAX_PREFIX_LEN;
/// Planned insert kind reexported for runtime batch integration.
pub const InsertPlanKind = prepared_insert.InsertPlanKind;
/// Reservation requirements reexported for runtime batch integration.
pub const InsertReservationSpec = prepared_insert.InsertReservationSpec;
/// Planned path-step metadata reexported for runtime batch integration.
pub const InsertPathStep = prepared_insert.InsertPathStep;
/// Prepared insert plan reexported for runtime batch integration.
pub const PreparedInsert = prepared_insert.PreparedInsert;
/// Reserved live insert payload reexported for runtime batch integration.
pub const ReservedInsert = prepared_insert.ReservedInsert;
/// Planner-only shadow tree reexported for runtime batch integration.
pub const ShadowTree = prepared_insert.ShadowTree;

/// Inclusive-start, exclusive-end bounds used by ART range scans.
///
/// Ownership:
/// - `start` and `end` are borrowed.
/// - Bound slices must remain valid for the duration of the consuming scan call.
pub const KeyRange = struct {
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
};

/// Root ART handle for one shard-local plain-key index.
pub const Tree = struct {
    root: Node = node.nodeEmpty(),
    allocator: std.mem.Allocator,

    /// Initializes one empty ART with the provided node allocator.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Stores `allocator` for all future internal-node and leaf allocation.
    pub fn init(allocator: std.mem.Allocator) Tree {
        return .{
            .allocator = allocator,
        };
    }

    /// Finds any leaf node in the tree.
    /// Time Complexity: O(k) where k is the length of the string key.
    /// Allocator: Does not allocate.
    pub fn findAnyLeaf(n: *const Node) *const Leaf {
        var cur = n.*;
        while (true) {
            switch (node.nodeDecode(cur)) {
                .leaf => |l| return l,
                .internal => |h| {
                    if (h.loadLeafValue()) |leaf| return leaf;
                    cur = h.firstChild().*;
                },
                .empty => unreachable,
            }
        }
    }

    fn findAnyLeafNode(n: Node) *const Leaf {
        var cur = n;
        while (true) {
            switch (node.nodeDecode(cur)) {
                .leaf => |l| return l,
                .internal => |h| {
                    if (h.loadLeafValue()) |leaf| return leaf;
                    cur = h.firstChild().*;
                },
                .empty => unreachable,
            }
        }
    }

    /// Walks the compressed prefix of an internal node against `key` starting at `depth`.
    /// Returns the updated depth when all prefix bytes match, or null on mismatch.
    ///
    /// Time Complexity: O(p) where `p` is the compressed-prefix length.
    ///
    /// Allocator: Does not allocate.
    fn matchPrefixExact(
        n: Node,
        header: *const NodeHeader,
        key: []const u8,
        depth_in: usize,
    ) ?usize {
        var depth = depth_in;
        const max_cmp = @min(header.prefix_len, MAX_PREFIX_LEN);
        for (0..max_cmp) |i| {
            if (depth >= key.len or header.prefix[i] != key[depth]) return null;
            depth += 1;
        }
        if (header.prefix_len > MAX_PREFIX_LEN) {
            const leaf = findAnyLeafNode(n);
            const limit = @min(leaf.key.len, key.len);
            for (MAX_PREFIX_LEN..header.prefix_len) |_| {
                if (depth >= limit or leaf.key[depth] != key[depth]) return null;
                depth += 1;
            }
        }
        return depth;
    }

    /// Allocates and initializes one leaf for `key` and `value`.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Allocates one `Leaf` via `self.allocator`.
    fn createLeaf(self: *Tree, key: []const u8, value: *Value) !*Leaf {
        const leaf = try self.allocator.create(Leaf);
        leaf.* = .{ .key = key, .value = value };
        return leaf;
    }

    /// Resolves the stored value pointer for one exact key when present.
    ///
    /// Time Complexity: O(k), where `k` is `key.len`.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Returns a borrowed stored value pointer owned by the caller-managed tree lifetime.
    pub fn lookup(self: *const Tree, key: []const u8) ?*Value {
        var current: Node = @atomicLoad(usize, &self.root, .acquire);
        var depth: usize = 0;

        while (!node.nodeIsEmpty(current)) {
            switch (node.nodeDecode(current)) {
                .leaf => |leaf| {
                    if (std.mem.eql(u8, leaf.key, key)) {
                        return leaf.loadValue();
                    }
                    return null;
                },
                .internal => |header| {
                    depth = matchPrefixExact(current, header, key, depth) orelse return null;

                    if (depth == key.len) {
                        return if (header.loadLeafValue()) |leaf| leaf.loadValue() else null;
                    }

                    const key_byte = key[depth];
                    const next_child_ptr = header.findChild(key_byte);

                    if (next_child_ptr) |child_ptr| {
                        current = @atomicLoad(usize, child_ptr, .acquire);
                        depth += 1;
                    } else {
                        return null;
                    }
                },
                .empty => unreachable,
            }
        }
        return null;
    }

    /// Finds the exact leaf record for `key`, whether stored as a direct leaf or as `leaf_value` on an internal node.
    ///
    /// Time Complexity: O(k) where `k` is key length.
    ///
    /// Allocator: Does not allocate.
    pub fn findLeafForExactKey(self: *Tree, key: []const u8) ?*Leaf {
        var current: Node = self.root;
        var depth: usize = 0;

        while (!node.nodeIsEmpty(current)) {
            switch (node.nodeDecode(current)) {
                .leaf => |leaf| {
                    if (std.mem.eql(u8, leaf.key, key)) return leaf;
                    return null;
                },
                .internal => |header| {
                    depth = matchPrefixExact(current, header, key, depth) orelse return null;

                    if (depth == key.len) {
                        if (header.loadLeafValue()) |leaf| {
                            if (std.mem.eql(u8, leaf.key, key)) return leaf;
                        }
                        return null;
                    }

                    const key_byte = key[depth];
                    const next_child_ptr = header.findChild(key_byte);

                    if (next_child_ptr) |child_ptr| {
                        current = child_ptr.*;
                        depth += 1;
                    } else {
                        return null;
                    }
                },
                .empty => unreachable,
            }
        }
        return null;
    }

    /// Inserts a new key-value pair into the ART.
    /// This operation covers standard insertions and the complexities of Adaptive Radix Trees:
    /// 1. **Path Compression Check**: Analyzes the stored prefix. If a mismatch is discovered,
    ///    an intermediate `Node4` is seamlessly injected (Node Split) capturing the divergence.
    /// 2. **Leaf Split**: If the inserted key perfectly matches the path of an existing Leaf but
    ///    extends it, or diverges at the final string suffix, the existing Leaf is exploded into
    ///    a `Node4` pushing both the old Leaf and the new Leaf underneath it.
    /// 3. **Node Tracing**: Traverses through intermediate classes evaluating SIMD arrays. If the
    ///    current node exhausts capacity, `addChild` invokes `.grow()` promoting it symmetrically.
    /// Time Complexity: O(k) where k is the length of the key, plus worst-case constant bounds for Node growth.
    /// Allocator: Allocates new `Leaf` and internal nodes (`Node4`) via `self.allocator` upon prefix mismatch or new child mapping.
    pub fn insert(self: *Tree, key: []const u8, value: *Value) !void {
        var node_ref: *Node = &self.root;
        var depth: usize = 0;

        while (!node.nodeIsEmpty(node_ref.*)) {
            switch (node.nodeDecode(node_ref.*)) {
                .leaf => |old_leaf| {
                    if (std.mem.eql(u8, old_leaf.key, key)) {
                        // Exact match overwrite
                        old_leaf.storeValue(value);
                        return;
                    }

                    // Split the leaf
                    var i: usize = depth;
                    const min_len = @min(old_leaf.key.len, key.len);
                    while (i < min_len and old_leaf.key[i] == key[i]) : (i += 1) {}

                    const new_n4 = try self.allocator.create(Node4);
                    new_n4.* = Node4.init();

                    const prefix_len = i - depth;
                    new_n4.header.prefix_len = @intCast(prefix_len);
                    const max_cmp = @min(prefix_len, MAX_PREFIX_LEN);
                    if (max_cmp > 0) {
                        @memcpy(new_n4.header.prefix[0..max_cmp], key[depth .. depth + max_cmp]);
                    }

                    var tmp_node = node.nodeInternal(&new_n4.header);

                    if (i < old_leaf.key.len) {
                        try node.addChild(&tmp_node, self.allocator, old_leaf.key[i], node_ref.*);
                    } else {
                        new_n4.header.storeLeafValue(old_leaf);
                    }

                    const new_leaf = try self.createLeaf(key, value);
                    if (i < key.len) {
                        try node.addChild(&tmp_node, self.allocator, key[i], node.nodeLeaf(new_leaf));
                    } else {
                        new_n4.header.storeLeafValue(new_leaf);
                    }

                    @atomicStore(usize, node_ref, tmp_node, .monotonic);
                    return;
                },
                .internal => |header| {
                    // Check path compression mismatch
                    const p_len = header.prefix_len;
                    var mismatch_idx: usize = 0;

                    // Check stored prefix
                    const max_cmp = @min(p_len, MAX_PREFIX_LEN);
                    while (mismatch_idx < max_cmp and depth < key.len and header.prefix[mismatch_idx] == key[depth]) {
                        mismatch_idx += 1;
                        depth += 1;
                    }

                    // Check hidden prefix
                    var any_leaf: ?*const Leaf = null;
                    if (mismatch_idx == MAX_PREFIX_LEN and p_len > MAX_PREFIX_LEN) {
                        any_leaf = Tree.findAnyLeaf(node_ref);
                        const limit = @min(any_leaf.?.key.len, key.len);
                        while (mismatch_idx < p_len and depth < limit and any_leaf.?.key[depth] == key[depth]) {
                            mismatch_idx += 1;
                            depth += 1;
                        }
                    }

                    if (mismatch_idx < p_len) {
                        // Prefix mismatched. Node splitting required.
                        if (any_leaf == null) any_leaf = Tree.findAnyLeaf(node_ref);

                        const new_n4 = try self.allocator.create(Node4);
                        new_n4.* = Node4.init();
                        new_n4.header.prefix_len = @intCast(mismatch_idx);

                        const new_n4_stored = @min(mismatch_idx, MAX_PREFIX_LEN);
                        if (new_n4_stored > 0) {
                            if (mismatch_idx <= MAX_PREFIX_LEN) {
                                @memcpy(new_n4.header.prefix[0..new_n4_stored], header.prefix[0..new_n4_stored]);
                            } else {
                                @memcpy(new_n4.header.prefix[0..MAX_PREFIX_LEN], header.prefix[0..MAX_PREFIX_LEN]);
                            }
                        }

                        // Adjust old node prefix
                        header.prefix_len -= @intCast(mismatch_idx + 1);

                        if (header.prefix_len > 0) {
                            const p_rem = @min(header.prefix_len, MAX_PREFIX_LEN);
                            @memcpy(header.prefix[0..p_rem], any_leaf.?.key[depth + 1 .. depth + 1 + p_rem]);
                        }

                        // Add old node to new_n4
                        var tmp_node = node.nodeInternal(&new_n4.header);
                        try node.addChild(&tmp_node, self.allocator, any_leaf.?.key[depth], node_ref.*);

                        // Add new leaf to new_n4
                        const new_leaf = try self.createLeaf(key, value);
                        if (depth == key.len) {
                            new_n4.header.storeLeafValue(new_leaf);
                        } else {
                            try node.addChild(&tmp_node, self.allocator, key[depth], node.nodeLeaf(new_leaf));
                        }

                        // Replace parent ptr
                        @atomicStore(usize, node_ref, tmp_node, .monotonic);
                        return;
                    }

                    // No mismatch, prefix traversed. Proceed to children.
                    if (depth == key.len) {
                        if (header.loadLeafValue()) |old_leaf| {
                            old_leaf.storeValue(value);
                        } else {
                            header.storeLeafValue(try self.createLeaf(key, value));
                        }
                        return;
                    }

                    const key_byte = key[depth];
                    const next_child = header.findChild(key_byte);

                    if (next_child) |child_ptr| {
                        node_ref = child_ptr; // Move down
                        depth += 1;
                    } else {
                        // Child not found, insert here
                        const new_leaf = try self.createLeaf(key, value);
                        try node.addChild(node_ref, self.allocator, key_byte, node.nodeLeaf(new_leaf));
                        return;
                    }
                },
                .empty => unreachable,
            }
        }

        // Tree is empty
        @atomicStore(usize, node_ref, node.nodeLeaf(try self.createLeaf(key, value)), .monotonic);
    }

    /// Builds a planner-only shadow tree from the current live ART.
    ///
    /// Time Complexity: O(n) over the live ART nodes copied into lightweight shadow metadata.
    ///
    /// Allocator: Allocates shadow metadata from `allocator`.
    pub fn buildShadowTree(self: *const Tree, allocator: std.mem.Allocator) !ShadowTree {
        return ShadowTree.initFromLive(allocator, &self.root);
    }

    /// Plans one insert against a mutable shadow tree without touching the live ART.
    ///
    /// Time Complexity: O(k) where `k` is key length, plus local shadow node mutation work.
    ///
    /// Allocator: Allocates prepared path metadata from `allocator`.
    pub fn planPreparedInsert(self: *const Tree, shadow: *ShadowTree, allocator: std.mem.Allocator, key: []const u8) !PreparedInsert {
        _ = self;
        return shadow.planInsert(allocator, key);
    }

    /// Applies one previously prepared insert using only preallocated reserved objects.
    ///
    /// Time Complexity: O(k) where `k` is key length.
    ///
    /// Allocator: Does not allocate.
    pub fn applyPreparedInsert(self: *Tree, prepared: *const PreparedInsert, reserved: *const ReservedInsert) !void {
        return prepared_insert.applyPreparedInsert(&self.root, prepared, reserved);
    }

    /// Deletes a key from the tree, shrinking bounds as necessary.
    /// Iteratively searches and removes a key from the tree.
    /// If the target is successfully matched at a Leaf:
    /// 1. Deletes the pointer dependency from its parent by calling `.removeChild()`.
    ///    This naturally handles memory density tracking, reducing `Node256` back towards `Node4`.
    /// 2. As nodes shrink and merge (Path Compression), intermediate unused pointer hierarchies
    ///    are orphaned. We allow these to organically leak within the scope of the shard's scoped
    ///    ArenaAllocator, resulting in zero-cost O(1) immediate reclamation upon `db.reset()`.
    /// Time Complexity: O(k) where k is the length of the key.
    /// Allocator: May allocate during shrink transitions. Detached leaves and internal nodes are reclaimed only with their arena.
    pub fn delete(self: *Tree, key: []const u8) !bool {
        var node_ref: *Node = &self.root;
        var parent_ptr: ?*Node = null;
        var parent_key_byte: ?u8 = null;
        var depth: usize = 0;

        while (!node.nodeIsEmpty(node_ref.*)) {
            switch (node.nodeDecode(node_ref.*)) {
                .leaf => |leaf| {
                    if (std.mem.eql(u8, leaf.key, key)) {
                        if (parent_ptr) |parent| {
                            try node.removeChild(parent, self.allocator, parent_key_byte.?);
                        } else {
                            @atomicStore(usize, &self.root, node.nodeEmpty(), .monotonic);
                        }
                        return true;
                    }
                    return false;
                },
                .internal => |header| {
                    depth = matchPrefixExact(node_ref.*, header, key, depth) orelse return false;

                    if (depth > key.len) return false;

                    if (depth == key.len) {
                        if (header.loadLeafValue()) |old_leaf| {
                            if (std.mem.eql(u8, old_leaf.key, key)) {
                                header.storeLeafValue(null);

                                if (header.num_children == 0) {
                                    if (parent_ptr) |parent| {
                                        try node.removeChild(parent, self.allocator, parent_key_byte.?);
                                    } else {
                                        @atomicStore(usize, &self.root, node.nodeEmpty(), .monotonic);
                                    }
                                } else if (header.num_children == 1) {
                                    try node.shrink(node_ref, self.allocator);
                                }
                                return true;
                            }
                        }
                        return false;
                    }
                    const key_byte = key[depth];
                    const next_child = header.findChild(key_byte);

                    if (next_child) |child_ptr| {
                        parent_ptr = node_ref;
                        parent_key_byte = key_byte;
                        node_ref = child_ptr;
                        depth += 1;
                    } else {
                        return false;
                    }
                },
                .empty => unreachable,
            }
        }
        return false;
    }

    /// Removes all keys that start with `prefix` and returns the deleted count.
    /// Empty prefix prunes the entire tree.
    /// Time Complexity: O(k) where k = prefix.len, plus O(N) where N is the number of descendants removed.
    /// Allocator: Recursively visits and can free matching subtree nodes using `self.allocator`.
    pub fn prunePrefix(self: *Tree, prefix: []const u8) !usize {
        if (node.nodeIsEmpty(self.root)) {
            return 0;
        }

        if (prefix.len == 0) {
            const removed_all = countSubtreeKeys(&self.root);
            @atomicStore(usize, &self.root, node.nodeEmpty(), .monotonic);
            return removed_all;
        }

        const cut = locatePrefixCut(&self.root, prefix) orelse return 0;
        const removed = countSubtreeKeys(cut.target);
        try detachAtCut(self, cut);
        return removed;
    }

    /// Holds the state of a prefix match, separating the identified
    /// subtree `target` from its `parent` link so it can be safely detached.
    const PrefixCut = struct {
        target: *Node,
        parent: ?*Node,
        parent_key_byte: ?u8,
    };

    /// Locates the node where the prefix ends and returns a `PrefixCut` struct.
    /// Time Complexity: O(k) where k = prefix.len.
    /// Allocator: Does not allocate.
    fn locatePrefixCut(root: *Node, prefix: []const u8) ?PrefixCut {
        var current: *Node = root;
        var parent: ?*Node = null;
        var parent_key_byte: ?u8 = null;
        var depth: usize = 0;

        while (true) {
            switch (node.nodeDecode(current.*)) {
                .empty => return null,
                .leaf => |leaf| {
                    if (!std.mem.startsWith(u8, leaf.key, prefix)) return null;
                    return .{
                        .target = current,
                        .parent = parent,
                        .parent_key_byte = parent_key_byte,
                    };
                },
                .internal => |header| {
                    const p_len = header.prefix_len;
                    const max_cmp = @min(p_len, MAX_PREFIX_LEN);
                    for (0..max_cmp) |i| {
                        if (depth >= prefix.len) {
                            return .{
                                .target = current,
                                .parent = parent,
                                .parent_key_byte = parent_key_byte,
                            };
                        }
                        if (header.prefix[i] != prefix[depth]) return null;
                        depth += 1;
                    }

                    if (p_len > MAX_PREFIX_LEN) {
                        const any = findAnyLeaf(current);
                        for (MAX_PREFIX_LEN..p_len) |_| {
                            if (depth >= prefix.len) {
                                return .{
                                    .target = current,
                                    .parent = parent,
                                    .parent_key_byte = parent_key_byte,
                                };
                            }
                            if (any.key[depth] != prefix[depth]) return null;
                            depth += 1;
                        }
                    }

                    if (depth == prefix.len) {
                        return .{
                            .target = current,
                            .parent = parent,
                            .parent_key_byte = parent_key_byte,
                        };
                    }

                    const key_byte = prefix[depth];
                    const next = header.findChild(key_byte);
                    if (next) |child| {
                        parent = current;
                        parent_key_byte = key_byte;
                        current = child;
                        depth += 1;
                    } else {
                        return null;
                    }
                },
            }
        }
    }

    /// Detaches the subtree at the given cut.
    /// Time Complexity: O(1)
    /// Allocator: Does not allocate.
    fn detachAtCut(self: *Tree, cut: PrefixCut) !void {
        if (cut.parent) |parent| {
            // Route through removeChild so existing shrink/merge logic keeps ART compact.
            try node.removeChild(parent, self.allocator, cut.parent_key_byte.?);
        } else {
            @atomicStore(usize, &self.root, node.nodeEmpty(), .monotonic);
        }
    }

    /// Counts the number of keys in the subtree rooted at `n`.
    /// Time Complexity: O(N) where N is the number of nodes in the subtree.
    /// Allocator: Does not allocate.
    fn countSubtreeKeys(n: *const Node) usize {
        return switch (node.nodeDecode(n.*)) {
            .empty => 0,
            .leaf => 1,
            .internal => |header| blk: {
                var total: usize = if (header.loadLeafValue() != null) 1 else 0;
                const Ctx = struct {
                    total: *usize,

                    fn visit(ctx: @This(), edge_byte: u8, child: *const Node) anyerror!bool {
                        _ = edge_byte;
                        ctx.total.* += countSubtreeKeys(child);
                        return true;
                    }
                };
                header.forEachChild(Ctx, .{ .total = &total }, Ctx.visit) catch unreachable;
                break :blk total;
            },
        };
    }

    /// Collects all (key, value) pairs rooted at `node` in lexicographic order.
    /// Recurses into children sorted by key byte; emits leaf_value before children.
    /// Time Complexity: O(N) where N is the number of keys in the subtree.
    /// Allocator: Allocates memory for the results.
    fn collectAll(allocator: std.mem.Allocator, n: *const Node, results: *std.ArrayList(ScanEntry)) !void {
        switch (node.nodeDecode(n.*)) {
            .empty => return,
            .leaf => |leaf| {
                try results.append(allocator, .{ .key = leaf.key, .value = leaf.loadValue() });
            },
            .internal => |header| {
                if (header.loadLeafValue()) |lv| {
                    try results.append(allocator, .{ .key = lv.key, .value = lv.loadValue() });
                }
                const Ctx = struct {
                    allocator: std.mem.Allocator,
                    results: *std.ArrayList(ScanEntry),

                    fn visit(ctx: @This(), edge_byte: u8, child: *const Node) anyerror!bool {
                        _ = edge_byte;
                        try collectAll(ctx.allocator, child, ctx.results);
                        return true;
                    }
                };
                try header.forEachChild(Ctx, .{ .allocator = allocator, .results = results }, Ctx.visit);
            },
        }
    }

    /// Appends all (key, value) pairs whose key starts with `prefix` to `results`.
    /// Results are in lexicographic order.
    /// An empty prefix collects the entire tree.
    /// Time Complexity: O(k + N) where k = prefix.len and N is the number of matching elements.
    /// Allocator: Does not allocate internally. `results` array may allocate if its capacity is exceeded.
    pub fn scan(self: *const Tree, prefix: []const u8, allocator: std.mem.Allocator, results: *std.ArrayList(ScanEntry)) !void {
        if (node.nodeIsEmpty(self.root)) return;

        // Navigate to the subtree root that covers `prefix`
        var current: *const Node = &self.root;
        var depth: usize = 0;

        while (depth <= prefix.len) {
            switch (node.nodeDecode(current.*)) {
                .empty => return,
                .leaf => |leaf| {
                    // Only emit if this leaf's key actually starts with prefix
                    if (std.mem.startsWith(u8, leaf.key, prefix)) {
                        try results.append(allocator, .{ .key = leaf.key, .value = leaf.loadValue() });
                    }
                    return;
                },
                .internal => |header| {
                    // Walk through path-compressed prefix bytes
                    const p_len = header.prefix_len;
                    const max_cmp = @min(p_len, MAX_PREFIX_LEN);
                    for (0..max_cmp) |i| {
                        if (depth >= prefix.len) {
                            // We've consumed the entire search prefix inside a node prefix,
                            // everything under this node matches. Collect all.
                            return collectAll(allocator, current, results);
                        }
                        if (header.prefix[i] != prefix[depth]) return; // prefix not in tree
                        depth += 1;
                    }
                    if (p_len > MAX_PREFIX_LEN) {
                        const any = findAnyLeaf(current);
                        for (MAX_PREFIX_LEN..p_len) |_| {
                            if (depth >= prefix.len) {
                                return collectAll(allocator, current, results);
                            }
                            if (any.key[depth] != prefix[depth]) return;
                            depth += 1;
                        }
                    }

                    if (depth == prefix.len) {
                        // We've matched the full prefix at this internal node, collect subtree
                        return collectAll(allocator, current, results);
                    }

                    // Follow the next byte of the prefix into the child
                    const byte = prefix[depth];
                    const child = header.findChild(byte);
                    if (child) |c| {
                        current = c;
                        depth += 1;
                    } else {
                        return; // prefix not present
                    }
                },
            }
        }
    }

    /// Incremental scan entrypoint used by paginated DB scans.
    /// Collects at most `max_items` entries that:
    ///   1) start with `prefix`, and
    ///   2) are lexicographically greater than `start_after_key` when provided.
    /// Returns true if the tree was fully traversed, false if collection stopped
    /// early because `max_items` was reached.
    /// Time Complexity: O(k + N) where k = cursor.len and N is the max_items collected. Constant-time sub-tree skips dramatically accelerate bounded scans.
    /// Allocator: Does not allocate internally.
    pub fn scanFrom(
        self: *const Tree,
        prefix: []const u8,
        start_after_key: ?[]const u8,
        allocator: std.mem.Allocator,
        results: *std.ArrayList(ScanEntry),
        max_items: usize,
    ) !bool {
        if (node.nodeIsEmpty(self.root)) return true;
        if (max_items == 0) return false;
        if (start_after_key) |cursor| {
            return collectMatchingLimitedSeek(&self.root, prefix, cursor, 0, allocator, results, max_items);
        }
        return collectMatchingLimitedNoCursor(&self.root, prefix, allocator, results, max_items);
    }

    /// Collects all (key, value) pairs whose key is within [start, end).
    /// Time Complexity: O(k + N) where k is the length of the interval and N is the number of matching elements.
    /// Allocator: Does not allocate internally.
    pub fn scanRange(
        self: *const Tree,
        range: KeyRange,
        allocator: std.mem.Allocator,
        results: *std.ArrayList(ScanEntry),
    ) !void {
        _ = try self.scanRangeFrom(range, null, allocator, results, std.math.maxInt(usize));
    }

    /// Incremental range scan entrypoint used by paginated DB range scans.
    /// Collects at most `max_items` entries that:
    ///   1) are in [range.start, range.end), and
    ///   2) are lexicographically greater than `start_after_key` when provided.
    /// Returns true if the tree was fully traversed, false if collection stopped
    /// early because `max_items` was reached.
    /// Time Complexity: O(k + N) where k is cursor length and N is the max_items bounds collected.
    /// Allocator: Does not allocate internally.
    pub fn scanRangeFrom(
        self: *const Tree,
        range: KeyRange,
        start_after_key: ?[]const u8,
        allocator: std.mem.Allocator,
        results: *std.ArrayList(ScanEntry),
        max_items: usize,
    ) !bool {
        var append_ctx = RangeAppendCtx{
            .allocator = allocator,
            .results = results,
        };
        return self.scanRangeWalkFrom(range, start_after_key, &append_ctx, appendRangeWalkEntry, max_items);
    }

    /// Visits at most `max_items` entries that:
    ///   1) are in [range.start, range.end), and
    ///   2) are lexicographically greater than `start_after_key` when provided.
    /// Returns true if the tree was fully traversed, false if visitation stopped
    /// early because `max_items` was reached.
    ///
    /// Time Complexity: O(k + N) where `k` is cursor length and `N` is the visited entry count up to `max_items`.
    ///
    /// Allocator: Does not allocate.
    pub fn scanRangeVisitFrom(
        self: *const Tree,
        range: KeyRange,
        start_after_key: ?[]const u8,
        ctx: *anyopaque,
        visit: VisitFn,
        max_items: usize,
    ) !bool {
        return self.scanRangeWalkFrom(range, start_after_key, ctx, visit, max_items);
    }

    /// Shared range-walk algorithm used by public scan wrappers.
    ///
    /// Time Complexity: O(k + N) where `k` is cursor length and `N` is the visited entry count up to `max_items`.
    ///
    /// Allocator: Does not allocate directly; callback behavior controls allocation.
    fn scanRangeWalkFrom(
        self: *const Tree,
        range: KeyRange,
        start_after_key: ?[]const u8,
        ctx: *anyopaque,
        visit: VisitFn,
        max_items: usize,
    ) !bool {
        if (!isRangeValid(range)) return error.InvalidRangeBounds;
        if (node.nodeIsEmpty(self.root)) return true;
        if (max_items == 0) return false;

        const lower_bound = effectiveLowerBound(range.start, start_after_key);
        if (range.end) |end_key| {
            if (lower_bound) |lower| {
                switch (std.mem.order(u8, lower.cursor, end_key)) {
                    .lt => {},
                    .eq, .gt => return true,
                }
            }
        }

        var state = RangeWalkState{
            .range = range,
            .lower_bound = lower_bound,
            .ctx = ctx,
            .visit = visit,
            .max_items = max_items,
        };
        if (lower_bound) |lower| {
            try rangeWalkSeekNode(&self.root, &state, lower.cursor, 0);
        } else {
            try rangeWalkNode(&self.root, &state);
        }
        return !state.hit_limit;
    }

    /// Function pointer type for iterating over the tree without allocating memory.
    /// Time Complexity: Dependent on the implementation of the callback.
    /// Allocator: Handled by the user-provided context.
    pub const VisitFn = *const fn (ctx: *anyopaque, key: []const u8, value: *const Value) anyerror!void;

    /// Visits every key/value pair in lexicographic order without allocating scan buffers.
    /// Returns the number of visited entries.
    /// Time Complexity: O(N) where N is the total number of items in the tree.
    /// Allocator: Does not allocate.
    pub fn forEach(self: *const Tree, ctx: *anyopaque, visit: VisitFn) !usize {
        if (node.nodeIsEmpty(self.root)) return 0;
        var visited: usize = 0;
        try visitNodeAll(&self.root, ctx, visit, &visited);
        return visited;
    }

    /// Collects all keys under a given subtree matching the prefix, up to `max_items`.
    /// Used when the scan has already passed or matched the pagination cursor.
    /// Time Complexity: O(N) where N is the number of visited nodes.
    /// Allocator: Allocates only when appending to `results`.
    fn collectMatchingLimitedNoCursor(
        n: *const Node,
        prefix: []const u8,
        allocator: std.mem.Allocator,
        results: *std.ArrayList(ScanEntry),
        max_items: usize,
    ) anyerror!bool {
        if (results.items.len >= max_items) return false;

        switch (node.nodeDecode(n.*)) {
            .empty => return true,
            .leaf => |leaf| {
                if (std.mem.startsWith(u8, leaf.key, prefix)) {
                    try results.append(allocator, .{ .key = leaf.key, .value = leaf.loadValue() });
                    if (results.items.len >= max_items) return false;
                }
                return true;
            },
            .internal => |header| {
                if (header.loadLeafValue()) |lv| {
                    if (std.mem.startsWith(u8, lv.key, prefix)) {
                        try results.append(allocator, .{ .key = lv.key, .value = lv.loadValue() });
                        if (results.items.len >= max_items) return false;
                    }
                }
                return collectChildrenNoCursor(header, prefix, allocator, results, max_items);
            },
        }
    }

    /// Tracks the relative position of the scan cursor compared to the
    /// path compressed bytes inside a node header.
    const PrefixCursorRelation = enum {
        before_cursor,
        at_or_after_cursor,
        equal_prefix,
    };

    /// Represents the evaluation result after comparing the compressed prefix.
    const PrefixCursorStep = struct {
        relation: PrefixCursorRelation,
        depth: usize,
    };

    /// Compares the compressed prefix of a node against the pagination cursor.
    /// Determines if the current path is before, exactly at, or after the cursor.
    /// Time Complexity: O(p) where p is the minimum of prefix length and cursor length.
    /// Allocator: Does not allocate.
    fn compareCompressedPrefixWithCursor(
        n: *const Node,
        header: *const NodeHeader,
        depth_in: usize,
        cursor: []const u8,
    ) PrefixCursorStep {
        var depth = depth_in;

        const p_len = header.prefix_len;
        const max_cmp = @min(p_len, MAX_PREFIX_LEN);
        for (0..max_cmp) |i| {
            if (depth >= cursor.len) return .{ .relation = .at_or_after_cursor, .depth = depth };
            const node_b = header.prefix[i];
            const cursor_b = cursor[depth];
            if (node_b < cursor_b) return .{ .relation = .before_cursor, .depth = depth };
            if (node_b > cursor_b) return .{ .relation = .at_or_after_cursor, .depth = depth };
            depth += 1;
        }

        if (p_len > MAX_PREFIX_LEN) {
            const any = findAnyLeaf(n);
            for (MAX_PREFIX_LEN..p_len) |_| {
                if (depth >= cursor.len) return .{ .relation = .at_or_after_cursor, .depth = depth };
                const node_b = any.key[depth];
                const cursor_b = cursor[depth];
                if (node_b < cursor_b) return .{ .relation = .before_cursor, .depth = depth };
                if (node_b > cursor_b) return .{ .relation = .at_or_after_cursor, .depth = depth };
                depth += 1;
            }
        }

        return .{ .relation = .equal_prefix, .depth = depth };
    }

    /// Traverses the tree for prefix matches while ensuring results are strictly
    /// after the pagination cursor. Skips branches that are lexicographically smaller.
    /// Time Complexity: O(k + N) where k is the cursor length and N is the nodes visited.
    /// Allocator: Allocates only when appending to `results`.
    fn collectMatchingLimitedSeek(
        n: *const Node,
        prefix: []const u8,
        cursor: []const u8,
        depth: usize,
        allocator: std.mem.Allocator,
        results: *std.ArrayList(ScanEntry),
        max_items: usize,
    ) anyerror!bool {
        if (results.items.len >= max_items) return false;

        switch (node.nodeDecode(n.*)) {
            .empty => return true,
            .leaf => |leaf| {
                if (std.mem.startsWith(u8, leaf.key, prefix) and isAfterCursor(leaf.key, cursor)) {
                    try results.append(allocator, .{ .key = leaf.key, .value = leaf.loadValue() });
                    if (results.items.len >= max_items) return false;
                }
                return true;
            },
            .internal => |header| {
                const step = compareCompressedPrefixWithCursor(n, header, depth, cursor);
                switch (step.relation) {
                    .before_cursor => return true,
                    .at_or_after_cursor => return collectMatchingLimitedNoCursor(n, prefix, allocator, results, max_items),
                    .equal_prefix => {},
                }

                const next_depth = step.depth;

                if (header.loadLeafValue()) |lv| {
                    if (std.mem.startsWith(u8, lv.key, prefix) and isAfterCursor(lv.key, cursor)) {
                        try results.append(allocator, .{ .key = lv.key, .value = lv.loadValue() });
                        if (results.items.len >= max_items) return false;
                    }
                }

                if (next_depth >= cursor.len) {
                    return collectChildrenNoCursor(header, prefix, allocator, results, max_items);
                }

                const cursor_byte = cursor[next_depth];
                switch (header.nodeType) {
                    .node4 => {
                        const n4 = @as(*const Node4, @alignCast(@fieldParentPtr("header", header)));
                        for (0..n4.header.num_children) |i| {
                            const child_byte = n4.keys[i];
                            if (child_byte < cursor_byte) continue;
                            if (child_byte == cursor_byte) {
                                if (!try collectMatchingLimitedSeek(&n4.children[i], prefix, cursor, next_depth + 1, allocator, results, max_items)) return false;
                            } else {
                                if (!try collectMatchingLimitedNoCursor(&n4.children[i], prefix, allocator, results, max_items)) return false;
                            }
                        }
                    },
                    .node16 => {
                        const n16 = @as(*const Node16, @alignCast(@fieldParentPtr("header", header)));
                        for (0..n16.header.num_children) |i| {
                            const child_byte = n16.keys[i];
                            if (child_byte < cursor_byte) continue;
                            if (child_byte == cursor_byte) {
                                if (!try collectMatchingLimitedSeek(&n16.children[i], prefix, cursor, next_depth + 1, allocator, results, max_items)) return false;
                            } else {
                                if (!try collectMatchingLimitedNoCursor(&n16.children[i], prefix, allocator, results, max_items)) return false;
                            }
                        }
                    },
                    .node48 => {
                        const n48 = @as(*const Node48, @alignCast(@fieldParentPtr("header", header)));
                        for (0..256) |b| {
                            const idx = n48.child_index[b];
                            if (idx == Node48.EMPTY_INDEX) continue;
                            const child_byte: u8 = @intCast(b);
                            if (child_byte < cursor_byte) continue;
                            if (child_byte == cursor_byte) {
                                if (!try collectMatchingLimitedSeek(&n48.children[idx], prefix, cursor, next_depth + 1, allocator, results, max_items)) return false;
                            } else {
                                if (!try collectMatchingLimitedNoCursor(&n48.children[idx], prefix, allocator, results, max_items)) return false;
                            }
                        }
                    },
                    .node256 => {
                        const n256 = @as(*const Node256, @alignCast(@fieldParentPtr("header", header)));
                        for (0..256) |b| {
                            if (node.nodeIsEmpty(n256.children[b])) continue;
                            const child_byte: u8 = @intCast(b);
                            if (child_byte < cursor_byte) continue;
                            if (child_byte == cursor_byte) {
                                if (!try collectMatchingLimitedSeek(&n256.children[b], prefix, cursor, next_depth + 1, allocator, results, max_items)) return false;
                            } else {
                                if (!try collectMatchingLimitedNoCursor(&n256.children[b], prefix, allocator, results, max_items)) return false;
                            }
                        }
                    },
                }
                return true;
            },
        }
    }

    /// Recursively collects all keys in the descendants of an internal node without cursor bounds.
    /// Called when the prefix and cursor requirements are fully satisfied.
    /// Time Complexity: O(N) where N is the number of descendants.
    /// Allocator: Allocates only when appending to `results`.
    fn collectChildrenNoCursor(
        header: *const NodeHeader,
        prefix: []const u8,
        allocator: std.mem.Allocator,
        results: *std.ArrayList(ScanEntry),
        max_items: usize,
    ) anyerror!bool {
        const Ctx = struct {
            prefix: []const u8,
            allocator: std.mem.Allocator,
            results: *std.ArrayList(ScanEntry),
            max_items: usize,
            stopped: *bool,

            fn visit(ctx: @This(), edge_byte: u8, child: *const Node) anyerror!bool {
                _ = edge_byte;
                const cont = try collectMatchingLimitedNoCursor(child, ctx.prefix, ctx.allocator, ctx.results, ctx.max_items);
                if (!cont) {
                    ctx.stopped.* = true;
                    return false;
                }
                return true;
            }
        };

        var stopped = false;
        try header.forEachChild(Ctx, .{
            .prefix = prefix,
            .allocator = allocator,
            .results = results,
            .max_items = max_items,
            .stopped = &stopped,
        }, Ctx.visit);
        return !stopped;
    }

    /// Checks if a given key is lexicographically strictly greater than the cursor.
    /// Time Complexity: O(k) where k is the common prefix length.
    /// Allocator: Does not allocate.
    fn isAfterCursor(key: []const u8, cursor_key: []const u8) bool {
        return std.mem.order(u8, cursor_key, key) == .lt;
    }

    const RangeAppendCtx = struct {
        allocator: std.mem.Allocator,
        results: *std.ArrayList(ScanEntry),
    };

    fn appendRangeWalkEntry(ctx_ptr: *anyopaque, key: []const u8, value: *const Value) !void {
        const ctx: *RangeAppendCtx = @ptrCast(@alignCast(ctx_ptr));
        try ctx.results.append(ctx.allocator, .{ .key = key, .value = value });
    }

    /// State container for paginated range walking with shared bounds and stop logic.
    const RangeWalkState = struct {
        range: KeyRange,
        lower_bound: ?LowerBound,
        ctx: *anyopaque,
        visit: VisitFn,
        max_items: usize,
        visited: usize = 0,
        stop: bool = false,
        hit_limit: bool = false,
    };

    const LowerBound = struct {
        cursor: []const u8,
        inclusive: bool,
    };

    /// Validates if a `KeyRange` is properly formed (start is strictly less than end).
    /// Time Complexity: O(k) where k is the prefix length.
    /// Allocator: Does not allocate.
    fn isRangeValid(range: KeyRange) bool {
        if (range.start == null or range.end == null) return true;
        return std.mem.order(u8, range.start.?, range.end.?) == .lt;
    }

    /// Checks if a given key is greater than or equal to the range start.
    /// Time Complexity: O(k) where k is the key length.
    /// Allocator: Does not allocate.
    fn keySatisfiesLower(range: KeyRange, key: []const u8) bool {
        if (range.start) |start| {
            return switch (std.mem.order(u8, key, start)) {
                .lt => false,
                .eq, .gt => true,
            };
        }
        return true;
    }

    /// Checks if a given key is strictly less than the range end.
    /// Time Complexity: O(k) where k is the key length.
    /// Allocator: Does not allocate.
    fn keyBeforeUpper(range: KeyRange, key: []const u8) bool {
        if (range.end) |end| {
            return std.mem.order(u8, key, end) == .lt;
        }
        return true;
    }

    /// Determines if a key satisfies the effective lower bound.
    /// Time Complexity: O(k) where k is the key length.
    /// Allocator: Does not allocate.
    fn keySatisfiesLowerBound(key: []const u8, lower_bound: LowerBound) bool {
        return switch (std.mem.order(u8, key, lower_bound.cursor)) {
            .lt => false,
            .eq => lower_bound.inclusive,
            .gt => true,
        };
    }

    fn effectiveLowerBound(range_start: ?[]const u8, start_after_key: ?[]const u8) ?LowerBound {
        if (range_start == null and start_after_key == null) return null;
        if (range_start == null) return .{ .cursor = start_after_key.?, .inclusive = false };
        if (start_after_key == null) return .{ .cursor = range_start.?, .inclusive = true };

        return switch (std.mem.order(u8, range_start.?, start_after_key.?)) {
            .lt, .eq => .{ .cursor = start_after_key.?, .inclusive = false },
            .gt => .{ .cursor = range_start.?, .inclusive = true },
        };
    }

    /// Evaluates bounds and limits before invoking the active range-walk consumer.
    ///
    /// Time Complexity: O(k) for range evaluation plus callback cost.
    ///
    /// Allocator: Does not allocate directly.
    fn rangeWalkMaybeVisitEntry(key: []const u8, value: *const Value, state: *RangeWalkState) !void {
        if (state.stop) return;

        if (!keyBeforeUpper(state.range, key)) {
            state.stop = true;
            return;
        }

        if (!keySatisfiesLower(state.range, key)) return;

        if (state.lower_bound) |lower| {
            if (!keySatisfiesLowerBound(key, lower)) return;
        }

        try state.visit(state.ctx, key, value);
        state.visited += 1;
        if (state.visited >= state.max_items) {
            state.stop = true;
            state.hit_limit = true;
        }
    }

    /// Shared traversal over child nodes for range-walk wrappers.
    ///
    /// Time Complexity: O(N) where N is the total evaluated children until stop.
    ///
    /// Allocator: Does not allocate directly.
    fn rangeWalkChildrenNoCursor(header: *const NodeHeader, state: *RangeWalkState) anyerror!void {
        const Ctx = struct {
            state: *RangeWalkState,

            fn visit(ctx: @This(), edge_byte: u8, child: *const Node) anyerror!bool {
                _ = edge_byte;
                try rangeWalkNode(child, ctx.state);
                return !ctx.state.stop;
            }
        };

        try header.forEachChild(Ctx, .{ .state = state }, Ctx.visit);
    }

    /// Shared traversal that seeks to the lower bound before visiting matching descendants.
    ///
    /// Time Complexity: O(k + N) where k is lower cursor length and N is the visited subtree count after seek.
    ///
    /// Allocator: Does not allocate directly.
    fn rangeWalkSeekNode(n: *const Node, state: *RangeWalkState, cursor: []const u8, depth: usize) anyerror!void {
        if (state.stop) return;

        switch (node.nodeDecode(n.*)) {
            .empty => return,
            .leaf => |leaf| {
                try rangeWalkMaybeVisitEntry(leaf.key, leaf.loadValue(), state);
            },
            .internal => |header| {
                const step = compareCompressedPrefixWithCursor(n, header, depth, cursor);
                switch (step.relation) {
                    .before_cursor => return,
                    .at_or_after_cursor => {
                        try rangeWalkNode(n, state);
                        return;
                    },
                    .equal_prefix => {},
                }

                const next_depth = step.depth;
                if (header.loadLeafValue()) |lv| {
                    try rangeWalkMaybeVisitEntry(lv.key, lv.loadValue(), state);
                }
                if (state.stop) return;
                if (next_depth >= cursor.len) {
                    try rangeWalkChildrenNoCursor(header, state);
                    return;
                }

                const cursor_byte = cursor[next_depth];
                switch (header.nodeType) {
                    .node4 => {
                        const n4 = @as(*const Node4, @alignCast(@fieldParentPtr("header", header)));
                        for (0..n4.header.num_children) |i| {
                            const child_byte = n4.keys[i];
                            if (child_byte < cursor_byte) continue;
                            if (child_byte == cursor_byte) {
                                try rangeWalkSeekNode(&n4.children[i], state, cursor, next_depth + 1);
                            } else {
                                try rangeWalkNode(&n4.children[i], state);
                            }
                            if (state.stop) return;
                        }
                    },
                    .node16 => {
                        const n16 = @as(*const Node16, @alignCast(@fieldParentPtr("header", header)));
                        for (0..n16.header.num_children) |i| {
                            const child_byte = n16.keys[i];
                            if (child_byte < cursor_byte) continue;
                            if (child_byte == cursor_byte) {
                                try rangeWalkSeekNode(&n16.children[i], state, cursor, next_depth + 1);
                            } else {
                                try rangeWalkNode(&n16.children[i], state);
                            }
                            if (state.stop) return;
                        }
                    },
                    .node48 => {
                        const n48 = @as(*const Node48, @alignCast(@fieldParentPtr("header", header)));
                        for (0..256) |b| {
                            const idx = n48.child_index[b];
                            if (idx == Node48.EMPTY_INDEX) continue;
                            const child_byte: u8 = @intCast(b);
                            if (child_byte < cursor_byte) continue;
                            if (child_byte == cursor_byte) {
                                try rangeWalkSeekNode(&n48.children[idx], state, cursor, next_depth + 1);
                            } else {
                                try rangeWalkNode(&n48.children[idx], state);
                            }
                            if (state.stop) return;
                        }
                    },
                    .node256 => {
                        const n256 = @as(*const Node256, @alignCast(@fieldParentPtr("header", header)));
                        for (0..256) |b| {
                            if (node.nodeIsEmpty(n256.children[b])) continue;
                            const child_byte: u8 = @intCast(b);
                            if (child_byte < cursor_byte) continue;
                            if (child_byte == cursor_byte) {
                                try rangeWalkSeekNode(&n256.children[b], state, cursor, next_depth + 1);
                            } else {
                                try rangeWalkNode(&n256.children[b], state);
                            }
                            if (state.stop) return;
                        }
                    },
                }
            },
        }
    }

    /// Top-level shared range-walk recursion.
    ///
    /// Time Complexity: O(N) bounded by deep recursion limits.
    ///
    /// Allocator: Does not allocate directly.
    fn rangeWalkNode(n: *const Node, state: *RangeWalkState) anyerror!void {
        if (state.stop) return;

        switch (node.nodeDecode(n.*)) {
            .empty => return,
            .leaf => |leaf| {
                try rangeWalkMaybeVisitEntry(leaf.key, leaf.loadValue(), state);
            },
            .internal => |header| {
                if (header.loadLeafValue()) |lv| {
                    try rangeWalkMaybeVisitEntry(lv.key, lv.loadValue(), state);
                }
                if (state.stop) return;
                try rangeWalkChildrenNoCursor(header, state);
            },
        }
    }

    /// Internal traversal handler feeding key-value pairs to the provided `VisitFn`.
    /// Time Complexity: O(N) across all leaf descendants.
    /// Allocator: Does not allocate.
    fn visitNodeAll(n: *const Node, ctx: *anyopaque, visit: VisitFn, visited: *usize) !void {
        switch (node.nodeDecode(n.*)) {
            .empty => return,
            .leaf => |leaf| {
                try visit(ctx, leaf.key, leaf.loadValue());
                visited.* += 1;
            },
            .internal => |header| {
                if (header.loadLeafValue()) |leaf| {
                    try visit(ctx, leaf.key, leaf.loadValue());
                    visited.* += 1;
                }
                const Ctx = struct {
                    ctx: *anyopaque,
                    visit: VisitFn,
                    visited: *usize,

                    fn visitChild(local: @This(), edge_byte: u8, child: *const Node) anyerror!bool {
                        _ = edge_byte;
                        try visitNodeAll(child, local.ctx, local.visit, local.visited);
                        return true;
                    }
                };

                try header.forEachChild(Ctx, .{ .ctx = ctx, .visit = visit, .visited = visited }, Ctx.visitChild);
            },
        }
    }
};

/// Holds a key-value pointer pair retrieved during tree scanning operations.
/// Returned directly via ArrayList without deep-copying keys to enforce zero unnecessary allocation.
pub const ScanEntry = struct {
    key: []const u8,
    value: *const Value,
};

/// A stateful Iterator for Adaptive Radix Tree, allowing incremental O(1) `next()` calls
/// after an initial O(k) seek setup. Eliminates per-item chunk allocations.
pub const Iterator = struct {
    allocator: std.mem.Allocator,
    prefix: []const u8,
    cursor: ?[]const u8,
    inline_stack: [inline_stack_capacity]Frame = undefined,
    inline_len: usize = 0,
    spill_stack: std.ArrayList(Frame) = .empty,

    pub const Frame = struct {
        node: *const Node,
        depth: usize,
        step: u16,
    };

    const inline_stack_capacity: usize = 16;

    /// Initializes a new Prefix Scanner Iterator.
    /// `cursor` must outlive the `Iterator` if provided.
    pub fn init(allocator: std.mem.Allocator, tree: *const Tree, prefix: []const u8, cursor: ?[]const u8) !Iterator {
        var it = Iterator{
            .allocator = allocator,
            .prefix = prefix,
            .cursor = cursor,
        };
        if (!node.nodeIsEmpty(tree.root)) {
            try it.push(.{
                .node = &tree.root,
                .depth = 0,
                .step = 0,
            });
        }
        return it;
    }

    /// Frees internal stack allocations.
    pub fn deinit(self: *Iterator) void {
        self.spill_stack.deinit(self.allocator);
        self.* = undefined;
    }

    /// Advances the iterator and returns the next matching element.
    /// Time Complexity: O(k) for the first call (seek), O(1) amortized for subsequent calls.
    pub fn next(self: *Iterator) !?ScanEntry {
        while (self.peek()) |frame| {
            switch (node.nodeDecode(frame.node.*)) {
                .empty => {
                    self.pop();
                    continue;
                },
                .leaf => |leaf| {
                    const is_match = std.mem.startsWith(u8, leaf.key, self.prefix);
                    var valid = is_match;

                    if (valid and self.cursor != null) {
                        if (std.mem.order(u8, self.cursor.?, leaf.key) != .lt) {
                            valid = false;
                        } else {
                            self.cursor = null;
                        }
                    }

                    self.pop();
                    if (valid) return ScanEntry{ .key = leaf.key, .value = leaf.loadValue() };
                    continue;
                },
                .internal => |header| {
                    if (frame.step == 0) {
                        frame.step = 1;

                        const p_len = header.prefix_len;
                        const max_cmp = @min(p_len, MAX_PREFIX_LEN);
                        var match = true;
                        var d = frame.depth;

                        for (0..max_cmp) |i| {
                            if (d >= self.prefix.len) break;
                            if (header.prefix[i] != self.prefix[d]) {
                                match = false;
                                break;
                            }
                            d += 1;
                        }

                        if (match and p_len > MAX_PREFIX_LEN) {
                            const any = Tree.findAnyLeaf(frame.node);
                            for (MAX_PREFIX_LEN..p_len) |_| {
                                if (d >= self.prefix.len) break;
                                if (any.key[d] != self.prefix[d]) {
                                    match = false;
                                    break;
                                }
                                d += 1;
                            }
                        }

                        if (!match) {
                            self.pop();
                            continue;
                        }

                        if (self.cursor) |c| {
                            const step_cmp = Tree.compareCompressedPrefixWithCursor(frame.node, header, frame.depth, c);
                            if (step_cmp.relation == .before_cursor) {
                                self.pop();
                                continue;
                            }
                        }

                        if (header.loadLeafValue()) |lv| {
                            var lv_valid = std.mem.startsWith(u8, lv.key, self.prefix);
                            if (lv_valid and self.cursor != null) {
                                if (std.mem.order(u8, self.cursor.?, lv.key) != .lt) {
                                    lv_valid = false;
                                } else {
                                    self.cursor = null;
                                }
                            }
                            if (lv_valid) return ScanEntry{ .key = lv.key, .value = lv.loadValue() };
                        }
                        continue;
                    }

                    const p_len = header.prefix_len;
                    var child_to_push: ?*const Node = null;

                    switch (header.nodeType) {
                        .node4 => {
                            const n4 = @as(*const Node4, @alignCast(@fieldParentPtr("header", header)));
                            while (frame.step - 1 < n4.header.num_children) {
                                const i = frame.step - 1;
                                frame.step += 1;
                                const cb = n4.keys[i];

                                if (self.cursor) |c| {
                                    const nd = frame.depth + p_len;
                                    if (nd < c.len and cb < c[nd]) continue;
                                }

                                child_to_push = &n4.children[i];
                                break;
                            }
                        },
                        .node16 => {
                            const n16 = @as(*const Node16, @alignCast(@fieldParentPtr("header", header)));
                            while (frame.step - 1 < n16.header.num_children) {
                                const i = frame.step - 1;
                                frame.step += 1;
                                const cb = n16.keys[i];

                                if (self.cursor) |c| {
                                    const nd = frame.depth + p_len;
                                    if (nd < c.len and cb < c[nd]) continue;
                                }

                                child_to_push = &n16.children[i];
                                break;
                            }
                        },
                        .node48 => {
                            const n48 = @as(*const Node48, @alignCast(@fieldParentPtr("header", header)));
                            while (frame.step - 1 < 256) {
                                const i = frame.step - 1;
                                frame.step += 1;
                                const idx = n48.child_index[i];
                                if (idx == Node48.EMPTY_INDEX) continue;

                                const cb: u8 = @intCast(i);
                                if (self.cursor) |c| {
                                    const nd = frame.depth + p_len;
                                    if (nd < c.len and cb < c[nd]) continue;
                                }

                                child_to_push = &n48.children[idx];
                                break;
                            }
                        },
                        .node256 => {
                            const n256 = @as(*const Node256, @alignCast(@fieldParentPtr("header", header)));
                            while (frame.step - 1 < 256) {
                                const i = frame.step - 1;
                                frame.step += 1;
                                if (node.nodeIsEmpty(n256.children[i])) continue;

                                const cb: u8 = @intCast(i);
                                if (self.cursor) |c| {
                                    const nd = frame.depth + p_len;
                                    if (nd < c.len and cb < c[nd]) continue;
                                }

                                child_to_push = &n256.children[i];
                                break;
                            }
                        },
                    }

                    if (child_to_push) |child| {
                        try self.push(.{
                            .node = child,
                            .depth = frame.depth + p_len + 1,
                            .step = 0,
                        });
                    } else {
                        self.pop();
                    }
                },
            }
        }
        return null;
    }

    fn push(self: *Iterator, frame: Frame) !void {
        if (self.spill_stack.items.len != 0 or self.inline_len == self.inline_stack.len) {
            try self.spill_stack.append(self.allocator, frame);
            return;
        }

        self.inline_stack[self.inline_len] = frame;
        self.inline_len += 1;
    }

    fn pop(self: *Iterator) void {
        if (self.spill_stack.items.len != 0) {
            _ = self.spill_stack.pop();
            return;
        }

        std.debug.assert(self.inline_len != 0);
        self.inline_len -= 1;
    }

    fn peek(self: *Iterator) ?*Frame {
        if (self.spill_stack.items.len != 0) {
            return &self.spill_stack.items[self.spill_stack.items.len - 1];
        }
        if (self.inline_len == 0) return null;
        return &self.inline_stack[self.inline_len - 1];
    }
};

fn createTestValue(allocator: std.mem.Allocator, value_int: i64) !*Value {
    const value = try allocator.create(Value);
    value.* = .{ .integer = value_int };
    return value;
}

fn expectLookupInt(tree: *Tree, key: []const u8, expected: i64) !void {
    const testing = std.testing;
    const value = tree.lookup(key) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(expected, value.integer);
}

test "insert lookup overwrite and delete handle shared prefixes and exact matches" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var tree = Tree.init(arena.allocator());

    try tree.insert(try arena.allocator().dupe(u8, "alpha"), try createTestValue(arena.allocator(), 1));
    try tree.insert(try arena.allocator().dupe(u8, "alphabet"), try createTestValue(arena.allocator(), 2));
    try tree.insert(try arena.allocator().dupe(u8, "alphanumeric"), try createTestValue(arena.allocator(), 3));

    try expectLookupInt(&tree, "alpha", 1);
    try expectLookupInt(&tree, "alphabet", 2);
    try expectLookupInt(&tree, "alphanumeric", 3);

    try tree.insert(try arena.allocator().dupe(u8, "alpha"), try createTestValue(arena.allocator(), 9));
    try expectLookupInt(&tree, "alpha", 9);

    try testing.expect(try tree.delete("alphabet"));
    try testing.expect(tree.lookup("alphabet") == null);
    try expectLookupInt(&tree, "alpha", 9);
    try expectLookupInt(&tree, "alphanumeric", 3);
}

test "insert handles keys when one key is a prefix of another" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var tree = Tree.init(arena.allocator());
    try tree.insert(try arena.allocator().dupe(u8, "a"), try createTestValue(arena.allocator(), 1));
    try tree.insert(try arena.allocator().dupe(u8, "ab"), try createTestValue(arena.allocator(), 2));
    try tree.insert(try arena.allocator().dupe(u8, "abc"), try createTestValue(arena.allocator(), 3));

    try expectLookupInt(&tree, "a", 1);
    try expectLookupInt(&tree, "ab", 2);
    try expectLookupInt(&tree, "abc", 3);
}

test "insert preserves long compressed prefixes and binary keys" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var tree = Tree.init(arena.allocator());
    try tree.insert(try arena.allocator().dupe(u8, "abcdefghijklmnop:1"), try createTestValue(arena.allocator(), 10));
    try tree.insert(try arena.allocator().dupe(u8, "abcdefghijklmnop:2"), try createTestValue(arena.allocator(), 20));
    try tree.insert(try arena.allocator().dupe(u8, "\x00bin"), try createTestValue(arena.allocator(), 30));
    try tree.insert(try arena.allocator().dupe(u8, "\x00bip"), try createTestValue(arena.allocator(), 40));

    try expectLookupInt(&tree, "abcdefghijklmnop:1", 10);
    try expectLookupInt(&tree, "abcdefghijklmnop:2", 20);
    try expectLookupInt(&tree, "\x00bin", 30);
    try expectLookupInt(&tree, "\x00bip", 40);
}

test "scan_from honors start_after_key and max_items" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var tree = Tree.init(arena.allocator());
    const keys = [_][]const u8{ "a", "aa", "ab", "ac", "b", "c" };
    for (keys, 0..) |key, index| {
        try tree.insert(try arena.allocator().dupe(u8, key), try createTestValue(arena.allocator(), @intCast(index + 1)));
    }

    var collected = std.ArrayList(ScanEntry).empty;
    defer collected.deinit(testing.allocator);

    const complete = try tree.scanFrom("a", "aa", testing.allocator, &collected, 2);
    try testing.expect(!complete);
    try testing.expectEqual(@as(usize, 2), collected.items.len);
    try testing.expectEqualStrings("ab", collected.items[0].key);
    try testing.expectEqualStrings("ac", collected.items[1].key);
}

test "scan_range_from returns binary keys in lexicographic order and stops at limit" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var tree = Tree.init(arena.allocator());
    const keys = [_][]const u8{ "\x00a", "\x00b", "\x01a", "\x01b" };
    for (keys, 0..) |key, index| {
        try tree.insert(try arena.allocator().dupe(u8, key), try createTestValue(arena.allocator(), @intCast(index + 1)));
    }

    var collected = std.ArrayList(ScanEntry).empty;
    defer collected.deinit(testing.allocator);

    const complete = try tree.scanRangeFrom(.{
        .start = "\x00a",
        .end = "\x01b",
    }, "\x00a", testing.allocator, &collected, 2);
    try testing.expect(!complete);
    try testing.expectEqual(@as(usize, 2), collected.items.len);
    try testing.expectEqualStrings("\x00b", collected.items[0].key);
    try testing.expectEqualStrings("\x01a", collected.items[1].key);
}

test "scan_range_from and scan_range_visit_from stay equivalent for same range cursor and limit" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var tree = Tree.init(arena.allocator());
    var values = [_]Value{
        .{ .integer = 1 },
        .{ .integer = 2 },
        .{ .integer = 3 },
        .{ .integer = 4 },
        .{ .integer = 5 },
    };
    const keys = [_][]const u8{ "a", "ab", "b", "ba", "c" };
    for (keys, 0..) |key, i| {
        try tree.insert(try arena.allocator().dupe(u8, key), &values[i]);
    }

    var collected = std.ArrayList(ScanEntry).empty;
    defer collected.deinit(testing.allocator);

    const range = KeyRange{
        .start = "a",
        .end = "c",
    };
    const start_after_key: ?[]const u8 = "a";
    const limit: usize = 2;
    const scan_complete = try tree.scanRangeFrom(range, start_after_key, testing.allocator, &collected, limit);

    const VisitCtx = struct {
        allocator: std.mem.Allocator,
        entries: std.ArrayList(ScanEntry),

        fn visit(ctx_ptr: *anyopaque, key: []const u8, value: *const Value) !void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            try ctx.entries.append(ctx.allocator, .{ .key = key, .value = value });
        }
    };

    var visit_ctx = VisitCtx{
        .allocator = testing.allocator,
        .entries = std.ArrayList(ScanEntry).empty,
    };
    defer visit_ctx.entries.deinit(testing.allocator);

    const visit_complete = try tree.scanRangeVisitFrom(range, start_after_key, &visit_ctx, VisitCtx.visit, limit);
    try testing.expectEqual(scan_complete, visit_complete);
    try testing.expectEqual(collected.items.len, visit_ctx.entries.items.len);
    for (collected.items, visit_ctx.entries.items) |lhs, rhs| {
        try testing.expectEqualStrings(lhs.key, rhs.key);
        try testing.expect(lhs.value == rhs.value);
    }
}

test "iterator uses inline stack for shallow prefix scans" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var tree = Tree.init(arena.allocator());
    var values = [_]Value{
        .{ .integer = 1 },
        .{ .integer = 2 },
        .{ .integer = 3 },
        .{ .integer = 4 },
    };
    const keys = [_][]const u8{ "alpha", "alphabet", "alpine", "beta" };
    for (keys, 0..) |key, i| {
        try tree.insert(try arena.allocator().dupe(u8, key), &values[i]);
    }

    var counting_state = std.testing.FailingAllocator.init(testing.allocator, .{});
    var it = try Iterator.init(counting_state.allocator(), &tree, "al", null);
    defer it.deinit();

    const expected = [_][]const u8{ "alpha", "alphabet", "alpine" };
    var next_index: usize = 0;
    while (try it.next()) |entry| {
        try testing.expect(next_index < expected.len);
        try testing.expectEqualStrings(expected[next_index], entry.key);
        next_index += 1;
    }

    try testing.expectEqual(expected.len, next_index);
    try testing.expectEqual(@as(usize, 0), counting_state.allocations);
    try testing.expectEqual(@as(usize, 0), counting_state.allocated_bytes);
}
