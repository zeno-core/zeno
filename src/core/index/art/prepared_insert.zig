//! Planner and reserved-apply helpers for batch plain-key ART inserts.
//!
//! This module separates structural insert planning from live-tree mutation so batch writes can reserve
//! all required nodes, leaves, keys, and values before touching the live ART. Prepared ops never retain
//! live node pointers and reserved apply never allocates.

const std = @import("std");
const Value = @import("../../types/value.zig").Value;
const Tree = @import("tree.zig").Tree;
const node = @import("node.zig");
const Node = node.Node;
const NodeHeader = node.NodeHeader;
const NodeType = node.NodeType;
const Node4 = node.Node4;
const Node16 = node.Node16;
const Node48 = node.Node48;
const Node256 = node.Node256;
const Leaf = node.Leaf;
const MAX_PREFIX_LEN = node.MAX_PREFIX_LEN;
const ReservedInternalNode = node.ReservedInternalNode;
const ValueOwner = node.ValueOwner;

/// Structural insert outcomes recognized by the ART planner.
///
/// Each tag describes one allocator-visible reservation shape and one live-tree
/// apply path so callers can reserve exactly the nodes, leaves, and key storage
/// required before entering an atomic publish window.
pub const InsertPlanKind = enum {
    create_root_leaf,
    overwrite_leaf,
    overwrite_leaf_value,
    attach_leaf_value,
    leaf_split,
    prefix_split,
    add_child,
    grow_and_add_child,
};

/// Preallocation requirements for one planned insert.
///
/// This records the exact owned objects that `apply_prepared_insert` expects the
/// caller to provide so the live apply path never allocates or duplicates work.
pub const InsertReservationSpec = struct {
    needs_stored_key: bool = false,
    needs_leaf: bool = false,
    split_node_type: ?NodeType = null,
    promoted_node_type: ?NodeType = null,
};

/// One expected internal-node step on the path to the planned mutation target.
///
/// The planner captures enough structural state to revalidate the live tree at
/// apply time without retaining live pointers.
pub const InsertPathStep = struct {
    child_byte: u8,
    expected_node_type: NodeType,
    expected_num_children: u16,
    expected_prefix_len: u16,
    expected_prefix: [MAX_PREFIX_LEN]u8,
    expected_exemplar_key: []const u8,
    expected_leaf_value_present: bool,
};

/// One prepared insert plan produced against a shadow ART view.
///
/// `PreparedInsert` stores the mutation kind, the validated navigation path, and
/// the exact structural expectations that must still hold when the caller later
/// applies the insert to the live tree.
pub const PreparedInsert = struct {
    kind: InsertPlanKind,
    key: []const u8,
    path: []InsertPathStep,
    reservation: InsertReservationSpec,
    target_depth: usize,

    expected_target_node_type: ?NodeType = null,
    expected_target_num_children: u16 = 0,
    expected_target_prefix_len: u16 = 0,
    expected_target_prefix: [MAX_PREFIX_LEN]u8 = [_]u8{0} ** MAX_PREFIX_LEN,
    expected_target_exemplar_key: ?[]const u8 = null,
    expected_target_leaf_value_present: bool = false,
    expected_target_leaf_key: ?[]const u8 = null,

    new_prefix_len: u16 = 0,
    new_prefix: [MAX_PREFIX_LEN]u8 = [_]u8{0} ** MAX_PREFIX_LEN,
    rewritten_old_prefix_len: u16 = 0,
    rewritten_old_prefix: [MAX_PREFIX_LEN]u8 = [_]u8{0} ** MAX_PREFIX_LEN,
    old_edge_byte: ?u8 = null,
    new_edge_byte: ?u8 = null,
};

/// Reserved live objects consumed by `apply_prepared_insert`.
///
/// Ownership: All pointers are caller-owned before apply. Successful apply
/// transfers any referenced leaf, stored key, promoted node, split node, and
/// value ownership into the live tree shape selected by `prepared.kind`.
pub const ReservedInsert = struct {
    value: *Value,
    value_owner: ValueOwner = .tree_allocator,
    stored_key: ?[]const u8 = null,
    leaf: ?*Leaf = null,
    split_node: ?ReservedInternalNode = null,
    promoted_node: ?ReservedInternalNode = null,
};

/// One sorted child edge tracked inside a planner-only shadow internal node.
const ShadowChild = struct {
    key_byte: u8,
    node: *ShadowNode,
};

/// Planner-only shadow representation of one live internal ART node.
///
/// The shadow tree keeps only structural metadata plus child ordering so the
/// planner can simulate inserts without mutating or retaining the live node.
const ShadowInternal = struct {
    node_type: NodeType,
    num_children: u16,
    prefix_len: u16,
    prefix: [MAX_PREFIX_LEN]u8,
    exemplar_key: []const u8,
    leaf_value_key: ?[]const u8,
    children: std.ArrayListUnmanaged(ShadowChild) = .{},
};

/// Returns the child-capacity reservation the shadow planner should keep for one node class.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
fn recommended_shadow_child_capacity(node_type: NodeType, minimum: u16) u16 {
    return switch (node_type) {
        .node4, .node16, .node48 => @max(minimum, node.capacity_for(node_type)),
        .node256 => minimum,
    };
}

/// Planner-only node wrapper used to mirror either deferred live nodes or shadow-owned structure.
const ShadowNode = union(enum) {
    empty: void,
    live: *const Node,
    leaf: []const u8,
    internal: *ShadowInternal,
};

/// Mutable planner-only ART view built from one live root.
///
/// The planner mutates this shadow tree while producing `PreparedInsert`
/// records, letting callers batch multiple insert plans without touching the
/// live ART until all reservations succeed.
pub const ShadowTree = struct {
    allocator: std.mem.Allocator,
    root: *ShadowNode,

    /// Initializes a shadow tree rooted at one live ART node.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Allocates one `ShadowNode` wrapper from `allocator`.
    ///
    /// Ownership: The returned `ShadowTree` owns the allocated shadow root and
    /// borrows `live_root` for later deferred materialization.
    pub fn init_from_live(allocator: std.mem.Allocator, live_root: *const Node) !ShadowTree {
        const root = try allocator.create(ShadowNode);
        root.* = .{ .live = live_root };
        return .{
            .allocator = allocator,
            .root = root,
        };
    }

    /// Plans one insert against the mutable shadow tree without mutating the live ART.
    ///
    /// Time Complexity: O(k + p), where `k` is `key.len` and `p` is local
    /// shadow mutation work needed to model node growth or splits on the path.
    ///
    /// Allocator: Uses `self.allocator` for shadow-node materialization and
    /// `scratch_allocator` for the returned path slice.
    ///
    /// Ownership: The returned `PreparedInsert.path` is owned by the caller
    /// through `scratch_allocator`.
    pub fn plan_insert(self: *ShadowTree, scratch_allocator: std.mem.Allocator, key: []const u8) !PreparedInsert {
        var path_builder = std.ArrayList(InsertPathStep).empty;
        defer path_builder.deinit(scratch_allocator);

        var current = self.root;
        var depth: usize = 0;

        while (true) {
            switch (current.*) {
                .empty => {
                    current.* = .{ .leaf = key };
                    return .{
                        .kind = .create_root_leaf,
                        .key = key,
                        .path = try path_builder.toOwnedSlice(scratch_allocator),
                        .reservation = .{
                            .needs_stored_key = true,
                            .needs_leaf = true,
                        },
                        .target_depth = depth,
                    };
                },
                .live => |live| switch (node.node_decode(live.*)) {
                    .empty => {
                        current.* = .{ .empty = {} };
                        continue;
                    },
                    .leaf => |leaf| {
                        if (std.mem.eql(u8, leaf.key, key)) {
                            return .{
                                .kind = .overwrite_leaf,
                                .key = key,
                                .path = try path_builder.toOwnedSlice(scratch_allocator),
                                .reservation = .{},
                                .target_depth = depth,
                                .expected_target_leaf_key = leaf.key,
                            };
                        }

                        const old_edge_byte, const new_edge_byte, const split_prefix_len, const split_prefix = leaf_split_details(leaf.key, key, depth);

                        const new_internal = try create_shadow_internal(self.allocator, .node4, split_prefix_len, split_prefix, leaf.key, null, 2);
                        const old_leaf_node = try create_shadow_leaf(self.allocator, leaf.key);
                        const new_leaf_node = try create_shadow_leaf(self.allocator, key);
                        if (old_edge_byte) |byte| {
                            try insert_shadow_child(self.allocator, new_internal, .{ .key_byte = byte, .node = old_leaf_node });
                        } else {
                            new_internal.leaf_value_key = leaf.key;
                        }
                        if (new_edge_byte) |byte| {
                            try insert_shadow_child(self.allocator, new_internal, .{ .key_byte = byte, .node = new_leaf_node });
                        } else {
                            new_internal.leaf_value_key = key;
                        }
                        current.* = .{ .internal = new_internal };

                        return .{
                            .kind = .leaf_split,
                            .key = key,
                            .path = try path_builder.toOwnedSlice(scratch_allocator),
                            .reservation = .{
                                .needs_stored_key = true,
                                .needs_leaf = true,
                                .split_node_type = .node4,
                            },
                            .target_depth = depth,
                            .expected_target_leaf_key = leaf.key,
                            .new_prefix_len = split_prefix_len,
                            .new_prefix = split_prefix,
                            .old_edge_byte = old_edge_byte,
                            .new_edge_byte = new_edge_byte,
                        };
                    },
                    .internal => {
                        _ = try materialize_live_internal(self.allocator, current, live);
                        continue;
                    },
                },
                .leaf => |old_key| {
                    if (std.mem.eql(u8, old_key, key)) {
                        return .{
                            .kind = .overwrite_leaf,
                            .key = key,
                            .path = try path_builder.toOwnedSlice(scratch_allocator),
                            .reservation = .{},
                            .target_depth = depth,
                            .expected_target_leaf_key = old_key,
                        };
                    }

                    const old_edge_byte, const new_edge_byte, const split_prefix_len, const split_prefix = leaf_split_details(old_key, key, depth);

                    const new_internal = try create_shadow_internal(self.allocator, .node4, split_prefix_len, split_prefix, old_key, null, 2);
                    const old_leaf_node = try create_shadow_leaf(self.allocator, old_key);
                    const new_leaf_node = try create_shadow_leaf(self.allocator, key);
                    if (old_edge_byte) |byte| {
                        try insert_shadow_child(self.allocator, new_internal, .{ .key_byte = byte, .node = old_leaf_node });
                    } else {
                        new_internal.leaf_value_key = old_key;
                    }
                    if (new_edge_byte) |byte| {
                        try insert_shadow_child(self.allocator, new_internal, .{ .key_byte = byte, .node = new_leaf_node });
                    } else {
                        new_internal.leaf_value_key = key;
                    }
                    current.* = .{ .internal = new_internal };

                    return .{
                        .kind = .leaf_split,
                        .key = key,
                        .path = try path_builder.toOwnedSlice(scratch_allocator),
                        .reservation = .{
                            .needs_stored_key = true,
                            .needs_leaf = true,
                            .split_node_type = .node4,
                        },
                        .target_depth = depth,
                        .expected_target_leaf_key = old_key,
                        .new_prefix_len = split_prefix_len,
                        .new_prefix = split_prefix,
                        .old_edge_byte = old_edge_byte,
                        .new_edge_byte = new_edge_byte,
                    };
                },
                .internal => |shadow| {
                    const target_node_type = shadow.node_type;
                    const target_num_children = shadow.num_children;
                    const target_prefix_len = shadow.prefix_len;
                    const target_prefix = shadow.prefix;
                    const target_exemplar = shadow.exemplar_key;
                    const target_leaf_value_present = shadow.leaf_value_key != null;
                    const prefix_state = compare_shadow_prefix(shadow, key, depth);
                    if (prefix_state.mismatch_idx < shadow.prefix_len) {
                        const old_exemplar = shadow.exemplar_key;
                        const new_parent_prefix_len: u16 = @intCast(prefix_state.mismatch_idx);
                        var new_parent_prefix = [_]u8{0} ** MAX_PREFIX_LEN;
                        copy_prefix_bytes(&new_parent_prefix, key, depth, new_parent_prefix_len);

                        const old_edge_byte = old_exemplar[depth + prefix_state.mismatch_idx];
                        const new_edge_byte: ?u8 = if (depth + prefix_state.mismatch_idx < key.len) key[depth + prefix_state.mismatch_idx] else null;

                        const rewritten_old_prefix_len: u16 = shadow.prefix_len - @as(u16, @intCast(prefix_state.mismatch_idx + 1));
                        var rewritten_old_prefix = [_]u8{0} ** MAX_PREFIX_LEN;
                        copy_prefix_bytes(&rewritten_old_prefix, old_exemplar, depth + prefix_state.mismatch_idx + 1, rewritten_old_prefix_len);

                        shadow.prefix_len = rewritten_old_prefix_len;
                        shadow.prefix = rewritten_old_prefix;

                        const new_parent = try create_shadow_internal(self.allocator, .node4, new_parent_prefix_len, new_parent_prefix, old_exemplar, null, 2);
                        const old_node = try self.allocator.create(ShadowNode);
                        old_node.* = .{ .internal = shadow };
                        if (new_edge_byte) |byte| {
                            const new_leaf = try create_shadow_leaf(self.allocator, key);
                            try insert_shadow_child(self.allocator, new_parent, .{ .key_byte = old_edge_byte, .node = old_node });
                            try insert_shadow_child(self.allocator, new_parent, .{ .key_byte = byte, .node = new_leaf });
                        } else {
                            new_parent.leaf_value_key = key;
                            try insert_shadow_child(self.allocator, new_parent, .{ .key_byte = old_edge_byte, .node = old_node });
                        }
                        current.* = .{ .internal = new_parent };

                        return .{
                            .kind = .prefix_split,
                            .key = key,
                            .path = try path_builder.toOwnedSlice(scratch_allocator),
                            .reservation = .{
                                .needs_stored_key = true,
                                .needs_leaf = true,
                                .split_node_type = .node4,
                            },
                            .target_depth = depth,
                            .expected_target_node_type = target_node_type,
                            .expected_target_num_children = target_num_children,
                            .expected_target_prefix_len = target_prefix_len,
                            .expected_target_prefix = target_prefix,
                            .expected_target_exemplar_key = old_exemplar,
                            .expected_target_leaf_value_present = target_leaf_value_present,
                            .new_prefix_len = new_parent_prefix_len,
                            .new_prefix = new_parent_prefix,
                            .rewritten_old_prefix_len = rewritten_old_prefix_len,
                            .rewritten_old_prefix = rewritten_old_prefix,
                            .old_edge_byte = old_edge_byte,
                            .new_edge_byte = new_edge_byte,
                        };
                    }

                    depth += shadow.prefix_len;
                    if (depth == key.len) {
                        if (shadow.leaf_value_key) |existing_key| {
                            return .{
                                .kind = .overwrite_leaf_value,
                                .key = key,
                                .path = try path_builder.toOwnedSlice(scratch_allocator),
                                .reservation = .{},
                                .target_depth = depth,
                                .expected_target_node_type = shadow.node_type,
                                .expected_target_num_children = shadow.num_children,
                                .expected_target_prefix_len = shadow.prefix_len,
                                .expected_target_prefix = shadow.prefix,
                                .expected_target_exemplar_key = shadow.exemplar_key,
                                .expected_target_leaf_value_present = true,
                                .expected_target_leaf_key = existing_key,
                            };
                        }

                        shadow.leaf_value_key = key;
                        return .{
                            .kind = .attach_leaf_value,
                            .key = key,
                            .path = try path_builder.toOwnedSlice(scratch_allocator),
                            .reservation = .{
                                .needs_stored_key = true,
                                .needs_leaf = true,
                            },
                            .target_depth = depth,
                            .expected_target_node_type = shadow.node_type,
                            .expected_target_num_children = shadow.num_children,
                            .expected_target_prefix_len = shadow.prefix_len,
                            .expected_target_prefix = shadow.prefix,
                            .expected_target_exemplar_key = shadow.exemplar_key,
                            .expected_target_leaf_value_present = false,
                        };
                    }

                    const step = InsertPathStep{
                        .child_byte = key[depth],
                        .expected_node_type = shadow.node_type,
                        .expected_num_children = shadow.num_children,
                        .expected_prefix_len = shadow.prefix_len,
                        .expected_prefix = shadow.prefix,
                        .expected_exemplar_key = shadow.exemplar_key,
                        .expected_leaf_value_present = shadow.leaf_value_key != null,
                    };

                    if (find_shadow_child(shadow, key[depth])) |child| {
                        try path_builder.append(scratch_allocator, step);
                        current = child;
                        depth += 1;
                        continue;
                    }

                    const new_leaf = try create_shadow_leaf(self.allocator, key);
                    try insert_shadow_child(self.allocator, shadow, .{ .key_byte = key[depth], .node = new_leaf });
                    if (target_num_children == node.capacity_for(target_node_type)) {
                        shadow.node_type = node.next_type(target_node_type).?;
                        try shadow.children.ensureTotalCapacity(
                            self.allocator,
                            recommended_shadow_child_capacity(shadow.node_type, shadow.num_children),
                        );
                        return .{
                            .kind = .grow_and_add_child,
                            .key = key,
                            .path = try path_builder.toOwnedSlice(scratch_allocator),
                            .reservation = .{
                                .needs_stored_key = true,
                                .needs_leaf = true,
                                .promoted_node_type = shadow.node_type,
                            },
                            .target_depth = depth,
                            .expected_target_node_type = target_node_type,
                            .expected_target_num_children = target_num_children,
                            .expected_target_prefix_len = target_prefix_len,
                            .expected_target_prefix = target_prefix,
                            .expected_target_exemplar_key = target_exemplar,
                            .expected_target_leaf_value_present = target_leaf_value_present,
                            .new_edge_byte = key[depth],
                        };
                    }

                    return .{
                        .kind = .add_child,
                        .key = key,
                        .path = try path_builder.toOwnedSlice(scratch_allocator),
                        .reservation = .{
                            .needs_stored_key = true,
                            .needs_leaf = true,
                        },
                        .target_depth = depth,
                        .expected_target_node_type = target_node_type,
                        .expected_target_num_children = target_num_children,
                        .expected_target_prefix_len = target_prefix_len,
                        .expected_target_prefix = target_prefix,
                        .expected_target_exemplar_key = target_exemplar,
                        .expected_target_leaf_value_present = target_leaf_value_present,
                        .new_edge_byte = key[depth],
                    };
                },
            }
        }
    }
};

/// Applies one prepared insert using only pre-reserved live objects.
///
/// Time Complexity: O(k), where `k` is `prepared.key.len`.
///
/// Allocator: Does not allocate.
///
/// Ownership: Transfers the portions of `reserved` referenced by
/// `prepared.kind` into the live tree on success.
pub fn apply_prepared_insert(root: *Node, prepared: *const PreparedInsert, reserved: *const ReservedInsert) !void {
    const target = try navigate_to_target(root, prepared);
    switch (prepared.kind) {
        .create_root_leaf => {
            if (!node.node_is_empty(target.node_ref.*)) return error.BatchPlanInvariantViolation;
            @atomicStore(usize, target.node_ref, node.node_leaf(reserved.leaf orelse return error.BatchPlanInvariantViolation), .monotonic);
        },
        .overwrite_leaf => {
            const leaf = switch (node.node_decode(target.node_ref.*)) {
                .leaf => |existing| existing,
                else => return error.BatchPlanInvariantViolation,
            };
            if (!std.mem.eql(u8, leaf.key, prepared.expected_target_leaf_key.?)) return error.BatchPlanInvariantViolation;
            leaf.store_value(reserved.value);
            leaf.value_owner = reserved.value_owner;
        },
        .overwrite_leaf_value => {
            const header = try validate_target_internal(target.node_ref, prepared, target.depth);
            const leaf = header.load_leaf_value() orelse return error.BatchPlanInvariantViolation;
            if (!std.mem.eql(u8, leaf.key, prepared.expected_target_leaf_key.?)) return error.BatchPlanInvariantViolation;
            leaf.store_value(reserved.value);
            leaf.value_owner = reserved.value_owner;
        },
        .attach_leaf_value => {
            const header = try validate_target_internal(target.node_ref, prepared, target.depth);
            if (header.load_leaf_value() != null) return error.BatchPlanInvariantViolation;
            header.store_leaf_value(reserved.leaf orelse return error.BatchPlanInvariantViolation);
        },
        .add_child => {
            _ = try validate_target_internal(target.node_ref, prepared, target.depth);
            try node.add_child_reserved(
                target.node_ref,
                prepared.new_edge_byte.?,
                node.node_leaf(reserved.leaf orelse return error.BatchPlanInvariantViolation),
                null,
            );
        },
        .grow_and_add_child => {
            _ = try validate_target_internal(target.node_ref, prepared, target.depth);
            try node.add_child_reserved(
                target.node_ref,
                prepared.new_edge_byte.?,
                node.node_leaf(reserved.leaf orelse return error.BatchPlanInvariantViolation),
                reserved.promoted_node,
            );
        },
        .leaf_split => {
            const old_leaf = switch (node.node_decode(target.node_ref.*)) {
                .leaf => |existing| existing,
                else => return error.BatchPlanInvariantViolation,
            };
            if (!std.mem.eql(u8, old_leaf.key, prepared.expected_target_leaf_key.?)) return error.BatchPlanInvariantViolation;
            const split = reserved.split_node orelse return error.BatchPlanInvariantViolation;
            if (split.node_type() != .node4) return error.BatchPlanInvariantViolation;
            split.node4.* = Node4.init();
            split.node4.header.prefix_len = prepared.new_prefix_len;
            split.node4.header.prefix = prepared.new_prefix;

            var new_node = split.as_node();
            if (prepared.old_edge_byte) |byte| {
                try node.add_child_reserved(&new_node, byte, target.node_ref.*, null);
            } else {
                split.node4.header.store_leaf_value(old_leaf);
            }
            if (prepared.new_edge_byte) |byte| {
                try node.add_child_reserved(&new_node, byte, node.node_leaf(reserved.leaf.?), null);
            } else {
                split.node4.header.store_leaf_value(reserved.leaf.?);
            }
            @atomicStore(usize, target.node_ref, new_node, .monotonic);
        },
        .prefix_split => {
            const header = try validate_target_internal(target.node_ref, prepared, target.depth);
            const split = reserved.split_node orelse return error.BatchPlanInvariantViolation;
            if (split.node_type() != .node4) return error.BatchPlanInvariantViolation;

            header.prefix_len = prepared.rewritten_old_prefix_len;
            header.prefix = prepared.rewritten_old_prefix;

            split.node4.* = Node4.init();
            split.node4.header.prefix_len = prepared.new_prefix_len;
            split.node4.header.prefix = prepared.new_prefix;

            var new_node = split.as_node();
            try node.add_child_reserved(&new_node, prepared.old_edge_byte.?, target.node_ref.*, null);
            if (prepared.new_edge_byte) |byte| {
                try node.add_child_reserved(&new_node, byte, node.node_leaf(reserved.leaf.?), null);
            } else {
                split.node4.header.store_leaf_value(reserved.leaf.?);
            }
            @atomicStore(usize, target.node_ref, new_node, .monotonic);
        },
    }
}

/// Reconstructs the full original prefix bytes for a node that will be rewritten by a prefix split.
///
/// Time Complexity: O(p), where `p` is the stored prefix length bounded by `MAX_PREFIX_LEN`.
///
/// Allocator: Does not allocate.
fn shadow_prefix_before_rewrite(
    exemplar_key: []const u8,
    stored_prefix: [MAX_PREFIX_LEN]u8,
    rewritten_prefix_len: u16,
    depth: usize,
    mismatch_idx: usize,
    old_rewritten_len: u16,
) [MAX_PREFIX_LEN]u8 {
    _ = old_rewritten_len;
    var prefix = [_]u8{0} ** MAX_PREFIX_LEN;
    const original_prefix_len = @as(u16, @intCast(mismatch_idx + 1)) + rewritten_prefix_len;
    const stored_len = @min(original_prefix_len, MAX_PREFIX_LEN);
    if (stored_len > 0) {
        if (original_prefix_len <= MAX_PREFIX_LEN) {
            copy_prefix_bytes(&prefix, exemplar_key, depth, original_prefix_len);
        } else {
            @memcpy(prefix[0..MAX_PREFIX_LEN], stored_prefix[0..MAX_PREFIX_LEN]);
        }
    }
    return prefix;
}

/// Result of comparing one compressed shadow prefix with a candidate key.
const PrefixCompare = struct {
    mismatch_idx: usize,
};

/// Compares a shadow node's compressed prefix against `key` starting at `depth_in`.
///
/// Time Complexity: O(p), where `p` is `shadow.prefix_len`.
///
/// Allocator: Does not allocate.
fn compare_shadow_prefix(shadow: *const ShadowInternal, key: []const u8, depth_in: usize) PrefixCompare {
    var depth = depth_in;
    var mismatch_idx: usize = 0;
    const stored_len = @min(shadow.prefix_len, MAX_PREFIX_LEN);
    while (mismatch_idx < stored_len and depth < key.len and shadow.prefix[mismatch_idx] == key[depth]) : ({
        mismatch_idx += 1;
        depth += 1;
    }) {}
    if (mismatch_idx == MAX_PREFIX_LEN and shadow.prefix_len > MAX_PREFIX_LEN) {
        while (mismatch_idx < shadow.prefix_len and depth < key.len and shadow.exemplar_key[depth] == key[depth]) : ({
            mismatch_idx += 1;
            depth += 1;
        }) {}
    }
    return .{ .mismatch_idx = mismatch_idx };
}

/// Allocates one planner-owned shadow leaf wrapper for `key`.
///
/// Time Complexity: O(1).
///
/// Allocator: Allocates one `ShadowNode` from `allocator`.
///
/// Ownership: The returned wrapper is owned by the caller; `key` is borrowed.
fn create_shadow_leaf(allocator: std.mem.Allocator, key: []const u8) !*ShadowNode {
    const shadow = try allocator.create(ShadowNode);
    shadow.* = .{ .leaf = key };
    return shadow;
}

/// Allocates one deferred-live shadow wrapper around `live`.
///
/// Time Complexity: O(1).
///
/// Allocator: Allocates one `ShadowNode` from `allocator`.
///
/// Ownership: The returned wrapper is owned by the caller and borrows `live`.
fn create_live_shadow_node(allocator: std.mem.Allocator, live: *const Node) !*ShadowNode {
    const shadow = try allocator.create(ShadowNode);
    shadow.* = .{ .live = live };
    return shadow;
}

/// Materializes one live internal node into shadow-owned planner metadata.
///
/// Time Complexity: O(c), where `c` is the current live child count plus any
/// fixed scan work needed for `Node48` and `Node256`.
///
/// Allocator: Allocates one `ShadowInternal` and child wrappers from `allocator`.
///
/// Ownership: The returned `ShadowInternal` is owned by the shadow tree, while
/// embedded exemplar keys and live child pointers remain borrowed from the live ART.
fn materialize_live_internal(allocator: std.mem.Allocator, current: *ShadowNode, live: *const Node) !*ShadowInternal {
    const header = switch (node.node_decode(live.*)) {
        .internal => |internal| internal,
        else => unreachable,
    };
    const internal = try create_shadow_internal(
        allocator,
        header.node_type,
        header.prefix_len,
        header.prefix,
        Tree.find_any_leaf(live).key,
        if (header.load_leaf_value()) |leaf_value| leaf_value.key else null,
        recommended_shadow_child_capacity(header.node_type, header.num_children),
    );
    const ChildCopyCtx = struct {
        allocator: std.mem.Allocator,
        internal: *ShadowInternal,

        fn visit(ctx: @This(), key_byte: u8, child: *const Node) anyerror!bool {
            try insert_shadow_child(ctx.allocator, ctx.internal, .{
                .key_byte = key_byte,
                .node = try create_live_shadow_node(ctx.allocator, child),
            });
            return true;
        }
    };
    header.for_each_child(ChildCopyCtx, .{
        .allocator = allocator,
        .internal = internal,
    }, ChildCopyCtx.visit) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => unreachable,
    };

    current.* = .{ .internal = internal };
    return internal;
}

/// Allocates one empty shadow internal node with reserved child capacity.
///
/// Time Complexity: O(1).
///
/// Allocator: Allocates one `ShadowInternal` plus child-list backing storage from `allocator`.
///
/// Ownership: The returned shadow node is owned by the caller and borrows
/// `exemplar_key` and `leaf_value_key`.
fn create_shadow_internal(
    allocator: std.mem.Allocator,
    node_type: NodeType,
    prefix_len: u16,
    prefix: [MAX_PREFIX_LEN]u8,
    exemplar_key: []const u8,
    leaf_value_key: ?[]const u8,
    child_capacity: u16,
) !*ShadowInternal {
    const internal = try allocator.create(ShadowInternal);
    internal.* = .{
        .node_type = node_type,
        .num_children = 0,
        .prefix_len = prefix_len,
        .prefix = prefix,
        .exemplar_key = exemplar_key,
        .leaf_value_key = leaf_value_key,
        .children = .{},
    };
    try internal.children.ensureTotalCapacity(
        allocator,
        recommended_shadow_child_capacity(node_type, child_capacity),
    );
    return internal;
}

/// Inserts one child edge into a shadow internal node while preserving byte ordering.
///
/// Time Complexity: O(c), where `c` is `internal.children.items.len`.
///
/// Allocator: May grow the child list through `allocator`.
fn insert_shadow_child(allocator: std.mem.Allocator, internal: *ShadowInternal, child: ShadowChild) !void {
    try internal.children.ensureUnusedCapacity(allocator, 1);
    var index: usize = 0;
    while (index < internal.children.items.len and internal.children.items[index].key_byte < child.key_byte) : (index += 1) {}
    internal.children.insertAssumeCapacity(index, child);
    internal.num_children += 1;
}

/// Finds one child edge in a shadow internal node by transition byte.
///
/// Time Complexity: O(c), where `c` is `internal.children.items.len`.
///
/// Allocator: Does not allocate.
fn find_shadow_child(internal: *ShadowInternal, key_byte: u8) ?*ShadowNode {
    for (internal.children.items) |child| {
        if (child.key_byte == key_byte) return child.node;
    }
    return null;
}

/// Computes the leaf-split edge bytes and shared prefix for two diverging keys.
///
/// Time Complexity: O(k), where `k` is the common-prefix length from `depth`.
///
/// Allocator: Does not allocate.
fn leaf_split_details(old_key: []const u8, new_key: []const u8, depth: usize) struct { ?u8, ?u8, u16, [MAX_PREFIX_LEN]u8 } {
    var i: usize = depth;
    const min_len = @min(old_key.len, new_key.len);
    while (i < min_len and old_key[i] == new_key[i]) : (i += 1) {}

    const prefix_len: u16 = @intCast(i - depth);
    var prefix = [_]u8{0} ** MAX_PREFIX_LEN;
    copy_prefix_bytes(&prefix, new_key, depth, prefix_len);
    const old_edge: ?u8 = if (i < old_key.len) old_key[i] else null;
    const new_edge: ?u8 = if (i < new_key.len) new_key[i] else null;
    return .{ old_edge, new_edge, prefix_len, prefix };
}

/// Copies up to `MAX_PREFIX_LEN` bytes of compressed-prefix data out of `key`.
///
/// Time Complexity: O(p), where `p` is `min(prefix_len, MAX_PREFIX_LEN)`.
///
/// Allocator: Does not allocate.
fn copy_prefix_bytes(prefix: *[MAX_PREFIX_LEN]u8, key: []const u8, depth: usize, prefix_len: u16) void {
    const stored_len = @min(prefix_len, MAX_PREFIX_LEN);
    if (stored_len == 0) return;
    @memcpy(prefix[0..stored_len], key[depth .. depth + stored_len]);
}

/// Navigates the live ART to the node referenced by one prepared path.
///
/// Time Complexity: O(k), where `k` is the combined prefix and edge length in `prepared.path`.
///
/// Allocator: Does not allocate.
fn navigate_to_target(root: *Node, prepared: *const PreparedInsert) !struct { node_ref: *Node, depth: usize } {
    var current = root;
    var depth: usize = 0;
    for (prepared.path) |step| {
        const header = switch (node.node_decode(current.*)) {
            .internal => |internal| internal,
            else => return error.BatchPlanInvariantViolation,
        };
        try validate_internal_preconditions(current, header, step.expected_node_type, step.expected_num_children, step.expected_prefix_len, step.expected_prefix, step.expected_exemplar_key, step.expected_leaf_value_present, depth);
        if (depth + step.expected_prefix_len >= prepared.key.len) return error.BatchPlanInvariantViolation;
        const child = header.find_child(step.child_byte) orelse return error.BatchPlanInvariantViolation;
        depth += step.expected_prefix_len + 1;
        current = child;
    }
    return .{ .node_ref = current, .depth = depth };
}

/// Validates that one prepared target still points at the expected live internal node.
///
/// Time Complexity: O(p), where `p` is the hidden-prefix validation work when the target prefix exceeds `MAX_PREFIX_LEN`.
///
/// Allocator: Does not allocate.
fn validate_target_internal(node_ref: *Node, prepared: *const PreparedInsert, depth: usize) !*NodeHeader {
    const header = switch (node.node_decode(node_ref.*)) {
        .internal => |internal| internal,
        else => return error.BatchPlanInvariantViolation,
    };
    try validate_internal_preconditions(
        node_ref,
        header,
        prepared.expected_target_node_type.?,
        prepared.expected_target_num_children,
        prepared.expected_target_prefix_len,
        prepared.expected_target_prefix,
        prepared.expected_target_exemplar_key.?,
        prepared.expected_target_leaf_value_present,
        depth,
    );
    return header;
}

/// Verifies that one live internal node still matches the structural expectations captured during planning.
///
/// Time Complexity: O(p), where `p` is the checked prefix length plus any hidden exemplar comparison.
///
/// Allocator: Does not allocate.
fn validate_internal_preconditions(
    node_ref: *const Node,
    header: *const NodeHeader,
    expected_node_type: NodeType,
    expected_num_children: u16,
    expected_prefix_len: u16,
    expected_prefix: [MAX_PREFIX_LEN]u8,
    expected_exemplar_key: []const u8,
    expected_leaf_value_present: bool,
    depth: usize,
) !void {
    if (header.node_type != expected_node_type) return error.BatchPlanInvariantViolation;
    if (header.num_children != expected_num_children) return error.BatchPlanInvariantViolation;
    if (header.prefix_len != expected_prefix_len) return error.BatchPlanInvariantViolation;
    if ((header.load_leaf_value() != null) != expected_leaf_value_present) return error.BatchPlanInvariantViolation;

    const stored_len = @min(expected_prefix_len, MAX_PREFIX_LEN);
    if (stored_len > 0 and !std.mem.eql(u8, header.prefix[0..stored_len], expected_prefix[0..stored_len])) {
        return error.BatchPlanInvariantViolation;
    }
    if (expected_prefix_len > MAX_PREFIX_LEN) {
        const exemplar = Tree.find_any_leaf(node_ref).key;
        const hidden_start = depth + MAX_PREFIX_LEN;
        const hidden_end = depth + expected_prefix_len;
        if (!std.mem.eql(u8, exemplar[hidden_start..hidden_end], expected_exemplar_key[hidden_start..hidden_end])) {
            return error.BatchPlanInvariantViolation;
        }
    }
}

/// Allocates one reserved internal node matching `node_type`.
///
/// Time Complexity: O(1).
///
/// Allocator: Allocates one reserved ART node from `allocator`.
///
/// Ownership: The returned reservation is owned by the caller.
fn reserve_internal_node(allocator: std.mem.Allocator, node_type: NodeType) !ReservedInternalNode {
    return switch (node_type) {
        .node4 => .{ .node4 = try allocator.create(Node4) },
        .node16 => .{ .node16 = try allocator.create(Node16) },
        .node48 => .{ .node48 = try allocator.create(Node48) },
        .node256 => .{ .node256 = try allocator.create(Node256) },
    };
}

/// Builds one test-only `ReservedInsert` from a value and a prepared reservation spec.
///
/// Time Complexity: O(v), where `v` is the clone work for `value`.
///
/// Allocator: Allocates cloned value storage, optional stored-key bytes, optional leaf storage,
/// and optional reserved internal nodes from `allocator`.
///
/// Ownership: The returned reservation is owned by the caller until one test applies it.
fn reserve_insert_for_test(
    allocator: std.mem.Allocator,
    key: []const u8,
    value: Value,
    prepared: PreparedInsert,
) !ReservedInsert {
    const cloned_value = try allocator.create(Value);
    cloned_value.* = try value.clone(allocator);

    var reserved: ReservedInsert = .{
        .value = cloned_value,
        .value_owner = .tree_allocator,
    };

    if (prepared.reservation.needs_stored_key) {
        reserved.stored_key = try allocator.dupe(u8, key);
    }
    if (prepared.reservation.needs_leaf) {
        const leaf = try allocator.create(Leaf);
        leaf.* = .{
            .key = reserved.stored_key.?,
            .value = cloned_value,
            .value_owner = .tree_allocator,
        };
        reserved.leaf = leaf;
    }
    if (prepared.reservation.split_node_type) |node_type| {
        reserved.split_node = try reserve_internal_node(allocator, node_type);
    }
    if (prepared.reservation.promoted_node_type) |node_type| {
        reserved.promoted_node = try reserve_internal_node(allocator, node_type);
    }

    return reserved;
}

/// Builds one live leaf owned by the test allocator.
///
/// Time Complexity: O(k + v), where `k` is `key.len` and `v` is the clone work for `value`.
///
/// Allocator: Allocates owned key bytes, one `Value`, and one `Leaf` from `allocator`.
///
/// Ownership: The returned leaf and its owned key/value storage are owned by the caller.
fn create_leaf_for_test(allocator: std.mem.Allocator, key: []const u8, value: Value) !*Leaf {
    const stored_key = try allocator.dupe(u8, key);
    const stored_value = try allocator.create(Value);
    stored_value.* = try value.clone(allocator);

    const leaf = try allocator.create(Leaf);
    leaf.* = .{
        .key = stored_key,
        .value = stored_value,
        .value_owner = .tree_allocator,
    };
    return leaf;
}

/// Looks up one key in a live ART during tests without depending on `tree.zig`.
///
/// Time Complexity: O(k), where `k` is `key.len`.
///
/// Allocator: Does not allocate.
///
/// Ownership: Returns a borrowed pointer into the live tree when present.
fn lookup_value_for_test(root: *const Node, key: []const u8) ?*Value {
    var current_node_ptr: *const Node = root;
    var depth: usize = 0;

    while (!node.node_is_empty(current_node_ptr.*)) {
        switch (node.node_decode(current_node_ptr.*)) {
            .empty => unreachable,
            .leaf => |leaf| {
                if (std.mem.eql(u8, leaf.key, key)) return leaf.load_value();
                return null;
            },
            .internal => |header| {
                if (header.prefix_len > 0) {
                    const stored_len = @min(header.prefix_len, MAX_PREFIX_LEN);
                    for (0..stored_len) |i| {
                        if (depth >= key.len or header.prefix[i] != key[depth]) return null;
                        depth += 1;
                    }
                    if (header.prefix_len > MAX_PREFIX_LEN) {
                        const exemplar = Tree.find_any_leaf(current_node_ptr).key;
                        const limit = @min(exemplar.len, key.len);
                        for (MAX_PREFIX_LEN..header.prefix_len) |_| {
                            if (depth >= limit or exemplar[depth] != key[depth]) return null;
                            depth += 1;
                        }
                    }
                }

                if (depth == key.len) {
                    return if (header.load_leaf_value()) |leaf| leaf.load_value() else null;
                }

                const child = header.find_child(key[depth]) orelse return null;
                current_node_ptr = child;
                depth += 1;
            },
        }
    }
    return null;
}

test "plan_insert and apply_prepared_insert create one root leaf" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var root = node.node_empty();
    var shadow = try ShadowTree.init_from_live(allocator, &root);
    const prepared = try shadow.plan_insert(allocator, "alpha");
    const reserved = try reserve_insert_for_test(allocator, "alpha", .{ .integer = 7 }, prepared);

    try testing.expectEqual(InsertPlanKind.create_root_leaf, prepared.kind);
    try apply_prepared_insert(&root, &prepared, &reserved);

    const stored = lookup_value_for_test(&root, "alpha") orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i64, 7), stored.integer);
}

test "plan_insert and apply_prepared_insert overwrite an exact existing leaf" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const existing = try create_leaf_for_test(allocator, "alpha", .{ .integer = 1 });
    var root = node.node_leaf(existing);
    var shadow = try ShadowTree.init_from_live(allocator, &root);
    const prepared = try shadow.plan_insert(allocator, "alpha");
    const reserved = try reserve_insert_for_test(allocator, "alpha", .{ .integer = 9 }, prepared);

    try testing.expectEqual(InsertPlanKind.overwrite_leaf, prepared.kind);
    try apply_prepared_insert(&root, &prepared, &reserved);

    const stored = lookup_value_for_test(&root, "alpha") orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i64, 9), stored.integer);
}

test "plan_insert and apply_prepared_insert split one leaf for diverging keys" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const existing = try create_leaf_for_test(allocator, "alpha", .{ .integer = 1 });
    var root = node.node_leaf(existing);
    var shadow = try ShadowTree.init_from_live(allocator, &root);
    const prepared = try shadow.plan_insert(allocator, "beta");
    const reserved = try reserve_insert_for_test(allocator, "beta", .{ .integer = 2 }, prepared);

    try testing.expectEqual(InsertPlanKind.leaf_split, prepared.kind);
    try apply_prepared_insert(&root, &prepared, &reserved);

    const alpha = lookup_value_for_test(&root, "alpha") orelse return error.TestUnexpectedResult;
    const beta = lookup_value_for_test(&root, "beta") orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i64, 1), alpha.integer);
    try testing.expectEqual(@as(i64, 2), beta.integer);
}

test "plan_insert and apply_prepared_insert prefix split a compressed path" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const existing = try create_leaf_for_test(allocator, "abcx", .{ .integer = 1 });
    const root_internal = try allocator.create(Node4);
    root_internal.* = Node4.init();
    root_internal.header.prefix_len = 2;
    root_internal.header.prefix[0] = 'a';
    root_internal.header.prefix[1] = 'b';

    var root = node.node_internal(&root_internal.header);
    try node.add_child_reserved(&root, 'c', node.node_leaf(existing), null);

    var shadow = try ShadowTree.init_from_live(allocator, &root);
    const prepared = try shadow.plan_insert(allocator, "adz");
    const reserved = try reserve_insert_for_test(allocator, "adz", .{ .integer = 2 }, prepared);

    try testing.expectEqual(InsertPlanKind.prefix_split, prepared.kind);
    try apply_prepared_insert(&root, &prepared, &reserved);

    const old_value = lookup_value_for_test(&root, "abcx") orelse return error.TestUnexpectedResult;
    const new_value = lookup_value_for_test(&root, "adz") orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i64, 1), old_value.integer);
    try testing.expectEqual(@as(i64, 2), new_value.integer);
}

test "plan_insert and apply_prepared_insert grow and add one child when the node is full" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const root_internal = try allocator.create(Node4);
    root_internal.* = Node4.init();
    var root = node.node_internal(&root_internal.header);

    const keys = [_][]const u8{ "a", "b", "c", "d" };
    for (keys, 0..) |key, index| {
        const leaf = try create_leaf_for_test(allocator, key, .{ .integer = @intCast(index + 1) });
        try node.add_child_reserved(&root, key[0], node.node_leaf(leaf), null);
    }

    var shadow = try ShadowTree.init_from_live(allocator, &root);
    const prepared = try shadow.plan_insert(allocator, "e");
    const reserved = try reserve_insert_for_test(allocator, "e", .{ .integer = 5 }, prepared);

    try testing.expectEqual(InsertPlanKind.grow_and_add_child, prepared.kind);
    try apply_prepared_insert(&root, &prepared, &reserved);

    try testing.expectEqual(NodeType.node16, node.node_to_internal(root).node_type);
    const stored = lookup_value_for_test(&root, "e") orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i64, 5), stored.integer);
}

test "shadow planning preserves hidden-prefix sequential inserts across repeated apply" {
    const testing = std.testing;
    const art_tree = @import("tree.zig");

    var live_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer live_arena.deinit();
    const live_allocator = live_arena.allocator();

    var tree = art_tree.Tree.init(live_allocator);
    const first_value = try live_allocator.create(Value);
    first_value.* = .{ .integer = 10 };
    try tree.insert(
        try live_allocator.dupe(u8, "abcdefghijklmnop:1"),
        first_value,
    );

    var shadow_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer shadow_arena.deinit();
    var shadow = try tree.build_shadow_tree(shadow_arena.allocator());
    const prepared_two = try tree.plan_prepared_insert(&shadow, shadow_arena.allocator(), "abcdefghijklmnop:2");
    const prepared_three = try tree.plan_prepared_insert(&shadow, shadow_arena.allocator(), "abcdefghijklmnop:3");

    var reserved_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer reserved_arena.deinit();
    const reserved_two = try reserve_insert_for_test(reserved_arena.allocator(), "abcdefghijklmnop:2", .{ .integer = 20 }, prepared_two);
    const reserved_three = try reserve_insert_for_test(reserved_arena.allocator(), "abcdefghijklmnop:3", .{ .integer = 30 }, prepared_three);

    try tree.apply_prepared_insert(&prepared_two, &reserved_two);
    try tree.apply_prepared_insert(&prepared_three, &reserved_three);

    try testing.expectEqual(@as(i64, 10), tree.lookup("abcdefghijklmnop:1").?.integer);
    try testing.expectEqual(@as(i64, 20), tree.lookup("abcdefghijklmnop:2").?.integer);
    try testing.expectEqual(@as(i64, 30), tree.lookup("abcdefghijklmnop:3").?.integer);
}

test "apply_prepared_insert detects invariant drift between planning and apply" {
    const testing = std.testing;

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const existing = try create_leaf_for_test(allocator, "alpha", .{ .integer = 1 });
    var root = node.node_leaf(existing);
    var shadow = try ShadowTree.init_from_live(allocator, &root);
    const prepared = try shadow.plan_insert(allocator, "alpha");
    const reserved = try reserve_insert_for_test(allocator, "alpha", .{ .integer = 2 }, prepared);

    existing.key = "beta";

    try testing.expectError(error.BatchPlanInvariantViolation, apply_prepared_insert(&root, &prepared, &reserved));
}
