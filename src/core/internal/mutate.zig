//! Internal physical mutation helpers shared by engine write and batch paths.
//! Cost: O(k) over key length for boundary validation.
//! Allocator: Does not allocate.

const std = @import("std");
const codec = @import("codec.zig");
const art_node = @import("../index/art/node.zig");
const runtime_shard = @import("../runtime/shard.zig");
const types = @import("../types.zig");

/// Error set for low-level physical mutation validation.
pub const MutationError = error{
    EmptyKey,
    KeyTooLarge,
};

/// Selects overwrite storage ownership for point upserts.
pub const OverwriteOwnership = enum {
    tree_arena,
    heap,
};

/// Outcome returned by upsert paths with overwrite accounting details.
pub const UpsertOutcome = struct {
    overwritten: bool,
    previous_heavy_bytes: u64,
    previous_heavy_event: bool,
};

/// Validates one physical key before it enters engine mutation planning.
///
/// Time Complexity: O(k), where `k` is `key.len`.
///
/// Allocator: Does not allocate.
pub fn validate_key(key: []const u8) MutationError!void {
    if (key.len == 0) return error.EmptyKey;
    if (key.len > codec.MAX_KEY_LEN) return error.KeyTooLarge;
}

/// Returns whether one key currently exists in shard-owned plain storage.
///
/// Time Complexity: O(k), where `k` is `key.len`.
///
/// Allocator: Does not allocate.
pub fn key_exists_unlocked(shard: *const runtime_shard.Shard, key: []const u8) bool {
    return shard.tree.lookup(key) != null;
}

/// Clones one stored value from shard-owned plain storage when present.
///
/// Time Complexity: O(k + v), where `k` is `key.len` and `v` is cloned value size.
///
/// Allocator: Allocates the returned clone through `allocator` when the key exists.
///
/// Ownership: Returns a caller-owned clone when non-null.
pub fn clone_stored_value_unlocked(
    shard: *const runtime_shard.Shard,
    allocator: std.mem.Allocator,
    key: []const u8,
) !?types.Value {
    const stored = shard.tree.lookup(key) orelse return null;
    return try stored.clone(allocator);
}

/// Returns whether one stored value currently equals `expected`.
///
/// Time Complexity: O(k + v), where `k` is `key.len` and `v` is the compared value tree size.
///
/// Allocator: Does not allocate.
pub fn stored_value_equals_unlocked(
    shard: *const runtime_shard.Shard,
    key: []const u8,
    expected: *const types.Value,
) bool {
    const stored = shard.tree.lookup(key) orelse return false;
    return values_equal(stored, expected);
}

/// Clones one key and value into shard-owned ART storage, inserting or replacing the live entry.
///
/// Time Complexity: O(k + v), where `k` is `key.len` and `v` is deep-clone work for `value`.
///
/// Allocator: Uses the shard arena allocator for inserts; overwrite storage strategy depends on `overwrite_ownership`.
///
/// Ownership: Clones `value` into shard-owned storage and releases previously heap-owned values when replaced.
pub fn upsert_value_unlocked_with_overwrite_ownership_and_outcome(
    shard: *runtime_shard.Shard,
    key: []const u8,
    value: *const types.Value,
    overwrite_ownership: OverwriteOwnership,
) !UpsertOutcome {
    if (shard.tree.find_leaf_for_exact_key(key)) |leaf| {
        const previous_heavy_bytes = estimated_value_bytes(leaf.value);
        const previous_heavy_event = previous_heavy_bytes != 0;

        if (is_scalar_value(value)) {
            if (leaf.value_owner == .heap_allocation) {
                leaf.value.deinit(shard.base_allocator);
            }
            leaf.value.* = value.*;
            return .{
                .overwritten = true,
                .previous_heavy_bytes = previous_heavy_bytes,
                .previous_heavy_event = previous_heavy_event,
            };
        }

        const ClonedOverwrite = struct {
            value: *types.Value,
            owner: art_node.ValueOwner,
        };

        const cloned: ClonedOverwrite = switch (overwrite_ownership) {
            .tree_arena => blk: {
                const arena_allocator = shard.arena.allocator();
                const cloned_value = try arena_allocator.create(types.Value);
                cloned_value.* = try value.clone(arena_allocator);
                break :blk .{
                    .value = cloned_value,
                    .owner = art_node.ValueOwner.tree_allocator,
                };
            },
            .heap => blk: {
                const cloned_value = try shard.base_allocator.create(types.Value);
                errdefer shard.base_allocator.destroy(cloned_value);
                cloned_value.* = try value.clone(shard.base_allocator);
                break :blk .{
                    .value = cloned_value,
                    .owner = art_node.ValueOwner.heap_allocation,
                };
            },
        };

        const previous_value = leaf.value;
        const previous_owner = leaf.value_owner;
        leaf.value = cloned.value;
        leaf.value_owner = cloned.owner;
        if (previous_owner == .heap_allocation) {
            free_heap_value(shard.base_allocator, previous_value);
        }
        return .{
            .overwritten = true,
            .previous_heavy_bytes = previous_heavy_bytes,
            .previous_heavy_event = previous_heavy_event,
        };
    }

    const arena_allocator = shard.arena.allocator();
    const cloned_value = try arena_allocator.create(types.Value);
    cloned_value.* = try value.clone(arena_allocator);
    const cloned_key = try arena_allocator.dupe(u8, key);
    shard.tree.insert(cloned_key, cloned_value) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.TreeFull, error.InvalidNodeGrowth, error.InvalidNodeType => unreachable,
    };
    return .{
        .overwritten = false,
        .previous_heavy_bytes = 0,
        .previous_heavy_event = false,
    };
}

/// Clones one key and value into shard-owned ART storage, inserting or replacing the live entry.
///
/// Overwrite Ownership: Uses the selected ownership strategy for overwrite replacements.
pub fn upsert_value_unlocked_with_overwrite_ownership(
    shard: *runtime_shard.Shard,
    key: []const u8,
    value: *const types.Value,
    overwrite_ownership: OverwriteOwnership,
) !bool {
    const outcome = try upsert_value_unlocked_with_overwrite_ownership_and_outcome(shard, key, value, overwrite_ownership);
    return outcome.overwritten;
}

/// Clones one key and value into shard-owned ART storage, inserting or replacing the live entry.
///
/// Overwrite Ownership: Uses shard arena storage for overwrite replacements.
pub fn upsert_value_unlocked(
    shard: *runtime_shard.Shard,
    key: []const u8,
    value: *const types.Value,
) !bool {
    return upsert_value_unlocked_with_overwrite_ownership(shard, key, value, .tree_arena);
}

/// Clones one key and value into shard-owned ART storage and returns overwrite accounting details.
///
/// Overwrite Ownership: Uses shard arena storage for overwrite replacements.
pub fn upsert_value_unlocked_with_outcome(
    shard: *runtime_shard.Shard,
    key: []const u8,
    value: *const types.Value,
) !UpsertOutcome {
    return upsert_value_unlocked_with_overwrite_ownership_and_outcome(shard, key, value, .tree_arena);
}

/// Attempts to overwrite an existing key's value without structural ART changes.
/// Returns one overwrite outcome when the key exists.
/// Returns null when the key does not exist and leaves the ART untouched.
/// This path does not mutate ART structure (no splits, grows, shrinks, or child edits).
pub fn try_overwrite_if_exists_unlocked(
    shard: *runtime_shard.Shard,
    key: []const u8,
    value: *const types.Value,
) !?UpsertOutcome {
    const leaf = shard.tree.find_leaf_for_exact_key(key) orelse return null;

    const previous_heavy_bytes = estimated_value_bytes(leaf.value);
    const previous_heavy_event = previous_heavy_bytes != 0;

    if (is_scalar_value(value)) {
        if (leaf.value_owner == .heap_allocation) {
            leaf.value.deinit(shard.base_allocator);
        }
        leaf.value.* = value.*;
        // Safety: publish with release to pair with GET's acquire load on the same
        // pointer address. The pointer value is unchanged; this acts as a release
        // fence ensuring the content write above is visible to readers.
        leaf.store_value(leaf.value);
        return UpsertOutcome{
            .overwritten = true,
            .previous_heavy_bytes = previous_heavy_bytes,
            .previous_heavy_event = previous_heavy_event,
        };
    }

    const arena_allocator = shard.arena.allocator();
    const cloned_value = try arena_allocator.create(types.Value);
    cloned_value.* = try value.clone(arena_allocator);

    const previous_value = leaf.value;
    const previous_owner = leaf.value_owner;
    leaf.store_value(cloned_value);
    leaf.value_owner = .tree_allocator;
    if (previous_owner == .heap_allocation) {
        free_heap_value(shard.base_allocator, previous_value);
    }

    return UpsertOutcome{
        .overwritten = true,
        .previous_heavy_bytes = previous_heavy_bytes,
        .previous_heavy_event = previous_heavy_event,
    };
}

fn is_scalar_value(value: *const types.Value) bool {
    return switch (value.*) {
        .null_val, .boolean, .integer, .float => true,
        .string, .array, .object => false,
    };
}

/// Returns whether a value is heavy (owns variable-sized payload storage).
pub fn is_heavy_value(value: *const types.Value) bool {
    return !is_scalar_value(value);
}

/// Estimates retained bytes for one stored value tree.
pub fn estimated_value_bytes(value: *const types.Value) u64 {
    return switch (value.*) {
        .null_val => 0,
        .boolean => 0,
        .integer => 0,
        .float => 0,
        .string => |payload| payload.len,
        .array => |items| blk: {
            var total: u64 = 0;
            for (items.items) |*item| total += estimated_value_bytes(item);
            break :blk total;
        },
        .object => |entries| blk: {
            var total: u64 = 0;
            var iterator = entries.iterator();
            while (iterator.next()) |entry| {
                total += entry.key_ptr.*.len;
                total += estimated_value_bytes(entry.value_ptr);
            }
            break :blk total;
        },
    };
}

/// Removes one stored key/value pair when present.
///
/// Time Complexity: O(k + v), where `k` is `key.len` and `v` is teardown cost for the removed value.
///
/// Allocator: Does not allocate directly; any ART restructuring uses the tree allocator.
pub fn remove_stored_value_unlocked(
    shard: *runtime_shard.Shard,
    key: []const u8,
) !bool {
    const existing_leaf = shard.tree.find_leaf_for_exact_key(key) orelse return false;
    const removed_value = existing_leaf.value;
    const removed_owner = existing_leaf.value_owner;

    const removed = shard.tree.delete(key) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidNodeType, error.InvalidNodeShrink => unreachable,
    };
    if (removed and removed_owner == .heap_allocation) {
        free_heap_value(shard.base_allocator, removed_value);
    }
    return removed;
}

/// Releases one individually heap-owned stored value tree.
///
/// Time Complexity: O(v), where `v` is the nested value size.
///
/// Allocator: Does not allocate; recursively frees `value` through `allocator`.
pub fn free_heap_value(allocator: std.mem.Allocator, value: *types.Value) void {
    value.deinit(allocator);
    allocator.destroy(value);
}

/// Returns whether one stored leaf currently owns its value through an individually reclaimable heap allocation.
///
/// Time Complexity: O(k), where `k` is `key.len`.
///
/// Allocator: Does not allocate.
pub fn value_is_heap_owned_unlocked(shard: *const runtime_shard.Shard, key: []const u8) bool {
    const leaf = @constCast(&shard.tree).find_leaf_for_exact_key(key) orelse return false;
    return leaf.value_owner == art_node.ValueOwner.heap_allocation;
}

/// Counts the currently retained committed delta arenas on one shard.
///
/// Time Complexity: O(a), where `a` is the committed arena chain length.
///
/// Allocator: Does not allocate.
pub fn count_committed_arenas_unlocked(shard: *const runtime_shard.Shard) usize {
    var count: usize = 0;
    var current = shard.committed_arenas_head;
    while (current) |arena| {
        count += 1;
        current = arena.next;
    }
    return count;
}

/// Returns whether two values are physically equal by full content.
///
/// Time Complexity: O(v), where `v` is the combined compared value tree size.
///
/// Allocator: Does not allocate.
pub fn values_equal(left: *const types.Value, right: *const types.Value) bool {
    return switch (left.*) {
        .null_val => switch (right.*) {
            .null_val => true,
            else => false,
        },
        .boolean => |payload| switch (right.*) {
            .boolean => |other| payload == other,
            else => false,
        },
        .integer => |payload| switch (right.*) {
            .integer => |other| payload == other,
            else => false,
        },
        .float => |payload| switch (right.*) {
            .float => |other| payload == other,
            else => false,
        },
        .string => |payload| switch (right.*) {
            .string => |other| std.mem.eql(u8, payload, other),
            else => false,
        },
        .array => |payload| switch (right.*) {
            .array => |other| blk: {
                if (payload.items.len != other.items.len) break :blk false;
                for (payload.items, other.items) |*item, *other_item| {
                    if (!values_equal(item, other_item)) break :blk false;
                }
                break :blk true;
            },
            else => false,
        },
        .object => |payload| switch (right.*) {
            .object => |other| blk: {
                if (payload.count() != other.count()) break :blk false;
                var iterator = payload.iterator();
                while (iterator.next()) |entry| {
                    const other_value = other.getPtr(entry.key_ptr.*) orelse break :blk false;
                    if (!values_equal(entry.value_ptr, other_value)) break :blk false;
                }
                break :blk true;
            },
            else => false,
        },
    };
}

test "validate_key rejects empty and oversized keys" {
    const testing = std.testing;

    try testing.expectError(error.EmptyKey, validate_key(""));
    try testing.expectError(error.KeyTooLarge, validate_key(&[_]u8{'a'} ** (codec.MAX_KEY_LEN + 1)));
}

test "values_equal compares nested values by content" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var left_entries = std.StringHashMapUnmanaged(types.Value){};
    defer {
        var iterator = left_entries.iterator();
        while (iterator.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(allocator);
        }
        left_entries.deinit(allocator);
    }
    try left_entries.put(allocator, try allocator.dupe(u8, "ok"), .{ .boolean = true });

    var right_entries = std.StringHashMapUnmanaged(types.Value){};
    defer {
        var iterator = right_entries.iterator();
        while (iterator.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(allocator);
        }
        right_entries.deinit(allocator);
    }
    try right_entries.put(allocator, try allocator.dupe(u8, "ok"), .{ .boolean = true });

    const left = types.Value{ .object = left_entries };
    const right = types.Value{ .object = right_entries };
    const different = types.Value{ .object = std.StringHashMapUnmanaged(types.Value){} };

    try testing.expect(values_equal(&left, &right));
    try testing.expect(!values_equal(&left, &different));
}
