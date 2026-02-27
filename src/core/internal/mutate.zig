//! Internal physical mutation helpers shared by engine write and batch paths.
//! Cost: O(k) over key length for boundary validation.
//! Allocator: Does not allocate.

const std = @import("std");
const codec = @import("codec.zig");
const runtime_shard = @import("../runtime/shard.zig");
const types = @import("../types.zig");

/// Error set for low-level physical mutation validation.
pub const MutationError = error{
    EmptyKey,
    KeyTooLarge,
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
    return shard.values.contains(key);
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
    const stored = shard.values.getPtr(key) orelse return null;
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
    const stored = shard.values.getPtr(key) orelse return false;
    return values_equal(stored, expected);
}

/// Applies one already-owned value into shard storage after capacity has been reserved.
///
/// Time Complexity: O(k + v), where `k` is `key.len` and `v` is teardown cost for any replaced stored value.
///
/// Allocator: Does not allocate; assumes any required hash-map capacity has already been reserved.
///
/// Ownership: Transfers ownership of `owned_insert_key` and `value` into shard storage on insert, or of `value` alone on replace.
pub fn apply_owned_put_assume_capacity_unlocked(
    shard: *runtime_shard.Shard,
    allocator: std.mem.Allocator,
    key: []const u8,
    owned_insert_key: ?[]u8,
    value: types.Value,
) void {
    if (shard.values.getPtr(key)) |stored| {
        stored.deinit(allocator);
        stored.* = value;
        return;
    }

    std.debug.assert(owned_insert_key != null);
    shard.values.putAssumeCapacityNoClobber(owned_insert_key.?, value);
}

/// Removes one stored key/value pair when present.
///
/// Time Complexity: O(k + v), where `k` is `key.len` and `v` is teardown cost for the removed value.
///
/// Allocator: Does not allocate; frees owned key and nested value storage through `allocator`.
pub fn remove_stored_value_unlocked(
    shard: *runtime_shard.Shard,
    allocator: std.mem.Allocator,
    key: []const u8,
) bool {
    const removed = shard.values.fetchRemove(key) orelse return false;
    allocator.free(removed.key);

    var owned_value = removed.value;
    owned_value.deinit(allocator);
    return true;
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
