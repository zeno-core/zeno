//! Public value model for zeno-core payload storage.
//! Cost: Scalar access is O(1); clone and teardown are O(n) over nested nodes.
//! Allocator: Uses explicit allocators for deep clone and recursive teardown.

const std = @import("std");

/// Tagged value union for scalar and nested payloads stored by the engine.
pub const Value = union(enum) {
    null_val: void,
    boolean: bool,
    integer: i64,
    float: f64,
    string: []const u8,
    array: std.ArrayList(Value),
    object: std.StringHashMapUnmanaged(Value),

    /// Deep-clones the full value tree into memory owned by `allocator`.
    ///
    /// Time Complexity: O(n + b), where `n` is nested value node count and `b` is total cloned string bytes.
    ///
    /// Allocator: Allocates duplicated strings plus array and object storage from `allocator`.
    ///
    /// Ownership: Caller owns the returned value and must later call `deinit` with the same allocator.
    pub fn clone(self: *const Value, allocator: std.mem.Allocator) !Value {
        return switch (self.*) {
            .null_val => .{ .null_val = {} },
            .boolean => |value| .{ .boolean = value },
            .integer => |value| .{ .integer = value },
            .float => |value| .{ .float = value },
            .string => |value| .{ .string = try allocator.dupe(u8, value) },
            .array => |items| {
                var cloned_items = try std.ArrayList(Value).initCapacity(allocator, items.items.len);
                for (items.items) |item| {
                    const cloned_item = try item.clone(allocator);
                    cloned_items.appendAssumeCapacity(cloned_item);
                }
                return .{ .array = cloned_items };
            },
            .object => |entries| {
                var cloned_entries = std.StringHashMapUnmanaged(Value){};
                try cloned_entries.ensureTotalCapacity(allocator, entries.count());
                var iterator = entries.iterator();
                while (iterator.next()) |entry| {
                    const cloned_key = try allocator.dupe(u8, entry.key_ptr.*);
                    const cloned_value = try entry.value_ptr.clone(allocator);
                    cloned_entries.putAssumeCapacity(cloned_key, cloned_value);
                }
                return .{ .object = cloned_entries };
            },
        };
    }

    /// Recursively releases memory owned by this value and all nested children.
    ///
    /// Time Complexity: O(n + b), where `n` is nested value node count and `b` is total freed string bytes.
    ///
    /// Allocator: Does not allocate; frees memory through `allocator`.
    ///
    /// Ownership: Releases only storage previously allocated for this value tree with `allocator`.
    pub fn deinit(self: *Value, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => |value| allocator.free(value),
            .array => |*items| {
                for (items.items) |*item| item.deinit(allocator);
                items.deinit(allocator);
            },
            .object => |*entries| {
                var iterator = entries.iterator();
                while (iterator.next()) |entry| {
                    allocator.free(entry.key_ptr.*);
                    entry.value_ptr.deinit(allocator);
                }
                entries.deinit(allocator);
            },
            else => {},
        }
    }
};

test "value clone duplicates owned nested storage" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var nested = std.StringHashMapUnmanaged(Value){};
    defer {
        var iterator = nested.iterator();
        while (iterator.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(allocator);
        }
        nested.deinit(allocator);
    }

    try nested.put(allocator, try allocator.dupe(u8, "message"), .{ .string = try allocator.dupe(u8, "hello") });

    var original = Value{ .object = nested };
    var cloned = try original.clone(allocator);
    defer cloned.deinit(allocator);

    switch (cloned) {
        .object => |entries| {
            const message = entries.get("message").?;
            try testing.expectEqualStrings("hello", message.string);
        },
        else => try testing.expect(false),
    }
}

test "value clone preserves scalar payloads without allocating nested storage" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var integer = Value{ .integer = 42 };
    var cloned_integer = try integer.clone(allocator);
    defer cloned_integer.deinit(allocator);
    try testing.expectEqual(@as(i64, 42), cloned_integer.integer);

    var boolean = Value{ .boolean = true };
    var cloned_boolean = try boolean.clone(allocator);
    defer cloned_boolean.deinit(allocator);
    try testing.expect(cloned_boolean.boolean);

    var null_value = Value{ .null_val = {} };
    var cloned_null = try null_value.clone(allocator);
    defer cloned_null.deinit(allocator);
    try testing.expect(cloned_null == .null_val);
}

test "value clone deep copies string storage" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const owned = try allocator.dupe(u8, "hello");
    var original = Value{ .string = owned };
    defer original.deinit(allocator);

    var cloned = try original.clone(allocator);
    defer cloned.deinit(allocator);

    try testing.expectEqualStrings("hello", cloned.string);
    try testing.expect(original.string.ptr != cloned.string.ptr);
}

test "value clone keeps object entries independent from the original" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var object = std.StringHashMapUnmanaged(Value){};
    defer {
        var iterator = object.iterator();
        while (iterator.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(allocator);
        }
        object.deinit(allocator);
    }

    try object.put(allocator, try allocator.dupe(u8, "message"), .{ .string = try allocator.dupe(u8, "hello") });

    var original = Value{ .object = object };
    var cloned = try original.clone(allocator);
    defer cloned.deinit(allocator);

    const original_message = original.object.getPtr("message").?;
    allocator.free(original_message.string);
    original_message.* = .{ .string = try allocator.dupe(u8, "jello") };

    const cloned_message = cloned.object.get("message").?;
    try testing.expectEqualStrings("hello", cloned_message.string);
}

test "value clone keeps array elements independent from the original" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var items = try std.ArrayList(Value).initCapacity(allocator, 2);
    errdefer {
        for (items.items) |*item| item.deinit(allocator);
        items.deinit(allocator);
    }
    items.appendAssumeCapacity(.{ .string = try allocator.dupe(u8, "alpha") });
    items.appendAssumeCapacity(.{ .integer = 7 });

    var original = Value{ .array = items };
    defer original.deinit(allocator);

    var cloned = try original.clone(allocator);
    defer cloned.deinit(allocator);

    allocator.free(original.array.items[0].string);
    original.array.items[0] = .{ .string = try allocator.dupe(u8, "omega") };

    try testing.expectEqualStrings("alpha", cloned.array.items[0].string);
    try testing.expectEqual(@as(i64, 7), cloned.array.items[1].integer);
}

test "value deinit releases deeply nested cloned storage safely" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var nested_array = try std.ArrayList(Value).initCapacity(allocator, 1);
    errdefer {
        for (nested_array.items) |*item| item.deinit(allocator);
        nested_array.deinit(allocator);
    }
    nested_array.appendAssumeCapacity(.{ .string = try allocator.dupe(u8, "leaf") });

    var object = std.StringHashMapUnmanaged(Value){};
    errdefer {
        var iterator = object.iterator();
        while (iterator.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(allocator);
        }
        object.deinit(allocator);
    }
    try object.put(allocator, try allocator.dupe(u8, "nested"), .{ .array = nested_array });

    var original = Value{ .object = object };
    defer original.deinit(allocator);

    var cloned = try original.clone(allocator);
    cloned.deinit(allocator);
}
