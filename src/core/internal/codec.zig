//! Internal physical codec for value serialization used by storage and recovery paths.
//! Cost: Serialization and deserialization are O(n + b), where `n` is nested node count and `b` is total payload bytes.
//! Allocator: Uses explicit allocators for output growth and decoded owned buffers.

const std = @import("std");
const Value = @import("../types/value.zig").Value;

/// Maximum nesting depth for recursive value serialization and deserialization.
pub const MAX_DEPTH: usize = 32;

/// Maximum accepted key length for durability records.
pub const MAX_KEY_LEN: u32 = 4_096;

/// Maximum accepted serialized value size for durability records.
pub const MAX_VAL_LEN: u32 = 16 * 1024 * 1024;

const val_null: u8 = 0x00;
const val_bool: u8 = 0x01;
const val_int: u8 = 0x02;
const val_float: u8 = 0x03;
const val_string: u8 = 0x04;
const val_array: u8 = 0x05;
const val_object: u8 = 0x06;

/// Serializes one value into the internal binary durability format.
///
/// Time Complexity: O(n + b), where `n` is nested node count and `b` is emitted bytes.
///
/// Allocator: Appends to `buf` using `allocator` for capacity growth.
pub fn serializeValue(
    allocator: std.mem.Allocator,
    value: *const Value,
    buf: *std.ArrayList(u8),
    depth: usize,
) !void {
    if (depth > MAX_DEPTH) return error.MaxDepthExceeded;
    switch (value.*) {
        .null_val => try buf.append(allocator, val_null),
        .boolean => |payload| {
            try buf.append(allocator, val_bool);
            try buf.append(allocator, if (payload) 1 else 0);
        },
        .integer => |payload| {
            try buf.append(allocator, val_int);
            var tmp: [8]u8 = undefined;
            std.mem.writeInt(i64, &tmp, payload, .little);
            try buf.appendSlice(allocator, &tmp);
        },
        .float => |payload| {
            try buf.append(allocator, val_float);
            var tmp: [8]u8 = undefined;
            std.mem.writeInt(u64, &tmp, @bitCast(payload), .little);
            try buf.appendSlice(allocator, &tmp);
        },
        .string => |payload| {
            try buf.append(allocator, val_string);
            try writeU32Le(allocator, buf, @intCast(payload.len));
            try buf.appendSlice(allocator, payload);
        },
        .array => |payload| {
            try buf.append(allocator, val_array);
            try writeU32Le(allocator, buf, @intCast(payload.items.len));
            for (payload.items) |*item| {
                try serializeValue(allocator, item, buf, depth + 1);
            }
        },
        .object => |payload| {
            try buf.append(allocator, val_object);
            try writeU32Le(allocator, buf, payload.count());
            var iterator = payload.iterator();
            while (iterator.next()) |entry| {
                try writeU32Le(allocator, buf, @intCast(entry.key_ptr.*.len));
                try buf.appendSlice(allocator, entry.key_ptr.*);
                try serializeValue(allocator, entry.value_ptr, buf, depth + 1);
            }
        },
    }
}

/// Deserializes one value from the internal binary durability format.
///
/// Time Complexity: O(n + b), where `n` is decoded node count and `b` is consumed bytes.
///
/// Allocator: Allocates decoded strings, arrays, and object storage from `allocator`.
///
/// Ownership: Caller owns the returned value and must later call `deinit`.
pub fn deserializeValue(reader: anytype, allocator: std.mem.Allocator, depth: usize) !Value {
    if (depth > MAX_DEPTH) return error.MaxDepthExceeded;

    const tag = try reader.takeByte();
    return switch (tag) {
        val_null => Value{ .null_val = {} },
        val_bool => Value{ .boolean = (try reader.takeByte()) != 0 },
        val_int => blk: {
            var tmp: [8]u8 = undefined;
            try reader.readSliceAll(&tmp);
            break :blk Value{ .integer = std.mem.readInt(i64, &tmp, .little) };
        },
        val_float => blk: {
            var tmp: [8]u8 = undefined;
            try reader.readSliceAll(&tmp);
            break :blk Value{ .float = @bitCast(std.mem.readInt(u64, &tmp, .little)) };
        },
        val_string => blk: {
            const len = try readU32Le(reader);
            const payload = try allocator.alloc(u8, len);
            errdefer allocator.free(payload);
            try reader.readSliceAll(payload);
            break :blk Value{ .string = payload };
        },
        val_array => blk: {
            const count = try readU32Le(reader);
            var items = try std.ArrayList(Value).initCapacity(allocator, count);
            errdefer items.deinit(allocator);
            for (0..count) |_| {
                items.appendAssumeCapacity(try deserializeValue(reader, allocator, depth + 1));
            }
            break :blk Value{ .array = items };
        },
        val_object => blk: {
            const count = try readU32Le(reader);
            var entries = std.StringHashMapUnmanaged(Value){};
            try entries.ensureTotalCapacity(allocator, count);
            errdefer {
                var iterator = entries.iterator();
                while (iterator.next()) |entry| {
                    allocator.free(entry.key_ptr.*);
                }
                entries.deinit(allocator);
            }

            for (0..count) |_| {
                const key_len = try readU32Le(reader);
                const key = try allocator.alloc(u8, key_len);
                errdefer allocator.free(key);
                try reader.readSliceAll(key);
                const value = try deserializeValue(reader, allocator, depth + 1);
                entries.putAssumeCapacity(key, value);
            }
            break :blk Value{ .object = entries };
        },
        else => error.UnknownValueTag,
    };
}

fn writeU32Le(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), value: u32) !void {
    var tmp: [4]u8 = undefined;
    std.mem.writeInt(u32, &tmp, value, .little);
    try buf.appendSlice(allocator, &tmp);
}

fn readU32Le(reader: anytype) !u32 {
    var tmp: [4]u8 = undefined;
    try reader.readSliceAll(&tmp);
    return std.mem.readInt(u32, &tmp, .little);
}

test "serialize_value and deserialize_value roundtrip nested payloads" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);

    var nested = std.StringHashMapUnmanaged(Value){};
    defer {
        var iterator = nested.iterator();
        while (iterator.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(allocator);
        }
        nested.deinit(allocator);
    }
    try nested.put(allocator, try allocator.dupe(u8, "ok"), .{ .boolean = true });

    const original = Value{ .object = nested };
    try serializeValue(allocator, &original, &buf, 0);

    var stream = std.Io.Reader.fixed(buf.items);
    var decoded = try deserializeValue(&stream, allocator, 0);
    defer decoded.deinit(allocator);

    switch (decoded) {
        .object => |entries| try testing.expect(entries.get("ok").?.boolean),
        else => try testing.expect(false),
    }
}
