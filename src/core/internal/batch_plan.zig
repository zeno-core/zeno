//! Internal batch planning helpers for physical validation and deterministic shard grouping.
//! Cost: O(n + s + b), where `n` is batch size, `s` is shard count touched during grouping, and `b` is total serialized value bytes measured during validation.
//! Allocator: Uses explicit allocators for one plan arena and temporary serialization scratch.

const std = @import("std");
const batch = @import("../types/batch.zig");
const codec = @import("codec.zig");
const mutate = @import("mutate.zig");
const runtime_shard = @import("../runtime/shard.zig");

/// Low-level batch planning error set.
pub const BatchPlanError = mutate.MutationError || error{
    ValueTooLarge,
    MaxDepthExceeded,
    OutOfMemory,
};

/// One normalized survivor write with owned key bytes and stable input index.
pub const PlannedWrite = struct {
    key: []const u8,
    value: *const @import("../types/value.zig").Value,
    shard_idx: u8,
    input_index: usize,
};

/// One shard-local write group in ascending shard order.
pub const ShardWriteGroup = struct {
    shard_idx: u8,
    writes: []PlannedWrite,
};

/// Owned batch plan for one apply attempt.
pub const BatchPlan = struct {
    arena: std.heap.ArenaAllocator,
    writes: []PlannedWrite,
    groups: []ShardWriteGroup,

    /// Releases all normalized key bytes and grouping metadata owned by the plan.
    ///
    /// Time Complexity: O(1), delegated to arena teardown.
    ///
    /// Allocator: Frees all arena-backed plan storage.
    pub fn deinit(self: *BatchPlan) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Validates one write entry at the planner boundary.
///
/// Time Complexity: O(k + v), where `k` is key length and `v` is serialized value size.
///
/// Allocator: Uses `scratch` growth through `scratch_allocator` and reuses retained capacity across writes.
fn validate_write(
    scratch_allocator: std.mem.Allocator,
    scratch: *std.ArrayList(u8),
    write: batch.PutWrite,
) BatchPlanError!void {
    try mutate.validate_key(write.key);
    scratch.clearRetainingCapacity();
    try codec.serialize_value(scratch_allocator, write.value, &scratch.*, 0);
    if (scratch.items.len > codec.MAX_VAL_LEN) return error.ValueTooLarge;
}

/// Validates one batch and returns one owned planning summary grouped by shard.
///
/// Time Complexity: O(n + s + b), where `n` is `writes.len`, `s` is runtime shard count, and `b` is total serialized value bytes measured during validation.
///
/// Allocator: Allocates the returned plan arena from `allocator` and uses temporary serializer scratch that is released before return.
pub fn plan_put_batch(allocator: std.mem.Allocator, writes: []const batch.PutWrite) BatchPlanError!BatchPlan {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_allocator = arena.allocator();

    var scratch = std.ArrayList(u8).empty;
    defer scratch.deinit(allocator);

    const normalized_writes = try arena_allocator.alloc(PlannedWrite, writes.len);
    const survivor_flags = try arena_allocator.alloc(bool, writes.len);
    @memset(survivor_flags, false);
    var last_by_key = std.StringHashMapUnmanaged(usize){};
    var shard_counts: [runtime_shard.NUM_SHARDS]usize = [_]usize{0} ** runtime_shard.NUM_SHARDS;
    var survivor_count: usize = 0;

    for (writes, 0..) |write, index| {
        try validate_write(allocator, &scratch, write);

        const owned_key = try arena_allocator.dupe(u8, write.key);
        const shard_idx = runtime_shard.get_shard_index(owned_key);
        normalized_writes[index] = .{
            .key = owned_key,
            .value = write.value,
            .shard_idx = @intCast(shard_idx),
            .input_index = index,
        };

        if (last_by_key.getEntry(owned_key)) |entry| {
            if (survivor_flags[entry.value_ptr.*]) {
                survivor_flags[entry.value_ptr.*] = false;
                survivor_count -= 1;
            }
            entry.value_ptr.* = index;
        } else {
            try last_by_key.put(arena_allocator, owned_key, index);
        }

        survivor_flags[index] = true;
        survivor_count += 1;
    }

    for (normalized_writes, 0..) |write, index| {
        if (!survivor_flags[index]) continue;
        shard_counts[write.shard_idx] += 1;
    }

    const grouped_writes = try arena_allocator.alloc(PlannedWrite, survivor_count);
    var non_empty_groups: usize = 0;
    for (shard_counts) |count| {
        if (count > 0) non_empty_groups += 1;
    }
    const groups = try arena_allocator.alloc(ShardWriteGroup, non_empty_groups);

    var write_offsets: [runtime_shard.NUM_SHARDS]usize = [_]usize{0} ** runtime_shard.NUM_SHARDS;
    var running_offset: usize = 0;
    var group_index: usize = 0;
    for (shard_counts, 0..) |count, shard_idx| {
        write_offsets[shard_idx] = running_offset;
        if (count == 0) continue;
        groups[group_index] = .{
            .shard_idx = @intCast(shard_idx),
            .writes = grouped_writes[running_offset .. running_offset + count],
        };
        running_offset += count;
        group_index += 1;
    }

    var next_write_index = write_offsets;
    for (normalized_writes, 0..) |write, index| {
        if (!survivor_flags[index]) continue;
        const slot = next_write_index[write.shard_idx];
        grouped_writes[slot] = write;
        next_write_index[write.shard_idx] += 1;
    }

    return .{
        .arena = arena,
        .writes = grouped_writes,
        .groups = groups,
    };
}

test "plan_put_batch keeps final values in first-declared key order within grouped survivors" {
    const testing = std.testing;
    const Value = @import("../types/value.zig").Value;

    const one = Value{ .integer = 1 };
    const two = Value{ .integer = 2 };
    var plan = try plan_put_batch(testing.allocator, &.{
        .{ .key = "a", .value = &one },
        .{ .key = "b", .value = &one },
        .{ .key = "a", .value = &two },
    });
    defer plan.deinit();

    try testing.expectEqual(@as(usize, 2), plan.writes.len);

    const shard_a = runtime_shard.get_shard_index("a");
    const shard_b = runtime_shard.get_shard_index("b");
    const first_expected = if (shard_a <= shard_b) "a" else "b";
    const second_expected = if (shard_a <= shard_b) "b" else "a";
    try testing.expectEqualStrings(first_expected, plan.writes[0].key);
    try testing.expectEqualStrings(second_expected, plan.writes[1].key);

    const a_index: usize = if (std.mem.eql(u8, plan.writes[0].key, "a")) 0 else 1;
    try testing.expectEqual(@as(i64, 2), plan.writes[a_index].value.*.integer);
}
