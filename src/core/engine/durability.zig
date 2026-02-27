//! Engine-owned coordinator for routing live physical mutations into WAL appends.
//! Cost: O(k + v) for point appends and O(n * (k + v)) for batch envelopes, matching the delegated WAL work.
//! Allocator: Uses explicit allocators only for temporary batch-envelope views.

const std = @import("std");
const error_mod = @import("error.zig");
const runtime_state = @import("../runtime/state.zig");
const storage_wal = @import("../storage/wal.zig");
const types = @import("../types.zig");

/// One borrowed plain-key write forwarded into a WAL put-batch envelope.
pub const PutBatchWrite = struct {
    key: []const u8,
    value: *const types.Value,
};

/// Appends one live PUT mutation when WAL is configured.
///
/// Time Complexity: O(k + v), where `k` is key length and `v` is serialized value size.
///
/// Allocator: Does not allocate outside delegated WAL scratch.
pub fn append_put_if_enabled(
    state: *runtime_state.DatabaseState,
    key: []const u8,
    value: *const types.Value,
) error_mod.EngineError!void {
    if (state.wal) |*wal| {
        wal.append_put(key, value) catch |err| return error_mod.map_persistence_error(err);
    }
}

/// Appends one live DELETE mutation when WAL is configured.
///
/// Time Complexity: O(k), where `k` is key length.
///
/// Allocator: Does not allocate outside delegated WAL scratch.
pub fn append_delete_if_enabled(state: *runtime_state.DatabaseState, key: []const u8) error_mod.EngineError!void {
    if (state.wal) |*wal| {
        wal.append_delete(key) catch |err| return error_mod.map_persistence_error(err);
    }
}

/// Appends one live EXPIRE mutation when WAL is configured.
///
/// Time Complexity: O(k), where `k` is key length.
///
/// Allocator: Does not allocate outside delegated WAL scratch.
pub fn append_expire_if_enabled(
    state: *runtime_state.DatabaseState,
    key: []const u8,
    expire_at_seconds: i64,
) error_mod.EngineError!void {
    if (state.wal) |*wal| {
        wal.append_expire(key, expire_at_seconds) catch |err| return error_mod.map_persistence_error(err);
    }
}

/// Appends one committed physical put-batch envelope when WAL is configured.
///
/// Time Complexity: O(n * (k + v)), where `n` is `writes.len`.
///
/// Allocator: Uses `allocator` only for the temporary batch-view slice.
pub fn append_put_batch_if_enabled(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    writes: []const PutBatchWrite,
) error_mod.EngineError!void {
    if (writes.len == 0) return;
    const wal = if (state.wal) |*owned_wal| owned_wal else return;

    const wal_writes = allocator.alloc(storage_wal.PutBatchWrite, writes.len) catch return error.OutOfMemory;
    defer allocator.free(wal_writes);

    for (writes, 0..) |write, index| {
        wal_writes[index] = .{
            .key = write.key,
            .value = write.value,
        };
    }

    _ = wal.append_put_batch(wal_writes) catch |err| return error_mod.map_persistence_error(err);
}
