//! Storage-owned replay adapter construction for WAL recovery.
//! Cost: O(1) fixed field mapping only.
//! Allocator: Does not allocate.

const wal = @import("wal.zig");
const Value = @import("../types/value.zig").Value;

/// Callback table used to connect engine-owned replay handlers to storage-owned WAL replay.
pub const ReplayFns = struct {
    put: *const fn (ctx: *anyopaque, key: []const u8, value: *const Value) anyerror!void,
    delete: *const fn (ctx: *anyopaque, key: []const u8) anyerror!void,
    expire: *const fn (ctx: *anyopaque, key: []const u8, expire_at_sec: i64) anyerror!void,
};

/// Builds one WAL replay adapter from explicit callback functions.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn build_replay_applier(ctx: *anyopaque, fns: ReplayFns) wal.ReplayApplier {
    return .{
        .ctx = ctx,
        .put = fns.put,
        .delete = fns.delete,
        .expire = fns.expire,
    };
}
