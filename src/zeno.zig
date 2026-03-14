//! Stable facade root for the zeno-core package.
//! Cost: O(1) module reexports only.
//! Allocator: Does not allocate.

const std = @import("std");

/// General public engine contract facade.
pub const public = @import("core/public.zig");

/// Official advanced engine contract facade.
pub const official = @import("core/official.zig");

/// Public access point for core-facing types.
pub const types = @import("core/types.zig");

const config = @import("config");
const builtin = @import("builtin");

/// Internal modules exposed for testing and benchmarking ONLY.
/// This is only available when the `expose_internals` build option is set,
/// or when running in test mode.
pub const testing_internal = if (config.expose_internals or builtin.is_test) struct {
    pub const art = @import("core/index/art/tree.zig");
    pub const wal = @import("core/storage/wal.zig");
    pub const runtime_state = @import("core/runtime/state.zig");
    pub const runtime_shard = @import("core/runtime/shard.zig");
} else struct {};

test "package root includes internal art module tests without exporting them" {
    _ = @import("core/index/art/node.zig");
    _ = @import("core/index/art/prepared_insert.zig");
    _ = @import("core/index/art/tree.zig");
    _ = @import("core/types/value.zig");
    _ = @import("core/runtime/shard.zig");
    _ = @import("core/engine/db.zig");
    _ = @import("core/storage/wal.zig");
    _ = @import("core/storage/snapshot.zig");
}

test "package root exposes contract modules" {
    _ = public;
    _ = official;
    _ = types;
}

test "package root exposes only the contract facades" {
    const testing = std.testing;

    try testing.expect(@hasDecl(@This(), "public"));
    try testing.expect(@hasDecl(@This(), "official"));
    try testing.expect(@hasDecl(@This(), "types"));
    try testing.expect(!@hasDecl(@This(), "engine"));
    try testing.expect(!@hasDecl(@This(), "runtime"));
    try testing.expect(!@hasDecl(@This(), "storage"));
    try testing.expect(!@hasDecl(@This(), "internal"));
}
