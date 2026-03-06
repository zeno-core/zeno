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

test "core facade excludes logical collection contract types" {
    const testing = std.testing;

    try testing.expect(!@hasDecl(types, "HashWrite"));
    try testing.expect(!@hasDecl(types, "HashGetAllResult"));
    try testing.expect(!@hasDecl(types, "SetMembersResult"));
    try testing.expect(!@hasDecl(types, "ListRangeResult"));
    try testing.expect(!@hasDecl(types, "ZSetRangeResult"));
    try testing.expect(!@hasDecl(types, "LogicalScanPageResult"));
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
