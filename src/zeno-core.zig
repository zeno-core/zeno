//! Stable facade root for the zeno-core package skeleton.
//! Cost: O(1) module reexports only.
//! Allocator: Does not allocate.

/// General public engine contract facade.
pub const public = @import("core/public.zig");

/// Official advanced engine contract facade.
pub const official = @import("core/official.zig");

/// Public access point for core-facing types.
pub const types = @import("core/types.zig");

test "step 1 facade exposes the migration skeleton" {
    _ = public;
    _ = official;
    _ = types;
}

test "core facade excludes logical collection contract types" {
    const testing = @import("std").testing;

    try testing.expect(!@hasDecl(types, "HashWrite"));
    try testing.expect(!@hasDecl(types, "HashGetAllResult"));
    try testing.expect(!@hasDecl(types, "SetMembersResult"));
    try testing.expect(!@hasDecl(types, "ListRangeResult"));
    try testing.expect(!@hasDecl(types, "ZSetRangeResult"));
    try testing.expect(!@hasDecl(types, "LogicalScanPageResult"));
}

test "package root exposes only the contract facades" {
    const testing = @import("std").testing;

    try testing.expect(@hasDecl(@This(), "public"));
    try testing.expect(@hasDecl(@This(), "official"));
    try testing.expect(@hasDecl(@This(), "types"));
    try testing.expect(!@hasDecl(@This(), "engine"));
    try testing.expect(!@hasDecl(@This(), "runtime"));
    try testing.expect(!@hasDecl(@This(), "storage"));
    try testing.expect(!@hasDecl(@This(), "internal"));
}
