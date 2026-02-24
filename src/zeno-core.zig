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
