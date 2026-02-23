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
