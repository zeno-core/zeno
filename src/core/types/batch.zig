//! Public request types for zeno-core batch operations.
//! Cost: O(1) type metadata only.
//! Allocator: Does not allocate.

const Value = @import("value.zig").Value;

/// One plain key/value write request for `apply_batch`.
///
/// Ownership:
/// - `key` is borrowed for the duration of the call that consumes this write.
/// - `value` is borrowed for the duration of the call that consumes this write.
/// - Callers must keep both slices and pointed values valid and immutable until the consuming batch call returns.
pub const PutWrite = struct {
    key: []const u8,
    value: *const Value,
};
