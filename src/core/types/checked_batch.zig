//! Public checked-batch contract types for guarded atomic writes.
//! Cost: O(1) type metadata only.
//! Allocator: Does not allocate.

const PutWrite = @import("batch.zig").PutWrite;
const Value = @import("value.zig").Value;

/// One physical guard evaluated before a checked batch becomes visible.
///
/// Ownership:
/// - All key slices are borrowed for the duration of the call that consumes the enclosing checked batch.
/// - `key_value_equals.value` is borrowed for the duration of the call that consumes the enclosing checked batch.
/// - Callers must keep borrowed slices and pointed values valid and immutable until the consuming checked-batch call returns.
pub const CheckedBatchGuard = union(enum) {
    key_exists: []const u8,
    key_not_exists: []const u8,
    key_value_equals: struct {
        key: []const u8,
        value: *const Value,
    },
};

/// Public checked-batch request with ordered writes plus physical guards.
///
/// Ownership:
/// - `writes` and `guards` are borrowed for the duration of the consuming checked-batch call.
/// - Nested `PutWrite` values and `CheckedBatchGuard.key_value_equals` values remain borrowed and must stay valid and immutable until that call returns.
pub const CheckedBatch = struct {
    writes: []const PutWrite,
    guards: []const CheckedBatchGuard = &.{},
};
