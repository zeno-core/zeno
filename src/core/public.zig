//! Thin public facade for the zeno-core general engine contract.
//! Cost: O(1) facade delegation plus downstream engine work.
//! Allocator: Delegates allocation behavior to engine entry points.

const std = @import("std");
const engine_db = @import("engine/db.zig");
const types = @import("types.zig");

/// Public database handle for the general engine contract.
pub const Database = engine_db.Database;

/// Public error set used by the engine contract.
pub const Error = engine_db.EngineError;

/// Creates an in-memory engine handle.
///
/// Time Complexity: O(s), where `s` is the runtime shard count.
///
/// Allocator: Allocates the engine handle and runtime state from `allocator`.
pub fn create(allocator: std.mem.Allocator) Error!*Database {
    return engine_db.create(allocator);
}

/// Opens an engine handle from the provided runtime options.
///
/// Time Complexity: O(s), where `s` is the runtime shard count, when persistence is not requested.
///
/// Allocator: Allocates the engine handle and runtime state from `allocator` when persistence is not requested.
pub fn open(allocator: std.mem.Allocator, options: types.DatabaseOptions) Error!*Database {
    return engine_db.open(allocator, options);
}
