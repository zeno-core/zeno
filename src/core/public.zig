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
///
/// Ownership: Borrows `io`; its provider must outlive the database, read views, and background workers.
pub fn create(io: std.Io, allocator: std.mem.Allocator) Error!*Database {
    return engine_db.create(io, allocator);
}

/// Opens an engine handle from the provided runtime options.
///
/// Time Complexity: O(s + n + r + e), where `s` is the runtime shard count, `n` is snapshot load work, `r` is replayed WAL work, and `e` is post-recovery expired-key purge work when persistence is configured.
///
/// Allocator: Allocates the engine handle and runtime state from `allocator` and uses explicit allocator paths for snapshot load and WAL replay scratch when persistence is configured.
///
/// Ownership: Borrows `io`; its provider must outlive the database, read views, and background workers.
pub fn open(io: std.Io, allocator: std.mem.Allocator, options: types.DatabaseOptions) Error!*Database {
    return engine_db.open(io, allocator, options);
}
