//! Engine coordination center for the zeno-core facade.
//! Cost: O(1) dispatch plus downstream runtime and storage work.
//! Allocator: Uses explicit allocators to own the engine handle, runtime state, and caller-visible cloned values.

const std = @import("std");
const lifecycle = @import("lifecycle.zig");
const read = @import("read.zig");
const runtime_state = @import("../runtime/state.zig");
const types = @import("../types.zig");
const write = @import("write.zig");

/// Shared error set for engine contract operations.
pub const EngineError = error{
    NotImplemented,
    OutOfMemory,
    KeyTooLarge,
};

/// Central engine handle coordinated by the future engine layer.
pub const Database = struct {
    allocator: std.mem.Allocator,
    state: runtime_state.DatabaseState,

    /// Flushes and closes engine-owned resources.
    ///
    /// Time Complexity: O(s), where `s` is the runtime shard count.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Not thread-safe; caller must ensure exclusive ownership of the engine handle.
    pub fn close(self: *Database) void {
        lifecycle.close(self);
    }

    /// Writes a consistent checkpoint of engine-owned state.
    ///
    /// Time Complexity: O(1) until checkpoint persistence is implemented.
    ///
    /// Allocator: Does not allocate; returns `error.NotImplemented` until checkpoint persistence is implemented.
    pub fn checkpoint(self: *Database) EngineError!void {
        return lifecycle.checkpoint(self);
    }

    /// Reads one key from the engine contract surface.
    ///
    /// Time Complexity: O(n^2 + k + v), where `n` is `key.len` for shard routing, `k` is hash-map lookup work, and `v` is cloned value size when the key exists.
    ///
    /// Allocator: Allocates the returned cloned value through `allocator` when the key exists.
    ///
    /// Ownership: Returns a caller-owned cloned value when non-null. The caller must later call `deinit` with `allocator`.
    pub fn get(self: *const Database, allocator: std.mem.Allocator, key: []const u8) EngineError!?types.Value {
        return read.get(&self.state, allocator, key);
    }

    /// Writes one plain key/value pair through the engine contract surface.
    ///
    /// Time Complexity: O(n^2 + k + v), where `n` is `key.len` for shard routing, `k` is hash-map lookup or insert work, and `v` is cloned value size.
    ///
    /// Allocator: Clones owned key and value storage through the engine base allocator.
    ///
    /// Ownership: Clones `value` into engine-owned storage before the call returns.
    ///
    /// Thread Safety: Safe for concurrent use with other point operations; acquires the global visibility gate exclusively before taking one shard-exclusive lock.
    pub fn put(self: *Database, key: []const u8, value: *const types.Value) EngineError!void {
        return write.put(&self.state, key, value);
    }

    /// Deletes one plain key from the engine contract surface.
    ///
    /// Time Complexity: O(n^2 + k), where `n` is `key.len` for shard routing and `k` is hash-map lookup and removal work.
    ///
    /// Allocator: Does not allocate; frees engine-owned key and value storage when the key exists.
    ///
    /// Thread Safety: Safe for concurrent use with other point operations; acquires the global visibility gate exclusively before taking one shard-exclusive lock.
    pub fn delete(self: *Database, key: []const u8) bool {
        return write.delete(&self.state, key);
    }

    /// Sets or clears key expiration at an absolute unix-second timestamp.
    ///
    /// Time Complexity: O(1) until expiration semantics are implemented.
    ///
    /// Allocator: Does not allocate; returns `error.NotImplemented` until expiration semantics are implemented.
    pub fn expire_at(self: *Database, key: []const u8, unix_seconds: ?i64) EngineError!bool {
        _ = self;
        _ = key;
        _ = unix_seconds;
        return error.NotImplemented;
    }

    /// Returns Redis-style TTL for one plain key.
    ///
    /// Time Complexity: O(1) until expiration semantics are implemented.
    ///
    /// Allocator: Does not allocate; returns `error.NotImplemented` until expiration semantics are implemented.
    pub fn ttl(self: *const Database, key: []const u8) EngineError!i64 {
        _ = self;
        _ = key;
        return error.NotImplemented;
    }

    /// Performs a full prefix scan over the current visible state.
    ///
    /// Time Complexity: O(1) until scan behavior is implemented.
    ///
    /// Allocator: Does not allocate; returns `error.NotImplemented` until scan behavior is implemented.
    ///
    /// Ownership: Does not return a result until scan behavior is implemented.
    pub fn scan_prefix(
        self: *const Database,
        allocator: std.mem.Allocator,
        prefix: []const u8,
    ) EngineError!types.ScanResult {
        _ = self;
        _ = allocator;
        _ = prefix;
        return error.NotImplemented;
    }

    /// Performs a full range scan over the current visible state.
    ///
    /// Time Complexity: O(1) until scan behavior is implemented.
    ///
    /// Allocator: Does not allocate; returns `error.NotImplemented` until scan behavior is implemented.
    ///
    /// Ownership: Does not return a result until scan behavior is implemented.
    pub fn scan_range(
        self: *const Database,
        allocator: std.mem.Allocator,
        range: types.KeyRange,
    ) EngineError!types.ScanResult {
        _ = self;
        _ = allocator;
        _ = range;
        return error.NotImplemented;
    }

    /// Applies one plain atomic batch.
    ///
    /// Time Complexity: O(1) until batch execution is implemented.
    ///
    /// Allocator: Does not allocate; returns `error.NotImplemented` until batch execution is implemented.
    pub fn apply_batch(self: *Database, writes: []const types.PutWrite) EngineError!void {
        _ = self;
        _ = writes;
        return error.NotImplemented;
    }

    /// Opens one consistent read view.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Returns a `ReadView` that borrows the engine runtime state until `deinit` is called.
    ///
    /// Thread Safety: Acquires the shared side of the global visibility gate and keeps it held for the lifetime of the returned `ReadView`.
    pub fn read_view(self: *Database) EngineError!types.ReadView {
        return read.read_view(&self.state);
    }
};

/// Creates an in-memory engine handle.
///
/// Time Complexity: O(s), where `s` is the runtime shard count.
///
/// Allocator: Allocates the engine handle and runtime state from `allocator`.
pub fn create(allocator: std.mem.Allocator) EngineError!*Database {
    return lifecycle.create(allocator);
}

/// Opens an engine handle from the provided runtime options.
///
/// Time Complexity: O(s), where `s` is the runtime shard count, when persistence is not requested.
///
/// Allocator: Allocates the engine handle from `allocator` when persistence is not requested.
pub fn open(allocator: std.mem.Allocator, options: types.DatabaseOptions) EngineError!*Database {
    return lifecycle.open(allocator, options);
}

test "create initializes runtime-owned database state" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close();

    try testing.expectEqual(@as(usize, runtime_state.NUM_SHARDS), db.state.shards.len);
    try testing.expect(db.state.snapshot_path == null);
}

test "plain point operations store clone and delete values" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close();

    const original = types.Value{ .string = "hello" };
    try db.put("alpha", &original);

    {
        var first_read = (try db.get(testing.allocator, "alpha")).?;
        defer first_read.deinit(testing.allocator);

        try testing.expectEqualStrings("hello", first_read.string);
    }

    var second_read = (try db.get(testing.allocator, "alpha")).?;
    defer second_read.deinit(testing.allocator);

    try testing.expectEqualStrings("hello", second_read.string);

    try testing.expect(db.delete("alpha"));
    try testing.expect(!db.delete("alpha"));
    try testing.expect((try db.get(testing.allocator, "alpha")) == null);
}

test "put overwrites existing plain value" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close();

    const first = types.Value{ .integer = 7 };
    try db.put("counter", &first);

    const second = types.Value{ .string = "updated" };
    try db.put("counter", &second);

    var stored = (try db.get(testing.allocator, "counter")).?;
    defer stored.deinit(testing.allocator);

    try testing.expectEqualStrings("updated", stored.string);
}

test "read view holds the visibility gate until released" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close();

    var view = try db.read_view();
    defer if (view.active) view.deinit();

    try testing.expect(!db.state.visibility_gate.try_lock_exclusive());

    view.deinit();
    try testing.expect(db.state.visibility_gate.try_lock_exclusive());
    db.state.visibility_gate.unlock_exclusive();
}

/// Scans the next prefix page inside a consistent read view.
///
/// Time Complexity: O(1) until scan behavior is implemented.
///
/// Allocator: Does not allocate; returns `error.NotImplemented` until scan behavior is implemented.
///
/// Ownership: `cursor` is borrowed when present and is never consumed by this call.
pub fn scan_prefix_from_in_view(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) EngineError!types.ScanPageResult {
    _ = view;
    _ = allocator;
    _ = prefix;
    _ = cursor;
    _ = limit;
    return error.NotImplemented;
}

/// Scans the next range page inside a consistent read view.
///
/// Time Complexity: O(1) until scan behavior is implemented.
///
/// Allocator: Does not allocate; returns `error.NotImplemented` until scan behavior is implemented.
///
/// Ownership: `cursor` is borrowed when present and is never consumed by this call.
pub fn scan_range_from_in_view(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    range: types.KeyRange,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) EngineError!types.ScanPageResult {
    _ = view;
    _ = allocator;
    _ = range;
    _ = cursor;
    _ = limit;
    return error.NotImplemented;
}

/// Applies one checked batch under the official advanced contract.
///
/// Time Complexity: O(1) until checked-batch execution is implemented.
///
/// Allocator: Does not allocate; returns `error.NotImplemented` until checked-batch execution is implemented.
pub fn apply_checked_batch(db: *Database, batch: types.CheckedBatch) EngineError!void {
    _ = db;
    _ = batch;
    return error.NotImplemented;
}
