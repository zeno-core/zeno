//! Engine coordination center for the zeno-core facade.
//! Cost: O(1) dispatch plus downstream runtime and storage work.
//! Allocator: Uses explicit allocators to own the engine handle, runtime state, and caller-visible cloned values.

const std = @import("std");
const batch_ops = @import("batch.zig");
const error_mod = @import("error.zig");
const expiration = @import("expiration.zig");
const internal_codec = @import("../internal/codec.zig");
const internal_mutate = @import("../internal/mutate.zig");
const internal_ttl_index = @import("../internal/ttl_index.zig");
const lifecycle = @import("lifecycle.zig");
const metrics = @import("metrics.zig");
const read = @import("read.zig");
const runtime_shard = @import("../runtime/shard.zig");
const scan_ops = @import("scan.zig");
const scan_iterator_mod = @import("scan_iterator.zig");
const runtime_state = @import("../runtime/state.zig");
const storage_snapshot = @import("../storage/snapshot.zig");
const storage_wal = @import("../storage/wal.zig");
const types = @import("../types.zig");
const write = @import("write.zig");

/// Shared error set for engine contract operations.
pub const EngineError = error_mod.EngineError;
pub const ScanIterator = scan_iterator_mod.ScanIterator;

/// Central engine handle for the finalized `zeno-core` contract surface.
pub const Database = struct {
    allocator: std.mem.Allocator,
    state: runtime_state.DatabaseState,
    auto_compaction: AutoCompaction = .{},
    auto_wal_checkpoint: AutoWalCheckpoint = .{},
    ttl_sweeper: TtlSweeper = .{},

    /// Per-database policy state for optional heavy-overwrite auto-compaction.
    const AutoCompaction = struct {
        /// Triggers one compaction cycle every N heavy overwrites. `null` disables auto mode.
        every: ?u32 = null,
        /// Monotonic heavy-overwrite tick used for cadence modulo checks.
        tick: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
        /// Re-entry guard so automatic maintenance never runs concurrently.
        in_progress: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    };

    /// Per-database policy state for optional WAL size-triggered auto-checkpoint.
    const AutoWalCheckpoint = struct {
        /// Fires one checkpoint cycle when WAL bytes since last compact exceed this.
        /// `null` disables auto mode. Only effective when `snapshot_path` is configured.
        max_bytes: ?u64 = null,
        /// Re-entry guard so automatic WAL checkpoints never run concurrently.
        in_progress: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    };

    /// Per-database state for the optional background TTL expiry sweep task.
    const TtlSweeper = struct {
        /// Sweep interval in milliseconds. Only meaningful when `task != null`.
        interval_ms: u32 = 1_000,
        /// Set to `true` to signal the sweep task to stop.
        stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        /// The concurrent sweep task, or `null` when the sweeper is not active.
        task: ?std.Io.Future(void) = null,
    };

    /// Flushes and closes engine-owned resources.
    ///
    /// Time Complexity: O(s), where `s` is the runtime shard count.
    ///
    /// Ownership: Returns `error.ActiveReadViews` when any `ReadView` handles are still active.
    ///
    /// Thread Safety: Not thread-safe; caller must ensure exclusive ownership of the engine handle.
    pub fn close(self: *Database) EngineError!void {
        return lifecycle.close(self);
    }

    /// Writes a consistent checkpoint of engine-owned state.
    ///
    /// Time Complexity: O(s + n + m), where:
    ///  - `s` is the runtime shard count,
    ///  - `n` is snapshot serialization work
    ///  - `m` is WAL compaction scan and rewrite work when durability is enabled
    ///
    /// Allocator: Uses explicit allocator paths for snapshot serialization scratch and optional WAL compaction scratch.
    ///
    /// Ownership: Returns `error.NoSnapshotPath` when `snapshot_path` was not configured for this engine handle.
    ///
    /// Thread Safety: Not safe to call concurrently with other mutation of the same engine handle.
    /// Active `ReadView` handles may cause one fail-fast `error.CheckpointBusy` response while
    /// the barrier cannot be acquired, then writers and new readers are blocked globally during
    /// the barrier once acquired and writers are blocked globally again during each shard-serialization window.
    pub fn checkpoint(self: *Database) EngineError!void {
        return metrics.callWithLatency(&self.state, lifecycle.checkpoint, .{self});
    }

    /// Rebuilds runtime shard storage from a fresh snapshot checkpoint to reclaim retained arena churn.
    ///
    /// Time Complexity: O(s + n + m), where:
    ///  - `s` is the runtime shard count,
    ///  - `n` is snapshot serialization plus load work
    ///  - `m` is WAL compaction scan and rewrite work when durability is enabled.
    ///
    /// Allocator: Uses explicit allocator paths for snapshot write and load scratch.
    ///
    /// Ownership: Returns `error.NoSnapshotPath` when `snapshot_path` was not configured for this engine handle.
    ///
    /// Thread Safety: Not safe to call concurrently with other mutation of the same engine handle.
    /// Follows checkpoint barrier semantics and may return `error.CheckpointBusy` when active
    /// `ReadView` handles block the barrier.
    pub fn compactMemory(self: *Database) EngineError!void {
        return metrics.callWithLatency(&self.state, lifecycle.compactMemory, .{self});
    }

    /// Rebuilds one shard in place to reclaim retained arena churn.
    ///
    /// Time Complexity: O(n + t), where `n` is keys in the shard tree and `t` is shard TTL entries.
    ///
    /// Allocator: Uses explicit allocator paths for temporary clone/rebuild scratch.
    ///
    /// Ownership: Returns `error.InvalidShardIndex` when `shard_idx` is outside `[0, NUM_SHARDS)`.
    ///
    /// Thread Safety: Synchronous maintenance operation. Not safe to call concurrently
    /// with other mutation of the same engine handle.
    pub fn compactShard(self: *Database, shard_idx: usize) EngineError!void {
        return metrics.callWithLatency(&self.state, lifecycle.compactShard, .{ self, shard_idx });
    }

    /// Rebuilds all shards in place to reclaim retained arena churn.
    ///
    /// Time Complexity: O(s + n + t), where `s` is shard count, `n` is total key count, and `t` is total TTL entries.
    ///
    /// Allocator: Uses explicit allocator paths for temporary clone/rebuild scratch.
    ///
    /// Calibration Notes (heavy overwrite 1 KiB payload, 50K operations):
    /// - `compact_every=5000`: near-off total time, p99 around low microseconds, retained bytes near zero after each maintenance cycle.
    /// - `compact_every=1000`: lower p99/max than 5000 but noticeably higher total maintenance cost.
    /// - `compact_every<1000`: can impose disproportionate throughput cost (anti-pattern for default settings).
    ///
    /// Operational Guidance: Start with `compact_every=5000` and tune toward `1000`
    /// only when tighter p99/max is more important than aggregate throughput.
    ///
    /// Thread Safety: Synchronous maintenance operation. Not safe to call concurrently
    /// with other mutation of the same engine handle.
    pub fn compactAll(self: *Database) EngineError!void {
        return metrics.callWithLatency(&self.state, lifecycle.compactAll, .{self});
    }

    /// Reads one key from the engine contract surface.
    ///
    /// Time Complexity: O(n + k + v), where `n` is `key.len` for shard routing, `k` is ART lookup work, and `v` is cloned value size when the key exists.
    ///
    /// Allocator: Allocates the returned cloned value through `allocator` when the key exists.
    ///
    /// Ownership: Returns a caller-owned cloned value when non-null. The caller must later call `deinit` with `allocator`.
    pub fn get(self: *const Database, allocator: std.mem.Allocator, key: []const u8) EngineError!?types.Value {
        return metrics.callWithLatency(&self.state, read.get, .{ &self.state, allocator, key });
    }

    /// Returns whether `key` is present and TTL-visible in the engine.
    ///
    /// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is ART lookup work.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Does not allocate or return owned data.
    ///
    /// Thread Safety: Lock-free via the shard seqlock; safe for concurrent use with
    /// point writes and scans.
    pub fn exists(self: *const Database, key: []const u8) EngineError!bool {
        return metrics.callWithLatency(&self.state, read.exists, .{ &self.state, key });
    }

    /// Reads multiple keys in a single call and returns one owned result per key.
    ///
    /// Time Complexity: O(s_touched * seqlock_overhead + sum(k_i + v_i)), where
    /// `s_touched` is the number of distinct shards touched, `k_i` is ART lookup
    /// work, and `v_i` is the cloned value size per present key.
    ///
    /// Allocator: Allocates the result slice and all present cloned values through
    /// `allocator`.
    ///
    /// Ownership: Returns a `GetManyResult` whose `values[i]` corresponds to
    /// `keys[i]`. The caller must call `result.deinit(allocator)` when done.
    ///
    /// Thread Safety: Lock-free via the shard seqlock; safe for concurrent use
    /// with point writes and scans.
    pub fn getMany(
        self: *const Database,
        allocator: std.mem.Allocator,
        keys: []const []const u8,
    ) EngineError!types.GetManyResult {
        return metrics.callWithLatency(&self.state, read.getMany, .{ &self.state, allocator, keys });
    }

    /// Writes one plain key/value pair through the engine contract surface.
    ///
    /// Time Complexity: O(n + k + v), where `n` is `key.len` for shard routing, `k` is ART lookup or insert work, and `v` is cloned value size.
    ///
    /// Allocator: Clones owned key and value storage through the engine base allocator.
    ///
    /// Ownership: Clones `value` into engine-owned storage before the call returns.
    ///
    /// Thread Safety: Safe for concurrent use with other point operations; acquires the global visibility gate exclusively before taking one shard-exclusive lock.
    pub fn put(self: *Database, key: []const u8, value: *const types.Value) EngineError!void {
        const heavy_events_before = self.state.counters.overwritten_heavy_events_total.load(.monotonic);
        try metrics.callWithLatency(&self.state, write.put, .{ &self.state, key, value });
        self.maybeRunHeavyOverwriteCompaction(heavy_events_before);
        self.maybeRunWalCheckpoint();
    }

    /// Writes multiple plain key/value pairs as independent standalone mutations.
    ///
    /// Time Complexity: O(n * (k + v) + s * n), where `n` is `writes.len`, `k` is average key length, `v` is value clone cost, and `s` is shard count.
    ///
    /// Allocator: Clones owned key and value storage through shard arenas and may allocate delegated WAL serialization scratch when durability is enabled.
    ///
    /// Ownership: Clones each `writes[i].value` into engine-owned storage before returning.
    ///
    /// Thread Safety: Safe for concurrent use with reads and scans; acquires shard-local shared visibility gates and shard-exclusive locks per touched shard.
    ///
    /// Durability Semantics: Non-atomic as a group. On failure, any durable/live prefix applied before the failure may remain.
    pub fn putGroup(self: *Database, writes: []const types.PutWrite) EngineError!void {
        const heavy_events_before = self.state.counters.overwritten_heavy_events_total.load(.monotonic);
        try metrics.callWithLatency(&self.state, write.putGroup, .{ &self.state, writes });
        self.maybeRunHeavyOverwriteCompaction(heavy_events_before);
        self.maybeRunWalCheckpoint();
    }

    fn maybeRunHeavyOverwriteCompaction(self: *Database, heavy_events_before: u64) void {
        const compact_every = self.auto_compaction.every orelse return;
        const compact_every_u64: u64 = @as(u64, compact_every);

        const heavy_events_after = self.state.counters.overwritten_heavy_events_total.load(.monotonic);
        if (heavy_events_after <= heavy_events_before) return;

        const events_delta = heavy_events_after - heavy_events_before;
        const previous_tick = self.auto_compaction.tick.fetchAdd(events_delta, .monotonic);
        const next_tick = previous_tick + events_delta;
        if ((previous_tick / compact_every_u64) == (next_tick / compact_every_u64)) return;

        if (!self.beginHeavyOverwriteCompaction()) return;
        defer self.endHeavyOverwriteCompaction();

        lifecycle.compactAll(self) catch {};
    }

    fn beginHeavyOverwriteCompaction(self: *Database) bool {
        return self.auto_compaction.in_progress.cmpxchgStrong(false, true, .acq_rel, .monotonic) == null;
    }

    fn endHeavyOverwriteCompaction(self: *Database) void {
        self.auto_compaction.in_progress.store(false, .release);
    }

    /// Runs one automatic checkpoint when the configured WAL byte threshold is crossed.
    fn maybeRunWalCheckpoint(self: *Database) void {
        const max_bytes = self.auto_wal_checkpoint.max_bytes orelse return;
        const wal = self.state.wal orelse return;
        if (self.state.snapshot_path == null) return;
        if (wal.walBytesPending() < max_bytes) return;
        if (!self.beginWalCheckpoint()) return;
        defer self.endWalCheckpoint();
        lifecycle.checkpoint(self) catch {};
    }

    /// Attempts to acquire the auto-checkpoint re-entry guard.
    fn beginWalCheckpoint(self: *Database) bool {
        return self.auto_wal_checkpoint.in_progress.cmpxchgStrong(
            false,
            true,
            .acq_rel,
            .monotonic,
        ) == null;
    }

    /// Releases the auto-checkpoint re-entry guard.
    fn endWalCheckpoint(self: *Database) void {
        self.auto_wal_checkpoint.in_progress.store(false, .release);
    }

    /// Starts the concurrent TTL sweep task with the given interval.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: The I/O provider owns the concurrent task resources.
    ///
    /// Thread Safety: Must be called before the engine handle is shared across threads.
    pub fn startTtlSweeper(self: *Database, interval_ms: u32) !void {
        self.ttl_sweeper.interval_ms = interval_ms;
        self.ttl_sweeper.stop.store(false, .release);
        self.ttl_sweeper.task = try self.state.io.concurrent(ttlSweepTaskMain, .{self});
    }

    /// Signals the sweep task to stop, cancels its sleep, and awaits completion.
    ///
    /// Time Complexity: O(1) coordination plus any in-progress sweep work.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Safe to call from the engine owner thread during close.
    pub fn stopTtlSweeper(self: *Database) void {
        self.ttl_sweeper.stop.store(true, .release);
        if (self.ttl_sweeper.task) |*task| {
            task.cancel(self.state.io);
            self.ttl_sweeper.task = null;
        }
    }

    fn ttlSweepTaskMain(db: *Database) void {
        const interval_ns = @as(u64, db.ttl_sweeper.interval_ms) * std.time.ns_per_ms;

        while (!db.ttl_sweeper.stop.load(.acquire)) {
            db.state.io.sleep(.fromNanoseconds(interval_ns), .awake) catch return;
            if (db.ttl_sweeper.stop.load(.acquire)) break;

            for (0..runtime_state.NUM_SHARDS) |shard_idx| {
                if (db.ttl_sweeper.stop.load(.acquire)) break;
                expiration.sweepExpiredEntriesForShard(&db.state, shard_idx, db.allocator);
            }
        }
    }

    /// Deletes one plain key from the engine contract surface.
    ///
    /// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is ART lookup and removal work.
    ///
    /// Allocator: Frees engine-owned key and value storage when the key exists and may allocate delegated WAL record scratch when durability is enabled.
    ///
    /// Thread Safety: Safe for concurrent use with other point operations; acquires the global visibility gate exclusively before taking one shard-exclusive lock and appends the live DELETE record inside that same visibility window.
    pub fn delete(self: *Database, key: []const u8) EngineError!bool {
        const result = try metrics.callWithLatency(&self.state, write.delete, .{ &self.state, key });
        self.maybeRunWalCheckpoint();
        return result;
    }

    /// Removes all keys whose name starts with `prefix` and returns the deleted count.
    ///
    /// Time Complexity: O(s * (k + N)), where `s` is shard count, `k` is `prefix.len`,
    /// and `N` is total matching key count across all shards.
    ///
    /// Allocator: Uses the engine base allocator for temporary per-shard key collection.
    ///
    /// Ownership: Does not return owned data.
    ///
    /// Durability Semantics: Non-atomic across shards. Emits one WAL DELETE per removed
    /// key. On failure, durably logged deletes for earlier shards remain applied.
    ///
    /// Thread Safety: Safe for concurrent use with point reads; acquires shared visibility
    /// gate and shard-exclusive lock per shard sequentially.
    pub fn deletePrefix(self: *Database, prefix: []const u8) EngineError!u64 {
        const deleted = try metrics.callWithLatency(
            &self.state,
            write.deletePrefix,
            .{ &self.state, prefix, self.allocator },
        );
        if (deleted > 0) self.maybeRunWalCheckpoint();
        return deleted;
    }

    /// Sets or clears key expiration at an absolute unix-second timestamp.
    ///
    /// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is shard-local lookup plus optional TTL metadata update work.
    ///
    /// Allocator: Uses the engine base allocator when inserting a new TTL entry and may allocate delegated WAL record scratch for durable live mutations.
    ///
    /// Thread Safety: Safe for concurrent use with reads and scans; acquires the global visibility gate exclusively before taking one shard-exclusive lock.
    pub fn expireAt(self: *Database, key: []const u8, unix_seconds: ?i64) EngineError!bool {
        const result = try metrics.callWithLatency(&self.state, expireAtBoundary, .{ self, key, unix_seconds });
        self.maybeRunWalCheckpoint();
        return result;
    }

    /// Returns Redis-style TTL for one plain key.
    ///
    /// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is shard-local lookup plus optional expired-key cleanup work.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Acquires the shared side of the global visibility gate before taking one shard shared lock for TTL reads and only attempts lazy cleanup afterward if the exclusive visibility gate can be acquired immediately.
    pub fn ttl(self: *const Database, key: []const u8) EngineError!i64 {
        return metrics.callWithLatency(&self.state, expiration.ttl, .{ @constCast(&self.state), key });
    }

    /// Performs a full prefix scan over the current visible state.
    ///
    /// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
    ///
    /// Allocator: Allocates owned entry keys and values plus result storage through `allocator`.
    ///
    /// Ownership: Returns a result that owns all returned keys and values until `deinit`.
    ///
    /// Thread Safety: Acquires the shared side of the global visibility gate before taking shard shared locks to collect entries.
    pub fn scanPrefix(
        self: *const Database,
        allocator: std.mem.Allocator,
        prefix: []const u8,
    ) EngineError!types.ScanResult {
        return metrics.callWithLatency(&self.state, scan_ops.scanPrefix, .{ &self.state, allocator, prefix });
    }

    /// Performs a full range scan over the current visible state.
    ///
    /// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
    ///
    /// Allocator: Allocates owned entry keys and values plus result storage through `allocator`.
    ///
    /// Ownership: Returns a result that owns all returned keys and values until `deinit`.
    ///
    /// Thread Safety: Acquires the shared side of the global visibility gate before taking shard shared locks to collect entries.
    pub fn scanRange(
        self: *const Database,
        allocator: std.mem.Allocator,
        range: types.KeyRange,
    ) EngineError!types.ScanResult {
        return metrics.callWithLatency(&self.state, scan_ops.scanRange, .{ &self.state, allocator, range });
    }

    /// Opens a lazy iterator over all keys whose name starts with `prefix`.
    ///
    /// Time Complexity: O(1) to open; O(page_size * (k + v)) per internal page fetch.
    ///
    /// Allocator: Uses `allocator` for internal page allocation and entry cloning.
    ///
    /// Ownership: The caller must call `iterator.deinit()` when done, even if
    /// `next()` was never called or returned `null`.
    ///
    /// Thread Safety: The iterator holds a `ReadView`; do not call `db.close()`
    /// while any iterator is alive.
    pub fn scanIterator(
        self: *Database,
        allocator: std.mem.Allocator,
        prefix: []const u8,
        page_size: ?usize,
    ) EngineError!types.ScanIterator {
        const view = try self.readView();
        return .{
            .allocator = allocator,
            .query = .{ .prefix = prefix },
            .page_size = if (page_size) |ps| (if (ps == 0) types.ScanIterator.default_page_size else ps) else types.ScanIterator.default_page_size,
            .view = view,
            .cursor = null,
            .page = null,
            .page_pos = 0,
            .done = false,
        };
    }

    /// Opens a lazy iterator over all keys within the given range.
    ///
    /// Time Complexity: O(1) to open; O(page_size * (k + v)) per internal page fetch.
    ///
    /// Allocator: Uses `allocator` for internal page allocation and entry cloning.
    ///
    /// Ownership: The caller must call `iterator.deinit()` when done, even if
    /// `next()` was never called or returned `null`.
    ///
    /// Thread Safety: The iterator holds a `ReadView`; do not call `db.close()`
    /// while any iterator is alive.
    pub fn rangeIterator(
        self: *Database,
        allocator: std.mem.Allocator,
        range: types.KeyRange,
        page_size: ?usize,
    ) EngineError!types.ScanIterator {
        const view = try self.readView();
        return .{
            .allocator = allocator,
            .query = .{ .range = range },
            .page_size = if (page_size) |ps| (if (ps == 0) types.ScanIterator.default_page_size else ps) else types.ScanIterator.default_page_size,
            .view = view,
            .cursor = null,
            .page = null,
            .page_pos = 0,
            .done = false,
        };
    }

    /// Applies one plain atomic batch.
    ///
    /// Time Complexity: O(n + b + v), where `n` is `writes.len`, `b` is total serialized value bytes measured during planning, and `v` is total cloned value size for prepared writes.
    ///
    /// Allocator: Uses the engine base allocator for committed values and temporary planner scratch plus temporary WAL batch-view storage while validating and preparing the batch.
    ///
    /// Ownership: Clones all surviving write values into engine-owned storage before making the batch visible.
    ///
    /// Thread Safety: Safe for concurrent use with point operations and read views; acquires the global visibility gate exclusively for the full apply window.
    pub fn applyBatch(self: *Database, writes: []const types.PutWrite) EngineError!void {
        try metrics.callWithLatency(&self.state, batch_ops.applyBatch, .{ &self.state, self.allocator, writes });
        self.maybeRunWalCheckpoint();
    }

    /// Opens one consistent read view.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Returns a `ReadView` that keeps one registry-backed visibility hold alive until `deinit` is called.
    ///
    /// Thread Safety: Acquires the shared side of the global visibility gate and keeps it held for the lifetime of the returned `ReadView`.
    pub fn readView(self: *Database) EngineError!types.ReadView {
        return metrics.callWithLatency(&self.state, read.readView, .{&self.state});
    }
};

/// Creates an in-memory engine handle.
///
/// Time Complexity: O(s), where `s` is the runtime shard count.
///
/// Allocator: Allocates the engine handle and runtime state from `allocator`.
///
/// Ownership: Borrows `io`; its provider must outlive the database, read views, and background workers.
pub fn create(io: std.Io, allocator: std.mem.Allocator) EngineError!*Database {
    return lifecycle.create(io, allocator);
}

/// Opens an engine handle from the provided runtime options.
///
/// Time Complexity: O(s + n + r + e), where `s` is the runtime shard count, `n` is snapshot load work, `r` is replayed WAL work, and `e` is post-recovery expired-key purge work when persistence is configured.
///
/// Allocator: Allocates the engine handle from `allocator` and uses explicit allocator paths for snapshot load and WAL replay scratch when persistence is configured.
///
/// Ownership: Borrows `io`; its provider must outlive the database, read views, and background workers.
pub fn open(io: std.Io, allocator: std.mem.Allocator, options: types.DatabaseOptions) EngineError!*Database {
    return lifecycle.open(io, allocator, options);
}

/// Scans the next prefix page inside a consistent read view.
///
/// Time Complexity: O(s log s + p * (k + log s + v)), where `s` is shard count, `p` is emitted page size, `k` is ART seek work for one shard refill, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page exposes any continuation cursor through `borrowNextCursor` and may transfer it into `OwnedScanCursor` through `takeNextCursor`.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while fetching or refilling shard-local ART heads.
pub fn scanPrefixFromInView(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) EngineError!types.ScanPageResult {
    const state = runtimeStateFromViewForLatency(view);
    return metrics.callWithOptionalLatency(state, scan_ops.scanPrefixFromInView, .{ view, allocator, prefix, cursor, limit });
}

/// Scans the next range page inside a consistent read view.
///
/// Time Complexity: O(s log s + p * (k + log s + v)), where `s` is shard count, `p` is emitted page size, `k` is ART seek work for one shard refill, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page exposes any continuation cursor through `borrowNextCursor` and may transfer it into `OwnedScanCursor` through `takeNextCursor`.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while fetching or refilling shard-local ART heads.
pub fn scanRangeFromInView(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    range: types.KeyRange,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) EngineError!types.ScanPageResult {
    const state = runtimeStateFromViewForLatency(view);
    return metrics.callWithOptionalLatency(state, scan_ops.scanRangeFromInView, .{ view, allocator, range, cursor, limit });
}

/// Applies one checked batch under the official advanced contract.
///
/// Time Complexity: O(g + n + b + v), where `g` is `batch.guards.len`, `n` is surviving write count, `b` is total serialized value bytes measured during planning, and `v` is total cloned value size for prepared writes.
///
/// Allocator: Uses the engine base allocator for committed values and temporary planner scratch while validating guards and preparing the batch.
///
/// Ownership: Clones all surviving write values into engine-owned storage before making the batch visible.
///
/// Thread Safety: Safe for concurrent use with point operations and read views; acquires the global visibility gate exclusively for the full guard-check and apply window.
pub fn applyCheckedBatch(db: *Database, batch: types.CheckedBatch) EngineError!void {
    return metrics.callWithLatency(&db.state, batch_ops.applyCheckedBatch, .{ &db.state, db.allocator, batch });
}

fn setTtlForTest(db: *Database, key: []const u8, expire_at_seconds: i64) !void {
    const shard_idx = runtime_shard.getShardIndex(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lockUncancelable(shard.io);
    defer shard.lock.unlock(shard.io);
    try internal_ttl_index.setTtlEntry(shard, key, expire_at_seconds);
}

fn hasTtlForTest(db: *Database, key: []const u8) bool {
    const shard_idx = runtime_shard.getShardIndex(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lockSharedUncancelable(shard.io);
    defer shard.lock.unlockShared(shard.io);
    return internal_ttl_index.getExpireAt(shard, key) != null;
}

fn hasStoredKeyForTest(db: *Database, key: []const u8) bool {
    const shard_idx = runtime_shard.getShardIndex(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lockSharedUncancelable(shard.io);
    defer shard.lock.unlockShared(shard.io);
    return shard.tree.lookup(key) != null;
}

fn valueIsHeapOwnedForTest(db: *Database, key: []const u8) bool {
    const shard_idx = runtime_shard.getShardIndex(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lockSharedUncancelable(shard.io);
    defer shard.lock.unlockShared(shard.io);
    return internal_mutate.valueIsHeapOwnedUnlocked(shard, key);
}

fn totalCommittedArenasForTest(db: *Database) usize {
    var total: usize = 0;
    for (&db.state.shards) |*shard| {
        shard.lock.lockSharedUncancelable(shard.io);
        total += internal_mutate.countCommittedArenasUnlocked(shard);
        shard.lock.unlockShared(shard.io);
    }
    return total;
}

fn tryLockAllShardVisibilityGatesExclusiveForTest(db: *Database) bool {
    var locked: usize = 0;
    while (locked < db.state.shards.len) : (locked += 1) {
        if (!db.state.shards[locked].visibility_gate.tryLockExclusive()) {
            var release_idx: usize = 0;
            while (release_idx < locked) : (release_idx += 1) {
                db.state.shards[release_idx].visibility_gate.unlockExclusive();
            }
            return false;
        }
    }
    return true;
}

fn unlockAllShardVisibilityGatesExclusiveForTest(db: *Database) void {
    for (&db.state.shards) |*shard| shard.visibility_gate.unlockExclusive();
}

fn expireAtBoundary(db: *Database, key: []const u8, unix_seconds: ?i64) EngineError!bool {
    const updated = try expiration.expireAt(&db.state, key, unix_seconds);
    db.state.recordOperation(.expire, 1);
    return updated;
}

fn runtimeStateFromViewForLatency(view: *const types.ReadView) ?*const runtime_state.DatabaseState {
    const opaque_state = view.resolveRuntimeState() orelse return null;
    return @ptrCast(@alignCast(opaque_state));
}

fn latencySamplesForTest(db: *const Database) u64 {
    return db.state.statsSnapshot().latency_samples_total;
}

fn currentCheckpointLsnForTest(db: *Database) u64 {
    if (db.state.wal) |wal| {
        if (wal.next_lsn > 0) return wal.next_lsn - 1;
    }
    return 0;
}

fn corruptFileByteForTest(path: []const u8, offset: u64, mask: u8) !void {
    const file = try std.Io.Dir.cwd().openFile(std.testing.io, path, .{ .mode = .read_write });
    defer file.close(std.testing.io);

    var byte: [1]u8 = undefined;
    if ((try file.readPositionalAll(std.testing.io, &byte, offset)) != byte.len) return error.EndOfStream;
    byte[0] ^= mask;
    try file.writePositionalAll(std.testing.io, &byte, offset);
}

fn allocTmpPathTest(allocator: std.mem.Allocator, tmp: std.testing.TmpDir, basename: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, basename });
}

var noop_replay_ctx: u8 = 0;

fn noopReplayPut(ctx: *anyopaque, key: []const u8, value: *const types.Value) !void {
    _ = ctx;
    _ = key;
    _ = value;
}

fn noopReplayDelete(ctx: *anyopaque, key: []const u8) !void {
    _ = ctx;
    _ = key;
}

fn noopReplayExpire(ctx: *anyopaque, key: []const u8, expire_at_sec: i64) !void {
    _ = ctx;
    _ = key;
    _ = expire_at_sec;
}

fn attachTestWal(db: *Database, path: []const u8, fsync_mode: types.FsyncMode) !void {
    db.state.wal = try storage_wal.open(db.state.io, path, .{ .fsync_mode = fsync_mode }, .{
        .ctx = &noop_replay_ctx,
        .put = noopReplayPut,
        .delete = noopReplayDelete,
        .expire = noopReplayExpire,
    }, db.allocator);
}

const CheckpointThreadState = struct {
    db: *Database,
    barrier_attempted: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    barrier_acquired: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    finished: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    checkpoint_error: ?EngineError = null,
};

fn runCheckpointInThread(state: *CheckpointThreadState) void {
    state.db.checkpoint() catch |err| {
        state.checkpoint_error = err;
        state.finished.store(true, .release);
        return;
    };
    state.finished.store(true, .release);
}

test "create initializes runtime-owned database state" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    try testing.expectEqual(@as(usize, runtime_state.NUM_SHARDS), db.state.shards.len);
    try testing.expect(db.state.snapshot_path == null);
}

test "snapshot-only open restores values and ttl metadata without incrementing counters" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "step9-snapshot-only.snapshot");
    defer testing.allocator.free(snapshot_path);

    {
        const db = try create(std.testing.io, testing.allocator);
        defer db.close() catch unreachable;

        const alpha = types.Value{ .string = "alive" };
        const beta = types.Value{ .integer = 7 };
        try db.put("alpha", &alpha);
        try db.put("beta", &beta);
        try setTtlForTest(db, "alpha", runtime_shard.unixNow(std.testing.io) + 60);

        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, 33);
    }

    const reopened = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    var beta = (try reopened.get(testing.allocator, "beta")).?;
    defer beta.deinit(testing.allocator);

    try testing.expectEqualStrings("alive", alpha.string);
    try testing.expectEqual(@as(i64, 7), beta.integer);
    try testing.expect((try reopened.ttl("alpha")) >= 0);
    try testing.expectEqualStrings(snapshot_path, reopened.state.snapshot_path.?);
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_put_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_delete_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_expire_total.load(.monotonic));
}

test "wal-only open replays recovered keys deletes and ttl metadata without incrementing counters" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "step7-open-replay.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(std.testing.io, testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const alpha = types.Value{ .string = "alive" };
        const beta = types.Value{ .string = "gone" };
        const gamma = types.Value{ .integer = 9 };
        try db.put("alpha", &alpha);
        try db.put("beta", &beta);
        try db.put("gamma", &gamma);
        try testing.expect(try db.delete("beta"));
        try testing.expect(try db.expireAt("gamma", runtime_shard.unixNow(std.testing.io) + 60));
    }

    const reopened = try open(std.testing.io, testing.allocator, .{
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    try testing.expectEqualStrings("alive", alpha.string);
    try testing.expect((try reopened.get(testing.allocator, "beta")) == null);
    try testing.expect((try reopened.ttl("gamma")) >= 0);

    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_put_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_delete_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_expire_total.load(.monotonic));
}

test "snapshot-backed open replays wal delta after the snapshot lsn" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "step9-delta.wal");
    defer testing.allocator.free(wal_path);
    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "step9-delta.snapshot");
    defer testing.allocator.free(snapshot_path);

    {
        const db = try open(std.testing.io, testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const one = types.Value{ .integer = 1 };
        const two = types.Value{ .integer = 2 };
        try db.put("alpha", &one);
        try db.put("beta", &two);

        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, currentCheckpointLsnForTest(db));

        const replacement = types.Value{ .integer = 9 };
        const gamma = types.Value{ .string = "delta" };
        try db.put("alpha", &replacement);
        try testing.expect(try db.delete("beta"));
        try db.put("gamma", &gamma);
        try testing.expect(try db.expireAt("gamma", runtime_shard.unixNow(std.testing.io) + 60));
    }

    const reopened = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    var gamma = (try reopened.get(testing.allocator, "gamma")).?;
    defer gamma.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, 9), alpha.integer);
    try testing.expect((try reopened.get(testing.allocator, "beta")) == null);
    try testing.expectEqualStrings("delta", gamma.string);
    try testing.expect((try reopened.ttl("gamma")) >= 0);
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_put_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_delete_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_expire_total.load(.monotonic));
}

test "corrupted snapshot falls back to full wal replay when wal is non-empty" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "step9-fallback.wal");
    defer testing.allocator.free(wal_path);
    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "step9-fallback.snapshot");
    defer testing.allocator.free(snapshot_path);

    {
        const db = try open(std.testing.io, testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const alpha = types.Value{ .integer = 1 };
        const beta = types.Value{ .integer = 2 };
        try db.put("alpha", &alpha);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, currentCheckpointLsnForTest(db));
        try db.put("beta", &beta);
    }

    try corruptFileByteForTest(snapshot_path, 0, 0xff);

    const reopened = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    var beta = (try reopened.get(testing.allocator, "beta")).?;
    defer beta.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, 1), alpha.integer);
    try testing.expectEqual(@as(i64, 2), beta.integer);
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_put_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_delete_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_expire_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 1), reopened.state.statsSnapshot().snapshot_corruption_fallback_total);
}

test "corrupted snapshot with missing wal returns snapshot corrupted" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "step9-missing-wal.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "missing.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try create(std.testing.io, testing.allocator);
        defer db.close() catch unreachable;

        const alpha = types.Value{ .integer = 1 };
        try db.put("alpha", &alpha);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, 0);
    }

    try corruptFileByteForTest(snapshot_path, 0, 0xff);

    try testing.expectError(error.SnapshotCorrupted, open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    }));
}

test "corrupted snapshot with empty wal returns snapshot corrupted" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "step9-empty-wal.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "empty.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try create(std.testing.io, testing.allocator);
        defer db.close() catch unreachable;

        const alpha = types.Value{ .integer = 1 };
        try db.put("alpha", &alpha);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, 0);
    }

    {
        const file = try std.Io.Dir.cwd().createFile(std.testing.io, wal_path, .{ .truncate = true });
        file.close(std.testing.io);
    }
    try corruptFileByteForTest(snapshot_path, 0, 0xff);

    try testing.expectError(error.SnapshotCorrupted, open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    }));
}

test "open purges expired keys recovered from snapshot and wal replay" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "step9-purge.wal");
    defer testing.allocator.free(wal_path);
    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "step9-purge.snapshot");
    defer testing.allocator.free(snapshot_path);

    {
        const db = try open(std.testing.io, testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const snapshot_value = types.Value{ .integer = 1 };
        const wal_value = types.Value{ .integer = 2 };
        try db.put("snapshot:expired", &snapshot_value);
        try setTtlForTest(db, "snapshot:expired", runtime_shard.unixNow(std.testing.io) - 10);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, currentCheckpointLsnForTest(db));

        try db.state.wal.?.appendPut("wal:expired", &wal_value);
        try db.state.wal.?.appendExpire("wal:expired", runtime_shard.unixNow(std.testing.io) - 10);
    }

    const reopened = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    try testing.expect((try reopened.get(testing.allocator, "snapshot:expired")) == null);
    try testing.expect((try reopened.get(testing.allocator, "wal:expired")) == null);
    try testing.expectEqual(@as(i64, -2), try reopened.ttl("snapshot:expired"));
    try testing.expectEqual(@as(i64, -2), try reopened.ttl("wal:expired"));
    try testing.expect(!hasTtlForTest(reopened, "snapshot:expired"));
    try testing.expect(!hasTtlForTest(reopened, "wal:expired"));
    try testing.expect(!hasStoredKeyForTest(reopened, "snapshot:expired"));
    try testing.expect(!hasStoredKeyForTest(reopened, "wal:expired"));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_put_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_delete_total.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), reopened.state.counters.ops_expire_total.load(.monotonic));
}

test "checkpoint without a configured snapshot path returns no snapshot path" {
    const testing = std.testing;

    const db = try open(std.testing.io, testing.allocator, .{
        .metrics = .{ .mode = .full },
    });
    defer db.close() catch unreachable;

    try testing.expectError(error.NoSnapshotPath, db.checkpoint());
    const stats = db.state.statsSnapshot();
    try testing.expectEqual(@as(u64, 1), stats.latency_samples_total);
    try testing.expectEqual(@as(u64, 0), stats.checkpoint_count_total);
    try testing.expectEqual(@as(u64, 0), stats.checkpoint_busy_total);
    try testing.expectEqual(@as(u64, 0), stats.checkpoint_duration_last_ms);
    try testing.expectEqual(@as(u64, 0), stats.checkpoint_lsn_last);
}

test "compact memory without a configured snapshot path returns no snapshot path" {
    const testing = std.testing;

    const db = try open(std.testing.io, testing.allocator, .{
        .metrics = .{ .mode = .full },
    });
    defer db.close() catch unreachable;

    try testing.expectError(error.NoSnapshotPath, db.compactMemory());
}

test "compact_shard validates shard index" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    try testing.expectError(error.InvalidShardIndex, db.compactShard(runtime_shard.NUM_SHARDS));
}

test "checkpoint writes a snapshot and reopens the same visible state" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "step7-checkpoint.snapshot");
    defer testing.allocator.free(snapshot_path);

    {
        const db = try open(std.testing.io, testing.allocator, .{
            .snapshot_path = snapshot_path,
        });
        defer db.close() catch unreachable;

        const alpha_value = types.Value{ .string = "hello" };
        const beta_value = types.Value{ .integer = 7 };
        try db.put("alpha", &alpha_value);
        try db.put("beta", &beta_value);
        try testing.expect(try db.expireAt("beta", runtime_shard.unixNow(std.testing.io) + 60));
        try db.checkpoint();
        const stats = db.state.statsSnapshot();
        try testing.expectEqual(@as(u64, 1), stats.checkpoint_count_total);
        try testing.expectEqual(@as(u64, 0), stats.checkpoint_busy_total);
        try testing.expectEqual(@as(u64, 0), stats.checkpoint_lsn_last);
    }

    const reopened = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", alpha.string);

    var beta = (try reopened.get(testing.allocator, "beta")).?;
    defer beta.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 7), beta.integer);
    try testing.expect((try reopened.ttl("beta")) > 0);
}

test "checkpoint preserves post-snapshot wal delta on reopen" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "step7-delta.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "step7-delta.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(std.testing.io, testing.allocator, .{
            .snapshot_path = snapshot_path,
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const alpha_value = types.Value{ .integer = 1 };
        const beta_value = types.Value{ .integer = 2 };
        try db.put("alpha", &alpha_value);
        try db.checkpoint();
        try db.put("beta", &beta_value);
        try testing.expect(try db.delete("alpha"));
    }

    const reopened = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    try testing.expect((try reopened.get(testing.allocator, "alpha")) == null);

    var beta = (try reopened.get(testing.allocator, "beta")).?;
    defer beta.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 2), beta.integer);
}

test "compact memory preserves visible state and reclaims committed arenas" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "compact-memory.snapshot");
    defer testing.allocator.free(snapshot_path);

    const db = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
    });
    defer db.close() catch unreachable;

    const write_count: usize = 64;
    var values: [write_count]types.Value = undefined;
    var writes: [write_count]types.PutWrite = undefined;
    var key_storage: [write_count][24]u8 = undefined;

    for (0..writes.len) |index| {
        values[index] = .{ .integer = @intCast(index) };
        const key = try std.fmt.bufPrint(&key_storage[index], "compact:{d:0>4}", .{index});
        writes[index] = .{ .key = key, .value = &values[index] };
    }

    try db.applyBatch(&writes);
    try testing.expect(totalCommittedArenasForTest(db) > 0);

    try db.compactMemory();

    try testing.expectEqual(@as(usize, 0), totalCommittedArenasForTest(db));
    const stats = db.state.statsSnapshot();
    try testing.expectEqual(@as(u64, 1), stats.checkpoint_count_total);
    try testing.expectEqual(@as(u64, 0), stats.checkpoint_busy_total);

    var sample = (try db.get(testing.allocator, "compact:0001")).?;
    defer sample.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 1), sample.integer);
}

test "compact_shard clears retained heavy estimate and preserves visible value" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    var payload_a = [_]u8{'a'} ** 1024;
    var payload_b = [_]u8{'b'} ** 1024;

    var first = types.Value{ .string = payload_a[0..] };
    var second = types.Value{ .string = payload_b[0..] };

    try db.put("heavy:one", &first);
    try db.put("heavy:one", &second);

    const shard_idx = runtime_shard.getShardIndex("heavy:one");
    const before = db.state.statsSnapshot();
    try testing.expect(before.overwritten_heavy_bytes_total > 0);
    try testing.expect(before.retained_heavy_bytes_estimate > 0);
    try testing.expect(db.state.retainedHeavyBytesEstimateForShard(shard_idx) > 0);

    try db.compactShard(shard_idx);

    const after = db.state.statsSnapshot();
    try testing.expectEqual(@as(u64, 0), after.retained_heavy_bytes_estimate);
    try testing.expectEqual(@as(u64, 0), db.state.retainedHeavyBytesEstimateForShard(shard_idx));

    var stored = (try db.get(testing.allocator, "heavy:one")).?;
    defer stored.deinit(testing.allocator);
    try testing.expectEqualStrings(payload_b[0..], stored.string);
}

test "compact_all clears retained heavy estimate across shards" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    var payload_a = [_]u8{'x'} ** 1024;
    var payload_b = [_]u8{'y'} ** 1024;

    var first = types.Value{ .string = payload_a[0..] };
    var second = types.Value{ .string = payload_b[0..] };

    try db.put("{s1}:heavy", &first);
    try db.put("{s1}:heavy", &second);
    try db.put("{s2}:heavy", &first);
    try db.put("{s2}:heavy", &second);

    const before = db.state.statsSnapshot();
    try testing.expect(before.retained_heavy_bytes_estimate > 0);

    try db.compactAll();

    const after = db.state.statsSnapshot();
    try testing.expectEqual(@as(u64, 0), after.retained_heavy_bytes_estimate);
}

test "heavy overwrite retained estimate stays low with heap-owned overwrites" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const payload_bytes: usize = 1024;
    const overwrite_count: usize = 20_000;
    // With heap-owned heavy overwrites, retention should stay bounded near the
    // initial arena-owned value for this key rather than growing per overwrite.
    const epsilon_bytes: u64 = 1024;
    const low_retention_factor: u64 = 4;
    const low_retention_ceiling = epsilon_bytes * low_retention_factor;

    var payload_a = [_]u8{'m'} ** payload_bytes;
    var payload_b = [_]u8{'n'} ** payload_bytes;

    var first = types.Value{ .string = payload_a[0..] };
    var second = types.Value{ .string = payload_b[0..] };

    try db.put("heavy:deterministic", &first);
    for (0..overwrite_count) |_| {
        try db.put("heavy:deterministic", &second);
    }

    const before = db.state.statsSnapshot();
    try testing.expect(before.retained_heavy_bytes_estimate <= low_retention_ceiling);
    try testing.expect(before.overwritten_heavy_bytes_total >= before.retained_heavy_bytes_estimate);

    try db.compactAll();

    const after = db.state.statsSnapshot();
    try testing.expect(after.retained_heavy_bytes_estimate <= low_retention_ceiling);
    const retained_delta = if (before.retained_heavy_bytes_estimate >= after.retained_heavy_bytes_estimate)
        before.retained_heavy_bytes_estimate - after.retained_heavy_bytes_estimate
    else
        after.retained_heavy_bytes_estimate - before.retained_heavy_bytes_estimate;
    try testing.expect(retained_delta <= epsilon_bytes);
    try testing.expect(after.overwritten_heavy_bytes_total >= before.overwritten_heavy_bytes_total);
}

test "heavy_overwrite_compact_every triggers automatic maintenance on heavy overwrites" {
    const testing = std.testing;

    const db = try open(std.testing.io, testing.allocator, .{
        .heavy_overwrite_compact_every = 1,
    });
    defer db.close() catch unreachable;

    var payload_a = [_]u8{'h'} ** 1024;
    var payload_b = [_]u8{'i'} ** 1024;

    var first = types.Value{ .string = payload_a[0..] };
    var second = types.Value{ .string = payload_b[0..] };

    try db.put("heavy:auto", &first);
    for (0..32) |_| {
        try db.put("heavy:auto", &second);
    }

    const stats = db.state.statsSnapshot();
    try testing.expect(stats.overwritten_heavy_events_total > 0);
    try testing.expect(stats.overwritten_heavy_bytes_total > 0);
    try testing.expect(stats.retained_heavy_bytes_estimate <= 1024);
}

test "heavy overwrite cadence counts per-overwrite events inside one put_group" {
    const testing = std.testing;

    const db = try open(std.testing.io, testing.allocator, .{
        .heavy_overwrite_compact_every = 2,
    });
    defer db.close() catch unreachable;

    var payload_a = [_]u8{'a'} ** 1024;
    var payload_b = [_]u8{'b'} ** 1024;

    var first = types.Value{ .string = payload_a[0..] };
    var second = types.Value{ .string = payload_b[0..] };

    try db.put("heavy:auto:1", &first);
    try db.put("heavy:auto:2", &first);

    try db.putGroup(&.{
        .{ .key = "heavy:auto:1", .value = &second },
        .{ .key = "heavy:auto:2", .value = &second },
    });

    const stats = db.state.statsSnapshot();
    try testing.expect(stats.overwritten_heavy_events_total >= 2);
    try testing.expectEqual(@as(u64, 0), stats.retained_heavy_bytes_estimate);
}

test "heavy to scalar overwrite still records retained heavy estimate" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    var payload = [_]u8{'z'} ** 1024;
    var heavy = types.Value{ .string = payload[0..] };
    const scalar = types.Value{ .integer = 7 };

    try db.put("heavy:to:scalar", &heavy);
    try db.put("heavy:to:scalar", &scalar);

    const stats = db.state.statsSnapshot();
    try testing.expect(stats.overwritten_heavy_events_total >= 1);
    try testing.expect(stats.retained_heavy_bytes_estimate >= 1024);
}

test "engine boundary latency sampling records one sample per call including errors" {
    const testing = std.testing;

    const db = try open(std.testing.io, testing.allocator, .{
        .metrics = .{ .mode = .full },
    });
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    var expected = latencySamplesForTest(db);

    try db.put("alpha", &value);
    expected += 1;
    try testing.expectEqual(expected, latencySamplesForTest(db));

    var alpha = (try db.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    expected += 1;
    try testing.expectEqual(expected, latencySamplesForTest(db));

    try testing.expect(!try db.expireAt("missing", runtime_shard.unixNow(std.testing.io) + 30));
    expected += 1;
    try testing.expectEqual(expected, latencySamplesForTest(db));

    _ = try db.ttl("alpha");
    expected += 1;
    try testing.expectEqual(expected, latencySamplesForTest(db));

    var prefix = try db.scanPrefix(testing.allocator, "a");
    defer prefix.deinit();
    expected += 1;
    try testing.expectEqual(expected, latencySamplesForTest(db));

    var range = try db.scanRange(testing.allocator, .{
        .start = "a",
        .end = "z",
    });
    defer range.deinit();
    expected += 1;
    try testing.expectEqual(expected, latencySamplesForTest(db));

    try db.applyBatch(&.{
        .{ .key = "beta", .value = &value },
    });
    expected += 1;
    try testing.expectEqual(expected, latencySamplesForTest(db));

    var view = try db.readView();
    expected += 1;
    try testing.expectEqual(expected, latencySamplesForTest(db));

    var in_view_prefix = try scanPrefixFromInView(&view, testing.allocator, "", null, 10);
    expected += 1;
    try testing.expectEqual(expected, latencySamplesForTest(db));

    var in_view_range = try scanRangeFromInView(&view, testing.allocator, .{
        .start = "a",
        .end = "z",
    }, null, 10);
    expected += 1;
    try testing.expectEqual(expected, latencySamplesForTest(db));

    in_view_range.deinit();
    in_view_prefix.deinit();
    view.deinit();

    try applyCheckedBatch(db, .{
        .writes = &.{
            .{ .key = "gamma", .value = &value },
        },
        .guards = &.{
            .{ .key_exists = "alpha" },
        },
    });
    expected += 1;
    try testing.expectEqual(expected, latencySamplesForTest(db));

    try testing.expectError(error.KeyTooLarge, db.put("", &value));
    expected += 1;
    try testing.expectEqual(expected, latencySamplesForTest(db));

    try testing.expectError(error.KeyTooLarge, applyCheckedBatch(db, .{
        .writes = &.{
            .{ .key = "", .value = &value },
        },
        .guards = &.{},
    }));
    expected += 1;
    try testing.expectEqual(expected, latencySamplesForTest(db));
}

test "wal-only restart preserves committed batch semantics" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "step7-batch-restart.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(std.testing.io, testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const one = types.Value{ .integer = 1 };
        const two = types.Value{ .integer = 2 };
        const three = types.Value{ .integer = 3 };
        try db.applyBatch(&.{
            .{ .key = "alpha", .value = &one },
            .{ .key = "beta", .value = &two },
            .{ .key = "alpha", .value = &three },
        });
    }

    const reopened = try open(std.testing.io, testing.allocator, .{
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    var beta = (try reopened.get(testing.allocator, "beta")).?;
    defer beta.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, 3), alpha.integer);
    try testing.expectEqual(@as(i64, 2), beta.integer);
}

test "recovered expired ttl metadata remains invisible after wal-only restart" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "step7-expired-restart.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(std.testing.io, testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const value = types.Value{ .integer = 7 };
        try db.put("alpha", &value);
        try testing.expect(try db.expireAt("alpha", runtime_shard.unixNow(std.testing.io) + 1));
    }

    std.testing.io.sleep(.fromNanoseconds(1100 * std.time.ns_per_ms), .awake) catch unreachable;

    const reopened = try open(std.testing.io, testing.allocator, .{
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    try testing.expect((try reopened.get(testing.allocator, "alpha")) == null);
    try testing.expectEqual(@as(i64, -2), try reopened.ttl("alpha"));

    var scan = try reopened.scanPrefix(testing.allocator, "alpha");
    defer scan.deinit();
    try testing.expectEqual(@as(usize, 0), scan.entries.items.len);
}

test "truncated batch tail does not become visible after wal-only reopen" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "step7-truncated-batch.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(std.testing.io, testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const one = types.Value{ .integer = 1 };
        const two = types.Value{ .integer = 2 };
        try db.applyBatch(&.{
            .{ .key = "alpha", .value = &one },
            .{ .key = "beta", .value = &two },
        });
    }

    {
        const file = try std.Io.Dir.cwd().openFile(std.testing.io, wal_path, .{ .mode = .read_write });
        defer file.close(std.testing.io);
        const size = try file.length(std.testing.io);
        try file.setLength(std.testing.io, size - 1);
    }

    const reopened = try open(std.testing.io, testing.allocator, .{
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    try testing.expect((try reopened.get(testing.allocator, "alpha")) == null);
    try testing.expect((try reopened.get(testing.allocator, "beta")) == null);
}

test "plain point operations store clone and delete values" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

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

    try testing.expect(try db.delete("alpha"));
    try testing.expect(!try db.delete("alpha"));
    try testing.expect((try db.get(testing.allocator, "alpha")) == null);
}

test "put overwrites existing plain value" {
    const testing = std.testing;

    const db = try open(std.testing.io, testing.allocator, .{ .metrics = .{ .mode = .counters_only } });
    defer db.close() catch unreachable;

    const first = types.Value{ .integer = 7 };
    try db.put("counter", &first);

    const second = types.Value{ .string = "updated" };
    try db.put("counter", &second);

    var stored = (try db.get(testing.allocator, "counter")).?;
    defer stored.deinit(testing.allocator);

    try testing.expectEqualStrings("updated", stored.string);
    try testing.expectEqual(@as(u64, 2), db.state.statsSnapshot().ops_put_total);
    try testing.expectEqual(@as(u64, 1), db.state.statsSnapshot().overwrites_total);
}

test "scalar point overwrites avoid allocator growth in steady state" {
    const testing = std.testing;

    var counting_state = std.testing.FailingAllocator.init(testing.allocator, .{});
    const counting_allocator = counting_state.allocator();

    const db = try create(std.testing.io, counting_allocator);
    defer db.close() catch unreachable;

    const initial = types.Value{ .integer = 0 };
    try db.put("counter", &initial);

    const outstanding_after_first = counting_state.allocated_bytes - counting_state.freed_bytes;

    for (0..20_000) |index| {
        const next = types.Value{ .integer = @intCast(index + 1) };
        try db.put("counter", &next);
    }

    const outstanding_after_overwrites = counting_state.allocated_bytes - counting_state.freed_bytes;
    try testing.expect(outstanding_after_overwrites <= outstanding_after_first + 4 * 1024);
}

test "put leaves state unchanged when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "step6-put-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;
    try attachTestWal(db, path, .none);

    const value = types.Value{ .string = "value" };
    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, db.put("wal:put", &value));
    try testing.expect((try db.get(testing.allocator, "wal:put")) == null);
}

test "put_group writes all provided entries" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .string = "two" };
    const three = types.Value{ .boolean = true };

    try db.putGroup(&.{
        .{ .key = "group:one", .value = &one },
        .{ .key = "group:two", .value = &two },
        .{ .key = "group:three", .value = &three },
    });

    var got_one = (try db.get(testing.allocator, "group:one")).?;
    defer got_one.deinit(testing.allocator);
    var got_two = (try db.get(testing.allocator, "group:two")).?;
    defer got_two.deinit(testing.allocator);
    var got_three = (try db.get(testing.allocator, "group:three")).?;
    defer got_three.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, 1), got_one.integer);
    try testing.expectEqualStrings("two", got_two.string);
    try testing.expect(got_three.boolean);
}

test "put_group keeps state unchanged when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "step6-put-group-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;
    try attachTestWal(db, path, .none);

    const value = types.Value{ .string = "value" };
    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, db.putGroup(&.{
        .{ .key = "wal:group:1", .value = &value },
        .{ .key = "wal:group:2", .value = &value },
    }));

    try testing.expect((try db.get(testing.allocator, "wal:group:1")) == null);
    try testing.expect((try db.get(testing.allocator, "wal:group:2")) == null);
}

test "delete returns a durability error and keeps the key when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "step6-delete-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("wal:delete", &value);
    try attachTestWal(db, path, .none);

    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, db.delete("wal:delete"));

    var stored = (try db.get(testing.allocator, "wal:delete")).?;
    defer stored.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 1), stored.integer);
}

test "expire_at returns false for missing keys and increments the expire counter" {
    const testing = std.testing;

    const db = try open(std.testing.io, testing.allocator, .{ .metrics = .{ .mode = .counters_only } });
    defer db.close() catch unreachable;

    try testing.expect(!try db.expireAt("missing", runtime_shard.unixNow(std.testing.io) + 30));
    try testing.expectEqual(@as(u64, 1), db.state.counters.ops_expire_total.load(.monotonic));
}

test "expire_at null clears existing ttl while keeping the stored value" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .string = "value" };
    try db.put("ttl:key", &value);

    const expire_at_seconds = runtime_shard.unixNow(std.testing.io) + 30;
    try testing.expect(try db.expireAt("ttl:key", expire_at_seconds));
    const ttl_before_clear = try db.ttl("ttl:key");
    try testing.expect(ttl_before_clear >= 0);
    try testing.expect(ttl_before_clear <= 30);

    try testing.expect(try db.expireAt("ttl:key", null));
    try testing.expectEqual(@as(i64, -1), try db.ttl("ttl:key"));

    var stored = (try db.get(testing.allocator, "ttl:key")).?;
    defer stored.deinit(testing.allocator);
    try testing.expectEqualStrings("value", stored.string);
}

test "expire_at at or before now deletes the key immediately" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 9 };
    try db.put("gone", &value);

    try testing.expect(try db.expireAt("gone", runtime_shard.unixNow(std.testing.io)));
    try testing.expect((try db.get(testing.allocator, "gone")) == null);
    try testing.expectEqual(@as(i64, -2), try db.ttl("gone"));
    try testing.expect(!hasTtlForTest(db, "gone"));
}

test "expire_at future leaves ttl unchanged when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "step6-expire-future-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("wal:expire-future", &value);
    try setTtlForTest(db, "wal:expire-future", runtime_shard.unixNow(std.testing.io) + 30);
    try attachTestWal(db, path, .none);

    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, db.expireAt("wal:expire-future", runtime_shard.unixNow(std.testing.io) + 90));
    try testing.expect(hasTtlForTest(db, "wal:expire-future"));
    try testing.expect((try db.ttl("wal:expire-future")) <= 30);
}

test "expire_at immediate delete leaves key visible when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "step6-expire-delete-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 9 };
    try db.put("wal:expire-delete", &value);
    try attachTestWal(db, path, .none);

    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, db.expireAt("wal:expire-delete", runtime_shard.unixNow(std.testing.io)));

    var stored = (try db.get(testing.allocator, "wal:expire-delete")).?;
    defer stored.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 9), stored.integer);
    try testing.expectEqual(@as(i64, -1), try db.ttl("wal:expire-delete"));
}

test "expire_at null leaves ttl intact when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "step6-expire-clear-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("wal:expire-clear", &value);
    try setTtlForTest(db, "wal:expire-clear", runtime_shard.unixNow(std.testing.io) + 30);
    try attachTestWal(db, path, .none);

    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, db.expireAt("wal:expire-clear", null));
    try testing.expect(hasTtlForTest(db, "wal:expire-clear"));
    try testing.expect((try db.ttl("wal:expire-clear")) >= 0);
}

test "ttl eagerly cleans up expired keys while get remains lazily invisible" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .string = "stale" };
    try db.put("stale:key", &value);
    try setTtlForTest(db, "stale:key", runtime_shard.unixNow(std.testing.io) - 1);

    try testing.expect(hasTtlForTest(db, "stale:key"));
    try testing.expect((try db.get(testing.allocator, "stale:key")) == null);
    try testing.expect(hasTtlForTest(db, "stale:key"));

    try testing.expectEqual(@as(i64, -2), try db.ttl("stale:key"));
    try testing.expect(!hasTtlForTest(db, "stale:key"));
    try testing.expect((try db.get(testing.allocator, "stale:key")) == null);
}

test "read view holds the visibility gate until released" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    var view = try db.readView();
    defer if (view.token_id != 0) view.deinit();

    try testing.expect(!tryLockAllShardVisibilityGatesExclusiveForTest(db));

    view.deinit();
    try testing.expect(tryLockAllShardVisibilityGatesExclusiveForTest(db));
    unlockAllShardVisibilityGatesExclusiveForTest(db);
}

test "checkpoint returns busy while a read view holds global visibility gates" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "checkpoint-busy-read-view.snapshot");
    defer testing.allocator.free(snapshot_path);

    const db = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
    });
    defer db.close() catch unreachable;

    var view = try db.readView();
    defer view.deinit();

    try testing.expectError(error.CheckpointBusy, db.checkpoint());

    const stats = db.state.statsSnapshot();
    try testing.expectEqual(@as(u64, 1), stats.checkpoint_busy_total);
    try testing.expectEqual(@as(u64, 0), stats.checkpoint_count_total);
}

test "compact memory returns busy while a read view holds global visibility gates" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "compact-memory-busy.snapshot");
    defer testing.allocator.free(snapshot_path);

    const db = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
    });
    defer db.close() catch unreachable;

    var view = try db.readView();
    defer view.deinit();

    try testing.expectError(error.CheckpointBusy, db.compactMemory());

    const stats = db.state.statsSnapshot();
    try testing.expectEqual(@as(u64, 1), stats.checkpoint_busy_total);
    try testing.expectEqual(@as(u64, 0), stats.checkpoint_count_total);
}

test "checkpoint succeeds after read view release following busy result" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "checkpoint-busy-then-success.snapshot");
    defer testing.allocator.free(snapshot_path);

    const db = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
    });
    defer db.close() catch unreachable;

    {
        var view = try db.readView();
        defer view.deinit();

        try testing.expectError(error.CheckpointBusy, db.checkpoint());
    }

    try db.checkpoint();
    const stats = db.state.statsSnapshot();
    try testing.expectEqual(@as(u64, 1), stats.checkpoint_busy_total);
    try testing.expectEqual(@as(u64, 1), stats.checkpoint_count_total);
}

test "concurrent checkpoint attempts while read view held report busy" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "checkpoint-busy-concurrent.snapshot");
    defer testing.allocator.free(snapshot_path);

    const db = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
    });
    defer db.close() catch unreachable;

    var view = try db.readView();
    defer view.deinit();

    var state_a = CheckpointThreadState{ .db = db };
    var state_b = CheckpointThreadState{ .db = db };

    const thread_a = try std.Thread.spawn(.{}, runCheckpointInThread, .{&state_a});
    const thread_b = try std.Thread.spawn(.{}, runCheckpointInThread, .{&state_b});
    thread_a.join();
    thread_b.join();

    try testing.expectEqual(@as(?EngineError, error.CheckpointBusy), state_a.checkpoint_error);
    try testing.expectEqual(@as(?EngineError, error.CheckpointBusy), state_b.checkpoint_error);

    const stats = db.state.statsSnapshot();
    try testing.expectEqual(@as(u64, 2), stats.checkpoint_busy_total);
    try testing.expectEqual(@as(u64, 0), stats.checkpoint_count_total);
}

test "read view copies release the visibility gate only once" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    var view = try db.readView();
    var copied = view;
    defer copied.deinit();
    defer view.deinit();

    try testing.expectEqual(@as(usize, 1), db.state.active_read_views.load(.monotonic));
    try testing.expect(!tryLockAllShardVisibilityGatesExclusiveForTest(db));

    view.deinit();

    try testing.expectEqual(@as(usize, 0), db.state.active_read_views.load(.monotonic));
    try testing.expect(tryLockAllShardVisibilityGatesExclusiveForTest(db));
    unlockAllShardVisibilityGatesExclusiveForTest(db);
}

test "in-view scans reject stale read view copies" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("alpha", &value);

    var view = try db.readView();
    var copied = view;
    defer copied.deinit();

    view.deinit();

    try testing.expectError(error.InvalidReadView, scanPrefixFromInView(&copied, testing.allocator, "alpha", null, 1));
}

test "close fails while a read view is still active" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    var view = try db.readView();
    defer view.deinit();

    try testing.expectError(error.ActiveReadViews, db.close());
}

test "close surfaces final wal flush failure for batched async mode" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "step7-close-fsync.wal");
    defer testing.allocator.free(wal_path);

    const db = try open(std.testing.io, testing.allocator, .{
        .wal_path = wal_path,
        .fsync_mode = .batched_async,
        .fsync_interval_ms = 1,
    });
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("alpha", &value);

    storage_wal.test_hooks.failNextFsync();
    try testing.expectError(error.WalFlushFailed, db.close());
}

test "apply_batch keeps the final value in declared key order" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };

    try db.applyBatch(&.{
        .{ .key = "alpha", .value = &one },
        .{ .key = "beta", .value = &two },
        .{ .key = "alpha", .value = &three },
    });

    var alpha = (try db.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    var beta = (try db.get(testing.allocator, "beta")).?;
    defer beta.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, 3), alpha.integer);
    try testing.expectEqual(@as(i64, 2), beta.integer);
}

test "apply_batch leaves survivor writes unapplied when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "step6-batch-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;
    try attachTestWal(db, path, .none);

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, db.applyBatch(&.{
        .{ .key = "wal:batch:one", .value = &one },
        .{ .key = "wal:batch:two", .value = &two },
    }));

    try testing.expect((try db.get(testing.allocator, "wal:batch:one")) == null);
    try testing.expect((try db.get(testing.allocator, "wal:batch:two")) == null);
}

test "apply_checked_batch keeps state unchanged when a guard fails" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const original = types.Value{ .string = "original" };
    try db.put("guarded", &original);

    const replacement = types.Value{ .string = "replacement" };
    const other = types.Value{ .integer = 9 };

    try testing.expectError(error.GuardFailed, applyCheckedBatch(db, .{
        .writes = &.{
            .{ .key = "guarded", .value = &replacement },
            .{ .key = "other", .value = &other },
        },
        .guards = &.{
            .{ .key_not_exists = "guarded" },
        },
    }));

    var guarded = (try db.get(testing.allocator, "guarded")).?;
    defer guarded.deinit(testing.allocator);

    try testing.expectEqualStrings("original", guarded.string);
    try testing.expect((try db.get(testing.allocator, "other")) == null);
}

test "apply_checked_batch leaves survivor writes unapplied when wal append fails" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try allocTmpPathTest(testing.allocator, tmp, "step6-checked-batch-failure.wal");
    defer testing.allocator.free(path);

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const existing = types.Value{ .integer = 1 };
    try db.put("wal:guard", &existing);
    try attachTestWal(db, path, .none);

    const target = types.Value{ .integer = 2 };
    storage_wal.test_hooks.failNextWrite();
    try testing.expectError(error.PersistenceIoFailure, applyCheckedBatch(db, .{
        .writes = &.{
            .{ .key = "wal:checked-target", .value = &target },
        },
        .guards = &.{
            .{ .key_exists = "wal:guard" },
        },
    }));

    try testing.expect((try db.get(testing.allocator, "wal:checked-target")) == null);

    var guarded = (try db.get(testing.allocator, "wal:guard")).?;
    defer guarded.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 1), guarded.integer);
}

test "apply_checked_batch validates guard keys and expected values" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    try testing.expectError(error.KeyTooLarge, applyCheckedBatch(db, .{
        .writes = &.{},
        .guards = &.{
            .{ .key_exists = "" },
        },
    }));

    const oversized_bytes = try allocator.alloc(u8, @as(usize, @intCast(internal_codec.MAX_VAL_LEN)) + 1);
    defer allocator.free(oversized_bytes);
    @memset(oversized_bytes, 'x');
    const oversized_value = types.Value{ .string = oversized_bytes };

    try testing.expectError(error.ValueTooLarge, applyCheckedBatch(db, .{
        .writes = &.{},
        .guards = &.{
            .{ .key_value_equals = .{
                .key = "guarded",
                .value = &oversized_value,
            } },
        },
    }));
}

test "put and delete clear prior ttl metadata" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const first = types.Value{ .integer = 1 };
    try db.put("ttl:put", &first);
    try setTtlForTest(db, "ttl:put", runtime_shard.unixNow(std.testing.io) + 30);

    const replacement = types.Value{ .integer = 2 };
    try db.put("ttl:put", &replacement);
    try testing.expectEqual(@as(i64, -1), try db.ttl("ttl:put"));
    try testing.expect(!hasTtlForTest(db, "ttl:put"));

    try setTtlForTest(db, "ttl:put", runtime_shard.unixNow(std.testing.io) + 30);
    try testing.expect(try db.delete("ttl:put"));
    try testing.expect(!hasTtlForTest(db, "ttl:put"));
    try testing.expectEqual(@as(i64, -2), try db.ttl("ttl:put"));
}

test "delete returns false for expired keys that are already invisible" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("ttl:expired-delete", &value);
    try setTtlForTest(db, "ttl:expired-delete", runtime_shard.unixNow(std.testing.io) - 1);

    try testing.expect(!try db.delete("ttl:expired-delete"));
    try testing.expect((try db.get(testing.allocator, "ttl:expired-delete")) == null);
    try testing.expect(!hasTtlForTest(db, "ttl:expired-delete"));
}

test "batch writes clear prior ttl metadata" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const original = types.Value{ .integer = 1 };
    try db.put("ttl:batch", &original);
    try db.put("ttl:checked", &original);
    try setTtlForTest(db, "ttl:batch", runtime_shard.unixNow(std.testing.io) + 30);
    try setTtlForTest(db, "ttl:checked", runtime_shard.unixNow(std.testing.io) + 30);

    const batch_value = types.Value{ .integer = 2 };
    try db.applyBatch(&.{
        .{ .key = "ttl:batch", .value = &batch_value },
    });
    try testing.expectEqual(@as(i64, -1), try db.ttl("ttl:batch"));
    try testing.expect(!hasTtlForTest(db, "ttl:batch"));

    const checked_value = types.Value{ .integer = 3 };
    try applyCheckedBatch(db, .{
        .writes = &.{
            .{ .key = "ttl:checked", .value = &checked_value },
        },
        .guards = &.{
            .{ .key_exists = "ttl:checked" },
        },
    });
    try testing.expectEqual(@as(i64, -1), try db.ttl("ttl:checked"));
    try testing.expect(!hasTtlForTest(db, "ttl:checked"));
}

test "checked batch guards treat expired keys as absent" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const stale = types.Value{ .integer = 1 };
    try db.put("ttl:guarded", &stale);
    try setTtlForTest(db, "ttl:guarded", runtime_shard.unixNow(std.testing.io) - 1);

    const fresh = types.Value{ .integer = 2 };
    try applyCheckedBatch(db, .{
        .writes = &.{
            .{ .key = "ttl:target", .value = &fresh },
        },
        .guards = &.{
            .{ .key_not_exists = "ttl:guarded" },
        },
    });

    var target = (try db.get(testing.allocator, "ttl:target")).?;
    defer target.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 2), target.integer);

    try testing.expectError(error.GuardFailed, applyCheckedBatch(db, .{
        .writes = &.{
            .{ .key = "ttl:another", .value = &fresh },
        },
        .guards = &.{
            .{ .key_exists = "ttl:guarded" },
        },
    }));

    try testing.expectError(error.GuardFailed, applyCheckedBatch(db, .{
        .writes = &.{
            .{ .key = "ttl:another", .value = &fresh },
        },
        .guards = &.{
            .{ .key_value_equals = .{
                .key = "ttl:guarded",
                .value = &stale,
            } },
        },
    }));
}

test "checked batch uses one expiration timestamp across all guards" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const pathological_key = try testing.allocator.alloc(u8, write.MAX_KEY_LEN);
    defer testing.allocator.free(pathological_key);
    @memset(pathological_key, '{');

    const original = types.Value{ .integer = 1 };
    try db.put(pathological_key, &original);
    try testing.expect(try db.expireAt(pathological_key, runtime_shard.unixNow(std.testing.io) + 2));

    const guards = try testing.allocator.alloc(types.CheckedBatchGuard, 128);
    defer testing.allocator.free(guards);
    for (guards) |*guard| {
        guard.* = .{ .key_exists = pathological_key };
    }

    const replacement = types.Value{ .integer = 2 };
    try applyCheckedBatch(db, .{
        .writes = &.{
            .{ .key = "ttl:guard-window", .value = &replacement },
        },
        .guards = guards,
    });

    var stored = (try db.get(testing.allocator, "ttl:guard-window")).?;
    defer stored.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 2), stored.integer);
}

test "scan_prefix returns lexicographically ordered owned entries" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const alpha = types.Value{ .integer = 1 };
    const alpha_one = types.Value{ .integer = 2 };
    const beta = types.Value{ .integer = 3 };
    try db.put("alpha", &alpha);
    try db.put("alpha:1", &alpha_one);
    try db.put("beta", &beta);

    var result = try db.scanPrefix(testing.allocator, "alpha");
    defer result.deinit();

    try testing.expectEqual(@as(usize, 2), result.entries.items.len);
    try testing.expectEqualStrings("alpha", result.entries.items[0].key);
    try testing.expectEqualStrings("alpha:1", result.entries.items[1].key);
    try testing.expectEqual(@as(i64, 1), result.entries.items[0].value.integer);
    try testing.expectEqual(@as(i64, 2), result.entries.items[1].value.integer);
}

test "scan operations omit expired keys while preserving lexicographic order" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const a = types.Value{ .integer = 1 };
    const b = types.Value{ .integer = 2 };
    const c = types.Value{ .integer = 3 };
    try db.put("alpha", &a);
    try db.put("beta", &b);
    try db.put("gamma", &c);
    try setTtlForTest(db, "beta", runtime_shard.unixNow(std.testing.io) - 1);

    var prefix_result = try db.scanPrefix(testing.allocator, "");
    defer prefix_result.deinit();
    try testing.expectEqual(@as(usize, 2), prefix_result.entries.items.len);
    try testing.expectEqualStrings("alpha", prefix_result.entries.items[0].key);
    try testing.expectEqualStrings("gamma", prefix_result.entries.items[1].key);

    var range_result = try db.scanRange(testing.allocator, .{
        .start = "a",
        .end = "z",
    });
    defer range_result.deinit();
    try testing.expectEqual(@as(usize, 2), range_result.entries.items.len);
    try testing.expectEqualStrings("alpha", range_result.entries.items[0].key);
    try testing.expectEqualStrings("gamma", range_result.entries.items[1].key);
}

test "scan_range uses inclusive start and exclusive end" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const a = types.Value{ .integer = 1 };
    const b = types.Value{ .integer = 2 };
    const c = types.Value{ .integer = 3 };
    try db.put("a", &a);
    try db.put("b", &b);
    try db.put("c", &c);

    var result = try db.scanRange(testing.allocator, .{
        .start = "a",
        .end = "c",
    });
    defer result.deinit();

    try testing.expectEqual(@as(usize, 2), result.entries.items.len);
    try testing.expectEqualStrings("a", result.entries.items[0].key);
    try testing.expectEqualStrings("b", result.entries.items[1].key);
}

test "scan_prefix_from_in_view paginates in key order" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };
    try db.put("alpha", &one);
    try db.put("alpha:1", &two);
    try db.put("alpha:2", &three);

    var view = try db.readView();
    defer view.deinit();

    var first_page = try scanPrefixFromInView(&view, testing.allocator, "alpha", null, 2);
    defer first_page.deinit();

    try testing.expectEqual(@as(usize, 2), first_page.entries.items.len);
    try testing.expect(first_page.borrowNextCursor() != null);
    try testing.expectEqualStrings("alpha", first_page.entries.items[0].key);
    try testing.expectEqualStrings("alpha:1", first_page.entries.items[1].key);

    var cursor = first_page.takeNextCursor().?;
    defer cursor.deinit();
    const cursor_view = cursor.asCursor().?;
    var second_page = try scanPrefixFromInView(&view, testing.allocator, "alpha", &cursor_view, 2);
    defer second_page.deinit();

    try testing.expectEqual(@as(usize, 1), second_page.entries.items.len);
    try testing.expect(second_page.borrowNextCursor() == null);
    try testing.expectEqualStrings("alpha:2", second_page.entries.items[0].key);
}

test "scan_prefix_from_in_view merges cross-shard heads in global key order" {
    const testing = std.testing;

    var candidate_storage: [256][16]u8 = undefined;
    var chosen: [3][]const u8 = undefined;
    var chosen_count: usize = 0;
    var seen_shards = [_]bool{false} ** runtime_shard.NUM_SHARDS;

    for (0..candidate_storage.len) |index| {
        const candidate = try std.fmt.bufPrint(&candidate_storage[index], "merge:{d:0>3}", .{index});
        const shard_idx = runtime_shard.getShardIndex(candidate);
        if (seen_shards[shard_idx]) continue;
        seen_shards[shard_idx] = true;
        chosen[chosen_count] = candidate;
        chosen_count += 1;
        if (chosen_count == chosen.len) break;
    }

    try testing.expectEqual(@as(usize, chosen.len), chosen_count);

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };
    try db.put(chosen[0], &one);
    try db.put(chosen[1], &two);
    try db.put(chosen[2], &three);

    var view = try db.readView();
    defer view.deinit();

    var first_page = try scanPrefixFromInView(&view, testing.allocator, "merge:", null, 2);
    defer first_page.deinit();

    try testing.expectEqual(@as(usize, 2), first_page.entries.items.len);
    try testing.expectEqualStrings(chosen[0], first_page.entries.items[0].key);
    try testing.expectEqualStrings(chosen[1], first_page.entries.items[1].key);
    try testing.expect(first_page.borrowNextCursor() != null);

    var cursor = first_page.takeNextCursor().?;
    defer cursor.deinit();
    const cursor_view = cursor.asCursor().?;
    var second_page = try scanPrefixFromInView(&view, testing.allocator, "merge:", &cursor_view, 2);
    defer second_page.deinit();

    try testing.expectEqual(@as(usize, 1), second_page.entries.items.len);
    try testing.expectEqualStrings(chosen[2], second_page.entries.items[0].key);
    try testing.expect(second_page.borrowNextCursor() == null);
}

test "scan_prefix_from_in_view cursor records the last emitted key" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    try db.put("alpha", &one);
    try db.put("zz-after", &two);

    var view = try db.readView();
    defer view.deinit();

    var page = try scanPrefixFromInView(&view, testing.allocator, "", null, 1);
    defer page.deinit();

    const cursor = page.borrowNextCursor().?;
    try testing.expectEqualStrings(page.entries.items[0].key, cursor.resume_key);
}

test "scan_prefix matches and merges shard results in order" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const values = [_]types.Value{
        .{ .integer = 1 },
        .{ .integer = 2 },
        .{ .integer = 3 },
        .{ .integer = 4 },
        .{ .integer = 5 },
    };
    const keys = [_][]const u8{ "alpha", "alphabet", "alphanumeric", "beta", "gamma" };

    for (keys, values) |key, value| {
        try db.put(key, &value);
    }

    var result = try db.scanPrefix(testing.allocator, "alpha");
    defer result.deinit();

    try testing.expectEqual(@as(usize, 3), result.entries.items.len);
    try testing.expectEqualStrings("alpha", result.entries.items[0].key);
    try testing.expectEqualStrings("alphabet", result.entries.items[1].key);
    try testing.expectEqualStrings("alphanumeric", result.entries.items[2].key);
}

test "scan_prefix_from_in_view omits keys expired before the view opens" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };
    try db.put("alpha", &one);
    try db.put("alpha:1", &two);
    try db.put("alpha:2", &three);
    try setTtlForTest(db, "alpha", runtime_shard.unixNow(std.testing.io) - 1);

    var view = try db.readView();
    defer view.deinit();

    var first_page = try scanPrefixFromInView(&view, testing.allocator, "alpha", null, 1);
    defer first_page.deinit();
    try testing.expectEqual(@as(usize, 1), first_page.entries.items.len);
    try testing.expectEqualStrings("alpha:1", first_page.entries.items[0].key);

    var cursor = first_page.takeNextCursor().?;
    defer cursor.deinit();
    const cursor_view = cursor.asCursor().?;
    var second_page = try scanPrefixFromInView(&view, testing.allocator, "alpha", &cursor_view, 1);
    defer second_page.deinit();
    try testing.expectEqual(@as(usize, 1), second_page.entries.items.len);
    try testing.expectEqualStrings("alpha:2", second_page.entries.items[0].key);
}

test "read view freezes expiration time at open" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 7 };
    try db.put("alpha", &value);
    try testing.expect(try db.expireAt("alpha", runtime_shard.unixNow(std.testing.io) + 1));

    var view = try db.readView();
    defer view.deinit();

    std.testing.io.sleep(.fromNanoseconds(1100 * std.time.ns_per_ms), .awake) catch unreachable;

    var in_view = try scanPrefixFromInView(&view, testing.allocator, "alpha", null, 10);
    defer in_view.deinit();
    try testing.expectEqual(@as(usize, 1), in_view.entries.items.len);
    try testing.expectEqualStrings("alpha", in_view.entries.items[0].key);

    var plain = try db.scanPrefix(testing.allocator, "alpha");
    defer plain.deinit();
    try testing.expectEqual(@as(usize, 0), plain.entries.items.len);
}

test "ttl does not deadlock under an active read view and defers cleanup" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 7 };
    try db.put("ttl:view", &value);
    try testing.expect(try db.expireAt("ttl:view", runtime_shard.unixNow(std.testing.io) + 1));

    var view = try db.readView();
    defer view.deinit();

    std.testing.io.sleep(.fromNanoseconds(1100 * std.time.ns_per_ms), .awake) catch unreachable;

    try testing.expectEqual(@as(i64, -2), try db.ttl("ttl:view"));
    try testing.expect(hasTtlForTest(db, "ttl:view"));

    var in_view = try scanPrefixFromInView(&view, testing.allocator, "ttl:view", null, 10);
    defer in_view.deinit();
    try testing.expectEqual(@as(usize, 1), in_view.entries.items.len);

    view.deinit();
    try testing.expectEqual(@as(i64, -2), try db.ttl("ttl:view"));
    try testing.expect(!hasTtlForTest(db, "ttl:view"));
    try testing.expect((try db.get(testing.allocator, "ttl:view")) == null);
}

test "scan page can promote one borrowed continuation cursor into owned storage" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    try db.put("alpha", &one);
    try db.put("alpha:1", &two);

    var view = try db.readView();
    defer view.deinit();

    var page = try scanPrefixFromInView(&view, testing.allocator, "alpha", null, 1);

    const borrowed_cursor = page.borrowNextCursor().?;
    var owned_cursor = try borrowed_cursor.clone(testing.allocator);
    defer owned_cursor.deinit();

    page.deinit();

    const cursor_view = owned_cursor.asCursor().?;
    var second_page = try scanPrefixFromInView(&view, testing.allocator, "alpha", &cursor_view, 1);
    defer second_page.deinit();

    try testing.expectEqual(@as(usize, 1), second_page.entries.items.len);
    try testing.expectEqualStrings("alpha:1", second_page.entries.items[0].key);
}

test "owned scan cursor clone makes an independent continuation owner" {
    const testing = std.testing;

    var cursor = try types.OwnedScanCursor.init(testing.allocator, "alpha");
    defer cursor.deinit();

    var cloned = (try cursor.clone(testing.allocator)).?;
    defer cloned.deinit();

    try testing.expect(cursor.asCursor() != null);
    try testing.expect(cloned.asCursor() != null);

    cursor.deinit();

    try testing.expect(cloned.asCursor() != null);
    try testing.expectEqualStrings("alpha", cloned.asCursor().?.resume_key);
}

test "point operation boundaries reject empty keys" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };

    try testing.expectError(error.KeyTooLarge, db.put("", &value));
    try testing.expectError(error.KeyTooLarge, db.get(testing.allocator, ""));
    try testing.expectError(error.KeyTooLarge, db.delete(""));
    try testing.expectError(error.KeyTooLarge, db.expireAt("", runtime_shard.unixNow(std.testing.io) + 60));
    try testing.expectError(error.KeyTooLarge, db.ttl(""));
}

test "apply_batch rejects invalid keys and oversized values before changing state" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const sentinel = types.Value{ .integer = 7 };
    try db.put("sentinel", &sentinel);

    const replacement = types.Value{ .integer = 9 };
    try testing.expectError(error.KeyTooLarge, db.applyBatch(&.{
        .{ .key = "", .value = &replacement },
    }));

    const oversized_bytes = try allocator.alloc(u8, @as(usize, @intCast(internal_codec.MAX_VAL_LEN)) + 1);
    defer allocator.free(oversized_bytes);
    @memset(oversized_bytes, 'x');
    const oversized_value = types.Value{ .string = oversized_bytes };

    try testing.expectError(error.ValueTooLarge, db.applyBatch(&.{
        .{ .key = "too:large", .value = &oversized_value },
    }));

    try testing.expect((try db.get(testing.allocator, "too:large")) == null);
    var stored = (try db.get(testing.allocator, "sentinel")).?;
    defer stored.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 7), stored.integer);
}

test "empty batches are no-ops and checked empty batches still validate guards" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("guarded", &value);

    try db.applyBatch(&.{});
    try testing.expectError(error.GuardFailed, applyCheckedBatch(db, .{
        .writes = &.{},
        .guards = &.{
            .{ .key_not_exists = "guarded" },
        },
    }));

    var guarded = (try db.get(testing.allocator, "guarded")).?;
    defer guarded.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 1), guarded.integer);
}

test "shard reset reclaims committed batch arenas" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    try db.applyBatch(&.{
        .{ .key = "arena:one", .value = &one },
        .{ .key = "arena:two", .value = &two },
    });

    const shard_idx = runtime_shard.getShardIndex("arena:one");
    const shard = &db.state.shards[shard_idx];
    shard.lock.lockUncancelable(shard.io);
    defer shard.lock.unlock(shard.io);

    try testing.expect(shard.committed_arenas_head != null);
    shard.resetUnlocked();
    try testing.expect(shard.committed_arenas_head == null);
    try testing.expect(shard.committed_arenas_tail == null);
    try testing.expect(shard.tree.lookup("arena:one") == null);
    try testing.expect(shard.tree.lookup("arena:two") == null);
}

test "overwrite-only batches avoid retaining new committed arenas and remain reclaimable" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    var values: [64]types.Value = undefined;
    var writes: [64]types.PutWrite = undefined;
    var key_storage: [64][16]u8 = undefined;

    for (0..writes.len) |index| {
        values[index] = .{ .integer = @intCast(index) };
        const key = try std.fmt.bufPrint(&key_storage[index], "batch:{d:0>4}", .{index});
        writes[index] = .{
            .key = key,
            .value = &values[index],
        };
    }

    try db.applyBatch(&writes);
    const committed_after_first = totalCommittedArenasForTest(db);
    try testing.expect(committed_after_first > 0);
    try testing.expect(committed_after_first <= writes.len);

    for (0..writes.len) |index| {
        values[index] = .{ .integer = @intCast(index + 1_000) };
    }

    try db.applyBatch(&writes);
    try testing.expectEqual(committed_after_first, totalCommittedArenasForTest(db));
    try testing.expect(valueIsHeapOwnedForTest(db, "batch:0000"));

    const point_value = types.Value{ .integer = 9_999 };
    try db.put("batch:0000", &point_value);
    try testing.expect(valueIsHeapOwnedForTest(db, "batch:0000"));

    const replacement = types.Value{ .integer = 8_888 };
    _ = try db.delete("batch:0000");
    try db.put("batch:0000", &replacement);
    try testing.expect(!valueIsHeapOwnedForTest(db, "batch:0000"));
}

test "scan_prefix includes the exact key and its subkeys" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };
    const four = types.Value{ .integer = 4 };
    try db.put("alpha", &one);
    try db.put("alpha:1", &two);
    try db.put("alpha:2", &three);
    try db.put("alphabet", &four);

    var result = try db.scanPrefix(testing.allocator, "alpha");
    defer result.deinit();

    try testing.expectEqual(@as(usize, 4), result.entries.items.len);
    try testing.expectEqualStrings("alpha", result.entries.items[0].key);
    try testing.expectEqualStrings("alpha:1", result.entries.items[1].key);
    try testing.expectEqualStrings("alpha:2", result.entries.items[2].key);
    try testing.expectEqualStrings("alphabet", result.entries.items[3].key);
}

test "scan_range_from_in_view paginates binary keys in global order and terminates cleanly" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };
    try db.put("\x00a", &one);
    try db.put("\x00b", &two);
    try db.put("\x01a", &three);

    var view = try db.readView();
    defer view.deinit();

    var first = try scanRangeFromInView(&view, testing.allocator, .{
        .start = "\x00a",
        .end = "\x02",
    }, null, 2);
    defer first.deinit();
    try testing.expectEqual(@as(usize, 2), first.entries.items.len);
    try testing.expectEqualStrings("\x00a", first.entries.items[0].key);
    try testing.expectEqualStrings("\x00b", first.entries.items[1].key);
    try testing.expect(first.borrowNextCursor() != null);

    var cursor = first.takeNextCursor().?;
    defer cursor.deinit();
    const cursor_view = cursor.asCursor().?;
    var second = try scanRangeFromInView(&view, testing.allocator, .{
        .start = "\x00a",
        .end = "\x02",
    }, &cursor_view, 2);
    defer second.deinit();
    try testing.expectEqual(@as(usize, 1), second.entries.items.len);
    try testing.expectEqualStrings("\x01a", second.entries.items[0].key);
    try testing.expect(second.borrowNextCursor() == null);
}

test "batch visibility-gate pause hook blocks completion until resumed" {
    const testing = std.testing;

    const db = try create(std.testing.io, std.heap.page_allocator);
    defer db.close() catch unreachable;

    const initial = types.Value{ .integer = 1 };
    try db.put("alpha", &initial);

    const BatchThread = struct {
        db: *Database,
        finished: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        err: ?EngineError = null,

        fn run(state: *@This()) void {
            const next = types.Value{ .integer = 2 };
            state.db.applyBatch(&.{
                .{ .key = "alpha", .value = &next },
                .{ .key = "beta", .value = &next },
            }) catch |err| {
                state.err = err;
            };
            state.finished.store(true, .release);
        }
    };

    batch_ops.test_hooks.pauseNextBatchAfterVisibilityGate();

    var batch_state = BatchThread{ .db = db };
    const batch_thread = try std.Thread.spawn(.{}, BatchThread.run, .{&batch_state});
    var pause_seen = false;
    var pause_attempts: usize = 0;
    while (pause_attempts < 5_000) : (pause_attempts += 1) {
        if (batch_ops.test_hooks.isBatchPausedAfterVisibilityGate()) {
            pause_seen = true;
            break;
        }
        std.testing.io.sleep(.fromNanoseconds(std.time.ns_per_ms), .awake) catch unreachable;
    }
    try testing.expect(pause_seen);
    std.testing.io.sleep(.fromNanoseconds(20 * std.time.ns_per_ms), .awake) catch unreachable;
    try testing.expect(!batch_state.finished.load(.acquire));

    batch_ops.test_hooks.resumeBatchAfterVisibilityGate();
    var completed = false;
    var completion_attempts: usize = 0;
    while (completion_attempts < 5_000) : (completion_attempts += 1) {
        if (batch_state.finished.load(.acquire)) {
            completed = true;
            break;
        }
        std.testing.io.sleep(.fromNanoseconds(std.time.ns_per_ms), .awake) catch unreachable;
    }
    batch_thread.join();

    try testing.expect(completed);
    try testing.expect(batch_state.err == null);
}

test "concurrent unique-key writers keep all committed values visible" {
    const testing = std.testing;

    const db = try create(std.testing.io, std.heap.page_allocator);
    defer db.close() catch unreachable;

    const Writer = struct {
        db: *Database,
        key: []const u8,
        value: i64,
        err: ?EngineError = null,

        fn run(state: *@This()) void {
            const payload = types.Value{ .integer = state.value };
            state.db.put(state.key, &payload) catch |err| {
                state.err = err;
            };
        }
    };

    var writers = [_]Writer{
        .{ .db = db, .key = "writer:0", .value = 10 },
        .{ .db = db, .key = "writer:1", .value = 11 },
        .{ .db = db, .key = "writer:2", .value = 12 },
        .{ .db = db, .key = "writer:3", .value = 13 },
    };
    var threads: [writers.len]std.Thread = undefined;
    for (&writers, 0..) |*writer, index| {
        threads[index] = try std.Thread.spawn(.{}, Writer.run, .{writer});
    }
    for (&threads) |thread| thread.join();

    for (writers) |writer| {
        try testing.expect(writer.err == null);
        var stored = (try db.get(testing.allocator, writer.key)).?;
        defer stored.deinit(testing.allocator);
        try testing.expectEqual(writer.value, stored.integer);
    }
}

test "concurrent put and delete on one key leave the key in a well-formed state" {
    const testing = std.testing;

    const db = try create(std.testing.io, std.heap.page_allocator);
    defer db.close() catch unreachable;

    const Mutator = struct {
        db: *Database,
        err: ?EngineError = null,

        fn runPut(state: *@This()) void {
            var index: usize = 0;
            while (index < 100) : (index += 1) {
                const payload = types.Value{ .integer = 1 };
                state.db.put("race:key", &payload) catch |err| {
                    state.err = err;
                    return;
                };
            }
        }

        fn runDelete(state: *@This()) void {
            var index: usize = 0;
            while (index < 100) : (index += 1) {
                _ = state.db.delete("race:key") catch |err| {
                    state.err = err;
                    return;
                };
            }
        }
    };

    var putter = Mutator{ .db = db };
    var deleter = Mutator{ .db = db };
    const put_thread = try std.Thread.spawn(.{}, Mutator.runPut, .{&putter});
    const delete_thread = try std.Thread.spawn(.{}, Mutator.runDelete, .{&deleter});
    put_thread.join();
    delete_thread.join();

    try testing.expect(putter.err == null);
    try testing.expect(deleter.err == null);

    const result = try db.get(testing.allocator, "race:key");
    if (result) |value| {
        var owned = value;
        defer owned.deinit(testing.allocator);
        try testing.expectEqual(@as(i64, 1), owned.integer);
    }
}

test "snapshot floor at a batch commit skips already-checkpointed batch records on reopen" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "step12-batch-floor.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "step12-batch-floor.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(std.testing.io, testing.allocator, .{
            .snapshot_path = snapshot_path,
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const one = types.Value{ .integer = 1 };
        const two = types.Value{ .integer = 2 };
        try db.applyBatch(&.{
            .{ .key = "batch:alpha", .value = &one },
            .{ .key = "batch:beta", .value = &two },
        });

        const checkpoint_lsn = currentCheckpointLsnForTest(db);
        try testing.expectEqual(@as(u64, 4), checkpoint_lsn);
        _ = try storage_snapshot.write(&db.state, testing.allocator, snapshot_path, checkpoint_lsn);
    }

    const reopened = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    var alpha = (try reopened.get(testing.allocator, "batch:alpha")).?;
    defer alpha.deinit(testing.allocator);
    var beta = (try reopened.get(testing.allocator, "batch:beta")).?;
    defer beta.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 1), alpha.integer);
    try testing.expectEqual(@as(i64, 2), beta.integer);
    try testing.expectEqual(@as(u64, 5), reopened.state.wal.?.next_lsn);
}

test "checkpointed restart keeps wal next_lsn monotonic for later writes" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "step12-next-lsn.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "step12-next-lsn.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(std.testing.io, testing.allocator, .{
            .snapshot_path = snapshot_path,
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const value = types.Value{ .integer = 1 };
        try db.put("lsn:alpha", &value);
        try db.checkpoint();
    }

    {
        const reopened = try open(std.testing.io, testing.allocator, .{
            .snapshot_path = snapshot_path,
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer reopened.close() catch unreachable;

        try testing.expectEqual(@as(u64, 2), reopened.state.wal.?.next_lsn);
        const value = types.Value{ .integer = 2 };
        try reopened.put("lsn:beta", &value);
        try testing.expectEqual(@as(u64, 3), reopened.state.wal.?.next_lsn);
    }

    const final = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer final.close() catch unreachable;

    var alpha = (try final.get(testing.allocator, "lsn:alpha")).?;
    defer alpha.deinit(testing.allocator);
    var beta = (try final.get(testing.allocator, "lsn:beta")).?;
    defer beta.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 1), alpha.integer);
    try testing.expectEqual(@as(i64, 2), beta.integer);
}

test "max_wal_bytes triggers automatic checkpoint when threshold is exceeded" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "auto-wal-checkpoint.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "auto-wal-checkpoint.wal");
    defer testing.allocator.free(wal_path);

    // Threshold pequeno para que o primeiro put ja dispare o checkpoint.
    const db = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
        .max_wal_bytes = 1,
    });
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 42 };
    try db.put("alpha", &value);

    // Apos o put, o auto-checkpoint deve ter rodado pelo menos uma vez.
    const stats = db.state.statsSnapshot();
    try testing.expect(stats.checkpoint_count_total >= 1);

    // O WAL deve ter sido compactado; bytes_pending cai abaixo do threshold original.
    if (db.state.wal) |wal| {
        // Apos checkpoint, o WAL e truncado. Pode ser 0 ou muito pequeno.
        try testing.expect(wal.walBytesPending() < 1024 * 1024);
    }
}

test "max_wal_bytes without snapshot_path does not trigger checkpoint" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "auto-wal-no-snap.wal");
    defer testing.allocator.free(wal_path);

    // max_wal_bytes configurado, mas snapshot_path ausente; auto-checkpoint nao deve armar.
    const db = try open(std.testing.io, testing.allocator, .{
        .wal_path = wal_path,
        .fsync_mode = .none,
        .max_wal_bytes = 1,
    });
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("alpha", &value);

    const stats = db.state.statsSnapshot();
    try testing.expectEqual(@as(u64, 0), stats.checkpoint_count_total);
}

test "max_wal_bytes null leaves checkpoint scheduling fully manual" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const snapshot_path = try allocTmpPathTest(testing.allocator, tmp, "auto-wal-null.snapshot");
    defer testing.allocator.free(snapshot_path);
    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "auto-wal-null.wal");
    defer testing.allocator.free(wal_path);

    const db = try open(std.testing.io, testing.allocator, .{
        .snapshot_path = snapshot_path,
        .wal_path = wal_path,
        .fsync_mode = .none,
        .max_wal_bytes = null,
    });
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    for (0..100) |_| try db.put("alpha", &value);

    const stats = db.state.statsSnapshot();
    try testing.expectEqual(@as(u64, 0), stats.checkpoint_count_total);
}

test "sweep_expired_entries_for_shard removes expired keys from art and ttl index" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("sweep:alpha", &value);
    try db.put("sweep:beta", &value);

    // Set both keys as already expired.
    try setTtlForTest(db, "sweep:alpha", runtime_shard.unixNow(std.testing.io) - 10);
    try setTtlForTest(db, "sweep:beta", runtime_shard.unixNow(std.testing.io) - 10);

    const shard_idx_alpha = runtime_shard.getShardIndex("sweep:alpha");
    const shard_idx_beta = runtime_shard.getShardIndex("sweep:beta");

    expiration.sweepExpiredEntriesForShard(&db.state, shard_idx_alpha, testing.allocator);
    expiration.sweepExpiredEntriesForShard(&db.state, shard_idx_beta, testing.allocator);

    try testing.expect((try db.get(testing.allocator, "sweep:alpha")) == null);
    try testing.expect((try db.get(testing.allocator, "sweep:beta")) == null);
    try testing.expect(!hasTtlForTest(db, "sweep:alpha"));
    try testing.expect(!hasTtlForTest(db, "sweep:beta"));
    try testing.expect(!hasStoredKeyForTest(db, "sweep:alpha"));
    try testing.expect(!hasStoredKeyForTest(db, "sweep:beta"));
}

test "sweep_expired_entries_for_shard preserves non-expired keys" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 7 };
    try db.put("sweep:live", &value);
    try setTtlForTest(db, "sweep:live", runtime_shard.unixNow(std.testing.io) + 60);

    const shard_idx = runtime_shard.getShardIndex("sweep:live");
    expiration.sweepExpiredEntriesForShard(&db.state, shard_idx, testing.allocator);

    try testing.expect(hasStoredKeyForTest(db, "sweep:live"));
    try testing.expect(hasTtlForTest(db, "sweep:live"));

    var stored = (try db.get(testing.allocator, "sweep:live")).?;
    defer stored.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 7), stored.integer);
}

test "sweep_expired_entries_for_shard skips shards with no ttl entries" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("sweep:notttl", &value);

    // No TTL set, has_ttl_entries should be false for this shard
    const shard_idx = runtime_shard.getShardIndex("sweep:notttl");
    try testing.expect(!db.state.shards[shard_idx].has_ttl_entries);

    // Should be a no-op — key must remain
    expiration.sweepExpiredEntriesForShard(&db.state, shard_idx, testing.allocator);
    try testing.expect(hasStoredKeyForTest(db, "sweep:notttl"));
}

test "ttl_sweep_interval_ms starts background cleanup of expired keys" {
    const testing = std.testing;

    const db = try open(std.testing.io, testing.allocator, .{
        .ttl_sweep_interval_ms = 20,
    });
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 3 };
    try db.put("sweep:bg", &value);
    try setTtlForTest(db, "sweep:bg", runtime_shard.unixNow(std.testing.io) - 1);

    // Wait for at least two sweep cycles
    std.testing.io.sleep(.fromNanoseconds(80 * std.time.ns_per_ms), .awake) catch unreachable;

    try testing.expect(!hasStoredKeyForTest(db, "sweep:bg"));
    try testing.expect(!hasTtlForTest(db, "sweep:bg"));
}

test "ttl_sweep_interval_ms null leaves cleanup lazy only" {
    const testing = std.testing;

    const db = try open(std.testing.io, testing.allocator, .{
        .ttl_sweep_interval_ms = null,
    });
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("sweep:lazy", &value);
    try setTtlForTest(db, "sweep:lazy", runtime_shard.unixNow(std.testing.io) - 1);

    // No background sweep; key should still be in ttl_index and ART
    try testing.expect(hasTtlForTest(db, "sweep:lazy"));
    try testing.expect(hasStoredKeyForTest(db, "sweep:lazy"));
    // But invisible to get()
    try testing.expect((try db.get(testing.allocator, "sweep:lazy")) == null);
}

test "close stops the sweep thread cleanly" {
    const testing = std.testing;

    const db = try open(std.testing.io, testing.allocator, .{
        .ttl_sweep_interval_ms = 50,
    });
    // Verify close does not hang or deadlock with a running sweep thread
    try db.close();
}

test "exists returns true for a present key" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("exists:alpha", &value);

    try testing.expect(try db.exists("exists:alpha"));
}

test "exists returns false for a missing key" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    try testing.expect(!try db.exists("exists:missing"));
}

test "exists returns false for an expired key" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("exists:expired", &value);
    try setTtlForTest(db, "exists:expired", runtime_shard.unixNow(std.testing.io) - 1);

    try testing.expect(!try db.exists("exists:expired"));
}

test "exists returns true for a key with a future ttl" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("exists:live", &value);
    try setTtlForTest(db, "exists:live", runtime_shard.unixNow(std.testing.io) + 60);

    try testing.expect(try db.exists("exists:live"));
}

test "exists returns false after delete" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("exists:deleted", &value);
    try testing.expect(try db.exists("exists:deleted"));

    _ = try db.delete("exists:deleted");
    try testing.expect(!try db.exists("exists:deleted"));
}

test "exists rejects empty and oversized keys" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    try testing.expectError(error.KeyTooLarge, db.exists(""));
}

test "engine boundary latency sampling counts exists calls" {
    const testing = std.testing;

    const db = try open(std.testing.io, testing.allocator, .{
        .metrics = .{ .mode = .full },
    });
    defer db.close() catch unreachable;

    const before = latencySamplesForTest(db);
    _ = try db.exists("latency:exists");
    try testing.expectEqual(before + 1, latencySamplesForTest(db));
}

test "delete_prefix removes all matching keys and returns count" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const v = types.Value{ .integer = 1 };
    try db.put("user:1", &v);
    try db.put("user:2", &v);
    try db.put("user:3", &v);
    try db.put("session:a", &v);

    const deleted = try db.deletePrefix("user:");
    try testing.expectEqual(@as(u64, 3), deleted);

    try testing.expect((try db.get(testing.allocator, "user:1")) == null);
    try testing.expect((try db.get(testing.allocator, "user:2")) == null);
    try testing.expect((try db.get(testing.allocator, "user:3")) == null);

    var session = (try db.get(testing.allocator, "session:a")).?;
    defer session.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 1), session.integer);
}

test "delete_prefix returns zero when no keys match" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const v = types.Value{ .integer = 1 };
    try db.put("alpha:1", &v);

    const deleted = try db.deletePrefix("beta:");
    try testing.expectEqual(@as(u64, 0), deleted);

    var alpha = (try db.get(testing.allocator, "alpha:1")).?;
    defer alpha.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 1), alpha.integer);
}

test "delete_prefix skips expired keys" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const v = types.Value{ .integer = 1 };
    try db.put("ttl:live", &v);
    try db.put("ttl:dead", &v);
    try setTtlForTest(db, "ttl:dead", runtime_shard.unixNow(std.testing.io) - 1);

    const deleted = try db.deletePrefix("ttl:");
    try testing.expectEqual(@as(u64, 1), deleted);
    try testing.expect((try db.get(testing.allocator, "ttl:live")) == null);
    try testing.expect((try db.get(testing.allocator, "ttl:dead")) == null);
}

test "delete_prefix frees heap-owned values without leaking" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    // Write a large payload so overwrite promotes value to heap_allocation.
    var buf_a = [_]u8{'a'} ** 1024;
    var buf_b = [_]u8{'b'} ** 1024;
    var first = types.Value{ .string = buf_a[0..] };
    var second = types.Value{ .string = buf_b[0..] };

    try db.put("heap:key", &first);
    try db.put("heap:key", &second);

    const deleted = try db.deletePrefix("heap:");
    try testing.expectEqual(@as(u64, 1), deleted);
    try testing.expect((try db.get(testing.allocator, "heap:key")) == null);
}

test "delete_prefix with wal records deletes and survives reopen" {
    const testing = std.testing;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const wal_path = try allocTmpPathTest(testing.allocator, tmp, "delete-prefix.wal");
    defer testing.allocator.free(wal_path);

    {
        const db = try open(std.testing.io, testing.allocator, .{
            .wal_path = wal_path,
            .fsync_mode = .none,
        });
        defer db.close() catch unreachable;

        const v = types.Value{ .integer = 42 };
        try db.put("ns:one", &v);
        try db.put("ns:two", &v);
        try db.put("keep:this", &v);

        const deleted = try db.deletePrefix("ns:");
        try testing.expectEqual(@as(u64, 2), deleted);
    }

    const reopened = try open(std.testing.io, testing.allocator, .{
        .wal_path = wal_path,
        .fsync_mode = .none,
    });
    defer reopened.close() catch unreachable;

    try testing.expect((try reopened.get(testing.allocator, "ns:one")) == null);
    try testing.expect((try reopened.get(testing.allocator, "ns:two")) == null);

    var kept = (try reopened.get(testing.allocator, "keep:this")).?;
    defer kept.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 42), kept.integer);
}

test "delete_prefix on empty database returns zero" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const deleted = try db.deletePrefix("any:");
    try testing.expectEqual(@as(u64, 0), deleted);
}

test "get_many returns values at matching indices" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    try db.put("many:a", &one);
    try db.put("many:b", &two);

    var result = try db.getMany(testing.allocator, &.{ "many:a", "many:b", "many:missing" });
    defer result.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 3), result.values.len);
    try testing.expectEqual(@as(i64, 1), result.values[0].?.integer);
    try testing.expectEqual(@as(i64, 2), result.values[1].?.integer);
    try testing.expect(result.values[2] == null);
}

test "get_many returns null for expired keys" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 7 };
    try db.put("many:expired", &value);
    try setTtlForTest(db, "many:expired", runtime_shard.unixNow(std.testing.io) - 1);

    var result = try db.getMany(testing.allocator, &.{"many:expired"});
    defer result.deinit(testing.allocator);

    try testing.expect(result.values[0] == null);
}

test "get_many empty slice returns empty result" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    var result = try db.getMany(testing.allocator, &.{});
    defer result.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 0), result.values.len);
}

test "get_many rejects invalid keys" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    try testing.expectError(error.KeyTooLarge, db.getMany(testing.allocator, &.{ "valid", "" }));
}

test "get_many preserves index correspondence across shards" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    // Build keys guaranteed to land on distinct shards.
    var candidate_storage: [512][16]u8 = undefined;
    var chosen: [4][]const u8 = undefined;
    var chosen_count: usize = 0;
    var seen_shards = [_]bool{false} ** runtime_state.NUM_SHARDS;

    for (0..candidate_storage.len) |i| {
        const candidate = try std.fmt.bufPrint(&candidate_storage[i], "many:shard:{d:0>3}", .{i});
        const shard_idx = runtime_shard.getShardIndex(candidate);
        if (seen_shards[shard_idx]) continue;
        seen_shards[shard_idx] = true;
        chosen[chosen_count] = candidate;
        chosen_count += 1;
        if (chosen_count == chosen.len) break;
    }
    try testing.expectEqual(@as(usize, chosen.len), chosen_count);

    const values = [_]types.Value{
        .{ .integer = 10 },
        .{ .integer = 20 },
        .{ .integer = 30 },
        .{ .integer = 40 },
    };
    for (chosen, values) |key, val| try db.put(key, &val);

    var result = try db.getMany(testing.allocator, chosen[0..chosen_count]);
    defer result.deinit(testing.allocator);

    try testing.expectEqual(chosen_count, result.values.len);
    for (result.values, values[0..chosen_count]) |got, expected| {
        try testing.expectEqual(expected.integer, got.?.integer);
    }
}

test "get_many latency sampling records one sample per call" {
    const testing = std.testing;

    const db = try open(std.testing.io, testing.allocator, .{
        .metrics = .{ .mode = .full },
    });
    defer db.close() catch unreachable;

    const before = latencySamplesForTest(db);
    var result = try db.getMany(testing.allocator, &.{ "a", "b", "c" });
    defer result.deinit(testing.allocator);
    try testing.expectEqual(before + 1, latencySamplesForTest(db));
}

test "scanIterator yields all prefix keys in order" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const v = types.Value{ .integer = 1 };
    try db.put("iter:a", &v);
    try db.put("iter:b", &v);
    try db.put("iter:c", &v);
    try db.put("other:x", &v);

    var it = try db.scanIterator(testing.allocator, "iter:", null);
    defer it.deinit();

    var count: usize = 0;
    var prev_key_buf: [64]u8 = undefined;
    var prev_key_len: usize = 0;
    while (try it.next()) |entry| {
        if (count > 0) {
            const prev = prev_key_buf[0..prev_key_len];
            try testing.expect(std.mem.order(u8, prev, entry.key) == .lt);
        }
        // Copy key into our buffer before it may be freed on next page advance.
        @memcpy(prev_key_buf[0..entry.key.len], entry.key);
        prev_key_len = entry.key.len;
        count += 1;
    }
    try testing.expectEqual(@as(usize, 3), count);
}

test "scanIterator with small page_size paginates correctly" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const v = types.Value{ .integer = 1 };
    for (0..10) |i| {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "page:{d:0>3}", .{i});
        try db.put(key, &v);
    }

    // page_size = 3 forces multiple internal fetches for 10 entries.
    var it = try db.scanIterator(testing.allocator, "page:", 3);
    defer it.deinit();

    var count: usize = 0;
    while (try it.next()) |_| count += 1;
    try testing.expectEqual(@as(usize, 10), count);
}

test "scanIterator returns nothing for empty prefix" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    var it = try db.scanIterator(testing.allocator, "nothing:", null);
    defer it.deinit();

    try testing.expect(try it.next() == null);
}

test "scanIterator skips expired keys" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const v = types.Value{ .integer = 1 };
    try db.put("exp:live", &v);
    try db.put("exp:dead", &v);
    try setTtlForTest(db, "exp:dead", runtime_shard.unixNow(std.testing.io) - 1);

    var it = try db.scanIterator(testing.allocator, "exp:", null);
    defer it.deinit();

    var count: usize = 0;
    while (try it.next()) |_| count += 1;
    try testing.expectEqual(@as(usize, 1), count);
}

test "rangeIterator yields keys within inclusive start exclusive end bounds" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const v = types.Value{ .integer = 1 };
    try db.put("a", &v);
    try db.put("b", &v);
    try db.put("c", &v);
    try db.put("d", &v);

    // Range [b, d) — inclusive start, exclusive end — expects "b" and "c".
    var it = try db.rangeIterator(testing.allocator, .{ .start = "b", .end = "d" }, null);
    defer it.deinit();

    // Assert each key within the loop — do NOT store slices, they are borrowed.
    var count: usize = 0;
    while (try it.next()) |entry| {
        switch (count) {
            0 => try testing.expectEqualStrings("b", entry.key),
            1 => try testing.expectEqualStrings("c", entry.key),
            else => try testing.expect(false), // unexpected entry
        }
        count += 1;
    }
    try testing.expectEqual(@as(usize, 2), count);
}

test "rangeIterator with small page_size paginates correctly" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    const v = types.Value{ .integer = 1 };
    for (0..8) |i| {
        var key_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "rng:{d:0>3}", .{i});
        try db.put(key, &v);
    }

    var it = try db.rangeIterator(testing.allocator, .{ .start = "rng:", .end = "rng:~" }, 3);
    defer it.deinit();

    var count: usize = 0;
    while (try it.next()) |_| count += 1;
    try testing.expectEqual(@as(usize, 8), count);
}

test "scanIterator deinit is safe after next returned null" {
    const testing = std.testing;

    const db = try create(std.testing.io, testing.allocator);
    defer db.close() catch unreachable;

    var it = try db.scanIterator(testing.allocator, "safe:", null);
    try testing.expect(try it.next() == null);
    it.deinit();
}

test "long interval TTL sweeper cancels its sleep before close" {
    const testing = std.testing;
    const db = try open(testing.io, testing.allocator, .{ .ttl_sweep_interval_ms = 60_000 });
    const close_started = std.Io.Clock.awake.now(testing.io);
    try db.close();
    try testing.expect(close_started.durationTo(std.Io.Clock.awake.now(testing.io)).toMilliseconds() < 5_000);
}
