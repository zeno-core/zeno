//! Benchmark entrypoint for coarse zeno-core engine performance checks.
//! Cost: Bench-dependent and intentionally end-to-end across public engine boundaries.
//! Allocator: Uses the benchmark-provided allocator for caller-owned result teardown and a process-wide page allocator for steady-state engine fixtures.

const std = @import("std");

var bench_io: std.Io = undefined;
const zbench = @import("zbench");
const zeno = @import("zeno");

const engine = zeno.public;
const official = zeno.official;
const types = zeno.types;
const internal = zeno.testing_internal;
const FailingAllocator = std.testing.FailingAllocator;

const scan_item_count: usize = 256;
const scan_large_item_count: usize = 4096;
const scan_page_item_count: usize = 64;
const batch_item_count: usize = 64;
const batch_key_storage_bytes: usize = 32;
const put_overwrite_key_cardinality: usize = 64;
const put_steady_key_cardinality: usize = 1_000;
const get_steady_key_cardinality: usize = 1_000;
const heavy_overwrite_payload_bytes: usize = 1024;
const heavy_overwrite_manual_compact_interval: usize = 1024;
const put_group_item_count: usize = 16;
const wal_group_item_count: usize = 16;
const wal_group_keys = [_][]const u8{
    "bench:wal:00",
    "bench:wal:01",
    "bench:wal:02",
    "bench:wal:03",
    "bench:wal:04",
    "bench:wal:05",
    "bench:wal:06",
    "bench:wal:07",
    "bench:wal:08",
    "bench:wal:09",
    "bench:wal:10",
    "bench:wal:11",
    "bench:wal:12",
    "bench:wal:13",
    "bench:wal:14",
    "bench:wal:15",
};

var bench_metrics_config: types.MetricsConfig = types.defaultMetricsConfig();
var steady_put_db: ?*engine.Database = null;
var steady_put_heavy_manual_db: ?*engine.Database = null;
var steady_get_db: ?*engine.Database = null;
var steady_get_ttl_mixed_db: ?*engine.Database = null;
var steady_scan_db: ?*engine.Database = null;
var steady_scan_view: ?types.ReadView = null;
var steady_scan_large_db: ?*engine.Database = null;
var steady_scan_large_view: ?types.ReadView = null;
var steady_batch_overwrite_db: ?*engine.Database = null;
var steady_batch_insert_db: ?*engine.Database = null;
var steady_checked_batch_overwrite_db: ?*engine.Database = null;
var steady_checked_batch_insert_db: ?*engine.Database = null;
var steady_put_uniform_seed = std.atomic.Value(usize).init(0);
var steady_get_uniform_seed = std.atomic.Value(usize).init(0);
var steady_batch_insert_seed = std.atomic.Value(usize).init(0);
var steady_checked_batch_insert_seed = std.atomic.Value(usize).init(0);
var steady_put_overwrite_seed = std.atomic.Value(usize).init(0);
var steady_put_overwrite_heavy_seed = std.atomic.Value(usize).init(0);
var steady_put_overwrite_heavy_manual_seed = std.atomic.Value(usize).init(0);
var steady_put_overwrite_heavy_manual_ops = std.atomic.Value(usize).init(0);
var steady_get_ttl_mixed_seed = std.atomic.Value(usize).init(0);
var art_lookup_seed = std.atomic.Value(usize).init(0);
var art_insert_seed = std.atomic.Value(usize).init(0);

var steady_art_tree: ?internal.art.Tree = null;
var steady_wal: ?internal.wal.Wal = null;
var wal_bench_path: ?[]const u8 = null;

const BenchCliConfig = struct {
    run_all_metrics_modes: bool = false,
    scan_allocation_profile: bool = false,
    scan_candidate_profile: bool = false,
    heavy_overwrite_profile: bool = false,
    metrics_config: types.MetricsConfig = types.defaultMetricsConfig(),
};

const default_sampled_latency_shift: u8 = 10;

const ScanProfileMode = enum {
    public_full,
    in_view_page,
};

const ScanAllocationProfile = struct {
    emitted_entries: usize,
    has_next_cursor: bool,
    allocations: usize,
    deallocations: usize,
    allocated_bytes: usize,
    freed_bytes: usize,
};

const ScanCandidateProfile = struct {
    emitted_entries: usize,
    allocations: usize,
    deallocations: usize,
    allocated_bytes: usize,
    freed_bytes: usize,
    elapsed_ns: u64,
};

const HeavyOverwriteProfile = struct {
    elapsed_ns: u64,
    p50_ns: u64,
    p99_ns: u64,
    max_ns: u64,
    overwritten_heavy_bytes_total: u64,
    retained_heavy_bytes_estimate: u64,
};

fn openBenchDb(allocator: std.mem.Allocator) !*engine.Database {
    return engine.open(bench_io, allocator, .{
        .metrics = bench_metrics_config,
    });
}

const PutFreshBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = openBenchDb(allocator) catch unreachable;
        defer db.close() catch unreachable;

        const value = types.Value{ .integer = 1 };
        db.put("bench:put", &value) catch unreachable;
    }
};

const PutSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_put_db orelse unreachable;
        const key_index = steady_put_uniform_seed.fetchAdd(1, .monotonic) % put_steady_key_cardinality;
        var key_buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "bench:put:uniform:{d:0>4}", .{key_index}) catch unreachable;
        const value = types.Value{ .integer = @intCast(key_index) };
        db.put(key, &value) catch unreachable;
    }
};

const PutSteadyOverwriteCardinalityBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_put_db orelse unreachable;
        const key_index = steady_put_overwrite_seed.fetchAdd(1, .monotonic) % put_overwrite_key_cardinality;

        var key_buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "bench:put:ovr:{d:0>2}", .{key_index}) catch unreachable;
        const value = types.Value{ .integer = @intCast(key_index) };
        db.put(key, &value) catch unreachable;
    }
};

const PutSteadyOverwriteHeavyCardinalityBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_put_db orelse unreachable;
        const key_index = steady_put_overwrite_heavy_seed.fetchAdd(1, .monotonic) % put_overwrite_key_cardinality;

        var key_buf: [40]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "bench:put:ovrheavy:{d:0>2}", .{key_index}) catch unreachable;

        var payload: [heavy_overwrite_payload_bytes]u8 = undefined;
        @memset(payload[0..], @as(u8, 'a' + @as(u8, @intCast(key_index % 26))));
        const value = types.Value{ .string = payload[0..] };
        db.put(key, &value) catch unreachable;
    }
};

const PutSteadyOverwriteHeavyManualCompactBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_put_heavy_manual_db orelse unreachable;
        const key_index = steady_put_overwrite_heavy_manual_seed.fetchAdd(1, .monotonic) % put_overwrite_key_cardinality;

        var key_buf: [40]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "bench:put:ovrheavy:{d:0>2}", .{key_index}) catch unreachable;

        var payload: [heavy_overwrite_payload_bytes]u8 = undefined;
        @memset(payload[0..], @as(u8, 'a' + @as(u8, @intCast(key_index % 26))));
        const value = types.Value{ .string = payload[0..] };
        db.put(key, &value) catch unreachable;

        const op_index = steady_put_overwrite_heavy_manual_ops.fetchAdd(1, .monotonic) + 1;
        if ((op_index % heavy_overwrite_manual_compact_interval) == 0) {
            const shard_idx = internal.runtime_shard.getShardIndex(key);
            db.compactShard(shard_idx) catch unreachable;
        }
    }
};

const PutGroupSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_put_db orelse unreachable;

        var values: [put_group_item_count]types.Value = undefined;
        var writes: [put_group_item_count]types.PutWrite = undefined;
        var key_storage: [put_group_item_count][24]u8 = undefined;

        for (0..put_group_item_count) |i| {
            values[i] = .{ .integer = @intCast(i) };
            const key = std.fmt.bufPrint(&key_storage[i], "bench:put:group:{d:0>2}", .{i}) catch unreachable;
            writes[i] = .{ .key = key, .value = &values[i] };
        }

        db.putGroup(&writes) catch unreachable;
    }
};

const GetExistingBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = openBenchDb(allocator) catch unreachable;
        defer db.close() catch unreachable;

        const value = types.Value{ .integer = 42 };
        db.put("bench:get", &value) catch unreachable;

        var stored = (db.get(allocator, "bench:get") catch unreachable).?;
        defer stored.deinit(allocator);
        std.mem.doNotOptimizeAway(stored.integer);
    }
};

const GetExistingSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = steady_get_db orelse unreachable;
        const key_index = steady_get_uniform_seed.fetchAdd(1, .monotonic) % get_steady_key_cardinality;
        var key_buf: [32]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "bench:get:uniform:{d:0>4}", .{key_index}) catch unreachable;
        var stored = (db.get(allocator, key) catch unreachable).?;
        defer stored.deinit(allocator);
        std.mem.doNotOptimizeAway(stored.integer);
    }
};

const GetExistingSteadyTtlMixedBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = steady_get_ttl_mixed_db orelse unreachable;
        const key_idx = steady_get_ttl_mixed_seed.fetchAdd(1, .monotonic) % 10;

        const key = switch (key_idx) {
            0 => "bench:get:ttl-mixed:0",
            1 => "bench:get:ttl-mixed:1",
            2 => "bench:get:ttl-mixed:2",
            3 => "bench:get:ttl-mixed:3",
            4 => "bench:get:ttl-mixed:4",
            5 => "bench:get:ttl-mixed:5",
            6 => "bench:get:ttl-mixed:6",
            7 => "bench:get:ttl-mixed:7",
            8 => "bench:get:ttl-mixed:8",
            9 => "bench:get:ttl-mixed:9",
            else => unreachable,
        };

        var stored = (db.get(allocator, key) catch unreachable).?;
        defer stored.deinit(allocator);
        std.mem.doNotOptimizeAway(stored.integer);
    }
};

const ScanPrefixBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = openBenchDb(allocator) catch unreachable;
        defer db.close() catch unreachable;

        loadScanFixture(scan_item_count, db);

        var result = db.scanPrefix(allocator, "scan:") catch unreachable;
        defer result.deinit();
        std.mem.doNotOptimizeAway(result.entries.items.len);
    }
};

const ScanPrefixSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = steady_scan_db orelse unreachable;
        var result = db.scanPrefix(allocator, "scan:") catch unreachable;
        defer result.deinit();
        std.mem.doNotOptimizeAway(result.entries.items.len);
    }
};

const ScanPrefixInViewSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const view = if (steady_scan_view) |*stored| stored else unreachable;
        var page = official.scanPrefixFromInView(view, allocator, "scan:", null, scan_page_item_count) catch unreachable;
        defer page.deinit();
        std.mem.doNotOptimizeAway(page.entries.items.len);
    }
};

const ScanPrefixLargeSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = steady_scan_large_db orelse unreachable;
        var result = db.scanPrefix(allocator, "scan:") catch unreachable;
        defer result.deinit();
        std.mem.doNotOptimizeAway(result.entries.items.len);
    }
};

const ScanPrefixLargeInViewSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const view = if (steady_scan_large_view) |*stored| stored else unreachable;
        var page = official.scanPrefixFromInView(view, allocator, "scan:", null, scan_page_item_count) catch unreachable;
        defer page.deinit();
        std.mem.doNotOptimizeAway(page.entries.items.len);
    }
};

const ApplyBatchBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = openBenchDb(allocator) catch unreachable;
        defer db.close() catch unreachable;

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fillBatchWrites(&values, &writes, &key_storage, "batch", 0, 0);

        db.applyBatch(&writes) catch unreachable;
    }
};

const ApplyBatchSteadyOverwriteBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_batch_overwrite_db orelse unreachable;

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fillBatchWrites(&values, &writes, &key_storage, "batch", 1_000, 0);

        db.applyBatch(&writes) catch unreachable;
    }
};

const ApplyBatchSteadyInsertBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_batch_insert_db orelse unreachable;
        const key_base = steady_batch_insert_seed.fetchAdd(batch_item_count, .monotonic);

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fillBatchWrites(&values, &writes, &key_storage, "batchi", 10_000, key_base);

        db.applyBatch(&writes) catch unreachable;
    }
};

const ApplyCheckedBatchBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = openBenchDb(allocator) catch unreachable;
        defer db.close() catch unreachable;

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fillBatchWrites(&values, &writes, &key_storage, "guard", 0, 0);

        official.applyCheckedBatch(db, .{
            .writes = &writes,
            .guards = &.{},
        }) catch unreachable;
    }
};

const ApplyCheckedBatchSteadyOverwriteBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_checked_batch_overwrite_db orelse unreachable;

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fillBatchWrites(&values, &writes, &key_storage, "guard", 2_000, 0);

        official.applyCheckedBatch(db, .{
            .writes = &writes,
            .guards = &.{},
        }) catch unreachable;
    }
};

const ApplyCheckedBatchSteadyInsertBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_checked_batch_insert_db orelse unreachable;
        const key_base = steady_checked_batch_insert_seed.fetchAdd(batch_item_count, .monotonic);

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fillBatchWrites(&values, &writes, &key_storage, "guardi", 20_000, key_base);

        official.applyCheckedBatch(db, .{
            .writes = &writes,
            .guards = &.{},
        }) catch unreachable;
    }
};

const ArtLookupBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const tree = if (steady_art_tree) |*t| t else unreachable;
        const idx = art_lookup_seed.fetchAdd(1, .monotonic) % scan_item_count;
        var key_buf: [16]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "scan:{d:0>4}", .{idx}) catch unreachable;
        const value = tree.lookup(key) orelse unreachable;
        std.mem.doNotOptimizeAway(value.integer);
    }
};

const ArtInsertBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const tree = if (steady_art_tree) |*t| t else unreachable;
        const idx = art_insert_seed.fetchAdd(1, .monotonic);
        var key_buf: [24]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "art:ins:{d:0>8}", .{idx}) catch unreachable;
        var value = types.Value{ .integer = @intCast(idx % 1_000_000) };
        tree.insert(key, &value) catch unreachable;
    }
};

const WalAppendBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const wal = if (steady_wal) |*w| w else unreachable;
        const value = types.Value{ .integer = 42 };
        wal.appendPut("bench:wal", &value) catch unreachable;
    }
};

const WalAppendGroupedBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const wal = if (steady_wal) |*w| w else unreachable;

        const value = types.Value{ .integer = 42 };
        var writes: [wal_group_item_count]internal.wal.PutBatchWrite = undefined;

        for (0..wal_group_item_count) |i| {
            writes[i] = .{ .key = wal_group_keys[i], .value = &value };
        }

        wal.appendPutGroup(&writes) catch unreachable;
    }
};

/// Runs the benchmark suite with process-owned I/O and explicit benchmark allocators.
///
/// Time Complexity: Depends on the selected workloads and iteration counts.
///
/// Allocator: Owns temporary CLI and measurement storage; workload fixtures have explicit teardown.
pub fn main(init: std.process.Init) !void {
    bench_io = init.io;
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var stdout_buffer: [4 * 1024]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(bench_io, &stdout_buffer);
    const cli_config = try parseCliConfig(try init.minimal.args.toSlice(init.arena.allocator()));

    if (cli_config.scan_allocation_profile) {
        try runScanAllocationProfile(&stdout.interface, cli_config.metrics_config);
    } else if (cli_config.scan_candidate_profile) {
        try runScanCandidateProfile(&stdout.interface, cli_config.metrics_config);
    } else if (cli_config.heavy_overwrite_profile) {
        try runHeavyOverwriteProfile(&stdout.interface, cli_config.metrics_config);
    } else if (cli_config.run_all_metrics_modes) {
        const configs = [_]types.MetricsConfig{
            .{ .mode = .disabled },
            .{ .mode = .counters_only },
            .{
                .mode = .sampled_latency,
                .latency_sample_shift = cli_config.metrics_config.latency_sample_shift,
            },
            .{ .mode = .full },
        };

        for (configs, 0..) |config, index| {
            if (index != 0) try stdout.interface.print("\n", .{});
            try runBenchSuite(allocator, &stdout.interface, config);
        }
    } else {
        try runBenchSuite(allocator, &stdout.interface, cli_config.metrics_config);
        try printThroughputSummary(allocator, &stdout.interface, cli_config.metrics_config);
        try runScalingBenchmarks(allocator, &stdout.interface);
        try runHeavyOverwriteProfile(&stdout.interface, cli_config.metrics_config);
    }

    try stdout.interface.flush();
}

fn printThroughputSummary(
    allocator: std.mem.Allocator,
    writer: anytype,
    metrics_config: types.MetricsConfig,
) !void {
    bench_metrics_config = metrics_config;
    try initSteadyStateBenches();
    defer deinitSteadyStateBenches();

    const warmup_iterations: usize = 2_000;
    const iterations: usize = 100_000;

    try writer.print("\nThroughput Summary\n", .{});
    try writer.print(
        "conditions: {d} keys rotating, {d} warmup + {d} measured iters, WAL=batched_async, in-memory\n",
        .{ put_steady_key_cardinality, warmup_iterations, iterations },
    );
    try writer.print("--------------------------------------------------------------------------------\n", .{});
    try writer.print("{s: <32} | {s: <33} | {s: <15}\n", .{ "Benchmark", "Latency (p50/p99/max)", "Throughput" });
    try writer.print("--------------------------------------------------------------------------------\n", .{});

    try runAndPrintThroughput(writer, "put steady", warmup_iterations, iterations, 1, struct {
        fn run(_: std.mem.Allocator) void {
            const db = steady_put_db orelse unreachable;
            const key_index = steady_put_uniform_seed.fetchAdd(1, .monotonic) % put_steady_key_cardinality;
            var key_buf: [32]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "bench:put:uniform:{d:0>4}", .{key_index}) catch unreachable;
            const value = types.Value{ .integer = @intCast(key_index) };
            db.put(key, &value) catch unreachable;
        }
    }.run, allocator);

    try runAndPrintThroughput(writer, "put overwrite64 steady", warmup_iterations, iterations, 1, struct {
        fn run(_: std.mem.Allocator) void {
            const db = steady_put_db orelse unreachable;
            const key_index = steady_put_overwrite_seed.fetchAdd(1, .monotonic) % put_overwrite_key_cardinality;

            var key_buf: [32]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "bench:put:ovr:{d:0>2}", .{key_index}) catch unreachable;
            const value = types.Value{ .integer = @intCast(key_index) };
            db.put(key, &value) catch unreachable;
        }
    }.run, allocator);

    try runAndPrintThroughput(writer, "put overwrite64 heavy1k steady", warmup_iterations, iterations, 1, struct {
        fn run(_: std.mem.Allocator) void {
            const db = steady_put_db orelse unreachable;
            const key_index = steady_put_overwrite_heavy_seed.fetchAdd(1, .monotonic) % put_overwrite_key_cardinality;

            var key_buf: [40]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "bench:put:ovrheavy:{d:0>2}", .{key_index}) catch unreachable;

            var payload: [heavy_overwrite_payload_bytes]u8 = undefined;
            @memset(payload[0..], @as(u8, 'a' + @as(u8, @intCast(key_index % 26))));
            const value = types.Value{ .string = payload[0..] };
            db.put(key, &value) catch unreachable;
        }
    }.run, allocator);

    try runAndPrintThroughput(writer, "put overwrite64 heavy1k manual", warmup_iterations, iterations, 1, struct {
        fn run(_: std.mem.Allocator) void {
            const db = steady_put_heavy_manual_db orelse unreachable;
            const key_index = steady_put_overwrite_heavy_manual_seed.fetchAdd(1, .monotonic) % put_overwrite_key_cardinality;

            var key_buf: [40]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "bench:put:ovrheavy:{d:0>2}", .{key_index}) catch unreachable;

            var payload: [heavy_overwrite_payload_bytes]u8 = undefined;
            @memset(payload[0..], @as(u8, 'a' + @as(u8, @intCast(key_index % 26))));
            const value = types.Value{ .string = payload[0..] };
            db.put(key, &value) catch unreachable;

            const op_index = steady_put_overwrite_heavy_manual_ops.fetchAdd(1, .monotonic) + 1;
            if ((op_index % heavy_overwrite_manual_compact_interval) == 0) {
                const shard_idx = internal.runtime_shard.getShardIndex(key);
                db.compactShard(shard_idx) catch unreachable;
            }
        }
    }.run, allocator);

    try runAndPrintThroughput(writer, "put_group16 steady", warmup_iterations, iterations, put_group_item_count, struct {
        fn run(_: std.mem.Allocator) void {
            const db = steady_put_db orelse unreachable;

            var values: [put_group_item_count]types.Value = undefined;
            var writes: [put_group_item_count]types.PutWrite = undefined;
            var key_storage: [put_group_item_count][24]u8 = undefined;

            for (0..put_group_item_count) |i| {
                values[i] = .{ .integer = @intCast(i) };
                const key = std.fmt.bufPrint(&key_storage[i], "bench:put:group:{d:0>2}", .{i}) catch unreachable;
                writes[i] = .{ .key = key, .value = &values[i] };
            }

            db.putGroup(&writes) catch unreachable;
        }
    }.run, allocator);

    try runAndPrintThroughput(writer, "get steady", warmup_iterations, iterations, 1, struct {
        fn run(alloc: std.mem.Allocator) void {
            const db = steady_get_db orelse unreachable;
            const key_index = steady_get_uniform_seed.fetchAdd(1, .monotonic) % get_steady_key_cardinality;
            var key_buf: [32]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "bench:get:uniform:{d:0>4}", .{key_index}) catch unreachable;
            var stored = (db.get(alloc, key) catch unreachable).?;
            defer stored.deinit(alloc);
            std.mem.doNotOptimizeAway(stored.integer);
        }
    }.run, allocator);

    try runAndPrintThroughput(writer, "get steady ttl-mixed10", warmup_iterations, iterations, 1, struct {
        fn run(alloc: std.mem.Allocator) void {
            const db = steady_get_ttl_mixed_db orelse unreachable;
            const key_idx = steady_get_ttl_mixed_seed.fetchAdd(1, .monotonic) % 10;

            const key = switch (key_idx) {
                0 => "bench:get:ttl-mixed:0",
                1 => "bench:get:ttl-mixed:1",
                2 => "bench:get:ttl-mixed:2",
                3 => "bench:get:ttl-mixed:3",
                4 => "bench:get:ttl-mixed:4",
                5 => "bench:get:ttl-mixed:5",
                6 => "bench:get:ttl-mixed:6",
                7 => "bench:get:ttl-mixed:7",
                8 => "bench:get:ttl-mixed:8",
                9 => "bench:get:ttl-mixed:9",
                else => unreachable,
            };

            var stored = (db.get(alloc, key) catch unreachable).?;
            defer stored.deinit(alloc);
            std.mem.doNotOptimizeAway(stored.integer);
        }
    }.run, allocator);

    try runAndPrintThroughput(writer, "scan256 steady", warmup_iterations, 1000, 256, struct {
        fn run(alloc: std.mem.Allocator) void {
            const db = steady_scan_db orelse unreachable;
            var result = db.scanPrefix(alloc, "scan:") catch unreachable;
            defer result.deinit();
            std.mem.doNotOptimizeAway(result.entries.items.len);
        }
    }.run, allocator);

    try runAndPrintThroughput(writer, "scan4096 steady", warmup_iterations, 100, 4096, struct {
        fn run(alloc: std.mem.Allocator) void {
            const db = steady_scan_large_db orelse unreachable;
            var result = db.scanPrefix(alloc, "scan:") catch unreachable;
            defer result.deinit();
            std.mem.doNotOptimizeAway(result.entries.items.len);
        }
    }.run, allocator);

    try runAndPrintThroughput(writer, "batch64 steady overwrite", warmup_iterations, 1000, 64, struct {
        fn run(_: std.mem.Allocator) void {
            const db = steady_batch_overwrite_db orelse unreachable;
            var values: [batch_item_count]types.Value = undefined;
            var writes: [batch_item_count]types.PutWrite = undefined;
            var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;
            fillBatchWrites(&values, &writes, &key_storage, "batch", 1_000, 0);
            db.applyBatch(&writes) catch unreachable;
        }
    }.run, allocator);

    try runAndPrintThroughput(writer, "art lookup", warmup_iterations, iterations, 1, struct {
        fn run(_: std.mem.Allocator) void {
            const tree = if (steady_art_tree) |*t| t else unreachable;
            const idx = art_lookup_seed.fetchAdd(1, .monotonic) % scan_item_count;
            var key_buf: [16]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "scan:{d:0>4}", .{idx}) catch unreachable;
            const value = tree.lookup(key) orelse unreachable;
            std.mem.doNotOptimizeAway(value.integer);
        }
    }.run, allocator);

    try runAndPrintThroughput(writer, "art insert", warmup_iterations, iterations, 1, struct {
        fn run(_: std.mem.Allocator) void {
            const tree = if (steady_art_tree) |*t| t else unreachable;
            const idx = art_insert_seed.fetchAdd(1, .monotonic);
            var key_buf: [24]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "art:ins:{d:0>8}", .{idx}) catch unreachable;
            var value = types.Value{ .integer = @intCast(idx % 1_000_000) };
            tree.insert(key, &value) catch unreachable;
        }
    }.run, allocator);

    try runAndPrintThroughput(writer, "wal append", warmup_iterations, iterations, 1, struct {
        fn run(_: std.mem.Allocator) void {
            const wal = if (steady_wal) |*w| w else unreachable;
            const value = types.Value{ .integer = 42 };
            wal.appendPut("bench:wal", &value) catch unreachable;
        }
    }.run, allocator);

    try runAndPrintThroughput(writer, "wal append grouped16", warmup_iterations, iterations, wal_group_item_count, struct {
        fn run(_: std.mem.Allocator) void {
            const wal = if (steady_wal) |*w| w else unreachable;

            const value = types.Value{ .integer = 42 };
            var writes: [wal_group_item_count]internal.wal.PutBatchWrite = undefined;

            for (0..wal_group_item_count) |i| {
                writes[i] = .{ .key = wal_group_keys[i], .value = &value };
            }

            wal.appendPutGroup(&writes) catch unreachable;
        }
    }.run, allocator);

    try writer.print("--------------------------------------------------------------------------------\n", .{});
}

fn runAndPrintThroughput(
    writer: anytype,
    label: []const u8,
    warmup_iterations: usize,
    iterations: usize,
    items_per_op: usize,
    func: fn (std.mem.Allocator) void,
    allocator: std.mem.Allocator,
) !void {
    for (0..warmup_iterations) |_| {
        func(allocator);
    }

    var latencies = try allocator.alloc(u64, iterations);
    defer allocator.free(latencies);

    var total_elapsed: u128 = 0;
    for (0..iterations) |i| {
        const op_timer = std.Io.Clock.awake.now(bench_io);
        func(allocator);
        const op_elapsed = @as(u64, @intCast(op_timer.durationTo(std.Io.Clock.awake.now(bench_io)).toNanoseconds()));
        latencies[i] = op_elapsed;
        total_elapsed += op_elapsed;
    }

    std.sort.heap(u64, latencies, {}, comptime std.sort.asc(u64));

    const p50 = latencies[(iterations * 50) / 100];
    const p99 = latencies[(iterations * 99) / 100];
    const max = latencies[iterations - 1];

    const safe_total_elapsed = if (total_elapsed == 0) @as(u128, 1) else total_elapsed;
    const ops_per_sec = (@as(u128, std.time.ns_per_s) * iterations) / safe_total_elapsed;
    const items_per_sec = ops_per_sec * items_per_op;

    var p50_buf: [24]u8 = undefined;
    var p99_buf: [24]u8 = undefined;
    var max_buf: [24]u8 = undefined;
    var latency_summary_buf: [96]u8 = undefined;

    const p50_text = try formatNsShort(&p50_buf, p50);
    const p99_text = try formatNsShort(&p99_buf, p99);
    const max_text = try formatNsShort(&max_buf, max);
    const latency_text = try std.fmt.bufPrint(&latency_summary_buf, "p50={s} p99={s} max={s}", .{ p50_text, p99_text, max_text });

    if (items_per_op > 1) {
        try writer.print("{s: <32} | {s: <33} | {d:.2}M items/sec ({d:.2}M ops/sec)\n", .{
            label,
            latency_text,
            @as(f64, @floatFromInt(items_per_sec)) / 1_000_000.0,
            @as(f64, @floatFromInt(ops_per_sec)) / 1_000_000.0,
        });
    } else {
        try writer.print("{s: <32} | {s: <33} | {d:.2}M ops/sec\n", .{
            label,
            latency_text,
            @as(f64, @floatFromInt(ops_per_sec)) / 1_000_000.0,
        });
    }
}

const ScalingBenchmarkType = enum { get, put };

const ScalingContention = enum {
    /// Each thread targets a distinct shard (best case).
    none,
    /// All threads target the same key (worst case).
    hotspot,
    /// Shared 10k-key keyspace with uniform access (realistic case).
    uniform,
};

const scaling_uniform_keyspace: usize = 10_000;

const ScalingWorkerContext = struct {
    db: *engine.Database,
    op: ScalingBenchmarkType,
    contention: ScalingContention,
    key: ?[]const u8,
    uniform_seed: ?*std.atomic.Value(usize),
    iterations: usize,
};

fn scalingWorker(ctx: ScalingWorkerContext) void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const value = types.Value{ .integer = 123 };

    switch (ctx.contention) {
        .uniform => {
            const seed = ctx.uniform_seed orelse unreachable;
            if (ctx.op == .put) {
                for (0..ctx.iterations) |_| {
                    const idx = seed.fetchAdd(1, .monotonic) % scaling_uniform_keyspace;
                    var key_buf: [24]u8 = undefined;
                    const key = std.fmt.bufPrint(&key_buf, "bench:scale:u{d:0>5}", .{idx}) catch unreachable;
                    ctx.db.put(key, &value) catch unreachable;
                }
            } else {
                for (0..ctx.iterations) |_| {
                    const idx = seed.fetchAdd(1, .monotonic) % scaling_uniform_keyspace;
                    var key_buf: [24]u8 = undefined;
                    const key = std.fmt.bufPrint(&key_buf, "bench:scale:u{d:0>5}", .{idx}) catch unreachable;
                    var stored = (ctx.db.get(allocator, key) catch unreachable).?;
                    defer stored.deinit(allocator);
                    std.mem.doNotOptimizeAway(stored.integer);
                }
            }
        },
        else => {
            const key = ctx.key orelse unreachable;
            if (ctx.op == .put) {
                for (0..ctx.iterations) |_| {
                    ctx.db.put(key, &value) catch unreachable;
                }
            } else {
                for (0..ctx.iterations) |_| {
                    var stored = (ctx.db.get(allocator, key) catch unreachable).?;
                    defer stored.deinit(allocator);
                    std.mem.doNotOptimizeAway(stored.integer);
                }
            }
        },
    }
}

fn allocateDistinctShardWorkerKeys(allocator: std.mem.Allocator, thread_count: usize) ![][]u8 {
    var keys = try allocator.alloc([]u8, thread_count);
    var filled: usize = 0;
    errdefer {
        for (keys[0..filled]) |key| allocator.free(key);
        allocator.free(keys);
    }

    var used_shards = std.StaticBitSet(internal.runtime_shard.NUM_SHARDS).initEmpty();
    var candidate: usize = 0;

    for (0..thread_count) |worker_idx| {
        while (true) : (candidate += 1) {
            const key = try std.fmt.allocPrint(allocator, "worker:{{scale:{d}}}:key", .{candidate});
            const shard_idx = internal.runtime_shard.getShardIndex(key);
            if (used_shards.isSet(shard_idx)) {
                allocator.free(key);
                continue;
            }

            used_shards.set(shard_idx);
            keys[worker_idx] = key;
            filled += 1;
            candidate += 1;
            break;
        }
    }

    return keys;
}

fn freeWorkerKeys(allocator: std.mem.Allocator, keys: [][]u8) void {
    for (keys) |key| allocator.free(key);
    allocator.free(keys);
}

fn runScalingForContention(
    allocator: std.mem.Allocator,
    writer: anytype,
    op: ScalingBenchmarkType,
    contention: ScalingContention,
    base_throughput: f64,
) !f64 {
    const thread_counts = [_]usize{ 1, 2, 4, 8, 16 };
    const ops_per_test = 1_000_000;

    var base = base_throughput;
    var observed_throughput: f64 = 0;

    for (thread_counts) |t_count| {
        const db = try openBenchDb(std.heap.page_allocator);
        defer db.close() catch unreachable;

        var maybe_worker_keys: ?[][]u8 = null;
        defer if (maybe_worker_keys) |keys| freeWorkerKeys(allocator, keys);

        var maybe_uniform_seeds: ?[]std.atomic.Value(usize) = null;
        defer if (maybe_uniform_seeds) |seeds| allocator.free(seeds);

        switch (contention) {
            .none => {
                const worker_keys = try allocateDistinctShardWorkerKeys(allocator, t_count);
                maybe_worker_keys = worker_keys;

                for (0..t_count) |i| {
                    const value = types.Value{ .integer = @intCast(i) };
                    try db.put(worker_keys[i], &value);
                }
            },
            .hotspot => {
                const value = types.Value{ .integer = 1 };
                try db.put("bench:scale:hotspot", &value);
            },
            .uniform => {
                var key_buf: [24]u8 = undefined;
                for (0..scaling_uniform_keyspace) |i| {
                    const key = try std.fmt.bufPrint(&key_buf, "bench:scale:u{d:0>5}", .{i});
                    const value = types.Value{ .integer = @intCast(i) };
                    try db.put(key, &value);
                }

                const seeds = try allocator.alloc(std.atomic.Value(usize), t_count);
                for (0..t_count) |i| {
                    seeds[i] = std.atomic.Value(usize).init(i * (scaling_uniform_keyspace / 64));
                }
                maybe_uniform_seeds = seeds;
            },
        }

        const iters_per_thread = ops_per_test / t_count;
        var threads = try allocator.alloc(std.Thread, t_count);
        defer allocator.free(threads);

        const timer = std.Io.Clock.awake.now(bench_io);
        for (0..t_count) |i| {
            const key = switch (contention) {
                .none => maybe_worker_keys.?[i],
                .hotspot => "bench:scale:hotspot",
                .uniform => null,
            };

            const seed = if (contention == .uniform) &maybe_uniform_seeds.?[i] else null;

            threads[i] = try std.Thread.spawn(.{}, scalingWorker, .{ScalingWorkerContext{
                .db = db,
                .op = op,
                .contention = contention,
                .key = key,
                .uniform_seed = seed,
                .iterations = iters_per_thread,
            }});
        }

        for (threads) |thread| thread.join();
        const elapsed = @as(u64, @intCast(timer.durationTo(std.Io.Clock.awake.now(bench_io)).toNanoseconds()));

        const throughput = @as(f64, @floatFromInt(ops_per_test)) /
            (@as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(std.time.ns_per_s)));

        if (t_count == 1) {
            base = throughput;
        }

        const scaling = throughput / base;
        const op_name = if (op == .get) "GET" else "PUT";

        try writer.print("{s: <15} | {d: <10} | {d:.2}M ops/sec | {d:.2}x\n", .{
            op_name,
            t_count,
            throughput / 1_000_000.0,
            scaling,
        });

        observed_throughput = throughput;
    }

    return observed_throughput;
}

fn runScalingBenchmarks(allocator: std.mem.Allocator, writer: anytype) !void {
    try writer.print("\nSharded Scalability Benchmark\n", .{});
    try writer.print("note: 'none' = each thread on a distinct shard (best case). 'uniform' = 10k shared keys (realistic).\n", .{});
    try writer.print("--------------------------------------------------------------------------------\n", .{});
    for ([_]ScalingBenchmarkType{ .get, .put }) |op| {
        const op_name = if (op == .get) "GET" else "PUT";

        for ([_]ScalingContention{ .none, .hotspot, .uniform }) |contention| {
            const mode_name = switch (contention) {
                .none => "no contention (best case)",
                .hotspot => "hotspot (worst case)",
                .uniform => "uniform 10k keys (realistic)",
            };

            try writer.print("\n--- {s}: {s} ---\n", .{ op_name, mode_name });
            try writer.print("{s: <15} | {s: <10} | {s: <15} | {s: <10}\n", .{ "Workload", "Threads", "Throughput", "Scaling" });
            try writer.print("--------------------------------------------------------------------------------\n", .{});
            _ = try runScalingForContention(allocator, writer, op, contention, 0);
            try writer.print("--------------------------------------------------------------------------------\n", .{});
        }
        try writer.print("--------------------------------------------------------------------------------\n", .{});
    }
}

fn parseCliConfig(args: []const [:0]const u8) !BenchCliConfig {
    var cli = BenchCliConfig{};
    var saw_metrics_mode = false;
    var saw_latency_sample_shift = false;

    for (args[1..]) |arg| {
        if (std.mem.eql(u8, arg, "--help")) {
            printUsage();
            std.process.exit(0);
        }
        if (std.mem.startsWith(u8, arg, "--metrics-mode=")) {
            const value = arg["--metrics-mode=".len..];
            if (std.mem.eql(u8, value, "all")) {
                cli.run_all_metrics_modes = true;
                saw_metrics_mode = true;
                continue;
            }
            cli.metrics_config.mode = parseMetricsMode(value) orelse return error.InvalidArgument;
            saw_metrics_mode = true;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--latency-sample-shift=")) {
            const value = arg["--latency-sample-shift=".len..];
            cli.metrics_config.latency_sample_shift = std.fmt.parseUnsigned(u8, value, 10) catch return error.InvalidArgument;
            saw_latency_sample_shift = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--scan-allocation-profile")) {
            cli.scan_allocation_profile = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--scan-candidate-profile")) {
            cli.scan_candidate_profile = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--heavy-overwrite-profile")) {
            cli.heavy_overwrite_profile = true;
            continue;
        }
        return error.InvalidArgument;
    }

    if (saw_metrics_mode and !saw_latency_sample_shift and (cli.run_all_metrics_modes or cli.metrics_config.mode == .sampled_latency)) {
        cli.metrics_config.latency_sample_shift = default_sampled_latency_shift;
    }

    return cli;
}

fn parseMetricsMode(raw: []const u8) ?types.MetricsMode {
    if (std.mem.eql(u8, raw, "disabled")) return .disabled;
    if (std.mem.eql(u8, raw, "counters_only")) return .counters_only;
    if (std.mem.eql(u8, raw, "sampled_latency")) return .sampled_latency;
    if (std.mem.eql(u8, raw, "full")) return .full;
    return null;
}

fn printUsage() void {
    std.debug.print(
        \\usage: zeno-core-bench [--metrics-mode=disabled|counters_only|sampled_latency|full|all] [--latency-sample-shift=N] [--scan-allocation-profile] [--scan-candidate-profile] [--heavy-overwrite-profile]
        \\
    , .{});
}

fn runHeavyOverwriteProfile(writer: anytype, metrics_config: types.MetricsConfig) !void {
    bench_metrics_config = metrics_config;
    try printMetricsConfig(writer, metrics_config);
    try writer.print("heavy overwrite calibration (payload=1KB, keys=64, ops=50000)\n", .{});
    try writer.print(
        "{s: <15} | {s: <8} | {s: <8} | {s: <10} | {s: <14} | {s: <12}\n",
        .{ "compact_every_N", "p50", "p99", "max", "retained_final", "elapsed_total" },
    );
    try writer.print("--------------------------------------------------------------------------------\n", .{});

    const compact_intervals = [_]usize{ 100, 500, 1_000, 5_000, 10_000 };
    for (compact_intervals) |interval| {
        const profile = try profileHeavyOverwriteCase(interval);
        try printHeavyOverwriteCalibrationRow(writer, interval, profile);
    }

    const off = try profileHeavyOverwriteCase(null);
    try printHeavyOverwriteCalibrationRow(writer, null, off);
}

fn printHeavyOverwriteCalibrationRow(writer: anytype, compact_every_n: ?usize, profile: HeavyOverwriteProfile) !void {
    var p50_buf: [24]u8 = undefined;
    var p99_buf: [24]u8 = undefined;
    var max_buf: [24]u8 = undefined;
    var retained_buf: [24]u8 = undefined;
    var elapsed_buf: [24]u8 = undefined;

    const p50_text = formatNsShort(&p50_buf, profile.p50_ns) catch unreachable;
    const p99_text = formatNsShort(&p99_buf, profile.p99_ns) catch unreachable;
    const max_text = formatNsShort(&max_buf, profile.max_ns) catch unreachable;
    const retained_text = formatBytesShort(&retained_buf, profile.retained_heavy_bytes_estimate) catch unreachable;
    const elapsed_text = formatNsShort(&elapsed_buf, profile.elapsed_ns) catch unreachable;

    var mode_buf: [24]u8 = undefined;
    const mode_text = if (compact_every_n) |n|
        try std.fmt.bufPrint(&mode_buf, "{d}", .{n})
    else
        "off";

    try writer.print("{s: <15} | {s: <8} | {s: <8} | {s: <10} | {s: <14} | {s: <12}\n", .{
        mode_text,
        p50_text,
        p99_text,
        max_text,
        retained_text,
        elapsed_text,
    });
}

fn formatNsShort(buf: []u8, ns: u64) ![]const u8 {
    if (ns < 1_000) return std.fmt.bufPrint(buf, "{d}ns", .{ns});
    if (ns < std.time.ns_per_ms) return std.fmt.bufPrint(buf, "{d:.2}us", .{@as(f64, @floatFromInt(ns)) / 1_000.0});
    return std.fmt.bufPrint(buf, "{d:.2}ms", .{@as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms))});
}

fn formatBytesShort(buf: []u8, bytes: u64) ![]const u8 {
    if (bytes >= 1024 * 1024) {
        return std.fmt.bufPrint(buf, "{d:.2}MB", .{@as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0)});
    }
    if (bytes >= 1024) {
        return std.fmt.bufPrint(buf, "{d:.2}KB", .{@as(f64, @floatFromInt(bytes)) / 1024.0});
    }
    return std.fmt.bufPrint(buf, "{d}B", .{bytes});
}

fn profileHeavyOverwriteCase(compact_every_n: ?usize) !HeavyOverwriteProfile {
    const iterations: usize = 50_000;

    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();

    const db = try openBenchDb(allocator);
    defer db.close() catch unreachable;

    var key_buf: [40]u8 = undefined;
    var payload: [heavy_overwrite_payload_bytes]u8 = undefined;

    for (0..put_overwrite_key_cardinality) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "bench:put:ovrheavy:{d:0>2}", .{i});
        @memset(payload[0..], @as(u8, 'a' + @as(u8, @intCast(i % 26))));
        const value = types.Value{ .string = payload[0..] };
        try db.put(key, &value);
    }

    var latencies = try allocator.alloc(u64, iterations);
    defer allocator.free(latencies);

    const total_timer = std.Io.Clock.awake.now(bench_io);
    for (0..iterations) |i| {
        const key_index = i % put_overwrite_key_cardinality;
        const key = try std.fmt.bufPrint(&key_buf, "bench:put:ovrheavy:{d:0>2}", .{key_index});
        @memset(payload[0..], @as(u8, 'a' + @as(u8, @intCast((key_index + i) % 26))));
        const value = types.Value{ .string = payload[0..] };

        const op_timer = std.Io.Clock.awake.now(bench_io);
        try db.put(key, &value);
        latencies[i] = @as(u64, @intCast(op_timer.durationTo(std.Io.Clock.awake.now(bench_io)).toNanoseconds()));

        if (compact_every_n) |interval| {
            if (((i + 1) % interval) == 0) {
                try db.compactAll();
            }
        }
    }
    const elapsed_ns = @as(u64, @intCast(total_timer.durationTo(std.Io.Clock.awake.now(bench_io)).toNanoseconds()));

    std.sort.heap(u64, latencies, {}, comptime std.sort.asc(u64));

    const p50_idx = (iterations * 50) / 100;
    const p99_idx = (iterations * 99) / 100;
    const max_idx = iterations - 1;
    const stats = db.state.statsSnapshot();

    return .{
        .elapsed_ns = elapsed_ns,
        .p50_ns = latencies[p50_idx],
        .p99_ns = latencies[p99_idx],
        .max_ns = latencies[max_idx],
        .overwritten_heavy_bytes_total = stats.overwritten_heavy_bytes_total,
        .retained_heavy_bytes_estimate = stats.retained_heavy_bytes_estimate,
    };
}

fn runScanAllocationProfile(writer: anytype, metrics_config: types.MetricsConfig) !void {
    bench_metrics_config = metrics_config;
    try printMetricsConfig(writer, metrics_config);
    try writer.print("scan allocation profile (prefix=\"scan:\", page_limit={d})\n", .{scan_page_item_count});

    const scan256_full = try profileScanAllocations(scan_item_count, .public_full);
    try printScanAllocationProfile(writer, "scan256 steady", scan256_full);

    const scan256_in_view = try profileScanAllocations(scan_item_count, .in_view_page);
    try printScanAllocationProfile(writer, "scan64 in-view steady", scan256_in_view);

    const scan4096_full = try profileScanAllocations(scan_large_item_count, .public_full);
    try printScanAllocationProfile(writer, "scan4096 steady", scan4096_full);

    const scan4096_in_view = try profileScanAllocations(scan_large_item_count, .in_view_page);
    try printScanAllocationProfile(writer, "scan64 in-view 4096 steady", scan4096_in_view);
}

fn runScanCandidateProfile(writer: anytype, metrics_config: types.MetricsConfig) !void {
    bench_metrics_config = metrics_config;
    try printMetricsConfig(writer, metrics_config);
    try writer.print(
        "scan candidate profile (prefix=\"scan:\", page_limit={d})\n",
        .{scan_page_item_count},
    );

    const scan256_full = try profilePublicFullScan(256);
    try printScanCandidateProfile(writer, "scan256 steady", scan256_full);

    const scan4096_full = try profilePublicFullScan(4096);
    try printScanCandidateProfile(writer, "scan4096 steady", scan4096_full);
}

fn printScanAllocationProfile(
    writer: anytype,
    label: []const u8,
    profile: ScanAllocationProfile,
) !void {
    try writer.print(
        "{s}: entries={d} next_cursor={s} allocations={d} deallocations={d} allocated_bytes={d} freed_bytes={d}\n",
        .{
            label,
            profile.emitted_entries,
            if (profile.has_next_cursor) "yes" else "no",
            profile.allocations,
            profile.deallocations,
            profile.allocated_bytes,
            profile.freed_bytes,
        },
    );
}

fn printScanCandidateProfile(
    writer: anytype,
    label: []const u8,
    profile: ScanCandidateProfile,
) !void {
    try writer.print(
        "{s}: entries={d} allocations={d} allocated_bytes={d} freed_bytes={d} elapsed_ns={d}\n",
        .{
            label,
            profile.emitted_entries,
            profile.allocations,
            profile.allocated_bytes,
            profile.freed_bytes,
            profile.elapsed_ns,
        },
    );
}

fn profileScanAllocations(
    comptime fixture_items: usize,
    mode: ScanProfileMode,
) !ScanAllocationProfile {
    var db_gpa = std.heap.DebugAllocator(.{}){};
    defer std.debug.assert(db_gpa.deinit() == .ok);

    const db = try openBenchDb(db_gpa.allocator());
    defer db.close() catch unreachable;
    loadScanFixture(fixture_items, db);

    var result_gpa = std.heap.DebugAllocator(.{}){};
    defer std.debug.assert(result_gpa.deinit() == .ok);

    var counting_state = FailingAllocator.init(result_gpa.allocator(), .{});
    const counting_allocator = counting_state.allocator();

    var emitted_entries: usize = 0;
    var has_next_cursor = false;

    switch (mode) {
        .public_full => {
            var result = try db.scanPrefix(counting_allocator, "scan:");
            emitted_entries = result.entries.items.len;
            result.deinit();
        },
        .in_view_page => {
            var view = try db.readView();
            defer view.deinit();

            var page = try official.scanPrefixFromInView(&view, counting_allocator, "scan:", null, scan_page_item_count);
            emitted_entries = page.entries.items.len;
            has_next_cursor = page.borrowNextCursor() != null;
            page.deinit();
        },
    }

    std.debug.assert(counting_state.allocated_bytes == counting_state.freed_bytes);

    return .{
        .emitted_entries = emitted_entries,
        .has_next_cursor = has_next_cursor,
        .allocations = counting_state.allocations,
        .deallocations = counting_state.deallocations,
        .allocated_bytes = counting_state.allocated_bytes,
        .freed_bytes = counting_state.freed_bytes,
    };
}

fn profilePublicFullScan(comptime fixture_items: usize) !ScanCandidateProfile {
    var db_gpa = std.heap.DebugAllocator(.{}){};
    defer std.debug.assert(db_gpa.deinit() == .ok);

    const db = try openBenchDb(db_gpa.allocator());
    defer db.close() catch unreachable;
    loadScanFixture(fixture_items, db);

    var result_gpa = std.heap.DebugAllocator(.{}){};
    defer std.debug.assert(result_gpa.deinit() == .ok);

    var counting_state = FailingAllocator.init(result_gpa.allocator(), .{});
    const counting_allocator = counting_state.allocator();

    const timer = std.Io.Clock.awake.now(bench_io);
    var result = try db.scanPrefix(counting_allocator, "scan:");
    const elapsed_ns = @as(u64, @intCast(timer.durationTo(std.Io.Clock.awake.now(bench_io)).toNanoseconds()));

    const emitted_entries = result.entries.items.len;
    std.debug.assert(emitted_entries == fixture_items);
    result.deinit();
    std.debug.assert(counting_state.allocated_bytes == counting_state.freed_bytes);

    return .{
        .emitted_entries = emitted_entries,
        .allocations = counting_state.allocations,
        .deallocations = counting_state.deallocations,
        .allocated_bytes = counting_state.allocated_bytes,
        .freed_bytes = counting_state.freed_bytes,
        .elapsed_ns = elapsed_ns,
    };
}

fn runBenchSuite(
    allocator: std.mem.Allocator,
    writer: anytype,
    metrics_config: types.MetricsConfig,
) !void {
    bench_metrics_config = metrics_config;
    try initSteadyStateBenches();
    defer deinitSteadyStateBenches();

    var stable_bench = zbench.Benchmark.init(allocator, .{
        .max_iterations = 8_192,
        .time_budget_ns = 750 * std.time.ns_per_ms,
    });
    defer stable_bench.deinit();

    var growing_bench = zbench.Benchmark.init(allocator, .{
        .max_iterations = 8_192,
        .time_budget_ns = 750 * std.time.ns_per_ms,
    });
    defer growing_bench.deinit();

    const put_fresh = PutFreshBenchmark{};
    const put_steady = PutSteadyBenchmark{};
    const put_steady_overwrite_cardinality = PutSteadyOverwriteCardinalityBenchmark{};
    const put_steady_overwrite_heavy_cardinality = PutSteadyOverwriteHeavyCardinalityBenchmark{};
    const put_steady_overwrite_heavy_manual = PutSteadyOverwriteHeavyManualCompactBenchmark{};
    const put_group_steady = PutGroupSteadyBenchmark{};
    const get_existing = GetExistingBenchmark{};
    const get_existing_steady = GetExistingSteadyBenchmark{};
    const get_existing_steady_ttl_mixed = GetExistingSteadyTtlMixedBenchmark{};
    const scanPrefix = ScanPrefixBenchmark{};
    const scan_prefix_steady = ScanPrefixSteadyBenchmark{};
    const scan_prefix_in_view_steady = ScanPrefixInViewSteadyBenchmark{};
    const scan_prefix_large_steady = ScanPrefixLargeSteadyBenchmark{};
    const scan_prefix_large_in_view_steady = ScanPrefixLargeInViewSteadyBenchmark{};
    const applyBatch = ApplyBatchBenchmark{};
    const apply_batch_steady_overwrite = ApplyBatchSteadyOverwriteBenchmark{};
    const apply_batch_steady_insert = ApplyBatchSteadyInsertBenchmark{};
    const applyCheckedBatch = ApplyCheckedBatchBenchmark{};
    const apply_checked_batch_steady_overwrite = ApplyCheckedBatchSteadyOverwriteBenchmark{};
    const apply_checked_batch_steady_insert = ApplyCheckedBatchSteadyInsertBenchmark{};

    try stable_bench.addParam("put isolated", &put_fresh, .{});
    try stable_bench.addParam("put steady", &put_steady, .{});
    try stable_bench.addParam("put overwrite64 steady", &put_steady_overwrite_cardinality, .{});
    try stable_bench.addParam("put overwrite64 heavy1k steady", &put_steady_overwrite_heavy_cardinality, .{});
    try stable_bench.addParam("put overwrite64 heavy1k manual", &put_steady_overwrite_heavy_manual, .{});
    try stable_bench.addParam("put_group16 steady", &put_group_steady, .{});
    try stable_bench.addParam("get isolated", &get_existing, .{});
    try stable_bench.addParam("get steady", &get_existing_steady, .{});
    try stable_bench.addParam("get steady ttl-mixed10", &get_existing_steady_ttl_mixed, .{});
    try stable_bench.addParam("scan256 isolated", &scanPrefix, .{});
    try stable_bench.addParam("scan256 steady", &scan_prefix_steady, .{});
    try stable_bench.addParam("scan64 in-view steady", &scan_prefix_in_view_steady, .{});
    try stable_bench.addParam("scan4096 steady", &scan_prefix_large_steady, .{});
    try stable_bench.addParam("scan64 in-view 4096 steady", &scan_prefix_large_in_view_steady, .{});
    try stable_bench.addParam("batch64 isolated", &applyBatch, .{});
    try stable_bench.addParam("batch64 steady overwrite", &apply_batch_steady_overwrite, .{});
    try stable_bench.addParam("checked64 isolated", &applyCheckedBatch, .{});
    try stable_bench.addParam("checked64 steady overwrite", &apply_checked_batch_steady_overwrite, .{});

    try growing_bench.addParam("batch64 growing insert", &apply_batch_steady_insert, .{});
    try growing_bench.addParam("checked64 growing insert", &apply_checked_batch_steady_insert, .{});

    try stable_bench.addParam("art lookup", &ArtLookupBenchmark{}, .{});
    try stable_bench.addParam("art insert", &ArtInsertBenchmark{}, .{});
    try stable_bench.addParam("wal append", &WalAppendBenchmark{}, .{});
    try stable_bench.addParam("wal append grouped16", &WalAppendGroupedBenchmark{}, .{});

    try printMetricsConfig(writer, metrics_config);
    try writer.flush();
    try stable_bench.run(bench_io, std.Io.File.stdout());
    try writer.print("\n", .{});
    try writer.print("growing workloads\n", .{});
    try writer.flush();
    try growing_bench.run(bench_io, std.Io.File.stdout());
}

fn printMetricsConfig(writer: anytype, metrics_config: types.MetricsConfig) !void {
    switch (metrics_config.mode) {
        .sampled_latency => {
            try writer.print(
                "metrics mode: {s} (latency_sample_shift={d})\n",
                .{ @tagName(metrics_config.mode), metrics_config.latency_sample_shift },
            );
        },
        else => {
            try writer.print("metrics mode: {s}\n", .{@tagName(metrics_config.mode)});
        },
    }
}

fn loadScanFixture(comptime count: usize, db: *engine.Database) void {
    var key_storage: [count][16]u8 = undefined;
    for (0..count) |index| {
        const key = std.fmt.bufPrint(&key_storage[index], "scan:{d:0>4}", .{index}) catch unreachable;
        const value = types.Value{ .integer = @intCast(index) };
        db.put(key, &value) catch unreachable;
    }
}

fn initSteadyStateBenches() !void {
    steady_put_db = try openBenchDb(std.heap.page_allocator);
    {
        const value = types.Value{ .integer = 1 };
        try steady_put_db.?.put("bench:put", &value);

        var steady_key_buf: [32]u8 = undefined;
        for (0..put_steady_key_cardinality) |i| {
            const key = try std.fmt.bufPrint(&steady_key_buf, "bench:put:uniform:{d:0>4}", .{i});
            const steady_value = types.Value{ .integer = @intCast(i) };
            try steady_put_db.?.put(key, &steady_value);
        }
        steady_put_uniform_seed.store(0, .monotonic);

        var key_buf: [32]u8 = undefined;
        for (0..put_overwrite_key_cardinality) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "bench:put:ovr:{d:0>2}", .{i});
            const overwrite_value = types.Value{ .integer = @intCast(i) };
            try steady_put_db.?.put(key, &overwrite_value);
        }

        var heavy_payload: [heavy_overwrite_payload_bytes]u8 = undefined;
        for (0..put_overwrite_key_cardinality) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "bench:put:ovrheavy:{d:0>2}", .{i});
            @memset(heavy_payload[0..], @as(u8, 'a' + @as(u8, @intCast(i % 26))));
            const heavy_value = types.Value{ .string = heavy_payload[0..] };
            try steady_put_db.?.put(key, &heavy_value);
        }
        steady_put_overwrite_seed.store(0, .monotonic);
        steady_put_overwrite_heavy_seed.store(0, .monotonic);
    }

    steady_get_db = try openBenchDb(std.heap.page_allocator);
    {
        var key_buf: [32]u8 = undefined;
        for (0..get_steady_key_cardinality) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "bench:get:uniform:{d:0>4}", .{i});
            const value = types.Value{ .integer = @intCast(i) };
            try steady_get_db.?.put(key, &value);
        }
        steady_get_uniform_seed.store(0, .monotonic);
    }

    steady_get_ttl_mixed_db = try openBenchDb(std.heap.page_allocator);
    {
        var key_buf: [32]u8 = undefined;
        for (0..10) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "bench:get:ttl-mixed:{d}", .{i});
            const value = types.Value{ .integer = @intCast(i) };
            try steady_get_ttl_mixed_db.?.put(key, &value);
        }
        _ = try steady_get_ttl_mixed_db.?.expireAt("bench:get:ttl-mixed:0", internal.runtime_shard.unixNow(bench_io) + 3600);
        steady_get_ttl_mixed_seed.store(0, .monotonic);
    }

    steady_scan_db = try openBenchDb(std.heap.page_allocator);
    loadScanFixture(scan_item_count, steady_scan_db.?);
    steady_scan_view = try steady_scan_db.?.readView();

    steady_scan_large_db = try openBenchDb(std.heap.page_allocator);
    loadScanFixture(scan_large_item_count, steady_scan_large_db.?);
    steady_scan_large_view = try steady_scan_large_db.?.readView();

    steady_batch_overwrite_db = try openBenchDb(std.heap.page_allocator);
    primeBatchFixture(steady_batch_overwrite_db.?, "batch");

    steady_batch_insert_db = try openBenchDb(std.heap.page_allocator);
    steady_batch_insert_seed.store(0, .monotonic);

    steady_checked_batch_overwrite_db = try openBenchDb(std.heap.page_allocator);
    primeBatchFixture(steady_checked_batch_overwrite_db.?, "guard");

    steady_checked_batch_insert_db = try openBenchDb(std.heap.page_allocator);
    steady_checked_batch_insert_seed.store(0, .monotonic);

    steady_put_heavy_manual_db = try openBenchDb(std.heap.page_allocator);
    {
        var key_buf: [40]u8 = undefined;
        var heavy_payload: [heavy_overwrite_payload_bytes]u8 = undefined;
        for (0..put_overwrite_key_cardinality) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "bench:put:ovrheavy:{d:0>2}", .{i});
            @memset(heavy_payload[0..], @as(u8, 'a' + @as(u8, @intCast(i % 26))));
            const heavy_value = types.Value{ .string = heavy_payload[0..] };
            try steady_put_heavy_manual_db.?.put(key, &heavy_value);
        }
        steady_put_overwrite_heavy_manual_seed.store(0, .monotonic);
        steady_put_overwrite_heavy_manual_ops.store(0, .monotonic);
    }

    steady_art_tree = internal.art.Tree.init(std.heap.page_allocator);
    loadScanFixtureToArt(scan_item_count, &steady_art_tree.?);

    wal_bench_path = try std.fmt.allocPrint(std.heap.page_allocator, "/tmp/zeno-bench-{d}.wal", .{std.Io.Clock.real.now(bench_io).toSeconds()});
    steady_wal = try internal.wal.Wal.open(bench_io, wal_bench_path.?, .{ .fsync_mode = .batched_async }, .{
        .ctx = undefined,
        .put = struct {
            fn func(_: *anyopaque, _: []const u8, _: *const types.Value) anyerror!void {}
        }.func,
        .delete = struct {
            fn func(_: *anyopaque, _: []const u8) anyerror!void {}
        }.func,
        .expire = struct {
            fn func(_: *anyopaque, _: []const u8, _: i64) anyerror!void {}
        }.func,
    }, std.heap.page_allocator);
}

fn loadScanFixtureToArt(comptime count: usize, tree: *internal.art.Tree) void {
    // We need to keep the values alive if they are heap-cloned, but here we use stack integer values
    // In ART, we store pointers to Value.
    // For benchmark stability, we'll pre-allocate a pool of values.
    const values = std.heap.page_allocator.alloc(types.Value, count) catch unreachable;
    for (0..count) |index| {
        values[index] = .{ .integer = @intCast(index) };
        const key = std.fmt.allocPrint(std.heap.page_allocator, "scan:{d:0>4}", .{index}) catch unreachable;
        tree.insert(key, &values[index]) catch unreachable;
    }
}

fn deinitSteadyStateBenches() void {
    steady_put_uniform_seed.store(0, .monotonic);
    steady_get_uniform_seed.store(0, .monotonic);
    art_lookup_seed.store(0, .monotonic);
    art_insert_seed.store(0, .monotonic);

    if (steady_wal) |*wal| {
        wal.close();
        if (wal_bench_path) |path| {
            std.Io.Dir.cwd().deleteFile(bench_io, path) catch {};
            std.heap.page_allocator.free(path);
        }
        steady_wal = null;
    }
    if (steady_scan_view) |*view| {
        view.deinit();
        steady_scan_view = null;
    }
    if (steady_scan_large_view) |*view| {
        view.deinit();
        steady_scan_large_view = null;
    }
    if (steady_checked_batch_insert_db) |db| {
        db.close() catch unreachable;
        steady_checked_batch_insert_db = null;
    }
    if (steady_checked_batch_overwrite_db) |db| {
        db.close() catch unreachable;
        steady_checked_batch_overwrite_db = null;
    }
    if (steady_batch_insert_db) |db| {
        db.close() catch unreachable;
        steady_batch_insert_db = null;
    }
    if (steady_batch_overwrite_db) |db| {
        db.close() catch unreachable;
        steady_batch_overwrite_db = null;
    }
    if (steady_scan_db) |db| {
        db.close() catch unreachable;
        steady_scan_db = null;
    }
    if (steady_scan_large_db) |db| {
        db.close() catch unreachable;
        steady_scan_large_db = null;
    }
    if (steady_get_db) |db| {
        db.close() catch unreachable;
        steady_get_db = null;
    }
    if (steady_get_ttl_mixed_db) |db| {
        db.close() catch unreachable;
        steady_get_ttl_mixed_db = null;
    }
    if (steady_put_heavy_manual_db) |db| {
        db.close() catch unreachable;
        steady_put_heavy_manual_db = null;
    }
    if (steady_put_db) |db| {
        db.close() catch unreachable;
        steady_put_db = null;
    }
}

fn primeBatchFixture(db: *engine.Database, prefix: []const u8) void {
    var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;
    for (0..batch_item_count) |index| {
        const key = std.fmt.bufPrint(&key_storage[index], "{s}:{d:0>8}", .{ prefix, index }) catch unreachable;
        const value = types.Value{ .integer = @intCast(index) };
        db.put(key, &value) catch unreachable;
    }
}

fn fillBatchWrites(
    values: *[batch_item_count]types.Value,
    writes: *[batch_item_count]types.PutWrite,
    key_storage: *[batch_item_count][batch_key_storage_bytes]u8,
    prefix: []const u8,
    value_base: usize,
    key_base: usize,
) void {
    for (0..batch_item_count) |index| {
        values[index] = .{ .integer = @intCast(value_base + index) };
        const key = std.fmt.bufPrint(&key_storage[index], "{s}:{d:0>8}", .{ prefix, key_base + index }) catch unreachable;
        writes[index] = .{
            .key = key,
            .value = &values[index],
        };
    }
}
