//! Benchmark entrypoint for coarse zeno-core engine performance checks.
//! Cost: Bench-dependent and intentionally end-to-end across public engine boundaries.
//! Allocator: Uses the benchmark-provided allocator for caller-owned result teardown and a process-wide page allocator for steady-state engine fixtures.

const std = @import("std");
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

var bench_metrics_config: types.MetricsConfig = types.default_metrics_config();
var steady_put_db: ?*engine.Database = null;
var steady_get_db: ?*engine.Database = null;
var steady_scan_db: ?*engine.Database = null;
var steady_scan_view: ?types.ReadView = null;
var steady_scan_large_db: ?*engine.Database = null;
var steady_scan_large_view: ?types.ReadView = null;
var steady_batch_overwrite_db: ?*engine.Database = null;
var steady_batch_insert_db: ?*engine.Database = null;
var steady_checked_batch_overwrite_db: ?*engine.Database = null;
var steady_checked_batch_insert_db: ?*engine.Database = null;
var steady_batch_insert_seed = std.atomic.Value(usize).init(0);
var steady_checked_batch_insert_seed = std.atomic.Value(usize).init(0);

var steady_art_tree: ?internal.art.Tree = null;
var steady_wal: ?internal.wal.Wal = null;
var wal_bench_path: ?[]const u8 = null;

const BenchCliConfig = struct {
    run_all_metrics_modes: bool = false,
    scan_allocation_profile: bool = false,
    scan_candidate_profile: bool = false,
    metrics_config: types.MetricsConfig = types.default_metrics_config(),
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

fn open_bench_db(allocator: std.mem.Allocator) !*engine.Database {
    return engine.open(allocator, .{
        .metrics = bench_metrics_config,
    });
}

const PutFreshBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = open_bench_db(allocator) catch unreachable;
        defer db.close() catch unreachable;

        const value = types.Value{ .integer = 1 };
        db.put("bench:put", &value) catch unreachable;
    }
};

const PutSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_put_db orelse unreachable;
        const value = types.Value{ .integer = 2 };
        db.put("bench:put", &value) catch unreachable;
    }
};

const GetExistingBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = open_bench_db(allocator) catch unreachable;
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
        var stored = (db.get(allocator, "bench:get") catch unreachable).?;
        defer stored.deinit(allocator);
        std.mem.doNotOptimizeAway(stored.integer);
    }
};

const ScanPrefixBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = open_bench_db(allocator) catch unreachable;
        defer db.close() catch unreachable;

        load_scan_fixture(scan_item_count, db);

        var result = db.scan_prefix(allocator, "scan:") catch unreachable;
        defer result.deinit();
        std.mem.doNotOptimizeAway(result.entries.items.len);
    }
};

const ScanPrefixSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = steady_scan_db orelse unreachable;
        var result = db.scan_prefix(allocator, "scan:") catch unreachable;
        defer result.deinit();
        std.mem.doNotOptimizeAway(result.entries.items.len);
    }
};

const ScanPrefixInViewSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const view = if (steady_scan_view) |*stored| stored else unreachable;
        var page = official.scan_prefix_from_in_view(view, allocator, "scan:", null, scan_page_item_count) catch unreachable;
        defer page.deinit();
        std.mem.doNotOptimizeAway(page.entries.items.len);
    }
};

const ScanPrefixLargeSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = steady_scan_large_db orelse unreachable;
        var result = db.scan_prefix(allocator, "scan:") catch unreachable;
        defer result.deinit();
        std.mem.doNotOptimizeAway(result.entries.items.len);
    }
};

const ScanPrefixLargeInViewSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const view = if (steady_scan_large_view) |*stored| stored else unreachable;
        var page = official.scan_prefix_from_in_view(view, allocator, "scan:", null, scan_page_item_count) catch unreachable;
        defer page.deinit();
        std.mem.doNotOptimizeAway(page.entries.items.len);
    }
};

const ApplyBatchBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = open_bench_db(allocator) catch unreachable;
        defer db.close() catch unreachable;

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fill_batch_writes(&values, &writes, &key_storage, "batch", 0, 0);

        db.apply_batch(&writes) catch unreachable;
    }
};

const ApplyBatchSteadyOverwriteBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_batch_overwrite_db orelse unreachable;

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fill_batch_writes(&values, &writes, &key_storage, "batch", 1_000, 0);

        db.apply_batch(&writes) catch unreachable;
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

        fill_batch_writes(&values, &writes, &key_storage, "batchi", 10_000, key_base);

        db.apply_batch(&writes) catch unreachable;
    }
};

const ApplyCheckedBatchBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = open_bench_db(allocator) catch unreachable;
        defer db.close() catch unreachable;

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fill_batch_writes(&values, &writes, &key_storage, "guard", 0, 0);

        official.apply_checked_batch(db, .{
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

        fill_batch_writes(&values, &writes, &key_storage, "guard", 2_000, 0);

        official.apply_checked_batch(db, .{
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

        fill_batch_writes(&values, &writes, &key_storage, "guardi", 20_000, key_base);

        official.apply_checked_batch(db, .{
            .writes = &writes,
            .guards = &.{},
        }) catch unreachable;
    }
};

const ArtLookupBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const tree = if (steady_art_tree) |*t| t else unreachable;
        const value = tree.lookup("scan:0000") orelse unreachable;
        std.mem.doNotOptimizeAway(value.integer);
    }
};

const ArtInsertBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const tree = if (steady_art_tree) |*t| t else unreachable;
        var value = types.Value{ .integer = 999 };
        // Overwrite existing key to stay steady state
        tree.insert("scan:0000", &value) catch unreachable;
    }
};

const WalAppendBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const wal = if (steady_wal) |*w| w else unreachable;
        const value = types.Value{ .integer = 42 };
        wal.append_put("bench:wal", &value) catch unreachable;
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var stdout_buffer: [4 * 1024]u8 = undefined;
    var stdout = std.fs.File.stdout().writer(&stdout_buffer);
    const cli_config = try parse_cli_config(allocator);

    if (cli_config.scan_allocation_profile) {
        try run_scan_allocation_profile(&stdout.interface, cli_config.metrics_config);
    } else if (cli_config.scan_candidate_profile) {
        try run_scan_candidate_profile(&stdout.interface, cli_config.metrics_config);
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
            try run_bench_suite(allocator, &stdout.interface, config);
        }
    } else {
        try run_bench_suite(allocator, &stdout.interface, cli_config.metrics_config);
        try print_throughput_summary(allocator, &stdout.interface, cli_config.metrics_config);
        try run_scaling_benchmarks(allocator, &stdout.interface);
    }

    try stdout.interface.flush();
}

fn print_throughput_summary(
    allocator: std.mem.Allocator,
    writer: anytype,
    metrics_config: types.MetricsConfig,
) !void {
    bench_metrics_config = metrics_config;
    try init_steady_state_benches();
    defer deinit_steady_state_benches();

    try writer.print("\nThroughput Summary (Approximate)\n", .{});
    try writer.print("--------------------------------------------------------------------------------\n", .{});
    try writer.print("{s: <30} | {s: <15} | {s: <15}\n", .{ "Benchmark", "Mean Latency", "Throughput" });
    try writer.print("--------------------------------------------------------------------------------\n", .{});

    // We run a fixed number of iterations for throughput estimation
    const iterations = 10_000;

    try run_and_print_throughput(writer, "put steady", iterations, 1, struct {
        fn run(_: std.mem.Allocator) void {
            const db = steady_put_db orelse unreachable;
            const value = types.Value{ .integer = 2 };
            db.put("bench:put", &value) catch unreachable;
        }
    }.run, allocator);

    try run_and_print_throughput(writer, "get steady", iterations, 1, struct {
        fn run(alloc: std.mem.Allocator) void {
            const db = steady_get_db orelse unreachable;
            var stored = (db.get(alloc, "bench:get") catch unreachable).?;
            defer stored.deinit(alloc);
            std.mem.doNotOptimizeAway(stored.integer);
        }
    }.run, allocator);

    try run_and_print_throughput(writer, "scan256 steady", 1000, 256, struct {
        fn run(alloc: std.mem.Allocator) void {
            const db = steady_scan_db orelse unreachable;
            var result = db.scan_prefix(alloc, "scan:") catch unreachable;
            defer result.deinit();
            std.mem.doNotOptimizeAway(result.entries.items.len);
        }
    }.run, allocator);

    try run_and_print_throughput(writer, "scan4096 steady", 100, 4096, struct {
        fn run(alloc: std.mem.Allocator) void {
            const db = steady_scan_large_db orelse unreachable;
            var result = db.scan_prefix(alloc, "scan:") catch unreachable;
            defer result.deinit();
            std.mem.doNotOptimizeAway(result.entries.items.len);
        }
    }.run, allocator);

    try run_and_print_throughput(writer, "batch64 steady overwrite", 1000, 64, struct {
        fn run(_: std.mem.Allocator) void {
            const db = steady_batch_overwrite_db orelse unreachable;
            var values: [batch_item_count]types.Value = undefined;
            var writes: [batch_item_count]types.PutWrite = undefined;
            var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;
            fill_batch_writes(&values, &writes, &key_storage, "batch", 1_000, 0);
            db.apply_batch(&writes) catch unreachable;
        }
    }.run, allocator);

    try run_and_print_throughput(writer, "art lookup", iterations, 1, struct {
        fn run(_: std.mem.Allocator) void {
            const tree = if (steady_art_tree) |*t| t else unreachable;
            const value = tree.lookup("scan:0000") orelse unreachable;
            std.mem.doNotOptimizeAway(value.integer);
        }
    }.run, allocator);

    try run_and_print_throughput(writer, "art insert", iterations, 1, struct {
        fn run(_: std.mem.Allocator) void {
            const tree = if (steady_art_tree) |*t| t else unreachable;
            var value = types.Value{ .integer = 999 };
            tree.insert("scan:0000", &value) catch unreachable;
        }
    }.run, allocator);

    try run_and_print_throughput(writer, "wal append", iterations, 1, struct {
        fn run(_: std.mem.Allocator) void {
            const wal = if (steady_wal) |*w| w else unreachable;
            const value = types.Value{ .integer = 42 };
            wal.append_put("bench:wal", &value) catch unreachable;
        }
    }.run, allocator);

    try writer.print("--------------------------------------------------------------------------------\n", .{});
}

fn run_and_print_throughput(
    writer: anytype,
    label: []const u8,
    iterations: usize,
    items_per_op: usize,
    func: fn (std.mem.Allocator) void,
    allocator: std.mem.Allocator,
) !void {
    var timer = try std.time.Timer.start();
    for (0..iterations) |_| {
        func(allocator);
    }
    const elapsed = timer.read();
    const avg_ns = elapsed / iterations;
    const ops_per_sec = (std.time.ns_per_s * iterations) / elapsed;
    const items_per_sec = ops_per_sec * items_per_op;

    var buf: [32]u8 = undefined;
    const latency_text = if (avg_ns < 1000)
        try std.fmt.bufPrint(&buf, "{d} ns", .{avg_ns})
    else
        try std.fmt.bufPrint(&buf, "{d:.2} us", .{@as(f64, @floatFromInt(avg_ns)) / 1000.0});

    if (items_per_op > 1) {
        try writer.print("{s: <30} | {s: <15} | {d:.2}M items/sec ({d:.2}M ops/sec)\n", .{
            label,
            latency_text,
            @as(f64, @floatFromInt(items_per_sec)) / 1_000_000.0,
            @as(f64, @floatFromInt(ops_per_sec)) / 1_000_000.0,
        });
    } else {
        try writer.print("{s: <30} | {s: <15} | {d:.2}M ops/sec\n", .{
            label,
            latency_text,
            @as(f64, @floatFromInt(ops_per_sec)) / 1_000_000.0,
        });
    }
}

const ScalingBenchmarkType = enum { get, put };

const ScalingWorkerContext = struct {
    db: *engine.Database,
    op: ScalingBenchmarkType,
    key: []const u8,
    iterations: usize,
};

fn scaling_worker(ctx: ScalingWorkerContext) void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const value = types.Value{ .integer = 123 };

    if (ctx.op == .put) {
        for (0..ctx.iterations) |_| {
            ctx.db.put(ctx.key, &value) catch unreachable;
        }
    } else {
        for (0..ctx.iterations) |_| {
            var stored = (ctx.db.get(allocator, ctx.key) catch unreachable).?;
            defer stored.deinit(allocator);
            std.mem.doNotOptimizeAway(stored.integer);
        }
    }
}

fn allocate_distinct_shard_worker_keys(allocator: std.mem.Allocator, thread_count: usize) ![][]u8 {
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
            const shard_idx = internal.runtime_shard.get_shard_index(key);
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

fn free_worker_keys(allocator: std.mem.Allocator, keys: [][]u8) void {
    for (keys) |key| allocator.free(key);
    allocator.free(keys);
}

fn run_scaling_benchmarks(allocator: std.mem.Allocator, writer: anytype) !void {
    try writer.print("\nSharded Scalability Benchmark\n", .{});
    try writer.print("--------------------------------------------------------------------------------\n", .{});
    try writer.print("{s: <15} | {s: <10} | {s: <15} | {s: <10}\n", .{ "Workload", "Threads", "Throughput", "Scaling" });
    try writer.print("--------------------------------------------------------------------------------\n", .{});

    const thread_counts = [_]usize{ 1, 2, 4, 8, 16 };
    const ops_per_test = 1_000_000;

    for ([_]ScalingBenchmarkType{ .get, .put }) |op| {
        var base_throughput: f64 = 0;
        for (thread_counts) |t_count| {
            const db = try open_bench_db(std.heap.page_allocator);
            defer db.close() catch unreachable;

            const worker_keys = try allocate_distinct_shard_worker_keys(allocator, t_count);
            defer free_worker_keys(allocator, worker_keys);

            // Pre-prime keys
            for (0..t_count) |i| {
                const value = types.Value{ .integer = @intCast(i) };
                try db.put(worker_keys[i], &value);
            }

            const iters_per_thread = ops_per_test / t_count;
            var threads = try allocator.alloc(std.Thread, t_count);
            defer allocator.free(threads);

            var timer = try std.time.Timer.start();
            for (0..t_count) |i| {
                threads[i] = try std.Thread.spawn(.{}, scaling_worker, .{ScalingWorkerContext{
                    .db = db,
                    .op = op,
                    .key = worker_keys[i],
                    .iterations = iters_per_thread,
                }});
            }

            for (threads) |thread| thread.join();
            const elapsed = timer.read();

            const throughput = @as(f64, @floatFromInt(ops_per_test)) / (@as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(std.time.ns_per_s)));
            if (t_count == 1) base_throughput = throughput;

            const scaling = throughput / base_throughput;
            const op_name = if (op == .get) "GET (Shared)" else "PUT (Sharded)";

            try writer.print("{s: <15} | {d: <10} | {d:.2}M ops/sec | {d:.2}x\n", .{
                op_name,
                t_count,
                throughput / 1_000_000.0,
                scaling,
            });
        }
        try writer.print("--------------------------------------------------------------------------------\n", .{});
    }
}

fn parse_cli_config(allocator: std.mem.Allocator) !BenchCliConfig {
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var cli = BenchCliConfig{};
    var saw_metrics_mode = false;
    var saw_latency_sample_shift = false;

    for (args[1..]) |arg| {
        if (std.mem.eql(u8, arg, "--help")) {
            print_usage();
            std.process.exit(0);
        }
        if (std.mem.startsWith(u8, arg, "--metrics-mode=")) {
            const value = arg["--metrics-mode=".len..];
            if (std.mem.eql(u8, value, "all")) {
                cli.run_all_metrics_modes = true;
                saw_metrics_mode = true;
                continue;
            }
            cli.metrics_config.mode = parse_metrics_mode(value) orelse return error.InvalidArgument;
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
        return error.InvalidArgument;
    }

    if (saw_metrics_mode and !saw_latency_sample_shift and (cli.run_all_metrics_modes or cli.metrics_config.mode == .sampled_latency)) {
        cli.metrics_config.latency_sample_shift = default_sampled_latency_shift;
    }

    return cli;
}

fn parse_metrics_mode(raw: []const u8) ?types.MetricsMode {
    if (std.mem.eql(u8, raw, "disabled")) return .disabled;
    if (std.mem.eql(u8, raw, "counters_only")) return .counters_only;
    if (std.mem.eql(u8, raw, "sampled_latency")) return .sampled_latency;
    if (std.mem.eql(u8, raw, "full")) return .full;
    return null;
}

fn print_usage() void {
    std.debug.print(
        \\usage: zeno-core-bench [--metrics-mode=disabled|counters_only|sampled_latency|full|all] [--latency-sample-shift=N] [--scan-allocation-profile] [--scan-candidate-profile]
        \\
    , .{});
}

fn run_scan_allocation_profile(writer: anytype, metrics_config: types.MetricsConfig) !void {
    bench_metrics_config = metrics_config;
    try print_metrics_config(writer, metrics_config);
    try writer.print("scan allocation profile (prefix=\"scan:\", page_limit={d})\n", .{scan_page_item_count});

    const scan256_full = try profile_scan_allocations(scan_item_count, .public_full);
    try print_scan_allocation_profile(writer, "scan256 steady", scan256_full);

    const scan256_in_view = try profile_scan_allocations(scan_item_count, .in_view_page);
    try print_scan_allocation_profile(writer, "scan64 in-view steady", scan256_in_view);

    const scan4096_full = try profile_scan_allocations(scan_large_item_count, .public_full);
    try print_scan_allocation_profile(writer, "scan4096 steady", scan4096_full);

    const scan4096_in_view = try profile_scan_allocations(scan_large_item_count, .in_view_page);
    try print_scan_allocation_profile(writer, "scan64 in-view 4096 steady", scan4096_in_view);
}

fn run_scan_candidate_profile(writer: anytype, metrics_config: types.MetricsConfig) !void {
    bench_metrics_config = metrics_config;
    try print_metrics_config(writer, metrics_config);
    try writer.print(
        "scan candidate profile (prefix=\"scan:\", page_limit={d})\n",
        .{scan_page_item_count},
    );

    const scan256_full = try profile_public_full_scan(256);
    try print_scan_candidate_profile(writer, "scan256 steady", scan256_full);

    const scan4096_full = try profile_public_full_scan(4096);
    try print_scan_candidate_profile(writer, "scan4096 steady", scan4096_full);
}

fn print_scan_allocation_profile(
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

fn print_scan_candidate_profile(
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

fn profile_scan_allocations(
    comptime fixture_items: usize,
    mode: ScanProfileMode,
) !ScanAllocationProfile {
    var db_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(db_gpa.deinit() == .ok);

    const db = try open_bench_db(db_gpa.allocator());
    defer db.close() catch unreachable;
    load_scan_fixture(fixture_items, db);

    var result_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(result_gpa.deinit() == .ok);

    var counting_state = FailingAllocator.init(result_gpa.allocator(), .{});
    const counting_allocator = counting_state.allocator();

    var emitted_entries: usize = 0;
    var has_next_cursor = false;

    switch (mode) {
        .public_full => {
            var result = try db.scan_prefix(counting_allocator, "scan:");
            emitted_entries = result.entries.items.len;
            result.deinit();
        },
        .in_view_page => {
            var view = try db.read_view();
            defer view.deinit();

            var page = try official.scan_prefix_from_in_view(&view, counting_allocator, "scan:", null, scan_page_item_count);
            emitted_entries = page.entries.items.len;
            has_next_cursor = page.borrow_next_cursor() != null;
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

fn profile_public_full_scan(comptime fixture_items: usize) !ScanCandidateProfile {
    var db_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(db_gpa.deinit() == .ok);

    const db = try open_bench_db(db_gpa.allocator());
    defer db.close() catch unreachable;
    load_scan_fixture(fixture_items, db);

    var result_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(result_gpa.deinit() == .ok);

    var counting_state = FailingAllocator.init(result_gpa.allocator(), .{});
    const counting_allocator = counting_state.allocator();

    var timer = try std.time.Timer.start();
    var result = try db.scan_prefix(counting_allocator, "scan:");
    const elapsed_ns = timer.read();

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

fn run_bench_suite(
    allocator: std.mem.Allocator,
    writer: anytype,
    metrics_config: types.MetricsConfig,
) !void {
    bench_metrics_config = metrics_config;
    try init_steady_state_benches();
    defer deinit_steady_state_benches();

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
    const get_existing = GetExistingBenchmark{};
    const get_existing_steady = GetExistingSteadyBenchmark{};
    const scan_prefix = ScanPrefixBenchmark{};
    const scan_prefix_steady = ScanPrefixSteadyBenchmark{};
    const scan_prefix_in_view_steady = ScanPrefixInViewSteadyBenchmark{};
    const scan_prefix_large_steady = ScanPrefixLargeSteadyBenchmark{};
    const scan_prefix_large_in_view_steady = ScanPrefixLargeInViewSteadyBenchmark{};
    const apply_batch = ApplyBatchBenchmark{};
    const apply_batch_steady_overwrite = ApplyBatchSteadyOverwriteBenchmark{};
    const apply_batch_steady_insert = ApplyBatchSteadyInsertBenchmark{};
    const apply_checked_batch = ApplyCheckedBatchBenchmark{};
    const apply_checked_batch_steady_overwrite = ApplyCheckedBatchSteadyOverwriteBenchmark{};
    const apply_checked_batch_steady_insert = ApplyCheckedBatchSteadyInsertBenchmark{};

    try stable_bench.addParam("put isolated", &put_fresh, .{});
    try stable_bench.addParam("put steady", &put_steady, .{});
    try stable_bench.addParam("get isolated", &get_existing, .{});
    try stable_bench.addParam("get steady", &get_existing_steady, .{});
    try stable_bench.addParam("scan256 isolated", &scan_prefix, .{});
    try stable_bench.addParam("scan256 steady", &scan_prefix_steady, .{});
    try stable_bench.addParam("scan64 in-view steady", &scan_prefix_in_view_steady, .{});
    try stable_bench.addParam("scan4096 steady", &scan_prefix_large_steady, .{});
    try stable_bench.addParam("scan64 in-view 4096 steady", &scan_prefix_large_in_view_steady, .{});
    try stable_bench.addParam("batch64 isolated", &apply_batch, .{});
    try stable_bench.addParam("batch64 steady overwrite", &apply_batch_steady_overwrite, .{});
    try stable_bench.addParam("checked64 isolated", &apply_checked_batch, .{});
    try stable_bench.addParam("checked64 steady overwrite", &apply_checked_batch_steady_overwrite, .{});

    try growing_bench.addParam("batch64 growing insert", &apply_batch_steady_insert, .{});
    try growing_bench.addParam("checked64 growing insert", &apply_checked_batch_steady_insert, .{});

    try stable_bench.addParam("art lookup", &ArtLookupBenchmark{}, .{});
    try stable_bench.addParam("art insert", &ArtInsertBenchmark{}, .{});
    try stable_bench.addParam("wal append", &WalAppendBenchmark{}, .{});

    try print_metrics_config(writer, metrics_config);
    try stable_bench.run(writer);
    try writer.print("\n", .{});
    try writer.print("growing workloads\n", .{});
    try growing_bench.run(writer);
}

fn print_metrics_config(writer: anytype, metrics_config: types.MetricsConfig) !void {
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

fn load_scan_fixture(comptime count: usize, db: *engine.Database) void {
    var key_storage: [count][16]u8 = undefined;
    for (0..count) |index| {
        const key = std.fmt.bufPrint(&key_storage[index], "scan:{d:0>4}", .{index}) catch unreachable;
        const value = types.Value{ .integer = @intCast(index) };
        db.put(key, &value) catch unreachable;
    }
}

fn init_steady_state_benches() !void {
    steady_put_db = try open_bench_db(std.heap.page_allocator);
    {
        const value = types.Value{ .integer = 1 };
        try steady_put_db.?.put("bench:put", &value);
    }

    steady_get_db = try open_bench_db(std.heap.page_allocator);
    {
        const value = types.Value{ .integer = 42 };
        try steady_get_db.?.put("bench:get", &value);
    }

    steady_scan_db = try open_bench_db(std.heap.page_allocator);
    load_scan_fixture(scan_item_count, steady_scan_db.?);
    steady_scan_view = try steady_scan_db.?.read_view();

    steady_scan_large_db = try open_bench_db(std.heap.page_allocator);
    load_scan_fixture(scan_large_item_count, steady_scan_large_db.?);
    steady_scan_large_view = try steady_scan_large_db.?.read_view();

    steady_batch_overwrite_db = try open_bench_db(std.heap.page_allocator);
    prime_batch_fixture(steady_batch_overwrite_db.?, "batch");

    steady_batch_insert_db = try open_bench_db(std.heap.page_allocator);
    steady_batch_insert_seed.store(0, .monotonic);

    steady_checked_batch_overwrite_db = try open_bench_db(std.heap.page_allocator);
    prime_batch_fixture(steady_checked_batch_overwrite_db.?, "guard");

    steady_checked_batch_insert_db = try open_bench_db(std.heap.page_allocator);
    steady_checked_batch_insert_seed.store(0, .monotonic);

    steady_art_tree = internal.art.Tree.init(std.heap.page_allocator);
    load_scan_fixture_to_art(scan_item_count, &steady_art_tree.?);

    wal_bench_path = try std.fmt.allocPrint(std.heap.page_allocator, "/tmp/zeno-bench-{d}.wal", .{std.time.timestamp()});
    steady_wal = try internal.wal.Wal.open(wal_bench_path.?, .{ .fsync_mode = .batched_async }, .{
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

fn load_scan_fixture_to_art(comptime count: usize, tree: *internal.art.Tree) void {
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

fn deinit_steady_state_benches() void {
    if (steady_wal) |*wal| {
        wal.close();
        if (wal_bench_path) |path| {
            std.fs.cwd().deleteFile(path) catch {};
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
    if (steady_put_db) |db| {
        db.close() catch unreachable;
        steady_put_db = null;
    }
}

fn prime_batch_fixture(db: *engine.Database, prefix: []const u8) void {
    var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;
    for (0..batch_item_count) |index| {
        const key = std.fmt.bufPrint(&key_storage[index], "{s}:{d:0>8}", .{ prefix, index }) catch unreachable;
        const value = types.Value{ .integer = @intCast(index) };
        db.put(key, &value) catch unreachable;
    }
}

fn fill_batch_writes(
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
