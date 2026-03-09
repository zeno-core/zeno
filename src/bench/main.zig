//! Benchmark entrypoint for coarse zeno-core engine performance checks.
//! Cost: Bench-dependent and intentionally end-to-end across public engine boundaries.
//! Allocator: Uses the benchmark-provided allocator for caller-owned result teardown and a process-wide page allocator for steady-state engine fixtures.

const std = @import("std");
const zbench = @import("zbench");
const zeno_core = @import("zeno_core");

const engine = zeno_core.public;
const official = zeno_core.official;
const types = zeno_core.types;
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
    page_calls: usize,
    cursor_handoffs: usize,
    initial_fetch_calls: usize,
    refill_fetch_calls: usize,
    art_fetches: usize,
    visibility_skips: usize,
    empty_fetches: usize,
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
    }

    try stdout.interface.flush();
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
    try writer.print("scan candidate profile (prefix=\"scan:\", page_limit={d})\n", .{scan_page_item_count});

    const scan256_full = try profile_public_full_scan(scan_item_count);
    try print_scan_candidate_profile(writer, "scan256 steady", scan256_full);

    const scan256_candidate = try profile_merged_full_scan_candidate(scan_item_count);
    try print_scan_candidate_profile(writer, "scan256 merged page-loop candidate", scan256_candidate);

    const scan4096_full = try profile_public_full_scan(scan_large_item_count);
    try print_scan_candidate_profile(writer, "scan4096 steady", scan4096_full);

    const scan4096_candidate = try profile_merged_full_scan_candidate(scan_large_item_count);
    try print_scan_candidate_profile(writer, "scan4096 merged page-loop candidate", scan4096_candidate);
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
        "{s}: entries={d} pages={d} cursor_handoffs={d} initial_fetch_calls={d} refill_fetch_calls={d} art_fetches={d} visibility_skips={d} empty_fetches={d} allocations={d} deallocations={d} allocated_bytes={d} freed_bytes={d} elapsed_ns={d}\n",
        .{
            label,
            profile.emitted_entries,
            profile.page_calls,
            profile.cursor_handoffs,
            profile.initial_fetch_calls,
            profile.refill_fetch_calls,
            profile.art_fetches,
            profile.visibility_skips,
            profile.empty_fetches,
            profile.allocations,
            profile.deallocations,
            profile.allocated_bytes,
            profile.freed_bytes,
            profile.elapsed_ns,
        },
    );
}

fn accumulate_merge_profile_stats(
    totals: *official.MergePageProfileStats,
    page_stats: official.MergePageProfileStats,
) void {
    totals.initial_fetch_calls += page_stats.initial_fetch_calls;
    totals.refill_fetch_calls += page_stats.refill_fetch_calls;
    totals.art_fetches += page_stats.art_fetches;
    totals.visibility_skips += page_stats.visibility_skips;
    totals.empty_fetches += page_stats.empty_fetches;
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
        .page_calls = 1,
        .cursor_handoffs = 0,
        .initial_fetch_calls = 0,
        .refill_fetch_calls = 0,
        .art_fetches = 0,
        .visibility_skips = 0,
        .empty_fetches = 0,
        .allocations = counting_state.allocations,
        .deallocations = counting_state.deallocations,
        .allocated_bytes = counting_state.allocated_bytes,
        .freed_bytes = counting_state.freed_bytes,
        .elapsed_ns = elapsed_ns,
    };
}

fn profile_merged_full_scan_candidate(comptime fixture_items: usize) !ScanCandidateProfile {
    var db_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(db_gpa.deinit() == .ok);

    const db = try open_bench_db(db_gpa.allocator());
    defer db.close() catch unreachable;
    load_scan_fixture(fixture_items, db);

    var result_gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(result_gpa.deinit() == .ok);

    var counting_state = FailingAllocator.init(result_gpa.allocator(), .{});
    const counting_allocator = counting_state.allocator();

    var view = try db.read_view();
    defer view.deinit();

    var collected = std.ArrayList(types.ScanEntry).empty;
    var cursor_owner: ?types.OwnedScanCursor = null;
    var page_calls: usize = 0;
    var cursor_handoffs: usize = 0;
    var merge_stats = official.MergePageProfileStats{};

    var timer = try std.time.Timer.start();
    defer if (cursor_owner) |*cursor| cursor.deinit();

    while (true) {
        var borrowed_cursor: ?types.ScanCursor = null;
        if (cursor_owner) |owned_cursor| {
            borrowed_cursor = owned_cursor.as_cursor();
        }

        const profiled_page = try official.scan_prefix_from_in_view_profiled(
            &view,
            counting_allocator,
            "scan:",
            if (borrowed_cursor) |*cursor| cursor else null,
            scan_page_item_count,
        );
        page_calls += 1;
        accumulate_merge_profile_stats(&merge_stats, profiled_page.stats);

        var page = profiled_page.page;

        errdefer page.deinit();
        errdefer {
            for (collected.items) |entry| {
                counting_allocator.free(entry.key);
                const owned_value: *types.Value = @constCast(entry.value);
                owned_value.deinit(counting_allocator);
                counting_allocator.destroy(owned_value);
            }
            collected.deinit(counting_allocator);
        }

        try collected.ensureTotalCapacity(counting_allocator, collected.items.len + page.entries.items.len);

        var page_entries = page.entries;
        page.entries = std.ArrayList(types.ScanEntry).empty;
        for (page_entries.items) |entry| {
            collected.appendAssumeCapacity(entry);
        }
        page_entries.deinit(counting_allocator);

        if (cursor_owner) |*cursor| {
            cursor.deinit();
        }
        cursor_owner = page.take_next_cursor();
        if (cursor_owner != null) cursor_handoffs += 1;

        page.deinit();

        if (cursor_owner == null) break;
    }

    const elapsed_ns = timer.read();
    const emitted_entries = collected.items.len;
    std.debug.assert(emitted_entries == fixture_items);

    var result = types.ScanResult{
        .entries = collected,
        .allocator = counting_allocator,
    };
    result.deinit();

    std.debug.assert(counting_state.allocated_bytes == counting_state.freed_bytes);

    return .{
        .emitted_entries = emitted_entries,
        .page_calls = page_calls,
        .cursor_handoffs = cursor_handoffs,
        .initial_fetch_calls = merge_stats.initial_fetch_calls,
        .refill_fetch_calls = merge_stats.refill_fetch_calls,
        .art_fetches = merge_stats.art_fetches,
        .visibility_skips = merge_stats.visibility_skips,
        .empty_fetches = merge_stats.empty_fetches,
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
}

fn deinit_steady_state_benches() void {
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
