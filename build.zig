//! Package modules, tests, and benchmark build graph.
//! Time Complexity: O(1) build graph construction.
//! Allocator: Build nodes and options use the build allocator.

const std = @import("std");

/// Configures the exported module and executable test and benchmark steps.
///
/// Time Complexity: O(1).
///
/// Allocator: Uses the build allocator for graph nodes and generated options.
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zeno_options = b.addOptions();
    const expose_internals = b.option(bool, "expose_internals", "Expose internal modules for testing and benchmarking") orelse false;
    zeno_options.addOption(bool, "expose_internals", expose_internals);

    const zeno_module = b.addModule("zeno", .{
        .root_source_file = b.path("src/zeno.zig"),
        .target = target,
        .optimize = optimize,
    });
    zeno_module.addOptions("config", zeno_options);

    const module_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/zeno.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    module_tests.root_module.addOptions("config", zeno_options);
    const run_module_tests = b.addRunArtifact(module_tests);

    const test_step = b.step("test", "Run zeno-core tests");
    test_step.dependOn(&run_module_tests.step);

    const zbench_dep = b.dependency("zbench", .{
        .target = target,
        .optimize = optimize,
    });

    const bench_zeno_options = b.addOptions();
    bench_zeno_options.addOption(bool, "expose_internals", true);
    const bench_zeno_module = b.createModule(.{
        .root_source_file = b.path("src/zeno.zig"),
        .target = target,
        .optimize = optimize,
    });
    bench_zeno_module.addOptions("config", bench_zeno_options);

    const bench_exe = b.addExecutable(.{
        .name = "zeno-core-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    bench_exe.root_module.addImport("zeno", bench_zeno_module);
    bench_exe.root_module.addImport("zbench", zbench_dep.module("zbench"));

    const check_step = b.step("check", "Compile tests and benchmarks without running them");
    check_step.dependOn(&module_tests.step);
    check_step.dependOn(&bench_exe.step);

    const run_bench = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench", "Run zeno-core benchmarks");
    if (b.args) |args| {
        run_bench.addArgs(args);
    }
    bench_step.dependOn(&run_bench.step);
}
