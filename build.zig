const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const core_module = b.addModule("zeno_core", .{
        .root_source_file = b.path("src/zeno-core.zig"),
        .target = target,
    });

    const module_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/zeno-core.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_module_tests = b.addRunArtifact(module_tests);

    const test_step = b.step("test", "Run zeno-core tests");
    test_step.dependOn(&run_module_tests.step);

    const zbench_dep = b.dependency("zbench", .{
        .target = target,
        .optimize = optimize,
    });
    const bench_exe = b.addExecutable(.{
        .name = "zeno-core-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    bench_exe.root_module.addImport("zeno_core", core_module);
    bench_exe.root_module.addImport("zbench", zbench_dep.module("zbench"));

    const run_bench = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench", "Run zeno-core benchmarks");
    bench_step.dependOn(&run_bench.step);
}
