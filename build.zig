const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    _ = b.addModule("zeno_core", .{
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
}
