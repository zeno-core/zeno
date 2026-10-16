//! Internal engine metrics helpers for boundary-scoped latency sampling.
//! Cost: O(1) timer setup and one runtime counter update per wrapped call.
//! Allocator: Does not allocate.

const std = @import("std");
const runtime_state = @import("../runtime/state.zig");

/// Runs one engine-boundary operation and records one latency sample against `state`.
///
/// Time Complexity: O(1) wrapper overhead plus the delegated operation cost.
///
/// Allocator: Does not allocate.
///
/// Thread Safety: Uses runtime-state atomic metric updates only; delegated operation retains its own thread-safety contract.
pub fn callWithLatency(
    state: *const runtime_state.DatabaseState,
    comptime operation: anytype,
    args: anytype,
) @TypeOf(@call(.auto, operation, args)) {
    if (!state.shouldRecordLatency()) {
        return @call(.auto, operation, args);
    }
    const started = std.Io.Clock.awake.now(state.io);
    defer state.recordLatencySample(@intCast(started.durationTo(std.Io.Clock.awake.now(state.io)).toNanoseconds()));
    return @call(.auto, operation, args);
}

/// Runs one engine-boundary operation and records latency only when `state` is known.
///
/// Time Complexity: O(1) wrapper overhead plus the delegated operation cost.
///
/// Allocator: Does not allocate.
///
/// Thread Safety: Uses runtime-state atomic metric updates only when `state` is non-null; delegated operation retains its own thread-safety contract.
pub fn callWithOptionalLatency(
    state: ?*const runtime_state.DatabaseState,
    comptime operation: anytype,
    args: anytype,
) @TypeOf(@call(.auto, operation, args)) {
    if (state) |resolved_state| {
        if (!resolved_state.shouldRecordLatency()) {
            return @call(.auto, operation, args);
        }
    } else {
        return @call(.auto, operation, args);
    }
    const started = std.Io.Clock.awake.now(state.?.io);
    defer if (state) |resolved_state| {
        resolved_state.recordLatencySample(@intCast(started.durationTo(std.Io.Clock.awake.now(resolved_state.io)).toNanoseconds()));
    };
    return @call(.auto, operation, args);
}

fn incrementCounter(counter: *u32) void {
    counter.* += 1;
}

test "call_with_latency skips latency counters when disabled" {
    const testing = std.testing;

    var state = runtime_state.DatabaseState.initWithMetrics(std.testing.io, testing.allocator, null, .{
        .mode = .disabled,
    });
    defer state.deinit();

    var counter: u32 = 0;
    callWithLatency(&state, incrementCounter, .{&counter});

    try testing.expectEqual(@as(u32, 1), counter);
    try testing.expectEqual(@as(u64, 0), state.statsSnapshot().latency_samples_total);
}

test "call_with_optional_latency records when full mode is enabled" {
    const testing = std.testing;

    var state = runtime_state.DatabaseState.initWithMetrics(std.testing.io, testing.allocator, null, .{
        .mode = .full,
    });
    defer state.deinit();

    var counter: u32 = 0;
    callWithOptionalLatency(&state, incrementCounter, .{&counter});

    try testing.expectEqual(@as(u32, 1), counter);
    try testing.expectEqual(@as(u64, 1), state.statsSnapshot().latency_samples_total);
}
