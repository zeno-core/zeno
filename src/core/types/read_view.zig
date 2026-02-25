//! Public consistent-read view handle for official advanced reads.
//! Cost: O(1) handle storage plus O(1) release of the held visibility gate.
//! Allocator: Does not allocate.

const std = @import("std");
const runtime_visibility = @import("../runtime/visibility.zig");

const ReadViewCounter = std.atomic.Value(usize);

const ReadViewToken = struct {
    visibility_gate: *runtime_visibility.VisibilityGate,
    active_read_views: *ReadViewCounter,
};

var next_read_view_token_id = std.atomic.Value(u64).init(1);
var read_view_tokens_mutex: std.Thread.Mutex = .{};
var read_view_tokens = std.AutoHashMapUnmanaged(u64, ReadViewToken){};

fn register_read_view_token(
    visibility_gate: *runtime_visibility.VisibilityGate,
    active_read_views: *ReadViewCounter,
) std.mem.Allocator.Error!u64 {
    const token_id = next_read_view_token_id.fetchAdd(1, .monotonic);
    read_view_tokens_mutex.lock();
    defer read_view_tokens_mutex.unlock();
    try read_view_tokens.put(std.heap.page_allocator, token_id, .{
        .visibility_gate = visibility_gate,
        .active_read_views = active_read_views,
    });
    return token_id;
}

fn release_read_view_token(token_id: u64) ?ReadViewToken {
    read_view_tokens_mutex.lock();
    defer read_view_tokens_mutex.unlock();
    const removed = read_view_tokens.fetchRemove(token_id) orelse return null;
    return removed.value;
}

/// Logical consistent-read window handle.
pub const ReadView = struct {
    runtime_state: *const anyopaque,
    token_id: u64,

    /// Registers one active read-view token and returns an initialized handle.
    ///
    /// Time Complexity: O(1) expected.
    ///
    /// Allocator: Uses `std.heap.page_allocator` only for registry growth.
    ///
    /// Ownership: The returned handle borrows `runtime_state` and owns one registry token tracked until `deinit`.
    pub fn init(
        runtime_state: *const anyopaque,
        visibility_gate: *runtime_visibility.VisibilityGate,
        active_read_views: *ReadViewCounter,
    ) std.mem.Allocator.Error!ReadView {
        const token_id = try register_read_view_token(visibility_gate, active_read_views);
        _ = active_read_views.fetchAdd(1, .monotonic);
        return .{
            .runtime_state = runtime_state,
            .token_id = token_id,
        };
    }

    /// Releases the consistent-read window and its borrowed visibility hold.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Does not own `runtime_state`; releases the shared visibility hold tracked by the registry token when still active.
    ///
    /// Thread Safety: Not thread-safe; callers must not race `deinit` against other mutation of the same `ReadView` handle.
    pub fn deinit(self: *ReadView) void {
        const token = release_read_view_token(self.token_id) orelse {
            self.token_id = 0;
            return;
        };
        token.visibility_gate.unlock_shared();
        _ = token.active_read_views.fetchSub(1, .monotonic);
        self.token_id = 0;
    }
};
