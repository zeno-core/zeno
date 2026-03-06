//! Public consistent-read view handle for official advanced reads.
//! Cost: O(1) handle storage plus O(1) release of the held visibility gate.
//! Allocator: Does not allocate.

const std = @import("std");
const runtime_visibility = @import("../runtime/visibility.zig");

const ReadViewCounter = std.atomic.Value(usize);

/// Registry payload for one active read-view token.
const ReadViewToken = struct {
    runtime_state: *const anyopaque,
    visibility_gate: *runtime_visibility.VisibilityGate,
    active_read_views: *ReadViewCounter,
    opened_at_unix_seconds: i64,
};

var next_read_view_token_id = std.atomic.Value(u64).init(1);
var read_view_tokens_mutex: std.Thread.Mutex = .{};
var read_view_tokens = std.AutoHashMapUnmanaged(u64, ReadViewToken){};

/// Registers one active read-view token for a borrowed runtime-state handle.
///
/// Time Complexity: O(1) expected.
///
/// Allocator: Uses `std.heap.page_allocator` only when the registry grows.
///
/// Ownership: Retains borrowed pointers inside the registry until `release_read_view_token` removes the token.
///
/// Thread Safety: Serializes registry mutation through `read_view_tokens_mutex`.
fn register_read_view_token(
    runtime_state: *const anyopaque,
    visibility_gate: *runtime_visibility.VisibilityGate,
    active_read_views: *ReadViewCounter,
    opened_at_unix_seconds: i64,
) std.mem.Allocator.Error!u64 {
    const token_id = next_read_view_token_id.fetchAdd(1, .monotonic);
    read_view_tokens_mutex.lock();
    defer read_view_tokens_mutex.unlock();
    try read_view_tokens.put(std.heap.page_allocator, token_id, .{
        .runtime_state = runtime_state,
        .visibility_gate = visibility_gate,
        .active_read_views = active_read_views,
        .opened_at_unix_seconds = opened_at_unix_seconds,
    });
    return token_id;
}

/// Resolves one active read-view token without transferring ownership.
///
/// Time Complexity: O(1) expected.
///
/// Allocator: Does not allocate.
///
/// Ownership: Returns borrowed registry payload that remains valid only while the token stays active.
///
/// Thread Safety: Serializes registry access through `read_view_tokens_mutex`.
fn get_read_view_token(token_id: u64) ?ReadViewToken {
    read_view_tokens_mutex.lock();
    defer read_view_tokens_mutex.unlock();
    return read_view_tokens.get(token_id);
}

/// Removes one active read-view token from the registry.
///
/// Time Complexity: O(1) expected.
///
/// Allocator: Does not allocate.
///
/// Ownership: Transfers the removed token payload to the caller, which then becomes responsible for releasing the borrowed visibility hold exactly once.
///
/// Thread Safety: Serializes registry mutation through `read_view_tokens_mutex`.
fn release_read_view_token(token_id: u64) ?ReadViewToken {
    read_view_tokens_mutex.lock();
    defer read_view_tokens_mutex.unlock();
    const removed = read_view_tokens.fetchRemove(token_id) orelse return null;
    return removed.value;
}

/// Consistent-read window handle for the physical `zeno-core` engine contract.
pub const ReadView = struct {
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
        opened_at_unix_seconds: i64,
    ) std.mem.Allocator.Error!ReadView {
        const token_id = try register_read_view_token(runtime_state, visibility_gate, active_read_views, opened_at_unix_seconds);
        _ = active_read_views.fetchAdd(1, .monotonic);
        return .{
            .token_id = token_id,
        };
    }

    /// Resolves the borrowed runtime state for one still-active read view.
    ///
    /// Time Complexity: O(1) expected.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Returns the borrowed runtime-state pointer only while the registry token is still active.
    pub fn resolve_runtime_state(self: *const ReadView) ?*const anyopaque {
        const token = get_read_view_token(self.token_id) orelse return null;
        return token.runtime_state;
    }

    /// Releases the consistent-read window and its borrowed visibility hold.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Releases the shared visibility hold tracked by the registry token when still active.
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

/// Resolves the read-view timestamp captured when the handle was created.
///
/// Time Complexity: O(1) expected.
///
/// Allocator: Does not allocate.
///
/// Ownership: Returns the borrowed timestamp only while the registry token is still active.
pub fn resolve_opened_at_unix_seconds(view: *const ReadView) ?i64 {
    const token = get_read_view_token(view.token_id) orelse return null;
    return token.opened_at_unix_seconds;
}
