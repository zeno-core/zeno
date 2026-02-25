//! Public consistent-read view handle for official advanced reads.
//! Cost: O(1) handle storage plus O(1) release of the held visibility gate.
//! Allocator: Does not allocate.

const runtime_visibility = @import("../runtime/visibility.zig");

/// Logical consistent-read window handle.
pub const ReadView = struct {
    runtime_state: *const anyopaque,
    visibility_gate: *runtime_visibility.VisibilityGate,
    active: bool = true,

    /// Releases the consistent-read window and its borrowed visibility hold.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Does not own `runtime_state` or `visibility_gate`; only releases the borrowed shared visibility hold when active.
    ///
    /// Thread Safety: Not thread-safe; callers must not race `deinit` against other mutation of the same `ReadView` handle.
    pub fn deinit(self: *ReadView) void {
        if (!self.active) return;
        self.visibility_gate.unlock_shared();
        self.active = false;
    }
};
