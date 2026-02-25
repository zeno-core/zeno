//! Runtime-owned visibility coordination primitives for consistent reads and atomic writes.
//! Cost: O(1) lock coordination per acquire or release call.
//! Allocator: Does not allocate.

const std = @import("std");

/// Global reader-visible commit gate used to coordinate covered reads and batch visibility.
pub const VisibilityGate = struct {
    lock: std.Thread.RwLock = .{},

    /// Acquires shared visibility access for read-side coordination.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Thread-safe; acquires the shared side of the runtime visibility gate.
    pub fn lock_shared(self: *VisibilityGate) void {
        self.lock.lockShared();
    }

    /// Releases shared visibility access for read-side coordination.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Thread-safe; releases the shared side of the runtime visibility gate.
    pub fn unlock_shared(self: *VisibilityGate) void {
        self.lock.unlockShared();
    }

    /// Acquires exclusive visibility access for write-side coordination.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Thread-safe; acquires the exclusive side of the runtime visibility gate.
    pub fn lock_exclusive(self: *VisibilityGate) void {
        self.lock.lock();
    }

    /// Attempts to acquire exclusive visibility access without blocking.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Thread-safe; returns whether the exclusive side of the runtime visibility gate was acquired.
    pub fn try_lock_exclusive(self: *VisibilityGate) bool {
        return self.lock.tryLock();
    }

    /// Releases exclusive visibility access for write-side coordination.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Thread-safe; releases the exclusive side of the runtime visibility gate.
    pub fn unlock_exclusive(self: *VisibilityGate) void {
        self.lock.unlock();
    }
};
