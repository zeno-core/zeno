//! Runtime-owned visibility coordination primitives for consistent reads and atomic writes.
//! Cost: O(1) lock coordination per acquire or release call.
//! Allocator: Does not allocate.

const std = @import("std");

/// Global reader-visible commit gate used to coordinate covered reads and batch visibility.
pub const VisibilityGate = struct {
    io: std.Io,
    lock: std.Io.RwLock = .init,

    /// Acquires shared visibility access for read-side coordination.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Thread-safe; acquires the shared side of the runtime visibility gate.
    pub fn lockShared(self: *VisibilityGate) void {
        self.lock.lockSharedUncancelable(self.io);
    }

    /// Releases shared visibility access for read-side coordination.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Thread-safe; releases the shared side of the runtime visibility gate.
    pub fn unlockShared(self: *VisibilityGate) void {
        self.lock.unlockShared(self.io);
    }

    /// Acquires exclusive visibility access for write-side coordination.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Thread-safe; acquires the exclusive side of the runtime visibility gate.
    pub fn lockExclusive(self: *VisibilityGate) void {
        self.lock.lockUncancelable(self.io);
    }

    /// Attempts to acquire exclusive visibility access without blocking.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Thread-safe; returns whether the exclusive side of the runtime visibility gate was acquired.
    pub fn tryLockExclusive(self: *VisibilityGate) bool {
        return self.lock.tryLock(self.io);
    }

    /// Releases exclusive visibility access for write-side coordination.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Thread-safe; releases the exclusive side of the runtime visibility gate.
    pub fn unlockExclusive(self: *VisibilityGate) void {
        self.lock.unlock(self.io);
    }
};
