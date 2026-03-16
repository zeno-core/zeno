//! Shared error contract for engine coordination and internal engine modules.
//! Cost: O(1) module reexports plus declared error metadata.
//! Allocator: Does not allocate.

/// Shared error set for engine contract operations, including durability and recovery failures.
pub const EngineError = error{
    NotImplemented,
    OutOfMemory,
    KeyTooLarge,
    InvalidShardIndex,
    NoSnapshotPath,
    CheckpointBusy,
    ActiveReadViews,
    ValueTooLarge,
    ValueTooDeep,
    GuardFailed,
    InvalidReadView,
    WalFlushFailed,
    SnapshotCorrupted,
    PersistenceIoFailure,
};

/// Translates storage-layer failures into the public engine error surface.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn map_persistence_error(err: anyerror) EngineError {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        error.NoSnapshotPath => error.NoSnapshotPath,
        error.WalFlushFailed => error.WalFlushFailed,
        error.SnapshotCorrupted => error.SnapshotCorrupted,
        error.NotImplemented => error.NotImplemented,
        else => error.PersistenceIoFailure,
    };
}
