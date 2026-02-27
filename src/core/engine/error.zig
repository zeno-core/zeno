//! Shared error contract for engine coordination and internal engine modules.
//! Cost: O(1) module reexports plus declared error metadata.
//! Allocator: Does not allocate.

/// Shared error set for engine contract operations, including live durability failures.
pub const EngineError = error{
    NotImplemented,
    OutOfMemory,
    KeyTooLarge,
    ActiveReadViews,
    ValueTooLarge,
    ValueTooDeep,
    GuardFailed,
    InvalidReadView,
    WalFlushFailed,
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
        error.WalFlushFailed => error.WalFlushFailed,
        error.NotImplemented => error.NotImplemented,
        else => error.PersistenceIoFailure,
    };
}
