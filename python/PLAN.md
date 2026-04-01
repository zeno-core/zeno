# Zeno Python Port - Implementation Plan

## Overview
Port Zeno KV store from Zig to Python with:
- **Location**: `python/zeno/`
- **Dependency Management**: uv
- **Dependencies**: Only standard library + pickle (zero external deps)
- **Concurrency**: asyncio with `asyncio.to_thread` for CPU-bound tasks

## Architecture Mapping

| Zig Component | Python Implementation |
|---------------|----------------------|
| Value (union) | Python class with type hints |
| ART Index | Custom Python classes (Node4/16/48/256) |
| 256 Shards | List of Shard instances |
| RwLock + Seqlock | asyncio.Lock (simpler, sufficient for Python) |
| Arena Allocator | Python object lifecycle (GC) |
| WAL | Append-only log file |
| Snapshot | pickle-based serialization |

## File Structure

```
python/zeno/
├── __init__.py              # Public API exports
├── types.py                 # Value, Key types
├── exceptions.py            # ZenoError, KeyTooLarge, etc.
├── constants.py             # MAX_KEY_LEN, NUM_SHARDS, etc.
├── art/
│   ├── __init__.py          # ART Tree class
│   ├── node.py              # Node4, Node16, Node48, Node256
│   ├── leaf.py              # Leaf node
│   └── utils.py             # Helper functions
├── shard.py                 # Shard (lock + ART + TTL)
├── database.py              # Main Database class (256 shards)
├── operations.py            # get, put, delete implementations
├── scan.py                  # Prefix/range scan
├── batch.py                 # Atomic batch operations
├── ttl.py                   # TTL handling
└── persistence/
    ├── __init__.py
    ├── wal.py               # Write-ahead log
    ├── snapshot.py          # Snapshot save/load
    └── codec.py             # Serialization (pickle)
```

## Implementation Phases

### Phase 1: Core Types and ART (Week 1)
**Files**: `types.py`, `art/`, `constants.py`, `exceptions.py`

1. **Value Type** (`types.py`):
   - Support: None, bool, int, float, str, bytes, list, dict
   - Deep clone and deinit methods
   - JSON-compatible but preserves bytes

2. **ART Index** (`art/`):
   - **Node4**: 4 children, linear search, 64 bytes
   - **Node16**: 16 children, binary/linear search, 128 bytes
   - **Node48**: 48 children, 256-byte index array, 168 bytes
   - **Node256**: 256 children, direct array, 288 bytes
   - **Leaf**: key + value storage
   - **Tree**: insert, lookup, delete, scan operations
   - Path compression (11-byte inline prefix)
   - Node growth/shrink transitions

3. **Key Validation**:
   - Max key length: 4096 bytes
   - Empty key rejection

### Phase 2: Shard and Basic Operations (Week 2)
**Files**: `shard.py`, `database.py`, `operations.py`

1. **Shard** (`shard.py`):
   - Single asyncio.Lock (simpler than RwLock + seqlock)
   - ART instance
   - TTL index (dict: key -> expire_time)
   - Hash routing: Wyhash-based (consistent with Zig)
   - Hash tag support: `{user:1}:profile` syntax

2. **Database** (`database.py`):
   - 256 shards
   - Key routing: `shard_index = hash(key) & 255`
   - Initialization and cleanup
   - Metrics counters (optional)

3. **Operations** (`operations.py`):
   - `get(key) -> Optional[Value]`
   - `put(key, value) -> None`
   - `delete(key) -> bool`
   - `exists(key) -> bool`
   - Lock acquisition per shard

### Phase 3: Scan and Batch (Week 3)
**Files**: `scan.py`, `batch.py`

1. **Scan Operations** (`scan.py`):
   - Prefix scan: all keys starting with prefix
   - Range scan: keys in [start, end) range
   - Pagination support (cursor-based)
   - Merge results from all shards
   - Sort by key lexicographically

2. **Batch Operations** (`batch.py`):
   - Atomic batch apply
   - All-or-nothing semantics
   - Lock all shards involved
   - WAL batch envelope

### Phase 4: Persistence (Week 4)
**Files**: `persistence/`

1. **WAL** (`wal.py`):
   - Record types: PUT, DELETE, EXPIRE, BATCH_BEGIN, BATCH_COMMIT
   - Append-only file
   - CRC32 checksums
   - LSN (log sequence number) tracking
   - Fsync modes (configurable)

2. **Snapshot** (`snapshot.py`):
   - Format: pickle-based
   - Header: magic + version + checkpoint LSN
   - Per-shard data: keys, values, TTL metadata
   - CRC checksum

3. **Recovery**:
   - Load snapshot if exists
   - Replay WAL from checkpoint LSN + 1
   - Purge expired keys

### Phase 5: TTL and Optimization (Week 5)
**Files**: `ttl.py`, optimization

1. **TTL Support** (`ttl.py`):
   - `expire_at(key, timestamp)`
   - `ttl(key) -> seconds_remaining`
   - Background sweeper thread (optional)
   - Lazy expiration on read

2. **Optimization**:
   - `compact_memory()` - snapshot + reload
   - `compact_shard(shard_idx)` - rebuild one shard
   - Auto-compaction triggers (optional)

3. **Testing**:
   - Unit tests for all components
   - Property-based tests (optional)
   - Concurrency tests
   - Crash recovery tests

## Key Design Decisions

### 1. Locking Strategy
- **Decision**: Use single `asyncio.Lock` per shard
- **Rationale**: 
  - Python's GIL limits true parallelism
  - Simpler than RwLock + seqlock
  - Asyncio-friendly
  - Sufficient for I/O-bound workloads

### 2. ART Node Search
- **Decision**: Linear search for Node16 (no SIMD in Python)
- **Rationale**:
  - Python overhead dominates
  - Binary search complexity not worth it for 16 elements
  - Keep code simple and readable

### 3. Serialization
- **Decision**: Use pickle for snapshots
- **Rationale**:
  - Zero external dependencies
  - Fast for Python objects
  - Not compatible with Zig format (acceptable)

### 4. Memory Management
- **Decision**: Rely on Python GC
- **Rationale**:
  - No manual memory management in Python
  - Compaction via snapshot/reload
  - TTL sweeper for cleanup

### 5. Concurrency Model
- **Decision**: Asyncio with thread pool for CPU tasks
- **Rationale**:
  - Modern Python async pattern
  - `asyncio.to_thread()` for CPU-bound work (ART operations)
  - Main event loop never blocked

## API Surface

```python
# Public API (python/zeno/__init__.py)
from .database import Database
from .types import Value
from .exceptions import ZenoError, KeyTooLarge, KeyNotFound

__all__ = ['Database', 'Value', 'ZenoError', 'KeyTooLarge', 'KeyNotFound']

# Usage Example:
import asyncio
from zeno import Database

async def main():
    db = Database()
    
    # Basic operations
    await db.put("key", {"nested": "value"})
    value = await db.get("key")
    
    # Scan
    results = await db.scan_prefix("user:")
    
    # Batch
    await db.batch_put([
        ("key1", "value1"),
        ("key2", "value2"),
    ])
    
    # With persistence
    db = Database(
        wal_path="data.wal",
        snapshot_path="data.snapshot"
    )
    await db.checkpoint()

asyncio.run(main())
```

## Testing Strategy

1. **Unit Tests**: Each component tested in isolation
2. **Integration Tests**: Full workflow tests
3. **Concurrency Tests**: Multiple async tasks
4. **Recovery Tests**: Crash simulation
5. **Compatibility Tests**: Compare behavior with Zig version

## Timeline

- **Week 1**: Phase 1 (Types + ART)
- **Week 2**: Phase 2 (Shard + Database + Basic Ops)
- **Week 3**: Phase 3 (Scan + Batch)
- **Week 4**: Phase 4 (Persistence)
- **Week 5**: Phase 5 (TTL + Optimization + Testing)

Total: ~5 weeks for complete port

## Success Criteria

1. All basic operations work (GET, PUT, DELETE)
2. Prefix and range scans work
3. Atomic batch operations work
4. WAL recovery works
5. Snapshot save/load works
6. TTL expiration works
7. Performance: Within 10x of Zig version (acceptable for Python)
8. Test coverage: >90%

## Next Steps

1. Initialize uv project in `python/zeno/`
2. Create `pyproject.toml` with minimal deps
3. Start Phase 1: Implement `types.py` and ART
4. Write tests as we go (TDD approach)
