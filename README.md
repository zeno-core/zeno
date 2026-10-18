# Zeno - Zig Engine for Node Operations

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Zig Version](https://img.shields.io/badge/Zig-0.16.0-orange.svg)](https://ziglang.org)

Zeno is a high-performance, embedded key-value storage engine written in pure Zig. Designed for modern workloads, it prioritizes predictable low latency, zero-implicit allocation, and efficient sharded concurrency. Its name (Node) reflects the core index and storage nodes that power each operation, not to be confused with Node.js.

Zeno began as a learning experiment into database storage internals and the Adaptive Radix Tree (ART). The results and performance were promising enough that it evolved into a standalone engine.

---

## 🚀 Key Features

*   **Adaptive Radix Tree (ART) Index**: O(k) lookups with SIMD-optimized node transitions (Node4 to Node256).
*   **Sharded Concurrency**: 256-shard architecture with lock-free GET via seqlock + tagged-pointer ART. Concurrent readers never block each other; writers serialize per shard.
*   **Zero-Implicit Allocation**: Every function that allocates accepts an explicit `Allocator`, following strict Zig practices.
*   **Durable Persistence**: 
    *   **WAL (Write-Ahead Log)**: Batched-async durability mode for high-throughput writes.
    *   **Snapshots**: Efficient, streaming snapshot-backed recovery.
*   **Structured Values**: Support for complex values via `union(enum)` for strict runtime type safety.

## 📊 Performance Benchmarks

Zeno is built for speed. Numbers below are from the current benchmark suite running
on Ubuntu 24.04.4, AMD Ryzen 7 5700X, 32GB DDR4 @ 3200MHz, using Zig 0.15.2.
These are historical measurements; rerun the suite on your target hardware with
Zig 0.16.0 before comparing performance.

**Benchmark methodology:** steady-state benchmarks use 1,000 rotating keys, 2,000
warmup iterations, and 100,000 measured iterations. Latency columns show p50/p99.
Scaling benchmarks run 1,000,000 ops per configuration.

### Point operation throughput

| Operation | Throughput | p50 | p99 |
| :--- | :--- | :--- | :--- |
| **DB PUT (overwrite, steady)** | **14.75M ops/sec** | **70 ns** | **90 ns** |
| **DB GET (steady)** | **10.71M ops/sec** | **90 ns** | **110 ns** |
| **DB GET (steady, TTL mixed)** | **17.47M ops/sec** | **50 ns** | **100 ns** |
| **DB PUT Group16 (steady)** | **1.18M items/sec** | **12.98 µs** | **19.83 µs** |
| ART Lookup | 20.98M ops/sec | 50 ns | 60 ns |
| ART Insert | 30.27M ops/sec | 30 ns | 40 ns |
| WAL Append (async) | 0.66M ops/sec | 1.38 µs | 3.71 µs |

### Sharded scalability

GET is lock-free via seqlock — multiple readers on the same shard proceed in parallel
with no serialization between them. PUT serializes writers via shard mutex; inserts
that modify ART structure additionally bracket with the seqlock sequence counter.

**GET — no contention (each thread on a distinct shard):**

| Threads | 1 | 2 | 4 | 8 | 16 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Throughput | 35.58M | 67.24M | 119.28M | 169.73M | 203.11M ops/sec |
| Scaling | 1.00x | 1.89x | 3.35x | 4.77x | 5.71x |

**GET — hotspot (all threads on the same key):**

| Threads | 1 | 2 | 4 | 8 | 16 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Throughput | 34.05M | 65.10M | 121.84M | 226.88M | 281.13M ops/sec |
| Scaling | 1.00x | 1.91x | 3.58x | 6.66x | 8.26x |

*GET hotspot scales super-linearly because multiple readers traverse the same cached
ART path simultaneously without contention.*

**GET — uniform 10k keys (realistic workload):**

| Threads | 1 | 2 | 4 | 8 | 16 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Throughput | 10.83M | 18.99M | 34.10M | 56.15M | 89.70M ops/sec |
| Scaling | 1.00x | 1.75x | 3.15x | 5.19x | 8.29x |

**PUT — no contention (each thread on a distinct shard):**

| Threads | 1 | 2 | 4 | 8 | 16 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Throughput | 41.76M | 68.33M | 122.99M | 248.82M | 203.71M ops/sec |
| Scaling | 1.00x | 1.64x | 2.95x | 5.96x | 4.88x |

**PUT — uniform 10k keys (realistic workload):**

| Threads | 1 | 2 | 4 | 8 | 16 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Throughput | 12.02M | 16.04M | 27.25M | 43.20M | 55.18M ops/sec |
| Scaling | 1.00x | 1.33x | 2.27x | 3.59x | 4.59x |

### Heavy overwrite calibration

For workloads with frequent overwrites of large values (strings, arrays), Zeno
accumulates retained arena bytes until `compact_shard` is called. The table below
shows the trade-off between compaction frequency, p99 latency, and retained memory
(`payload=1KB`, `keys=64`, `ops=50k`):

| compact_every_N | p50 | p99 | max | retained_final | elapsed_total |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1000 | 110 ns | 6.14 µs | 138.54 µs | 0 B | 106.27 ms |
| 5000 | 100 ns | 4.38 µs | 175.05 µs | 0 B | 48.33 ms |
| 10000 | 100 ns | 3.76 µs | 63.37 µs | 0 B | 39.42 ms |
| off | 90 ns | 3.69 µs | 123.54 µs | 48.83 MB | 25.10 ms |

- Use `5000` when you need bounded retained bytes with moderate maintenance overhead.
- Use `off` only when peak throughput is the priority and high retained bytes are
    acceptable.

To reproduce all numbers on your machine:

```bash
zig build bench -Doptimize=ReleaseFast
```

## 🛠 Usage

Zeno requires **Zig 0.16.0** (also recorded in `.zigversion`). Add the package with Zig's dependency manager, which records both its URL and content hash:

```bash
zig fetch --save=zeno https://github.com/zeno-core/zeno/archive/refs/heads/main.tar.gz
```

For reproducible upgrades, use a release tag or commit URL when choosing the dependency revision.

Then, in your `build.zig`:

```zig
const zeno = b.dependency("zeno", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("zeno", zeno.module("zeno"));
```

### Quick Example

```zig
const std = @import("std");
const zeno = @import("zeno");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // In-memory engine (no persistence)
    const db = try zeno.public.create(init.io, allocator);
    defer db.close() catch unreachable;

    // Write a value
    const value = zeno.types.Value{ .string = "Alice" };
    try db.put("user:1", &value);

    // Read it back, caller owns the returned value
    if (try db.get(allocator, "user:1")) |owned_value| {
        var val = owned_value;
        defer val.deinit(allocator);
        std.debug.print("Found: {s}\n", .{val.string});
    }

    // Delete
    _ = try db.delete("user:1");
}
```

The database borrows the `std.Io` context supplied at creation. Keep its provider alive until the database, its read views, and its background workers have been closed. The process context in `main` satisfies this lifetime. Database operations retain their existing error contracts and acquire locks without cancellation.

For a persistent engine with WAL and snapshot recovery (inside the same `main`):

```zig
const db = try zeno.public.open(init.io, allocator, .{
    .wal_path      = "./data.wal",
    .snapshot_path = "./data.snapshot",
    .fsync_mode    = .batched_async,
});
defer db.close() catch unreachable;
```

## 🏗 Architecture

Zeno uses a shard-first architecture designed to keep hot paths predictable under concurrency:

- The keyspace is partitioned into 256 independent shards (hash-routed by key), and each shard owns its own ART index, lock, sequence counter, and memory arena.
- Point operations are shard-local after routing. `get` is lock-free via a seqlock — concurrent readers on the same shard proceed without serializing against each other. `put` acquires the shard-exclusive lock; overwrites of existing keys skip the seqlock sequence bracket for minimum latency.
- Read consistency is coordinated with visibility gates and `ReadView`, so scans and in-view reads can observe stable state while writes continue on other shards.
- Durability is handled by WAL + snapshot: WAL records live mutations for crash recovery, while snapshots provide faster restart and periodic state compaction.

This design gives strong single-key latency, good multicore scaling, and explicit trade-offs between throughput and durability policy (`fsync_mode`).

## ⚖️ License

Distributed under the MIT License. See `LICENSE` for more information.

## Development

Use the Zig version in `.zigversion`:

```bash
zig build test
zig build test -Doptimize=ReleaseSafe
zig build test -Dexpose_internals=true
zig build check -Dtarget=x86_64-linux
zig build bench -Doptimize=ReleaseFast
```

Benchmarks use zBench 0.13.0. Their timers use the monotonic awake clock;
TTL deadlines remain Unix wall-clock seconds. Existing WAL records and snapshot
versions 1 and 2 remain readable after migration to Zig 0.16.0.
