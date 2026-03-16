# Zeno - Zig Engine for Node Operations

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Zig Version](https://img.shields.io/badge/Zig-0.15.2-orange.svg)](https://ziglang.org)

Zeno is a high-performance, embedded key-value storage engine written in pure Zig. Designed for modern workloads, it prioritizes predictable low latency, zero-implicit allocation, and efficient sharded concurrency. Its name (Node) reflects the core index and storage nodes that power each operation, not to be confused with Node.js.

Zeno began as a learning experiment into database storage internals and the Adaptive Radix Tree (ART). The results and performance were promising enough that it evolved into a standalone engine.

---

## 🚀 Key Features

*   **Adaptive Radix Tree (ART) Index**: O(k) lookups with SIMD-optimized node transitions (Node4 to Node256).
*   **Sharded Concurrency**: Lock-sharding architecture to maximize multi-core throughput while minimizing contention.
*   **Zero-Implicit Allocation**: Every function that allocates accepts an explicit `Allocator`, following strict Zig practices.
*   **Durable Persistence**: 
    *   **WAL (Write-Ahead Log)**: Batched-async durability mode for high-throughput writes.
    *   **Snapshots**: Efficient, streaming snapshot-backed recovery.
*   **Structured Values**: Support for complex values via `union(enum)` for strict runtime type safety.

## 📊 Performance Benchmarks

Zeno is built for speed. Below are numbers from the latest benchmark run:

| Operation | Throughput | Latency (Mean) |
| :--- | :--- | :--- |
| **DB PUT (steady)** | **39.22M ops/sec** | **25 ns** |
| **DB PUT Group16 (steady)** | **1.80M items/sec (0.11M ops/sec)** | **8.90 us** |
| **DB GET (steady)** | **15.37M ops/sec** | **65 ns** |
| **ART Lookup (Hit)** | 102.76M ops/sec | 9 ns |
| **ART Insert (Sequential)**| 80.10M ops/sec | 12 ns |
| **WAL Append (Async)** | 0.69M ops/sec | 1.46 us |
| **WAL Append Grouped16 (Async)** | 1.03M items/sec (0.06M ops/sec) | 15.59 us |

Sharded scalability (latest run):

| Workload | 1 thread | 2 threads | 4 threads | 8 threads | 16 threads |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **GET (Shared)** | 14.51M | 26.87M | 51.52M | 100.60M | 141.17M ops/sec |
| **PUT (Sharded)** | 41.89M | 74.32M | 132.15M | 237.10M | 277.70M ops/sec |

*Benchmarks conducted on Ubuntu 24.04.4, AMD Ryzen 7 5700X, 32GB DDR4 @ 3200MHz*

Want to reproduce these numbers on your machine? From the repository root, run:

```bash
zig build bench -Doptimize=ReleaseFast
```

This executes the full benchmark suite (steady-state, throughput summary, and sharded scalability) and prints results directly to your terminal.

### Heavy Overwrite Metrics (Essential)

Latest `ReleaseFast` run (after heavy-overwrite maintenance + safety fixes):

| Metric | Result |
| :--- | :--- |
| `put overwrite64 steady` | `20.98M ops/sec` |
| `put overwrite64 heavy1k steady` | `1.94M ops/sec` |
| `put overwrite64 heavy1k manual` | `1.57M ops/sec` |

Heavy calibration command:

```bash
zig build bench -Doptimize=ReleaseFast -- --heavy-overwrite-profile
```

Most useful calibration points (`payload=1KB`, `keys=64`, `ops=50k`):

| compact_every | p99 | retained_final | elapsed_total |
| :--- | :--- | :--- | :--- |
| `1000` | `6.10us` | `0B` | `90.21ms` |
| `5000` | `3.88us` | `0B` | `45.25ms` |
| `off` | `3.73us` | `48.83MB` | `26.43ms` |

Operational note:
- Start with `5000` when you need bounded retained bytes with moderate maintenance overhead.
- Use `off` only when peak throughput is the priority and high retained bytes are acceptable.

## 🛠 Usage

Add `zeno` to your `build.zig.zon`:

```zig
.{
    .name = "my-project",
    .version = "0.1.0",
    .dependencies = .{
        .zeno = .{
            .url = "https://github.com/zeno-core/zeno/archive/refs/heads/main.tar.gz",
        },
    },
}
```

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
const zeno = @import("zeno");
var db = try zeno.Database.open(allocator, .{
    .path = "./data",
    .wal_mode = .batched_async,
});
defer db.close();

try db.put("user:123", .{ .string = "Enzo" });
if (try db.get("user:123")) |val| {
    std.debug.print("Found: {s}\n", .{val.string});
}
```

## 🏗 Architecture

Zeno uses a shard-first architecture designed to keep hot paths predictable under concurrency:

- The keyspace is partitioned into independent shards (hash-routed by key), and each shard owns its own ART index, lock, and memory arena.
- Point operations (`put`, `get`, `delete`) are shard-local after routing, minimizing cross-core contention and avoiding a single global lock bottleneck.
- Read consistency is coordinated with visibility gates and `ReadView`, so scans and in-view reads can observe stable state while writes continue on other shards.
- Durability is handled by WAL + snapshot: WAL records live mutations for crash recovery, while snapshots provide faster restart and periodic state compaction.

This design gives strong single-key latency, good multicore scaling, and explicit trade-offs between throughput and durability policy (`fsync_mode`).

## ⚖️ License

Distributed under the MIT License. See `LICENSE` for more information.
