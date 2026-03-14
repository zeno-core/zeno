# Zeno - Zig  Engine for Node Operations

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Zig Version](https://img.shields.io/badge/Zig-0.15.2-orange.svg)](https://ziglang.org)

Zeno is a high-performance, embedded key-value storage engine written in pure Zig. Designed for modern workloads, it prioritizes predictable low latency, zero-implicit allocation, and efficient sharded concurrency.

> **Origin**
> Zeno began as a learning experiment into database storage internals and the Adaptive Radix Tree (ART). The results were so promising and the performance so compelling, that it evolved into a fully-featured, standalone engine.

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

Zeno is built for speed. Below are numbers from the latest run with `zig build bench -Doptimize=ReleaseFast`:

| Operation | Throughput | Latency (Mean) |
| :--- | :--- | :--- |
| **DB PUT (steady)** | **21.45M ops/sec** | **46 ns** |
| **DB GET (steady)** | **15.35M ops/sec** | **65 ns** |
| **ART Lookup (Hit)** | 94.00M ops/sec | 10 ns |
| **ART Insert (Sequential)**| 83.97M ops/sec | 11 ns |
| **WAL Append (Async)** | 0.06M ops/sec (~60K) | 16.51 µs |

Sharded scalability (latest run):

| Workload | 1 thread | 2 threads | 4 threads | 8 threads | 16 threads |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **GET (Shared)** | 14.42M | 27.32M | 52.70M | 100.50M | 142.57M ops/sec |
| **PUT (Sharded)** | 23.47M | 42.24M | 86.41M | 140.05M | 148.16M ops/sec |

*Benchmarks conducted on Ubuntu 24.04.4, AMD Ryzen 7 5700X, 32GB DDR4 @ 3200MHz

## 🛠 Usage

Add `zeno` to your `build.zig.zon`:

```zig
.{
    .name = "my-project",
    .version = "0.1.0",
    .dependencies = .{
        .zeno = .{
            .url = "https://github.com/enzokpl/zeno/archive/refs/heads/main.tar.gz",
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

Zeno utilizes a shard-based architecture. Instead of a single global lock, the keyspace is partitioned into independent shards, each managing its own Adaptive Radix Tree and storage arena. This allows Zeno to scale linearly with CPU core counts for most workloads.

## ⚖️ License

Distributed under the MIT License. See `LICENSE` for more information.
