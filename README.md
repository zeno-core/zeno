# zeno

A high-performance storage engine written in Zig.

## Features
- LSM-tree based storage
- ART (Adaptive Radix Tree) index
- WAL (Write-Ahead Log) for durability
- Snapshot-backed recovery

## Usage

Add `zeno` as a dependency in your `build.zig.zon`:

```zig
.{
    .name = "my-project",
    .version = "0.1.0",
    .dependencies = .{
        .zeno = .{
            .url = "https://github.com/zeno-core/zeno/archive/refs/heads/main.tar.gz",
            // .hash = "...", // Zig will prompt you for the hash
        },
    },
}
```

Then in your `build.zig`:

```zig
const zeno = b.dependency("zeno", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("zeno", zeno.module("zeno"));
```

## License
MIT
