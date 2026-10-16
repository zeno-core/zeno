//! Recovery compatibility checks against files produced by Zig 0.15.2.
//! Time Complexity: O(n) over fixture bytes and recovered values.
//! Allocator: Tests use the testing allocator for database state and owned reads.

const std = @import("std");
const engine = @import("../public.zig");

test "Zig 0.15.2 snapshots and WAL recover and checkpoint with Zig 0.16.0" {
    const testing = std.testing;
    const io = testing.io;
    inline for (.{ @embedFile("testdata/zig015-v1.snapshot"), @embedFile("testdata/zig015-v2.snapshot") }) |snapshot_bytes| {
        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();
        const snapshot_file = try tmp.dir.createFile(io, "data.snapshot", .{});
        defer snapshot_file.close(io);
        try snapshot_file.writeStreamingAll(io, snapshot_bytes);
        const wal_file = try tmp.dir.createFile(io, "data.wal", .{});
        defer wal_file.close(io);
        try wal_file.writeStreamingAll(io, @embedFile("testdata/zig015.wal"));

        const snapshot_path = try std.fmt.allocPrint(testing.allocator, ".zig-cache/tmp/{s}/data.snapshot", .{tmp.sub_path});
        defer testing.allocator.free(snapshot_path);
        const wal_path = try std.fmt.allocPrint(testing.allocator, ".zig-cache/tmp/{s}/data.wal", .{tmp.sub_path});
        defer testing.allocator.free(wal_path);

        const options = @import("../types.zig").DatabaseOptions{
            .wal_path = wal_path,
            .snapshot_path = snapshot_path,
            .fsync_mode = .always,
        };
        {
            const db = try engine.open(io, testing.allocator, options);
            defer db.close() catch unreachable;
            try testing.expect(!try db.exists("legacy:number"));
            var batch = (try db.get(testing.allocator, "legacy:batch")).?;
            defer batch.deinit(testing.allocator);
            try testing.expectEqual(@as(i64, -42), batch.integer);
            var text = (try db.get(testing.allocator, "legacy:text")).?;
            defer text.deinit(testing.allocator);
            try testing.expectEqualStrings("persisted by Zig 0.15.2", text.string);
            try testing.expect(try db.ttl("legacy:text") > 0);
            var object = (try db.get(testing.allocator, "legacy:object")).?;
            defer object.deinit(testing.allocator);
            const items = object.object.get("items").?.array.items;
            try testing.expectEqual(@as(usize, 4), items.len);
            try testing.expect(items[0] == .null_val);
            try testing.expect(items[1].boolean);
            try testing.expectEqual(@as(f64, 3.5), items[2].float);
            try testing.expectEqualStrings("persisted by Zig 0.15.2", items[3].string);
            try db.checkpoint();
        }

        const reopened = try engine.open(io, testing.allocator, options);
        defer reopened.close() catch unreachable;
        try testing.expect(try reopened.exists("legacy:object"));
        try testing.expect(try reopened.exists("legacy:batch"));
        try testing.expect(!try reopened.exists("legacy:number"));
        try testing.expect(try reopened.ttl("legacy:text") > 0);
    }
}
