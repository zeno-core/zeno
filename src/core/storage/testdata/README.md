# Persistence compatibility fixtures

These files were generated with Zig 0.15.2 from Zeno commit `8e3d953`.
They intentionally remain unchanged when updating the compiler or storage code.

- `zig015-v1.snapshot` and `zig015-v2.snapshot` contain the same three keys:
  `legacy:text`, `legacy:number`, and `legacy:object`. The object contains an array
  with null, boolean, float, and string values. Both snapshots have checkpoint LSN 3.
- `zig015.wal` contains a committed two-member batch that creates `legacy:batch`
  and overwrites `legacy:text`, followed by deletion of `legacy:number` and an
  expiration timestamp of `4102444800` for `legacy:text`.

The v2 snapshot was created with `Database.checkpoint`; the v1 snapshot used the
same revision's existing `writeSnapshotFile` implementation with version 1.
The WAL was produced by `applyBatch`, `delete`, and `expireAt` after checkpoint.
Recovery tests exercise both snapshot versions with the same WAL and then create
and reopen a new checkpoint using the current implementation.
