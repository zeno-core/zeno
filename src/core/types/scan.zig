//! Public scan descriptors, entries, and owned scan result containers.
//! Cost: Scan descriptors are O(1); owned result cleanup is O(n) over stored entries and cursor bytes.
//! Allocator: Uses explicit allocators for owned scan results and direct owned continuation bytes.

const std = @import("std");
const Value = @import("value.zig").Value;

/// Inclusive-start, exclusive-end bounds for ordered-key scans.
///
/// Ownership:
/// - `start` and `end` are borrowed.
/// - Bound slices must remain valid for the duration of the consuming scan call.
pub const KeyRange = struct {
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
};

/// One owned key/value pair yielded during scan traversal.
///
/// Ownership:
/// - `key` is owned by the enclosing result container.
/// - `value` points to owned storage released by the enclosing result container.
pub const ScanEntry = struct {
    key: []const u8,
    value: *const Value,
};

/// Borrowed continuation view for a paginated scan.
///
/// Ownership:
/// - `resume_key` is borrowed.
/// - The cursor remains valid only while the owner of `resume_key` stays alive.
pub const ScanCursor = struct {
    resume_key: []const u8,

    /// Clones one borrowed continuation cursor into owned storage.
    ///
    /// Time Complexity: O(k), where `k` is `resume_key.len`.
    ///
    /// Allocator: Duplicates `resume_key` through `allocator`.
    ///
    /// Ownership: Returns one owned cursor that must later be released with `deinit`.
    pub fn clone(self: ScanCursor, allocator: std.mem.Allocator) std.mem.Allocator.Error!OwnedScanCursor {
        return OwnedScanCursor.init(allocator, self.resume_key);
    }
};

/// Owned continuation cursor retained independently of any page result.
///
/// Ownership:
/// - `resume_key` is owned by this cursor.
/// - Copying this value does not duplicate the continuation bytes; use `clone` when an independent owner is needed.
pub const OwnedScanCursor = struct {
    allocator: ?std.mem.Allocator = null,
    resume_key: ?[]u8 = null,

    /// Creates one owned continuation cursor by cloning `resume_key`.
    ///
    /// Time Complexity: O(k), where `k` is `resume_key.len`.
    ///
    /// Allocator: Duplicates `resume_key` through `allocator`.
    ///
    /// Ownership: Returns one owned cursor that must later release its continuation bytes with `deinit`.
    pub fn init(allocator: std.mem.Allocator, resume_key: []const u8) std.mem.Allocator.Error!OwnedScanCursor {
        return .{
            .allocator = allocator,
            .resume_key = try allocator.dupe(u8, resume_key),
        };
    }

    /// Returns a borrowed continuation view over this owned cursor.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: The returned cursor borrows continuation bytes from this owned cursor and remains valid only while this owned cursor stays active.
    pub fn as_cursor(self: OwnedScanCursor) ?ScanCursor {
        const resume_key = self.resume_key orelse return null;
        return .{ .resume_key = resume_key };
    }

    /// Clones one owned continuation cursor into an independent owner.
    ///
    /// Time Complexity: O(k), where `k` is `resume_key.len`.
    ///
    /// Allocator: Duplicates `resume_key` through `allocator`.
    ///
    /// Ownership: Returns one independent owned cursor when this cursor is still active, otherwise `null`.
    pub fn clone(self: OwnedScanCursor, allocator: std.mem.Allocator) std.mem.Allocator.Error!?OwnedScanCursor {
        const cursor = self.as_cursor() orelse return null;
        return @as(?OwnedScanCursor, try OwnedScanCursor.init(allocator, cursor.resume_key));
    }

    /// Releases one owned continuation cursor.
    ///
    /// Time Complexity: O(k), where `k` is `resume_key.len`.
    ///
    /// Allocator: Does not allocate; frees `resume_key` through the allocator captured in this cursor.
    ///
    /// Ownership: Releases the continuation bytes when still active. Callers that need multiple independent owners must use `clone`.
    pub fn deinit(self: *OwnedScanCursor) void {
        const resume_key = self.resume_key orelse return;
        const allocator = self.allocator orelse unreachable;
        allocator.free(resume_key);
        self.resume_key = null;
        self.allocator = null;
    }
};

/// Owned result of a scan that materializes all returned entries.
pub const ScanResult = struct {
    entries: std.ArrayList(ScanEntry),
    allocator: std.mem.Allocator,

    /// Releases the owned entry buffer.
    ///
    /// Time Complexity: O(n + b), where `n` is `entries.items.len` and `b` is total teardown work for stored values.
    ///
    /// Allocator: Does not allocate; frees owned entry keys, values, and the entry buffer through `allocator`.
    ///
    /// Ownership: Releases all entry keys and values owned by the result container.
    pub fn deinit(self: *ScanResult) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.key);
            const owned_value: *Value = @constCast(entry.value);
            owned_value.deinit(self.allocator);
            self.allocator.destroy(owned_value);
        }
        self.entries.deinit(self.allocator);
        self.* = undefined;
    }
};

/// Owned result for one paginated scan page.
///
/// Ownership:
/// - The page owns all `entries`.
/// - Any continuation cursor remains page-owned until `take_next_cursor`.
pub const ScanPageResult = struct {
    entries: std.ArrayList(ScanEntry),
    allocator: std.mem.Allocator,
    /// Private page-owned continuation cursor. Use `borrow_next_cursor` or `take_next_cursor` instead of depending on this representation.
    _next_cursor: ?OwnedScanCursor = null,

    /// Returns a borrowed continuation cursor while this page result stays alive.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Returns a borrowed cursor that remains valid only while this page result stays alive or until `take_next_cursor` transfers ownership away.
    pub fn borrow_next_cursor(self: *const ScanPageResult) ?ScanCursor {
        const owned_cursor = self._next_cursor orelse return null;
        return owned_cursor.as_cursor();
    }

    /// Transfers ownership of the optional continuation cursor out of this page result.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Returns one owned cursor when present and clears the page-owned slot so `deinit` will not release it twice.
    pub fn take_next_cursor(self: *ScanPageResult) ?OwnedScanCursor {
        const cursor = self._next_cursor orelse return null;
        self._next_cursor = null;
        return cursor;
    }

    /// Releases the owned entry buffer and optional continuation cursor.
    ///
    /// Time Complexity: O(n + b + k), where `n` is `entries.items.len`, `b` is total teardown work for stored values, and `k` is the continuation resume key length when present.
    ///
    /// Allocator: Does not allocate; frees owned buffers through `allocator`.
    ///
    /// Ownership: Releases all entry keys and values plus any continuation cursor bytes owned by this page result.
    pub fn deinit(self: *ScanPageResult) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.key);
            const owned_value: *Value = @constCast(entry.value);
            owned_value.deinit(self.allocator);
            self.allocator.destroy(owned_value);
        }
        self.entries.deinit(self.allocator);
        if (self._next_cursor) |*cursor| {
            cursor.deinit();
        }
        self.* = undefined;
    }
};
