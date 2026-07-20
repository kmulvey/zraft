//! Replicated log for Raft.
//!
//! An allocator-backed, append-only log of `LogEntry` values.
//! Index 0 is a sentinel — real entries start at index 1.

const std = @import("std");
const types = @import("types.zig");
const rpc = @import("rpc.zig");

pub const LogEntry = struct {
    term: types.Term,
    index: types.LogIndex,
    data: []const u8,

    pub fn deinit(self: *LogEntry, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

/// Growable, allocator-backed log.
pub const Log = struct {
    allocator: std.mem.Allocator,
    /// Dense array of entries. Index `i` in the array has raft-index `i`.
    /// Slot 0 is always a sentinel entry with term=0.
    entries: []LogEntry,
    /// Number of populated entries (including the sentinel at index 0).
    len: usize,
    /// Allocated capacity of `entries`.
    capacity: usize,

    pub fn init(allocator: std.mem.Allocator, initial_capacity: usize) !Log {
        const cap = @max(initial_capacity, 2);
        const entries = try allocator.alloc(LogEntry, cap);
        // Sentinel at index 0
        entries[0] = LogEntry{ .term = 0, .index = 0, .data = &.{} };
        return Log{
            .allocator = allocator,
            .entries = entries,
            .len = 1,
            .capacity = cap,
        };
    }

    pub fn deinit(self: *Log) void {
        for (self.entries[0..self.len]) |*e| {
            if (e.data.len > 0) e.deinit(self.allocator);
        }
        self.allocator.free(self.entries);
        self.* = undefined;
    }

    /// Last entry index (0 if empty beyond sentinel).
    pub fn lastIndex(self: *const Log) types.LogIndex {
        return @as(types.LogIndex, @intCast(self.len - 1));
    }

    /// Term at a given index. Returns 0 for index 0 or out-of-range.
    pub fn termAt(self: *const Log, index: types.LogIndex) types.Term {
        if (index >= self.len) return 0;
        return self.entries[@as(usize, @intCast(index))].term;
    }

    /// Append a new entry (allocator copies `data`). Returns the entry's index.
    pub fn append(self: *Log, term: types.Term, data: []const u8) !types.LogIndex {
        const index = self.lastIndex() + 1;
        const data_copy = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(data_copy);

        try self.grow();
        self.entries[self.len] = LogEntry{ .term = term, .index = index, .data = data_copy };
        self.len += 1;
        return index;
    }

    /// Get a slice of entries from `start` (inclusive) to the end.
    pub fn sliceFrom(self: *const Log, start: types.LogIndex) []const LogEntry {
        if (start >= self.len) return &.{};
        return self.entries[@as(usize, @intCast(start))..self.len];
    }

    /// Get a single entry. Returns null for index 0 or out-of-range.
    pub fn get(self: *const Log, index: types.LogIndex) ?*const LogEntry {
        if (index == 0 or index >= self.len) return null;
        return &self.entries[@as(usize, @intCast(index))];
    }

    /// Truncate all entries strictly after `last_kept`.
    /// Frees data for removed entries.
    pub fn truncate(self: *Log, last_kept: types.LogIndex) void {
        const keep = @as(usize, @intCast(last_kept)) + 1;
        if (keep >= self.len) return;
        for (self.entries[keep..self.len]) |*e| {
            if (e.data.len > 0) e.deinit(self.allocator);
        }
        self.len = @max(keep, 1); // always keep sentinel
    }

    /// Replace log contents entirely (for snapshot install).
    /// Frees all existing entries, then sets up a new log with one entry
    /// at `last_included_index` with `last_included_term`.
    pub fn replaceWithSnapshot(self: *Log, last_included_index: types.LogIndex, last_included_term: types.Term) !void {
        // Free existing entries
        for (self.entries[0..self.len]) |*e| {
            if (e.data.len > 0) e.deinit(self.allocator);
        }
        // Rebuild with a single sentinel-compact entry
        self.entries[0] = LogEntry{ .term = last_included_term, .index = last_included_index, .data = &.{} };
        self.len = 1;
    }

    fn grow(self: *Log) !void {
        if (self.len < self.capacity) return;
        const new_cap = self.capacity * 2;
        self.entries = try self.allocator.realloc(self.entries, new_cap);
        self.capacity = new_cap;
    }
};

test "log init and append" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    try std.testing.expectEqual(@as(types.LogIndex, 0), log.lastIndex());
    try std.testing.expectEqual(@as(types.Term, 0), log.termAt(0));

    const idx = try log.append(1, "hello");
    try std.testing.expectEqual(@as(types.LogIndex, 1), idx);
    try std.testing.expectEqual(@as(types.LogIndex, 1), log.lastIndex());
    try std.testing.expectEqual(@as(types.Term, 1), log.termAt(1));
}

test "log truncate" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    _ = try log.append(1, "a");
    _ = try log.append(1, "b");
    _ = try log.append(2, "c");
    try std.testing.expectEqual(@as(types.LogIndex, 3), log.lastIndex());

    log.truncate(1);
    try std.testing.expectEqual(@as(types.LogIndex, 1), log.lastIndex());
}

test "log slice from" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    _ = try log.append(1, "x");
    _ = try log.append(2, "y");

    const slice = log.sliceFrom(2);
    try std.testing.expectEqual(@as(usize, 1), slice.len);
    try std.testing.expectEqual(@as(types.Term, 2), slice[0].term);
}
