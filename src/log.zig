//! Replicated log for Raft.
//!
//! An allocator-backed, append-only log of `LogEntry` values backed by persistent storage.
//! Index 0 is a sentinel — real entries start at index 1.
//!
//! Generic over a storage type `ST` that implements the `Storage(ST)` interface.

const std = @import("std");
const types = @import("types.zig");
const rpc = @import("rpc.zig");

pub const LogEntry = struct {
    term: types.Term,
    index: types.LogIndex,
    data: []const u8,

    pub fn deinit(self: *LogEntry, allocator: std.mem.Allocator) void {
        if (self.data.len > 0) allocator.free(self.data);
    }
};

/// Growable, allocator-backed log with persistent storage.
pub fn Log(comptime ST: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        /// Persistent storage backend (metadata + entries).
        storage: *ST,

        /// Dense array of entries. Index `i` in the array has raft-index `i`.
        /// Slot 0 is always a sentinel entry with term=0.
        entries: []LogEntry,
        /// Number of populated entries (including the sentinel at index 0).
        len: usize,
        /// Allocated capacity of `entries`.
        capacity: usize,

        pub fn init(allocator: std.mem.Allocator, storage: *ST, initial_capacity: usize) !Self {
            const cap = @max(initial_capacity, 2);
            const entries = try allocator.alloc(LogEntry, cap);

            // Sentinel at index 0
            entries[0] = LogEntry{ .term = 0, .index = 0, .data = &.{} };

            var self = Self{
                .allocator = allocator,
                .storage = storage,
                .entries = entries,
                .len = 1,
                .capacity = cap,
            };

            // Load existing entries from storage into memory cache
            try self.loadFromStorage();
            return self;
        }

        pub fn deinit(self: *Self) void {
            for (self.entries[0..self.len]) |*e| {
                if (e.data.len > 0) e.deinit(self.allocator);
            }
            self.allocator.free(self.entries);
            self.* = undefined;
        }

        /// Last entry index (0 if empty).
        /// After snapshot compaction, returns the sentinel's stored index.
        pub fn lastIndex(self: *const Self) types.LogIndex {
            if (self.len <= 1) return self.entries[0].index;
            return @as(types.LogIndex, @intCast(self.len - 1));
        }

        /// Term at a given index. Returns 0 for index 0 or out-of-range.
        pub fn termAt(self: *const Self, index: types.LogIndex) types.Term {
            if (index >= self.len) {
                // After compaction, only the sentinel entry exists.
                // Any index <= sentinel.index returns the sentinel's term.
                if (self.len == 1 and index <= self.entries[0].index) return self.entries[0].term;
                return 0;
            }
            return self.entries[@as(usize, @intCast(index))].term;
        }

        /// Append a new entry. Writes to storage, then caches in memory.
        pub fn append(self: *Self, term: types.Term, data: []const u8) !types.LogIndex {
            const index = self.lastIndex() + 1;
            const data_copy = try self.allocator.dupe(u8, data);
            errdefer self.allocator.free(data_copy);

            // Persist to storage first (crash-safe ordering)
            // We use the storage's own allocator for the data copy it needs
            try self.storage.appendLogEntry(.{
                .term = term,
                .index = index,
                .data = data_copy,
            });

            // Then cache in memory
            try self.grow();
            self.entries[self.len] = LogEntry{ .term = term, .index = index, .data = data_copy };
            self.len += 1;
            return index;
        }

        /// Get a slice of entries from `start` (inclusive) to the end.
        pub fn sliceFrom(self: *const Self, start: types.LogIndex) []const LogEntry {
            if (start >= self.len) return &.{};
            return self.entries[@as(usize, @intCast(start))..self.len];
        }

        /// Get a single entry. Returns null for index 0 or out-of-range.
        pub fn get(self: *const Self, index: types.LogIndex) ?*const LogEntry {
            if (index == 0 or index >= self.len) return null;
            return &self.entries[@as(usize, @intCast(index))];
        }

        /// Truncate all entries strictly after `last_kept`.
        /// Frees data for removed entries and compacts storage.
        pub fn truncate(self: *Self, last_kept: types.LogIndex) !void {
            const keep = @as(usize, @intCast(last_kept)) + 1;
            if (keep >= self.len) return;

            // Free in-memory entries being removed
            for (self.entries[keep..self.len]) |*e| {
                if (e.data.len > 0) e.deinit(self.allocator);
            }
            self.len = @max(keep, 1); // always keep sentinel

            // Compact storage
            try self.storage.truncateLog(last_kept);
        }

        /// Replace log contents entirely (for snapshot install).
        pub fn replaceWithSnapshot(self: *Self, last_included_index: types.LogIndex, last_included_term: types.Term) !void {
            // Free existing entries
            for (self.entries[0..self.len]) |*e| {
                if (e.data.len > 0) e.deinit(self.allocator);
            }
            // Rebuild with a single sentinel-compact entry
            self.entries[0] = LogEntry{ .term = last_included_term, .index = last_included_index, .data = &.{} };
            self.len = 1;

            // Persist snapshot marker to storage
            try self.storage.truncateLog(last_included_index);
        }

        fn grow(self: *Self) !void {
            if (self.len < self.capacity) return;
            const new_cap = self.capacity * 2;
            self.entries = try self.allocator.realloc(self.entries, new_cap);
            self.capacity = new_cap;
        }

        fn loadFromStorage(self: *Self) !void {
            const last_idx = self.storage.loadLastLogIndex();
            if (last_idx == 0) return;

            // Ensure capacity for all entries
            const needed = @as(usize, @intCast(last_idx)) + 1;
            if (needed > self.capacity) {
                self.entries = try self.allocator.realloc(self.entries, needed);
                self.capacity = needed;
            }

            // Load each entry
            var i: types.LogIndex = 1;
            while (i <= last_idx) : (i += 1) {
                const owned = self.storage.loadLogEntry(i, self.allocator);
                if (owned) |entry| {
                    self.entries[@as(usize, @intCast(i))] = LogEntry{
                        .term = entry.term,
                        .index = entry.index,
                        .data = entry.data,
                    };
                    self.len = @as(usize, @intCast(i)) + 1;
                } else {
                    // GAP in the log — stop loading
                    break;
                }
            }
        }
    };
}
