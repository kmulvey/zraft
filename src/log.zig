//! Replicated log for Raft.
//!
//! An allocator-backed, append-only log of `LogEntry` values backed by persistent storage.
//! Index 0 is a sentinel — real entries start at index 1.
//!
//! Generic over a storage type `ST` that implements the `Storage(ST)` interface.

const std = @import("std");
const types = @import("types.zig");
const EntryType = types.EntryType;

pub const LogEntry = struct {
    term: types.Term,
    index: types.LogIndex,
    entry_type: EntryType,
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
            entries[0] = LogEntry{ .term = 0, .index = 0, .entry_type = .command, .data = &.{} };

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

        /// Raft index stored at array slot 0 (the compacted base).
        pub fn baseIndex(self: *const Self) types.LogIndex {
            return self.entries[0].index;
        }

        /// Last entry index (0 if empty, or the sentinel's stored index after compaction).
        pub fn lastIndex(self: *const Self) types.LogIndex {
            return self.entries[self.len - 1].index;
        }

        /// Term at a given index. Returns 0 for indices below the base or out of range.
        pub fn termAt(self: *const Self, index: types.LogIndex) types.Term {
            const base = self.baseIndex();
            if (index < base or index > self.lastIndex()) return 0;
            return self.entries[@as(usize, @intCast(index - base))].term;
        }

        /// Append a new command entry. Writes to storage, then caches in memory.
        pub fn append(self: *Self, term: types.Term, data: []const u8) !types.LogIndex {
            return try self.appendEntry(term, .command, data);
        }

        /// Append a configuration entry. Writes to storage, then caches in memory.
        pub fn appendConfig(self: *Self, term: types.Term, data: []const u8) !types.LogIndex {
            return try self.appendEntry(term, .configuration, data);
        }

        /// Append a new entry with an explicit type. Writes to storage, then caches in memory.
        pub fn appendEntry(self: *Self, term: types.Term, entry_type: EntryType, data: []const u8) !types.LogIndex {
            const index = self.lastIndex() + 1;
            const data_copy = try self.allocator.dupe(u8, data);
            errdefer self.allocator.free(data_copy);

            // Persist to storage first (crash-safe ordering)
            try self.storage.appendLogEntry(.{
                .term = term,
                .index = index,
                .entry_type = entry_type,
                .data = data_copy,
            });

            // Then cache in memory
            try self.grow();
            self.entries[self.len] = LogEntry{ .term = term, .index = index, .entry_type = entry_type, .data = data_copy };
            self.len += 1;
            return index;
        }

        /// Get a slice of entries from `start` (inclusive) to the end.
        pub fn sliceFrom(self: *const Self, start: types.LogIndex) []const LogEntry {
            const base = self.baseIndex();
            if (start > self.lastIndex()) return &.{};
            const pos = if (start <= base) 0 else @as(usize, @intCast(start - base));
            return self.entries[pos..self.len];
        }

        /// Get a single entry. Returns null for index 0 or out-of-range (including compacted indices).
        pub fn get(self: *const Self, index: types.LogIndex) ?*const LogEntry {
            const base = self.baseIndex();
            if (index == 0 or index < base or index > self.lastIndex()) return null;
            return &self.entries[@as(usize, @intCast(index - base))];
        }

        /// Truncate all entries strictly after `last_kept`.
        /// Frees data for removed entries and compacts storage.
        pub fn truncate(self: *Self, last_kept: types.LogIndex) !void {
            const base = self.baseIndex();
            if (last_kept < base) {
                // Cannot truncate below the compacted base; truncate only to the base.
                try self.truncate(base);
                return;
            }

            const keep = @as(usize, @intCast(last_kept - base)) + 1;
            if (keep >= self.len) return;

            // Free in-memory entries being removed
            for (self.entries[keep..self.len]) |*e| {
                if (e.data.len > 0) e.deinit(self.allocator);
            }
            self.len = keep; // always keeps the sentinel at position 0

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
            self.entries[0] = LogEntry{ .term = last_included_term, .index = last_included_index, .entry_type = .command, .data = &.{} };
            self.len = 1;

            // Persist snapshot marker to storage: drop the compacted prefix.
            try self.storage.dropLogPrefix(last_included_index);
        }

        fn grow(self: *Self) !void {
            if (self.len < self.capacity) return;
            const new_cap = self.capacity * 2;
            self.entries = try self.allocator.realloc(self.entries, new_cap);
            self.capacity = new_cap;
        }

        fn loadFromStorage(self: *Self) !void {
            const snap_index = self.storage.loadLastSnapshotIndex();
            const snap_term = self.storage.loadLastSnapshotTerm();

            // Use snapshot metadata as the compacted base if a snapshot exists.
            if (snap_index > 0) {
                if (self.entries[0].data.len > 0) self.allocator.free(self.entries[0].data);
                self.entries[0] = LogEntry{ .term = snap_term, .index = snap_index, .entry_type = .command, .data = &.{} };
            }

            const base = self.baseIndex();
            const last_idx = self.storage.loadLastLogIndex();
            if (last_idx <= base) return;

            // Ensure capacity for entries after the base.
            const count_after_base = last_idx - base;
            const needed = @as(usize, @intCast(count_after_base)) + 1;
            if (needed > self.capacity) {
                self.entries = try self.allocator.realloc(self.entries, needed);
                self.capacity = needed;
            }

            // Load entries strictly after the base, stopping at the first gap.
            var i: types.LogIndex = base + 1;
            while (i <= last_idx) : (i += 1) {
                const owned = self.storage.loadLogEntry(i, self.allocator);
                if (owned) |entry| {
                    if (entry.index != i) {
                        var to_free = entry;
                        to_free.deinit(self.allocator);
                        break;
                    }
                    const pos = @as(usize, @intCast(i - base));
                    self.entries[pos] = LogEntry{
                        .term = entry.term,
                        .index = entry.index,
                        .entry_type = entry.entry_type,
                        .data = entry.data,
                    };
                    self.len = pos + 1;
                } else {
                    // GAP in the log — stop loading
                    break;
                }
            }
        }
    };
}
