//! In-memory storage for Raft — used in tests and as a reference implementation.
//!
//! Keeps all state in heap-allocated fields (no I/O). Mirrors the `Storage(T)` interface.

const std = @import("std");
const types = @import("types.zig");
const storage = @import("storage.zig");

const ServerId = types.ServerId;
const Term = types.Term;
const LogIndex = types.LogIndex;
const LogEntryOwned = storage.LogEntryOwned;
const SnapshotData = storage.SnapshotData;

/// In-memory storage for testing.
pub const MemoryStorage = struct {
    allocator: std.mem.Allocator,
    current_term: Term = 0,
    voted_for: ?ServerId = null,
    entries: std.ArrayListUnmanaged(LogEntryOwned) = .empty,
    snapshot: ?SnapshotData = null,

    pub fn init(allocator: std.mem.Allocator) MemoryStorage {
        return MemoryStorage{ .allocator = allocator };
    }

    pub fn deinit(self: *MemoryStorage) void {
        for (self.entries.items) |*e| e.deinit(self.allocator);
        self.entries.deinit(self.allocator);
        if (self.snapshot) |*s| s.deinit(self.allocator);
        self.* = undefined;
    }

    // --- Storage interface methods ---

    pub fn loadTerm(ptr: *MemoryStorage) Term { return ptr.current_term; }
    pub fn storeTerm(ptr: *MemoryStorage, term: Term) !void { ptr.current_term = term; }
    pub fn loadVotedFor(ptr: *MemoryStorage) ?ServerId { return ptr.voted_for; }
    pub fn storeVotedFor(ptr: *MemoryStorage, voted_for: ?ServerId) !void { ptr.voted_for = voted_for; }

    pub fn loadLastLogIndex(ptr: *MemoryStorage) LogIndex {
        if (ptr.entries.items.len == 0) return 0;
        return ptr.entries.items[ptr.entries.items.len - 1].index;
    }

    pub fn loadLogEntry(ptr: *MemoryStorage, index: LogIndex, allocator: std.mem.Allocator) ?LogEntryOwned {
        for (ptr.entries.items) |entry| {
            if (entry.index == index) {
                const data = allocator.dupe(u8, entry.data) catch return null;
                return LogEntryOwned{ .term = entry.term, .index = entry.index, .entry_type = entry.entry_type, .data = data };
            }
        }
        return null;
    }

    pub fn appendLogEntry(ptr: *MemoryStorage, entry: LogEntryOwned) !void {
        const data_copy = try ptr.allocator.dupe(u8, entry.data);
        try ptr.entries.append(ptr.allocator, LogEntryOwned{ .term = entry.term, .index = entry.index, .entry_type = entry.entry_type, .data = data_copy });
    }

    pub fn truncateLog(ptr: *MemoryStorage, last_kept_index: LogIndex) !void {
        var i: usize = 0;
        while (i < ptr.entries.items.len) {
            if (ptr.entries.items[i].index > last_kept_index) {
                for (ptr.entries.items[i..]) |*e| e.deinit(ptr.allocator);
                ptr.entries.shrinkRetainingCapacity(i);
                return;
            }
            i += 1;
        }
    }

    pub fn dropLogPrefix(ptr: *MemoryStorage, last_included_index: LogIndex) !void {
        var i: usize = 0;
        while (i < ptr.entries.items.len) {
            if (ptr.entries.items[i].index > last_included_index) {
                if (i > 0) {
                    for (ptr.entries.items[0..i]) |*e| e.deinit(ptr.allocator);
                    ptr.entries.replaceRange(ptr.allocator, 0, i, &.{}) catch {};
                }
                return;
            }
            i += 1;
        }
        // All entries are <= last_included_index: drop everything.
        for (ptr.entries.items) |*e| e.deinit(ptr.allocator);
        ptr.entries.shrinkRetainingCapacity(0);
    }

    pub fn sync(ptr: *MemoryStorage) !void { _ = ptr; }

    // --- Snapshot methods ---

    pub fn storeSnapshot(ptr: *MemoryStorage, last_included_index: LogIndex, last_included_term: Term, data: []const u8) !void {
        // Free previous snapshot if any
        if (ptr.snapshot) |*s| s.deinit(ptr.allocator);
        const data_copy = try ptr.allocator.dupe(u8, data);
        ptr.snapshot = SnapshotData{ .last_included_index = last_included_index, .last_included_term = last_included_term, .data = data_copy };
    }

    pub fn loadSnapshot(ptr: *MemoryStorage, allocator: std.mem.Allocator) ?SnapshotData {
        const s = ptr.snapshot orelse return null;
        const data = allocator.dupe(u8, s.data) catch return null;
        return SnapshotData{ .last_included_index = s.last_included_index, .last_included_term = s.last_included_term, .data = data };
    }

    pub fn loadLastSnapshotIndex(ptr: *MemoryStorage) LogIndex {
        return if (ptr.snapshot) |s| s.last_included_index else 0;
    }

    pub fn loadLastSnapshotTerm(ptr: *MemoryStorage) Term {
        return if (ptr.snapshot) |s| s.last_included_term else 0;
    }

    /// Wrap this MemoryStorage in a Storage(MemoryStorage) interface.
    pub fn toStorage(ptr: *MemoryStorage) storage.Storage(MemoryStorage) {
        return storage.Storage(MemoryStorage){
            .ptr = ptr,
            .loadTermFn = MemoryStorage.loadTerm,
            .storeTermFn = MemoryStorage.storeTerm,
            .loadVotedForFn = MemoryStorage.loadVotedFor,
            .storeVotedForFn = MemoryStorage.storeVotedFor,
            .loadLastLogIndexFn = MemoryStorage.loadLastLogIndex,
            .loadLogEntryFn = MemoryStorage.loadLogEntry,
            .appendLogEntryFn = MemoryStorage.appendLogEntry,
            .truncateLogFn = MemoryStorage.truncateLog,
            .dropLogPrefixFn = MemoryStorage.dropLogPrefix,
            .syncFn = MemoryStorage.sync,
            .storeSnapshotFn = MemoryStorage.storeSnapshot,
            .loadSnapshotFn = MemoryStorage.loadSnapshot,
            .loadLastSnapshotIndexFn = MemoryStorage.loadLastSnapshotIndex,
            .loadLastSnapshotTermFn = MemoryStorage.loadLastSnapshotTerm,
        };
    }
};
