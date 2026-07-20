//! Storage interface for Raft persistent state.
//!
//! Raft requires durable storage for three things (paper §5.1):
//!   - `currentTerm` — latest term the server has seen
//!   - `votedFor` — candidate ID voted for in the current term
//!   - `log[]` — replicated log entries
//!
//! These must be persisted before responding to RPCs.
//!
//! # Implementation
//!
//! Callers implement `Storage(T)` for their concrete type `T` and pass it to
//! `Log(StorageType)` and eventually to `Node(SM, ST)`.
//!
//! A built-in `FileStorage` is provided in `file_storage.zig`.

const types = @import("types.zig");
const ServerId = types.ServerId;
const Term = types.Term;
const LogIndex = types.LogIndex;

/// Interface for persistent storage. Callers implement this for their type `T`.
pub fn Storage(comptime T: type) type {
    return struct {
        const Self = @This();

        ptr: *T,

        // --- Metadata ---

        loadTermFn: *const fn (ptr: *T) Term,
        storeTermFn: *const fn (ptr: *T, term: Term) void,

        loadVotedForFn: *const fn (ptr: *T) ?ServerId,
        storeVotedForFn: *const fn (ptr: *T, voted_for: ?ServerId) void,

        loadLastLogIndexFn: *const fn (ptr: *T) LogIndex,
        loadLogEntryFn: *const fn (ptr: *T, index: LogIndex, allocator: std.mem.Allocator) ?LogEntryOwned,

        // --- Log entries ---

        appendLogEntryFn: *const fn (ptr: *T, entry: LogEntryOwned) anyerror!void,
        truncateLogFn: *const fn (ptr: *T, last_kept_index: LogIndex) anyerror!void,
        syncFn: *const fn (ptr: *T) anyerror!void,

        pub fn loadTerm(self: Self) Term {
            return self.loadTermFn(self.ptr);
        }

        pub fn storeTerm(self: Self, term: Term) void {
            self.storeTermFn(self.ptr, term);
        }

        pub fn loadVotedFor(self: Self) ?ServerId {
            return self.loadVotedForFn(self.ptr);
        }

        pub fn storeVotedFor(self: Self, voted_for: ?ServerId) void {
            self.storeVotedForFn(self.ptr, voted_for);
        }

        pub fn loadLastLogIndex(self: Self) LogIndex {
            return self.loadLastLogIndexFn(self.ptr);
        }

        pub fn loadLogEntry(self: Self, index: LogIndex, allocator: std.mem.Allocator) ?LogEntryOwned {
            return self.loadLogEntryFn(self.ptr, index, allocator);
        }

        pub fn appendLogEntry(self: Self, entry: LogEntryOwned) !void {
            try self.appendLogEntryFn(self.ptr, entry);
        }

        pub fn truncateLog(self: Self, last_kept_index: LogIndex) !void {
            try self.truncateLogFn(self.ptr, last_kept_index);
        }

        pub fn sync(self: Self) !void {
            try self.syncFn(self.ptr);
        }
    };
}

/// A log entry whose data is owned by the storage layer.
/// The caller must free `data` if it takes ownership.
pub const LogEntryOwned = struct {
    term: Term,
    index: LogIndex,
    data: []const u8,

    pub fn deinit(self: *LogEntryOwned, allocator: std.mem.Allocator) void {
        if (self.data.len > 0) allocator.free(self.data);
    }
};

const std = @import("std");
