//! State machine interface for Raft.
//!
//! Callers implement this interface to apply committed log entries
//! to their application state.

const types = @import("types.zig");
const LogIndex = types.LogIndex;

/// Interface that application state machines must implement.
/// The Raft node calls `apply` after entries are committed.
pub fn StateMachine(comptime T: type) type {
    return struct {
        const Self = @This();

        ptr: *T,
        applyFn: *const fn (ptr: *T, index: LogIndex, data: []const u8) void,
        snapshotFn: *const fn (ptr: *T, writer: anytype) anyerror!void,
        restoreFn: *const fn (ptr: *T, reader: anytype) anyerror!void,

        pub fn apply(self: Self, index: LogIndex, data: []const u8) void {
            self.applyFn(self.ptr, index, data);
        }

        pub fn snapshot(self: Self, writer: anytype) !void {
            try self.snapshotFn(self.ptr, writer);
        }

        pub fn restore(self: Self, reader: anytype) !void {
            try self.restoreFn(self.ptr, reader);
        }
    };
}
