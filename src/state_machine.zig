//! State machine interface for Raft.
//!
//! Callers implement this interface to apply committed log entries
//! to their application state.

const std = @import("std");
const types = @import("types.zig");
const LogIndex = types.LogIndex;

/// Interface that application state machines must implement.
/// `snapshot` returns allocated bytes (caller frees).
/// `restore` takes a byte slice previously returned by `snapshot`.
pub fn StateMachine(comptime T: type) type {
    return struct {
        const Self = @This();

        ptr: *T,
        applyFn: *const fn (ptr: *T, index: LogIndex, data: []const u8) void,
        snapshotFn: *const fn (ptr: *T, allocator: std.mem.Allocator) anyerror![]u8,
        restoreFn: *const fn (ptr: *T, data: []const u8) anyerror!void,

        pub fn apply(self: Self, index: LogIndex, data: []const u8) void {
            self.applyFn(self.ptr, index, data);
        }

        pub fn snapshot(self: Self, allocator: std.mem.Allocator) ![]u8 {
            return try self.snapshotFn(self.ptr, allocator);
        }

        pub fn restore(self: Self, data: []const u8) !void {
            try self.restoreFn(self.ptr, data);
        }
    };
}
