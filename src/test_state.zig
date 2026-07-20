//! State initialisation and transition tests.

const std = @import("std");
const raft = @import("raft");

const types = raft.types;
const Log = raft.Log;

test "node init creates follower" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    const config = raft.Config{
        .id = 1,
        .peers = &.{ 2, 3 },
    };

    // Minimal state machine
    const TestSM = struct {
        pub fn apply(_: *@This(), _: u64, _: []const u8) void {}
        pub fn snapshot(_: *@This(), _: anytype) !void {}
        pub fn restore(_: *@This(), _: anytype) !void {}
    };
    var sm_impl = TestSM{};
    const sm = raft.StateMachine(TestSM){
        .ptr = &sm_impl,
        .applyFn = TestSM.apply,
        .snapshotFn = TestSM.snapshot,
        .restoreFn = TestSM.restore,
    };

    var node = try raft.Node(TestSM).init(allocator, config, &log, sm, 12345);
    defer node.deinit();

    try std.testing.expectEqual(types.Role.follower, node.role);
    try std.testing.expectEqual(@as(u64, 0), node.current_term);
    try std.testing.expectEqual(@as(?u64, null), node.voted_for);
}

test "node starts election" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    const config = raft.Config{
        .id = 1,
        .peers = &.{ 2, 3 },
        .election_timeout_min_ns = 50_000_000,
        .election_timeout_max_ns = 100_000_000,
    };

    const TestSM = struct {
        pub fn apply(_: *@This(), _: u64, _: []const u8) void {}
        pub fn snapshot(_: *@This(), _: anytype) !void {}
        pub fn restore(_: *@This(), _: anytype) !void {}
    };
    var sm_impl = TestSM{};
    const sm = raft.StateMachine(TestSM){
        .ptr = &sm_impl,
        .applyFn = TestSM.apply,
        .snapshotFn = TestSM.snapshot,
        .restoreFn = TestSM.restore,
    };

    var node = try raft.Node(TestSM).init(allocator, config, &log, sm, 67890);
    defer node.deinit();

    // Tick well past the election timeout
    try node.tick(200_000_000);
    try std.testing.expectEqual(types.Role.candidate, node.role);
    try std.testing.expectEqual(@as(u64, 1), node.current_term);
}
