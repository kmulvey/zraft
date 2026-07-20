//! Log replication tests.

const std = @import("std");
const raft = @import("raft");

const types = raft.types;
const rpc = raft.rpc;
const Log = raft.Log;

const TestSM = struct {
    pub fn apply(_: *@This(), _: u64, _: []const u8) void {}
    pub fn snapshot(_: *@This(), _: anytype) !void {}
    pub fn restore(_: *@This(), _: anytype) !void {}
};

fn makeNode(allocator: std.mem.Allocator, id: u64, peers: []const u64, log: *Log, sm_impl: *TestSM) !raft.Node(TestSM) {
    const config = raft.Config{
        .id = id,
        .peers = peers,
        .election_timeout_min_ns = 50_000_000,
        .election_timeout_max_ns = 100_000_000,
    };
    const sm = raft.StateMachine(TestSM){
        .ptr = sm_impl,
        .applyFn = TestSM.apply,
        .snapshotFn = TestSM.snapshot,
        .restoreFn = TestSM.restore,
    };
    return try raft.Node(TestSM).init(allocator, config, log, sm, 12345);
}

test "append entries heartbeat accepted" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    var sm_impl = TestSM{};
    var node = try makeNode(allocator, 1, &.{2, 3}, &log, &sm_impl);
    defer node.deinit();
    node.current_term = 2;

    const resp = node.handleAppendEntries(rpc.AppendEntriesRequest{
        .term = 2,
        .leader_id = 2,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    });

    try std.testing.expect(resp.success);
    try std.testing.expectEqual(types.Role.follower, node.role);
}

test "append entries rejected on old term" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    var sm_impl = TestSM{};
    var node = try makeNode(allocator, 1, &.{2, 3}, &log, &sm_impl);
    defer node.deinit();
    node.current_term = 5;

    const resp = node.handleAppendEntries(rpc.AppendEntriesRequest{
        .term = 3,
        .leader_id = 2,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    });

    try std.testing.expect(!resp.success);
}

test "follower appends entries from leader" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    var sm_impl = TestSM{};
    var node = try makeNode(allocator, 1, &.{2, 3}, &log, &sm_impl);
    defer node.deinit();
    node.current_term = 1;

    const wire_entry = rpc.LogEntryWire{ .term = 1, .index = 1, .data = "command" };

    const resp = node.handleAppendEntries(rpc.AppendEntriesRequest{
        .term = 1,
        .leader_id = 2,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{wire_entry},
        .leader_commit = 0,
    });

    try std.testing.expect(resp.success);
    try std.testing.expectEqual(@as(usize, 2), node.log.len); // sentinel + 1 entry
    try std.testing.expectEqual(@as(u64, 1), node.log.termAt(1));
}

test "leader can append client commands" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    var sm_impl = TestSM{};
    var node = try makeNode(allocator, 1, &.{2, 3}, &log, &sm_impl);
    defer node.deinit();
    node.role = .leader;
    node.current_term = 1;

    const idx = try node.clientAppend("my-command");
    try std.testing.expectEqual(@as(u64, 1), idx);
    try std.testing.expectEqual(@as(usize, 2), node.log.len);
}

test "client append fails when not leader" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    var sm_impl = TestSM{};
    var node = try makeNode(allocator, 1, &.{2, 3}, &log, &sm_impl);
    defer node.deinit();

    try std.testing.expectError(error.NotLeader, node.clientAppend("data"));
}
