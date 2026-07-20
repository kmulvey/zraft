//! Election-specific tests.

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

test "vote granted: candidate log is up-to-date" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    var sm_impl = TestSM{};
    var node = try makeNode(allocator, 1, &.{2, 3}, &log, &sm_impl);
    defer node.deinit();

    const resp = node.handleRequestVote(rpc.RequestVoteRequest{
        .term = 1,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    });

    try std.testing.expect(resp.vote_granted);
    try std.testing.expectEqual(@as(?u64, 2), node.voted_for);
}

test "vote denied: old term" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    var sm_impl = TestSM{};
    var node = try makeNode(allocator, 1, &.{2, 3}, &log, &sm_impl);
    defer node.deinit();
    node.current_term = 5;

    const resp = node.handleRequestVote(rpc.RequestVoteRequest{
        .term = 3,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    });

    try std.testing.expect(!resp.vote_granted);
}

test "vote denied: already voted for other" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    var sm_impl = TestSM{};
    var node = try makeNode(allocator, 1, &.{2, 3}, &log, &sm_impl);
    defer node.deinit();
    node.current_term = 1;
    node.voted_for = 2;

    const resp = node.handleRequestVote(rpc.RequestVoteRequest{
        .term = 1,
        .candidate_id = 3,
        .last_log_index = 0,
        .last_log_term = 0,
    });

    try std.testing.expect(!resp.vote_granted);
}

test "vote denied: candidate log is less up-to-date" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    _ = try log.append(2, "data"); // we have term 2 at index 1

    var sm_impl = TestSM{};
    var node = try makeNode(allocator, 1, &.{2, 3}, &log, &sm_impl);
    defer node.deinit();
    node.current_term = 2;

    const resp = node.handleRequestVote(rpc.RequestVoteRequest{
        .term = 2,
        .candidate_id = 2,
        .last_log_index = 1,
        .last_log_term = 1, // candidate's last entry is term 1, ours is term 2 -> deny
    });

    try std.testing.expect(!resp.vote_granted);
}

test "vote granted when new term forces step down" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    var sm_impl = TestSM{};
    var node = try makeNode(allocator, 1, &.{2, 3}, &log, &sm_impl);
    defer node.deinit();
    node.current_term = 1;
    node.role = .candidate;

    const resp = node.handleRequestVote(rpc.RequestVoteRequest{
        .term = 2,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    });

    try std.testing.expect(resp.vote_granted);
    try std.testing.expectEqual(types.Role.follower, node.role);
    try std.testing.expectEqual(@as(u64, 2), node.current_term);
}
