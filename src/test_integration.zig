//! Integration tests: multi-node cluster with in-memory transport.

const std = @import("std");
const raft = @import("raft");

const types = raft.types;
const rpc = raft.rpc;
const Log = raft.Log;
const Node = raft.Node;
const StateMachine = raft.StateMachine;

test "single node election timeout" {
    const allocator = std.testing.allocator;
    var log = try Log.init(allocator, 4);
    defer log.deinit();

    const config = raft.Config{
        .id = 1,
        .peers = &.{}, // single-node cluster
        .election_timeout_min_ns = 50_000_000,
        .election_timeout_max_ns = 100_000_000,
    };

    const TSM = struct {
        pub fn apply(_: *@This(), _: u64, _: []const u8) void {}
        pub fn snapshot(_: *@This(), _: anytype) !void {}
        pub fn restore(_: *@This(), _: anytype) !void {}
    };
    var sm_impl = TSM{};
    const sm = StateMachine(TSM){
        .ptr = &sm_impl,
        .applyFn = TSM.apply,
        .snapshotFn = TSM.snapshot,
        .restoreFn = TSM.restore,
    };

    var node = try Node(TSM).init(allocator, config, &log, sm, 12345);
    defer node.deinit();

    // Tick past election timeout
    try node.tick(200_000_000);
    try std.testing.expectEqual(types.Role.candidate, node.role);
    try std.testing.expectEqual(@as(u64, 1), node.current_term);
}

test "three node election basic" {
    const allocator = std.testing.allocator;

    const TestSM2 = struct {
        pub fn apply(_: *@This(), _: u64, _: []const u8) void {}
        pub fn snapshot(_: *@This(), _: anytype) !void {}
        pub fn restore(_: *@This(), _: anytype) !void {}
    };

    var logs = [_]Log{
        try Log.init(allocator, 4),
        try Log.init(allocator, 4),
        try Log.init(allocator, 4),
    };
    defer for (&logs) |*l| l.deinit();

    const NodeType = Node(TestSM2);

    var sm0 = TestSM2{};
    var sm1 = TestSM2{};
    var sm2 = TestSM2{};

    var node0 = try NodeType.init(allocator, .{
        .id = 1,
        .peers = &.{ 2, 3 },
        .election_timeout_min_ns = 50_000_000,
        .election_timeout_max_ns = 100_000_000,
    }, &logs[0], StateMachine(TestSM2){
        .ptr = &sm0,
        .applyFn = TestSM2.apply,
        .snapshotFn = TestSM2.snapshot,
        .restoreFn = TestSM2.restore,
    }, 12345);
    defer node0.deinit();

    var node1 = try NodeType.init(allocator, .{
        .id = 2,
        .peers = &.{ 1, 3 },
        .election_timeout_min_ns = 50_000_000,
        .election_timeout_max_ns = 100_000_000,
    }, &logs[1], StateMachine(TestSM2){
        .ptr = &sm1,
        .applyFn = TestSM2.apply,
        .snapshotFn = TestSM2.snapshot,
        .restoreFn = TestSM2.restore,
    }, 67890);
    defer node1.deinit();

    var node2 = try NodeType.init(allocator, .{
        .id = 3,
        .peers = &.{ 1, 2 },
        .election_timeout_min_ns = 50_000_000,
        .election_timeout_max_ns = 100_000_000,
    }, &logs[2], StateMachine(TestSM2){
        .ptr = &sm2,
        .applyFn = TestSM2.apply,
        .snapshotFn = TestSM2.snapshot,
        .restoreFn = TestSM2.restore,
    }, 99999);
    defer node2.deinit();

    // Tick node 0 past election timeout to trigger an election
    try node0.tick(200_000_000);
    try std.testing.expectEqual(types.Role.candidate, node0.role);

    // Other nodes should still be followers (no tick for them yet)
    try std.testing.expectEqual(types.Role.follower, node1.role);
    try std.testing.expectEqual(types.Role.follower, node2.role);

    // Simulate delivery of RequestVote from node 0 to nodes 1, 2
    for ([3]*NodeType{ &node0, &node1, &node2 }, 0..) |n, i| {
        if (i == 0) continue;
        const req = rpc.RequestVoteRequest{
            .term = node0.current_term,
            .candidate_id = node0.config.id,
            .last_log_index = node0.log.lastIndex(),
            .last_log_term = node0.log.termAt(node0.log.lastIndex()),
        };
        const resp = n.handleRequestVote(req);
        try std.testing.expect(resp.vote_granted);
        try std.testing.expectEqual(node0.current_term, resp.term);
    }

    // Node 0 should have enough votes to become leader
    try node0.becomeLeader();
    try std.testing.expectEqual(types.Role.leader, node0.role);
}

test "log replication across three nodes" {
    const allocator = std.testing.allocator;

    const TestSM3 = struct {
        pub fn apply(_: *@This(), _: u64, _: []const u8) void {}
        pub fn snapshot(_: *@This(), _: anytype) !void {}
        pub fn restore(_: *@This(), _: anytype) !void {}
    };

    var logs = [_]Log{
        try Log.init(allocator, 4),
        try Log.init(allocator, 4),
        try Log.init(allocator, 4),
    };
    defer for (&logs) |*l| l.deinit();

    const NodeType = Node(TestSM3);

    var sm_leader = TestSM3{};
    var sm_f1 = TestSM3{};
    var sm_f2 = TestSM3{};

    var leader = try NodeType.init(allocator, .{
        .id = 1,
        .peers = &.{ 2, 3 },
        .election_timeout_min_ns = 50_000_000,
        .election_timeout_max_ns = 100_000_000,
    }, &logs[0], StateMachine(TestSM3){
        .ptr = &sm_leader,
        .applyFn = TestSM3.apply,
        .snapshotFn = TestSM3.snapshot,
        .restoreFn = TestSM3.restore,
    }, 12345);
    defer leader.deinit();
    leader.role = .leader;
    leader.current_term = 1;

    var follower1 = try NodeType.init(allocator, .{
        .id = 2,
        .peers = &.{ 1, 3 },
        .election_timeout_min_ns = 50_000_000,
        .election_timeout_max_ns = 100_000_000,
    }, &logs[1], StateMachine(TestSM3){
        .ptr = &sm_f1,
        .applyFn = TestSM3.apply,
        .snapshotFn = TestSM3.snapshot,
        .restoreFn = TestSM3.restore,
    }, 67890);
    defer follower1.deinit();
    follower1.current_term = 1;

    var follower2 = try NodeType.init(allocator, .{
        .id = 3,
        .peers = &.{ 1, 2 },
        .election_timeout_min_ns = 50_000_000,
        .election_timeout_max_ns = 100_000_000,
    }, &logs[2], StateMachine(TestSM3){
        .ptr = &sm_f2,
        .applyFn = TestSM3.apply,
        .snapshotFn = TestSM3.snapshot,
        .restoreFn = TestSM3.restore,
    }, 99999);
    defer follower2.deinit();
    follower2.current_term = 1;

    // Append an entry to the leader
    const entry_idx = try leader.clientAppend("replicated-data");
    try std.testing.expectEqual(@as(u64, 1), entry_idx);

    // Replicate to followers
    for ([2]*NodeType{ &follower1, &follower2 }) |f| {
        const slice = leader.log.sliceFrom(1);
        var wire_entries: [1]rpc.LogEntryWire = undefined;
        wire_entries[0] = .{
            .term = slice[0].term,
            .index = slice[0].index,
            .data = slice[0].data,
        };
        const prev_idx = entry_idx - 1;
        const resp = f.handleAppendEntries(rpc.AppendEntriesRequest{
            .term = 1,
            .leader_id = 1,
            .prev_log_index = prev_idx,
            .prev_log_term = leader.log.termAt(prev_idx),
            .entries = &wire_entries,
            .leader_commit = entry_idx,
        });
        try std.testing.expect(resp.success);
        try std.testing.expectEqual(@as(u64, 1), f.log.termAt(1));
    }
}
