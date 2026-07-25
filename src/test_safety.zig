//! Safety regression tests for election rules and term handling.

const std = @import("std");
const raft = @import("raft");

const mem_storage = raft.memory_storage;
const Log = raft.Log;
const StateMachine = raft.StateMachine;
const Role = raft.Role;

const TestSM = struct {
    pub fn apply(_: *@This(), _: u64, _: []const u8) void {}
    pub fn snapshot(_: *@This(), _: std.mem.Allocator) ![]u8 { return &.{}; }
    pub fn restore(_: *@This(), _: []const u8) !void {}
};

const NodeType = raft.Node(TestSM, mem_storage.MemoryStorage);
const LogType = Log(mem_storage.MemoryStorage);

test "duplicate vote responses do not elect a leader without quorum" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    const sm = StateMachine(TestSM){ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore };

    // 5-node cluster: quorum = 3
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{ 2, 3, 4, 5 } }, &log, sm, mstore.toStorage(), 42);
    defer node.deinit();

    node.role = .candidate;
    node.current_term = 1;
    try node.votes_received.put(allocator, node.config.id, {}); // self vote

    // Same peer's grant delivered twice (e.g. retry/duplication).
    try node.handleRequestVoteResponse(2, .{ .term = 1, .vote_granted = true });
    try node.handleRequestVoteResponse(2, .{ .term = 1, .vote_granted = true });

    try std.testing.expectEqual(Role.candidate, node.role);
}

test "vote from server not in config does not count" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    const sm = StateMachine(TestSM){ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore };

    // 3-node cluster: quorum = 2
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{ 2, 3 } }, &log, sm, mstore.toStorage(), 42);
    defer node.deinit();

    node.role = .candidate;
    node.current_term = 1;
    try node.votes_received.put(allocator, node.config.id, {});

    // Server 99 is not a member; its grant must be ignored.
    try node.handleRequestVoteResponse(99, .{ .term = 1, .vote_granted = true });
    try std.testing.expectEqual(Role.candidate, node.role);
}

test "leader denies same-term RequestVote from another candidate" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    const sm = StateMachine(TestSM){ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore };

    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3} }, &log, sm, mstore.toStorage(), 42);
    defer node.deinit();

    node.role = .leader;
    node.current_term = 5;
    node.voted_for = node.config.id;

    const resp = try node.handleRequestVote(.{ .term = 5, .candidate_id = 2, .last_log_index = 0, .last_log_term = 0 });
    try std.testing.expect(!resp.vote_granted);
}

test "follower adopts higher term from AppendEntries" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    const sm = StateMachine(TestSM){ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore };

    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3} }, &log, sm, mstore.toStorage(), 42);
    defer node.deinit();

    // Vote in term 2 so voted_for is non-null.
    node.current_term = 2;
    node.voted_for = 2;
    try node.storage.storeTerm(2);
    try node.storage.storeVotedFor(2);

    _ = try node.handleAppendEntries(.{
        .term = 5, .leader_id = 2,
        .prev_log_index = 0, .prev_log_term = 0,
        .entries = &.{}, .leader_commit = 0,
    }, 1_000_000);

    try std.testing.expectEqual(@as(u64, 5), node.current_term);
    try std.testing.expectEqual(@as(?u64, null), node.voted_for);
    try std.testing.expectEqual(@as(?u64, null), node.storage.loadVotedFor());
}

test "same-term AppendEntries from another leader steps node down" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    const sm = StateMachine(TestSM){ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore };

    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3} }, &log, sm, mstore.toStorage(), 42);
    defer node.deinit();

    node.role = .leader;
    node.current_term = 3;
    node.voted_for = node.config.id;

    _ = try node.handleAppendEntries(.{
        .term = 3, .leader_id = 2,
        .prev_log_index = 0, .prev_log_term = 0,
        .entries = &.{}, .leader_commit = 0,
    }, 1_000_000);

    try std.testing.expectEqual(Role.follower, node.role);
}

test "freshly elected leader with no peer responses steps down within election timeout" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    const sm = StateMachine(TestSM){ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore };

    const timeout_max = 50_000_000;
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 10_000_000, .election_timeout_max_ns = timeout_max, .heartbeat_interval_ns = 5_000_000 }, &log, sm, mstore.toStorage(), 42);
    defer node.deinit();

    node.role = .leader;
    node.current_term = 1;
    node.last_quorum_ns = 0;

    var now: u64 = 0;
    while (now <= timeout_max + 20_000_000) : (now += 5_000_000) {
        try node.tick(now);
        if (node.role != .leader) break;
    }

    try std.testing.expectEqual(Role.follower, node.role);
}
