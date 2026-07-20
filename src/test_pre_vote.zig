//! Pre-vote protocol tests (§9.6).

const std = @import("std");
const raft = @import("raft");

const types = raft.types;
const rpc = raft.rpc;
const mem_storage = raft.memory_storage;
const Log = raft.Log;

const TestSM = struct {
    pub fn apply(_: *@This(), _: u64, _: []const u8) void {}
    pub fn snapshot(_: *@This(), _: std.mem.Allocator) ![]u8 { return &.{}; }
    pub fn restore(_: *@This(), _: []const u8) !void {}
};

const NodeType = raft.Node(TestSM, mem_storage.MemoryStorage);
const LogType = Log(mem_storage.MemoryStorage);

test "pre-vote grants based on log up-to-dateness" {
    const allocator = std.testing.allocator;

    // Follower with log
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();

    // Append some entries to the follower's log
    _ = try log.append(1, "entry1");
    _ = try log.append(1, "entry2");
    _ = try log.append(2, "entry3");

    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 2, .peers = &.{1, 3} }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();
    node.current_term = 2;

    // Candidate with less up-to-date log should be denied
    {
        const resp = node.handlePreVote(.{ .term = 3, .candidate_id = 1, .last_log_index = 1, .last_log_term = 1 });
        try std.testing.expect(!resp.vote_granted);
        try std.testing.expectEqual(@as(u64, 2), resp.term);
    }

    // Candidate with equal log should be granted
    {
        const resp = node.handlePreVote(.{ .term = 3, .candidate_id = 1, .last_log_index = 3, .last_log_term = 2 });
        try std.testing.expect(resp.vote_granted);
        try std.testing.expectEqual(@as(u64, 2), resp.term);
    }

    // Candidate with more up-to-date log should be granted
    {
        const resp = node.handlePreVote(.{ .term = 3, .candidate_id = 1, .last_log_index = 4, .last_log_term = 2 });
        try std.testing.expect(resp.vote_granted);
    }

    // Candidate with stale proposed term should be denied
    {
        const resp = node.handlePreVote(.{ .term = 1, .candidate_id = 1, .last_log_index = 3, .last_log_term = 2 });
        try std.testing.expect(!resp.vote_granted);
    }

    // Candidate not in active config should be denied
    {
        const resp = node.handlePreVote(.{ .term = 3, .candidate_id = 99, .last_log_index = 3, .last_log_term = 2 });
        try std.testing.expect(!resp.vote_granted);
    }
}

test "pre-vote does not modify state" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    _ = try log.append(1, "data");

    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 2, .peers = &.{1, 3} }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    // Grant a pre-vote
    _ = node.handlePreVote(.{ .term = 5, .candidate_id = 1, .last_log_index = 1, .last_log_term = 1 });

    // State should be untouched
    try std.testing.expectEqual(@as(u64, 0), mstore.current_term);
    try std.testing.expectEqual(@as(?u64, null), mstore.voted_for);
}

test "pre-vote transitions to candidate on quorum" {
    const allocator = std.testing.allocator;
    var m1 = mem_storage.MemoryStorage.init(allocator); defer m1.deinit();
    var l1 = try LogType.init(allocator, &m1, 4); defer l1.deinit();
    var sm1 = TestSM{};
    var n1 = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &l1, .{ .ptr = &sm1, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, m1.toStorage(), 12345);
    defer n1.deinit();

    var m2 = mem_storage.MemoryStorage.init(allocator); defer m2.deinit();
    var l2 = try LogType.init(allocator, &m2, 4); defer l2.deinit();
    var sm2 = TestSM{};
    var n2 = try NodeType.init(allocator, .{ .id = 2, .peers = &.{1, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &l2, .{ .ptr = &sm2, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, m2.toStorage(), 67890);
    defer n2.deinit();

    // n1 becomes pre_candidate after tick
    try n1.tick(200_000_000);
    try std.testing.expectEqual(types.Role.pre_candidate, n1.role);
    try std.testing.expectEqual(@as(u64, 0), n1.current_term);

    // Grant pre-votes from both peers → quorum → becomes candidate
    const pv_resp1 = n2.handlePreVote(.{ .term = 1, .candidate_id = 1, .last_log_index = 0, .last_log_term = 0 });
    try std.testing.expect(pv_resp1.vote_granted);
    try n1.handlePreVoteResponse(2, pv_resp1);

    try std.testing.expectEqual(types.Role.candidate, n1.role);
    try std.testing.expectEqual(@as(u64, 1), n1.current_term);
}

test "pre-vote stale peer term causes step down" {
    const allocator = std.testing.allocator;
    var m1 = mem_storage.MemoryStorage.init(allocator); defer m1.deinit();
    var l1 = try LogType.init(allocator, &m1, 4); defer l1.deinit();
    var sm1 = TestSM{};
    var n1 = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &l1, .{ .ptr = &sm1, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, m1.toStorage(), 12345);
    defer n1.deinit();

    // Become pre_candidate
    try n1.tick(200_000_000);
    try std.testing.expectEqual(types.Role.pre_candidate, n1.role);

    // Peer responds with a higher term → should step down
    try n1.handlePreVoteResponse(2, .{ .term = 5, .vote_granted = false });
    try std.testing.expectEqual(types.Role.follower, n1.role);
    try std.testing.expectEqual(@as(u64, 5), n1.current_term);
}
