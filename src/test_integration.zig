//! Integration tests: multi-node cluster with storage-backed nodes.

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

test "single node election timeout" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();
    try node.tick(200_000_000);
    try std.testing.expectEqual(types.Role.leader, node.role);
    try std.testing.expectEqual(@as(u64, 1), node.current_term);
    try std.testing.expectEqual(@as(u64, 1), mstore.current_term);
}

test "three node election basic" {
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
    var m3 = mem_storage.MemoryStorage.init(allocator); defer m3.deinit();
    var l3 = try LogType.init(allocator, &m3, 4); defer l3.deinit();
    var sm3 = TestSM{};
    var n3 = try NodeType.init(allocator, .{ .id = 3, .peers = &.{1, 2}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &l3, .{ .ptr = &sm3, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, m3.toStorage(), 99999);
    defer n3.deinit();

    try n1.tick(200_000_000);
    try std.testing.expectEqual(types.Role.candidate, n1.role);
    try std.testing.expectEqual(types.Role.follower, n2.role);
    try std.testing.expectEqual(types.Role.follower, n3.role);

    for ([3]*NodeType{ &n1, &n2, &n3 }, 0..) |n, i| {
        if (i == 0) continue;
        const resp = try n.handleRequestVote(.{ .term = n1.current_term, .candidate_id = n1.config.id, .last_log_index = n1.log.lastIndex(), .last_log_term = n1.log.termAt(n1.log.lastIndex()) });
        try std.testing.expect(resp.vote_granted);
        try n1.handleRequestVoteResponse(n.config.id, resp);
    }
    try std.testing.expectEqual(types.Role.leader, n1.role);
}

test "log replication across three nodes" {
    const allocator = std.testing.allocator;
    var m1 = mem_storage.MemoryStorage.init(allocator); defer m1.deinit();
    var l1 = try LogType.init(allocator, &m1, 4); defer l1.deinit();
    var sm1 = TestSM{};
    var leader = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &l1, .{ .ptr = &sm1, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, m1.toStorage(), 12345);
    defer leader.deinit();
    leader.role = .leader; leader.current_term = 1;

    var m2 = mem_storage.MemoryStorage.init(allocator); defer m2.deinit();
    var l2 = try LogType.init(allocator, &m2, 4); defer l2.deinit();
    var sm2 = TestSM{};
    var f1 = try NodeType.init(allocator, .{ .id = 2, .peers = &.{1, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &l2, .{ .ptr = &sm2, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, m2.toStorage(), 67890);
    defer f1.deinit();
    f1.current_term = 1;

    var m3 = mem_storage.MemoryStorage.init(allocator); defer m3.deinit();
    var l3 = try LogType.init(allocator, &m3, 4); defer l3.deinit();
    var sm3 = TestSM{};
    var f2 = try NodeType.init(allocator, .{ .id = 3, .peers = &.{1, 2}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &l3, .{ .ptr = &sm3, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, m3.toStorage(), 99999);
    defer f2.deinit();
    f2.current_term = 1;

    const entry_idx = try leader.clientAppend("replicated-data");
    try std.testing.expectEqual(@as(u64, 1), entry_idx);

    for ([2]*NodeType{ &f1, &f2 }) |f| {
        const slice = l1.sliceFrom(1);
        var wire_entries: [1]rpc.LogEntryWire = undefined;
        wire_entries[0] = .{ .term = slice[0].term, .index = slice[0].index, .data = slice[0].data };
        const resp = try f.handleAppendEntries(.{ .term = 1, .leader_id = 1, .prev_log_index = entry_idx - 1, .prev_log_term = l1.termAt(entry_idx - 1), .entries = &wire_entries, .leader_commit = entry_idx }, 100_000_000);
        try std.testing.expect(resp.success);
        try std.testing.expectEqual(@as(u64, 1), f.log.termAt(1));
    }
}
