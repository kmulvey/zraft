//! Election-specific tests (storage-backed).

const std = @import("std");
const raft = @import("raft");

const types = raft.types;
const rpc = raft.rpc;
const mem_storage = raft.memory_storage;
const Log = raft.Log;

const TestSM = struct {
    pub fn apply(_: *@This(), _: u64, _: []const u8) void {}
    pub fn snapshot(_: *@This(), _: anytype) !void {}
    pub fn restore(_: *@This(), _: anytype) !void {}
};

const NodeType = raft.Node(TestSM, mem_storage.MemoryStorage);
const LogType = Log(mem_storage.MemoryStorage);

test "vote granted: candidate log is up-to-date" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    const storage = mstore.toStorage();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();

    const config = raft.Config{
        .id = 1,
        .peers = &.{ 2, 3 },
        .election_timeout_min_ns = 50_000_000,
        .election_timeout_max_ns = 100_000_000,
    };
    var sm_impl = TestSM{};
    const sm = raft.StateMachine(TestSM){
        .ptr = &sm_impl,
        .applyFn = TestSM.apply,
        .snapshotFn = TestSM.snapshot,
        .restoreFn = TestSM.restore,
    };
    var node = try NodeType.init(allocator, config, &log, sm, storage, 12345);
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
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    const storage = mstore.toStorage();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();

    const config = raft.Config{
        .id = 1,
        .peers = &.{ 2, 3 },
        .election_timeout_min_ns = 50_000_000,
        .election_timeout_max_ns = 100_000_000,
    };
    var sm_impl = TestSM{};
    const sm = raft.StateMachine(TestSM){ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore };
    var node = try NodeType.init(allocator, config, &log, sm, storage, 12345);
    defer node.deinit();
    node.current_term = 5;

    try std.testing.expect(!node.handleRequestVote(.{ .term = 3, .candidate_id = 2, .last_log_index = 0, .last_log_term = 0 }).vote_granted);
}

test "vote denied: already voted for other" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    const storage = mstore.toStorage();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();

    const config = raft.Config{ .id = 1, .peers = &.{ 2, 3 }, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 };
    var sm_impl = TestSM{};
    const sm = raft.StateMachine(TestSM){ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore };
    var node = try NodeType.init(allocator, config, &log, sm, storage, 12345);
    defer node.deinit();
    node.current_term = 1;
    node.voted_for = 2;

    try std.testing.expect(!node.handleRequestVote(.{ .term = 1, .candidate_id = 3, .last_log_index = 0, .last_log_term = 0 }).vote_granted);
}

test "vote denied: candidate log is less up-to-date" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    const storage = mstore.toStorage();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();

    _ = try log.append(2, "data");

    const config = raft.Config{ .id = 1, .peers = &.{ 2, 3 }, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 };
    var sm_impl = TestSM{};
    const sm = raft.StateMachine(TestSM){ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore };
    var node = try NodeType.init(allocator, config, &log, sm, storage, 12345);
    defer node.deinit();
    node.current_term = 2;

    try std.testing.expect(!node.handleRequestVote(.{ .term = 2, .candidate_id = 2, .last_log_index = 1, .last_log_term = 1 }).vote_granted);
}

test "vote granted when new term forces step down" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    const storage = mstore.toStorage();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();

    const config = raft.Config{ .id = 1, .peers = &.{ 2, 3 }, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 };
    var sm_impl = TestSM{};
    const sm = raft.StateMachine(TestSM){ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore };
    var node = try NodeType.init(allocator, config, &log, sm, storage, 12345);
    defer node.deinit();
    node.current_term = 1;
    node.role = .candidate;

    const resp = node.handleRequestVote(.{ .term = 2, .candidate_id = 2, .last_log_index = 0, .last_log_term = 0 });
    try std.testing.expect(resp.vote_granted);
    try std.testing.expectEqual(types.Role.follower, node.role);
    try std.testing.expectEqual(@as(u64, 2), node.current_term);
}
