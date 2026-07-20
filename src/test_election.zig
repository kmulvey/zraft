//! Election-specific tests (storage-backed).

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

fn runTest(allocator: std.mem.Allocator, id: u64, peers: []const u64, fn_ctx: *const fn (ctx: *NodeType, log: *LogType, mstore: *mem_storage.MemoryStorage) anyerror!void) !void {
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = id, .peers = peers, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();
    try fn_ctx(&node, &log, &mstore);
}

test "vote granted: candidate log is up-to-date" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    const resp = try node.handleRequestVote(.{ .term = 1, .candidate_id = 2, .last_log_index = 0, .last_log_term = 0 });
    try std.testing.expect(resp.vote_granted);
    try std.testing.expectEqual(@as(?u64, 2), node.voted_for);
}

test "vote denied: old term" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();
    node.current_term = 5;

    try std.testing.expect(!(try node.handleRequestVote(.{ .term = 3, .candidate_id = 2, .last_log_index = 0, .last_log_term = 0 })).vote_granted);
}

test "vote denied: already voted for other" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();
    node.current_term = 1;
    node.voted_for = 2;

    try std.testing.expect(!(try node.handleRequestVote(.{ .term = 1, .candidate_id = 3, .last_log_index = 0, .last_log_term = 0 })).vote_granted);
}

test "vote denied: candidate log is less up-to-date" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    _ = try log.append(2, "data");
    node.current_term = 2;

    try std.testing.expect(!(try node.handleRequestVote(.{ .term = 2, .candidate_id = 2, .last_log_index = 1, .last_log_term = 1 })).vote_granted);
}

test "vote granted when new term forces step down" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();
    node.current_term = 1;
    node.role = .candidate;

    const resp = try node.handleRequestVote(.{ .term = 2, .candidate_id = 2, .last_log_index = 0, .last_log_term = 0 });
    try std.testing.expect(resp.vote_granted);
    try std.testing.expectEqual(types.Role.follower, node.role);
    try std.testing.expectEqual(@as(u64, 2), node.current_term);
}
