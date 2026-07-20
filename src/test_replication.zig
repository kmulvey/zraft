//! Log replication tests (storage-backed).

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

test "append entries heartbeat accepted" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator); defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4); defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();
    node.current_term = 2;
    const resp = try node.handleAppendEntries(.{ .term = 2, .leader_id = 2, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{}, .leader_commit = 0 }, 100_000_000);
    try std.testing.expect(resp.success);
    try std.testing.expectEqual(types.Role.follower, node.role);
}

test "append entries rejected on old term" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator); defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4); defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();
    node.current_term = 5;
    const resp = try node.handleAppendEntries(.{ .term = 3, .leader_id = 2, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{}, .leader_commit = 0 }, 100_000_000);
    try std.testing.expect(!resp.success);
}

test "follower appends entries from leader" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator); defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4); defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();
    node.current_term = 1;
    const wire_entry = rpc.LogEntryWire{ .term = 1, .index = 1, .entry_type = .command, .data = "command" };
    const resp = try node.handleAppendEntries(.{ .term = 1, .leader_id = 2, .prev_log_index = 0, .prev_log_term = 0, .entries = &.{wire_entry}, .leader_commit = 0 }, 100_000_000);
    try std.testing.expect(resp.success);
    try std.testing.expectEqual(@as(usize, 2), log.len);
    try std.testing.expectEqual(@as(u64, 1), log.termAt(1));
}

test "leader can append client commands" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator); defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4); defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();
    node.role = .leader; node.current_term = 1;
    const idx = try node.clientAppend("my-command");
    try std.testing.expectEqual(@as(u64, 1), idx);
    try std.testing.expectEqual(@as(usize, 2), log.len);
}

test "client append fails when not leader" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator); defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4); defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();
    try std.testing.expectError(error.NotLeader, node.clientAppend("data"));
}
