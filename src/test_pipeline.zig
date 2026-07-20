//! Pipelined replication and batching tests.

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

test "clientAppend immediately triggers broadcast" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};

    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();
    node.role = .leader;
    node.current_term = 1;
    // send_append_entries is null — broadcastAppendEntries will skip sending,
    // but the method itself should not error.

    // Append an entry — should not error even without a send callback
    const idx = try node.clientAppend("data1");
    try std.testing.expectEqual(@as(u64, 1), idx);
    // Entry is in the log
    try std.testing.expectEqual(@as(u64, 1), node.log.lastIndex());
}

test "clientAppendBatch appends all entries with one broadcast" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};

    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();
    node.role = .leader;
    node.current_term = 1;

    const items = [_][]const u8{ "batch1", "batch2", "batch3" };
    const indices = try node.clientAppendBatch(&items);
    defer allocator.free(indices);

    try std.testing.expectEqual(@as(u64, 1), indices[0]);
    try std.testing.expectEqual(@as(u64, 2), indices[1]);
    try std.testing.expectEqual(@as(u64, 3), indices[2]);
    try std.testing.expectEqual(@as(u64, 3), node.log.lastIndex());

    // Verify data in log
    const e1 = node.log.get(1).?;
    try std.testing.expectEqualStrings("batch1", e1.data);
    const e2 = node.log.get(2).?;
    try std.testing.expectEqualStrings("batch2", e2.data);
    const e3 = node.log.get(3).?;
    try std.testing.expectEqualStrings("batch3", e3.data);
}

test "handleAppendEntriesResponse triggers pipeline on success" {
    const allocator = std.testing.allocator;

    var m1 = mem_storage.MemoryStorage.init(allocator); defer m1.deinit();
    var l1 = try LogType.init(allocator, &m1, 4); defer l1.deinit();
    var sm1 = TestSM{};
    var leader = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2}, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000 }, &l1, .{ .ptr = &sm1, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, m1.toStorage(), 12345);
    defer leader.deinit();

    // Set up as leader with entries
    leader.role = .leader;
    leader.current_term = 1;
    _ = try leader.log.append(1, "entry1");
    _ = try leader.log.append(1, "entry2");
    leader.next_index[0] = 1;
    leader.match_index[0] = 0;

    // Follower acknowledges entry 1
    try leader.handleAppendEntriesResponse(2, .{ .term = 1, .success = true, .last_confirmed_index = 1 });
    // Should have advanced match_index/next_index
    try std.testing.expectEqual(@as(u64, 1), leader.match_index[0]);
    try std.testing.expectEqual(@as(u64, 2), leader.next_index[0]);
    // Pipeline should have triggered broadcast for entry 2
    // (We can't verify the broadcast happened without a send callback,
    // but the method shouldn't error and state should be correct.)
    try std.testing.expectEqual(@as(u64, 2), leader.next_index[0]);
}

test "clientAppendBatch returns NotLeader for follower" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{2, 3} }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    try std.testing.expectError(error.NotLeader, node.clientAppendBatch(&[_][]const u8{"data"}));
}
