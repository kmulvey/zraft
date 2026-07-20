//! Cluster membership change tests (§6).

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

test "cluster config serialization" {
    const cfg = types.ClusterConfig{ .servers = &.{ 1, 2, 3 } };
    try std.testing.expectEqual(@as(usize, 2), cfg.quorum());
    try std.testing.expect(cfg.contains(1));
    try std.testing.expect(!cfg.contains(4));
}

test "clusterChangeRequest appends config entry as leader" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{ 2, 3 }, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000, .heartbeat_interval_ns = 25_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    // Force leader
    node.role = .leader;
    node.current_term = 1;
    const last_idx = node.log.lastIndex() + 1;
    for (node.next_index, 0..) |*ni, i| { ni.* = last_idx; node.match_index[i] = 0; }

    try node.clusterChangeRequest(&.{ 1, 2, 3, 4 });

    // Should have appended a config entry
    const idx = node.log.lastIndex();
    try std.testing.expectEqual(@as(u64, 1), idx);
    const entry = node.log.get(1).?;
    try std.testing.expectEqual(types.EntryType.configuration, entry.entry_type);
}

test "not leader returns error" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{ 2, 3 }, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000, .heartbeat_interval_ns = 25_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    try std.testing.expectError(error.NotLeader, node.clusterChangeRequest(&.{ 1, 2, 3, 4 }));
}

test "initial active config contains self and peers" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 2, .peers = &.{ 1, 3 }, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000, .heartbeat_interval_ns = 25_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    try std.testing.expect(node.active_config.contains(1));
    try std.testing.expect(node.active_config.contains(2)); // self
    try std.testing.expect(node.active_config.contains(3));
    try std.testing.expect(!node.active_config.contains(4));
    try std.testing.expectEqual(@as(usize, 3), node.active_config.len());
}

test "follower learns leader config via AppendEntries" {
    const allocator = std.testing.allocator;

    // Leader node
    var mstore_leader = mem_storage.MemoryStorage.init(allocator);
    defer mstore_leader.deinit();
    var log_leader = try LogType.init(allocator, &mstore_leader, 4);
    defer log_leader.deinit();
    var sm_leader = TestSM{};
    var leader = try NodeType.init(allocator, .{ .id = 1, .peers = &.{ 2, 3 }, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000, .heartbeat_interval_ns = 25_000_000 }, &log_leader, .{ .ptr = &sm_leader, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore_leader.toStorage(), 12345);
    defer leader.deinit();

    // Follower node
    var mstore_follower = mem_storage.MemoryStorage.init(allocator);
    defer mstore_follower.deinit();
    var log_follower = try LogType.init(allocator, &mstore_follower, 4);
    defer log_follower.deinit();
    var sm_follower = TestSM{};
    var follower = try NodeType.init(allocator, .{ .id = 2, .peers = &.{ 1, 3 }, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000, .heartbeat_interval_ns = 25_000_000 }, &log_follower, .{ .ptr = &sm_follower, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore_follower.toStorage(), 67890);
    defer follower.deinit();

    // Serialize a config and pretend it's from index 5
    const new_config = types.ClusterConfig{ .servers = &.{ 1, 2, 3, 4 } };
    const config_data = try new_config.serialize(allocator);
    defer allocator.free(config_data);

    // Send AppendEntries with leader_config
    const resp = try follower.handleAppendEntries(.{
        .term = 2, .leader_id = 1,
        .prev_log_index = 0, .prev_log_term = 0,
        .entries = &.{}, .leader_commit = 0,
        .leader_config = config_data, .leader_config_index = 5,
    }, 100_000_000);
    try std.testing.expect(resp.success);

    // Follower should have updated its config
    try std.testing.expect(follower.active_config.contains(4));
    try std.testing.expectEqual(@as(usize, 4), follower.active_config.len());
    try std.testing.expectEqual(@as(u64, 5), follower.active_config_index);
}
