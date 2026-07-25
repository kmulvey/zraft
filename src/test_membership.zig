//! Cluster membership change tests (§6).

const std = @import("std");
const raft = @import("raft");

const types = raft.types;
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


test "joint consensus requires quorums in both configs to win election" {
    const allocator = std.testing.allocator;

    // Build a log that already contains a C_old,new (joint) config entry.
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();

    const joint_cfg = types.ClusterConfig{ .servers = &.{ 1, 2, 3, 4, 5 } };
    const joint_data = try joint_cfg.serialize(allocator);
    defer allocator.free(joint_data);
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);
    try buf.append(allocator, 0); // phase byte: joint
    try buf.appendSlice(allocator, joint_data);
    _ = try log.appendEntry(1, .configuration, buf.items);

    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{ 2, 3 }, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000, .heartbeat_interval_ns = 25_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    // Node should have recovered joint config and rebuilt peer tracking.
    try std.testing.expect(node.joint_config != null);
    try std.testing.expect(node.active_config.contains(3));
    try std.testing.expect(node.joint_config.?.contains(5));

    // Make node 1 a candidate for term 2.
    node.role = .candidate;
    node.current_term = 2;
    node.voted_for = 1;
    try node.votes_received.put(allocator, 1, {});

    // Vote from server 2 (in both configs): active quorum reached (2/3),
    // but joint quorum is not yet reached (2/5).
    try node.handleRequestVoteResponse(2, .{ .term = 2, .vote_granted = true });
    try std.testing.expectEqual(raft.Role.candidate, node.role);

    // Vote from server 4 (only in joint config): joint quorum reached (3/5).
    try node.handleRequestVoteResponse(4, .{ .term = 2, .vote_granted = true });
    try std.testing.expectEqual(raft.Role.leader, node.role);
}

test "RequestVote is sent to union of configs during joint consensus" {
    const allocator = std.testing.allocator;

    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();

    const joint_cfg = types.ClusterConfig{ .servers = &.{ 1, 2, 3, 4, 5 } };
    const joint_data = try joint_cfg.serialize(allocator);
    defer allocator.free(joint_data);
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);
    try buf.append(allocator, 0);
    try buf.appendSlice(allocator, joint_data);
    _ = try log.appendEntry(1, .configuration, buf.items);

    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{ 2, 3 }, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000, .heartbeat_interval_ns = 25_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    var recipients = std.ArrayListUnmanaged(u64).empty;
    defer recipients.deinit(allocator);

    const Closure = struct {
        var list: *std.ArrayListUnmanaged(u64) = undefined;
        fn cb(peer: u64, _: raft.RequestVoteRequest) void {
            list.append(std.testing.allocator, peer) catch {};
        }
    };
    Closure.list = &recipients;
    node.send_request_vote = Closure.cb;

    node.role = .candidate;
    node.current_term = 1;
    node.voted_for = 1;
    node.election_start_ns = 0;
    node.election_timeout_ns = 0;
    try node.votes_received.put(allocator, 1, {});
    try node.tick(1);

    // Should have sent to the union {2,3,4,5}.
    try std.testing.expectEqual(@as(usize, 4), recipients.items.len);
    const contains = struct {
        fn call(list: []const u64, id: u64) bool {
            for (list) |x| if (x == id) return true;
            return false;
        }
    }.call;
    try std.testing.expect(contains(recipients.items, 2));
    try std.testing.expect(contains(recipients.items, 3));
    try std.testing.expect(contains(recipients.items, 4));
    try std.testing.expect(contains(recipients.items, 5));
}

test "removed leader steps down when C_new is committed" {
    const allocator = std.testing.allocator;

    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();

    // Joint consensus: C_old={1,2,3}, C_new={2,3,4} (removes server 1).
    const joint_cfg = types.ClusterConfig{ .servers = &.{ 2, 3, 4 } };
    const joint_data = try joint_cfg.serialize(allocator);
    defer allocator.free(joint_data);
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);
    try buf.append(allocator, 0);
    try buf.appendSlice(allocator, joint_data);
    _ = try log.appendEntry(1, .configuration, buf.items);

    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{ 2, 3 }, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000, .heartbeat_interval_ns = 25_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    // Server 1 is the leader and has committed the joint entry.
    node.role = .leader;
    node.current_term = 1;
    node.commit_index = 1;
    node.last_applied = 0;

    // Append the final C_new entry (phase=1) and commit it.
    const final_cfg = types.ClusterConfig{ .servers = &.{ 2, 3, 4 } };
    const final_data = try final_cfg.serialize(allocator);
    defer allocator.free(final_data);
    var final_buf = std.ArrayListUnmanaged(u8).empty;
    defer final_buf.deinit(allocator);
    try final_buf.append(allocator, 1);
    try final_buf.appendSlice(allocator, final_data);
    _ = try log.appendEntry(1, .configuration, final_buf.items);
    node.commit_index = 2;
    try node.applyCommittedEntries();

    try std.testing.expectEqual(raft.Role.follower, node.role);
    try std.testing.expect(node.current_leader == null);
    try std.testing.expect(!node.active_config.contains(1));
}

test "config is rebuilt from persisted log on boot" {
    const allocator = std.testing.allocator;

    // First, build a log containing a completed membership change.
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();

    const joint_cfg = types.ClusterConfig{ .servers = &.{ 1, 2, 3, 4 } };
    const joint_data = try joint_cfg.serialize(allocator);
    defer allocator.free(joint_data);
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);
    try buf.append(allocator, 0);
    try buf.appendSlice(allocator, joint_data);
    _ = try log.appendEntry(1, .configuration, buf.items);

    const final_cfg = types.ClusterConfig{ .servers = &.{ 1, 2, 3, 4 } };
    const final_data = try final_cfg.serialize(allocator);
    defer allocator.free(final_data);
    var final_buf = std.ArrayListUnmanaged(u8).empty;
    defer final_buf.deinit(allocator);
    try final_buf.append(allocator, 1);
    try final_buf.appendSlice(allocator, final_data);
    _ = try log.appendEntry(1, .configuration, final_buf.items);

    // Create a node that was started with the OLD peer list {2,3} but whose
    // persisted log says the cluster is now {1,2,3,4}.
    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{ 2, 3 }, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000, .heartbeat_interval_ns = 25_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    try std.testing.expect(node.active_config.contains(4));
    try std.testing.expectEqual(@as(usize, 4), node.active_config.len());
    try std.testing.expectEqual(@as(u64, 2), node.active_config_index);
    try std.testing.expect(node.joint_config == null);
}

test "becomeLeader appends C_new when joint config is already committed" {
    const allocator = std.testing.allocator;

    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();

    const joint_cfg = types.ClusterConfig{ .servers = &.{ 1, 2, 3, 4 } };
    const joint_data = try joint_cfg.serialize(allocator);
    defer allocator.free(joint_data);
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);
    try buf.append(allocator, 0);
    try buf.appendSlice(allocator, joint_data);
    _ = try log.appendEntry(1, .configuration, buf.items);

    var sm_impl = TestSM{};
    var node = try NodeType.init(allocator, .{ .id = 1, .peers = &.{ 2, 3 }, .election_timeout_min_ns = 50_000_000, .election_timeout_max_ns = 100_000_000, .heartbeat_interval_ns = 25_000_000 }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    // Joint entry is already committed; leader has not yet appended C_new.
    node.role = .candidate;
    node.current_term = 2;
    node.voted_for = 1;
    node.commit_index = 1;
    try node.votes_received.put(allocator, 1, {});

    // Grant votes from 2 and 3 (in both configs) to win the election.
    try node.handleRequestVoteResponse(2, .{ .term = 2, .vote_granted = true });
    try node.handleRequestVoteResponse(3, .{ .term = 2, .vote_granted = true });
    try std.testing.expectEqual(raft.Role.leader, node.role);

    // A no-op plus a final C_new config entry should have been appended.
    try std.testing.expect(log.lastIndex() >= 2);
    const last_entry = log.get(log.lastIndex()).?;
    try std.testing.expectEqual(types.EntryType.configuration, last_entry.entry_type);
    try std.testing.expectEqual(@as(u8, 1), last_entry.data[0]);
}
