//! State initialisation and transition tests (storage-backed).

const std = @import("std");
const raft = @import("raft");

const types = raft.types;
const mem_storage = raft.memory_storage;
const Log = raft.Log;

const TestSM = struct {
    pub fn apply(_: *@This(), _: u64, _: []const u8) void {}
    pub fn snapshot(_: *@This(), _: anytype) !void {}
    pub fn restore(_: *@This(), _: anytype) !void {}
};

test "node init creates follower with zero state" {
    const allocator = std.testing.allocator;

    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    const storage = mstore.toStorage();

    var log = try Log(mem_storage.MemoryStorage).init(allocator, &mstore, 4);
    defer log.deinit();

    const config = raft.Config{
        .id = 1,
        .peers = &.{ 2, 3 },
    };

    var sm_impl = TestSM{};
    const sm = raft.StateMachine(TestSM){
        .ptr = &sm_impl,
        .applyFn = TestSM.apply,
        .snapshotFn = TestSM.snapshot,
        .restoreFn = TestSM.restore,
    };

    var node = try raft.Node(TestSM, mem_storage.MemoryStorage).init(allocator, config, &log, sm, storage, 12345);
    defer node.deinit();

    try std.testing.expectEqual(types.Role.follower, node.role);
    try std.testing.expectEqual(@as(u64, 0), node.current_term);
    try std.testing.expectEqual(@as(?u64, null), node.voted_for);
}

test "node loads persisted state from storage on init" {
    const allocator = std.testing.allocator;

    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();

    // Pre-set state in storage
    mstore.current_term = 7;
    mstore.voted_for = 3;

    const storage = mstore.toStorage();

    var log = try Log(mem_storage.MemoryStorage).init(allocator, &mstore, 4);
    defer log.deinit();

    const config = raft.Config{
        .id = 1,
        .peers = &.{ 2, 3 },
    };

    var sm_impl = TestSM{};
    const sm = raft.StateMachine(TestSM){
        .ptr = &sm_impl,
        .applyFn = TestSM.apply,
        .snapshotFn = TestSM.snapshot,
        .restoreFn = TestSM.restore,
    };

    var node = try raft.Node(TestSM, mem_storage.MemoryStorage).init(allocator, config, &log, sm, storage, 12345);
    defer node.deinit();

    try std.testing.expectEqual(@as(u64, 7), node.current_term);
    try std.testing.expectEqual(@as(?u64, 3), node.voted_for);
}

test "node starts election and persists term" {
    const allocator = std.testing.allocator;

    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    const storage = mstore.toStorage();

    var log = try Log(mem_storage.MemoryStorage).init(allocator, &mstore, 4);
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

    var node = try raft.Node(TestSM, mem_storage.MemoryStorage).init(allocator, config, &log, sm, storage, 67890);
    defer node.deinit();

    try node.tick(200_000_000);
    try std.testing.expectEqual(types.Role.candidate, node.role);
    try std.testing.expectEqual(@as(u64, 1), node.current_term);

    // Term should be persisted
    try std.testing.expectEqual(@as(u64, 1), mstore.current_term);
    try std.testing.expectEqual(@as(?u64, 1), mstore.voted_for);
}
