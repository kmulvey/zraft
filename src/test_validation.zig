//! Config validation tests.

const std = @import("std");
const raft = @import("raft");

const Config = raft.Config;

test "valid config passes validation" {
    const cfg = Config{
        .id = 1,
        .peers = &.{ 2, 3 },
        .election_timeout_min_ns = 150_000_000,
        .election_timeout_max_ns = 300_000_000,
        .heartbeat_interval_ns = 50_000_000,
        .initial_log_capacity = 1024,
    };
    try cfg.validate();
}

test "election_timeout_min_ns zero rejected" {
    const cfg = Config{
        .id = 1,
        .peers = &.{2},
        .election_timeout_min_ns = 0,
    };
    try std.testing.expectError(error.ElectionTimeoutMinZero, cfg.validate());
}

test "election_timeout_max_ns less than min rejected" {
    const cfg = Config{
        .id = 1,
        .peers = &.{2},
        .election_timeout_min_ns = 200_000_000,
        .election_timeout_max_ns = 100_000_000,
    };
    try std.testing.expectError(error.ElectionTimeoutMaxLessThanMin, cfg.validate());
}

test "heartbeat_interval_ns zero rejected" {
    const cfg = Config{
        .id = 1,
        .peers = &.{2},
        .heartbeat_interval_ns = 0,
    };
    try std.testing.expectError(error.HeartbeatIntervalZero, cfg.validate());
}

test "heartbeat not less than election timeout rejected" {
    const cfg = Config{
        .id = 1,
        .peers = &.{2},
        .election_timeout_min_ns = 50_000_000,
        .heartbeat_interval_ns = 50_000_000,
    };
    try std.testing.expectError(error.HeartbeatNotLessThanElectionTimeout, cfg.validate());
}

test "initial_log_capacity zero rejected" {
    const cfg = Config{
        .id = 1,
        .peers = &.{2},
        .initial_log_capacity = 0,
    };
    try std.testing.expectError(error.InitialLogCapacityZero, cfg.validate());
}

test "peer equals self rejected" {
    const cfg = Config{
        .id = 1,
        .peers = &.{ 2, 1 },
    };
    try std.testing.expectError(error.PeerEqualsSelf, cfg.validate());
}

test "duplicate peers rejected" {
    const cfg = Config{
        .id = 1,
        .peers = &.{ 2, 3, 2 },
    };
    try std.testing.expectError(error.DuplicatePeer, cfg.validate());
}

test "node init validates config and propagates error" {
    const allocator = std.testing.allocator;
    const raft_mod = @import("raft");
    const mem_storage = raft_mod.memory_storage;
    const Log = raft_mod.Log;

    const TestSM = struct {
        pub fn apply(_: *@This(), _: u64, _: []const u8) void {}
        pub fn snapshot(_: *@This(), _: std.mem.Allocator) ![]u8 { return &.{}; }
        pub fn restore(_: *@This(), _: []const u8) !void {}
    };

    const NodeType = raft_mod.Node(TestSM, mem_storage.MemoryStorage);
    const LogType = Log(mem_storage.MemoryStorage);

    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};

    // Invalid config (heartbeat=0) should cause init to fail
    const bad_cfg = Config{
        .id = 1,
        .peers = &.{2},
        .heartbeat_interval_ns = 0,
    };
    const result = NodeType.init(allocator, bad_cfg, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    try std.testing.expectError(error.HeartbeatIntervalZero, result);
}
