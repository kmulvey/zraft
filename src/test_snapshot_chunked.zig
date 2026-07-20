//! Chunked snapshot transfer tests (§7).

const std = @import("std");
const raft = @import("raft");

const mem_storage = raft.memory_storage;
const Log = raft.Log;
const NodeType = raft.Node;
const LogType = Log;

const TestSM = struct {
    data: std.ArrayListUnmanaged(u8) = .empty,

    pub fn apply(self: *@This(), _: u64, data: []const u8) void {
        self.data.appendSlice(std.testing.allocator, data) catch {};
    }
    pub fn snapshot(self: *@This(), allocator: std.mem.Allocator) ![]u8 {
        return allocator.dupe(u8, self.data.items);
    }
    pub fn restore(self: *@This(), data: []const u8) !void {
        self.data.clearRetainingCapacity();
        try self.data.appendSlice(std.testing.allocator, data);
    }
};

test "chunked snapshot accumulate and restore" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType(mem_storage.MemoryStorage).init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    defer sm_impl.data.deinit(allocator);
    var node = try NodeType(TestSM, mem_storage.MemoryStorage).init(allocator, .{ .id = 2, .peers = &.{1, 3} }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    const snap_data = "Hello, this is a snapshot that is long enough to be split into multiple chunks!";
    const chunk_size: u64 = 20;
    var offset: u64 = 0;
    var last_idx: u64 = 0;
    var last_term: u64 = 0;
    var done = false;

    while (!done) {
        const end = @min(offset + chunk_size, snap_data.len);
        const chunk = snap_data[offset..end];
        done = end >= snap_data.len;
        const resp = try node.handleInstallSnapshot(.{
            .term = 1,
            .leader_id = 1,
            .last_included_index = 5,
            .last_included_term = 2,
            .offset = offset,
            .data = chunk,
            .done = done,
        });
        try std.testing.expect(resp.success);
        last_idx = 5;
        last_term = 2;
        offset = end;
    }

    // After all chunks, the snapshot should be applied
    try std.testing.expectEqual(@as(u64, 5), node.snapshot_index);
    try std.testing.expectEqual(@as(u64, 2), node.snapshot_term);
    try std.testing.expectEqual(@as(u64, 5), node.last_applied);
}

test "chunked snapshot out-of-order chunk ignored" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType(mem_storage.MemoryStorage).init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    defer sm_impl.data.deinit(allocator);
    var node = try NodeType(TestSM, mem_storage.MemoryStorage).init(allocator, .{ .id = 2, .peers = &.{1, 3} }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    // Send chunk at offset 10 first (out of order) — should be ignored
    const resp1 = try node.handleInstallSnapshot(.{
        .term = 1, .leader_id = 1, .last_included_index = 5, .last_included_term = 2,
        .offset = 10, .data = "later data", .done = false,
    });
    try std.testing.expect(resp1.success);
    // Buffer should still be empty
    try std.testing.expectEqual(@as(u64, 0), node.snapshot_recv_offset);

    // Send chunk at offset 0 — starts new receive
    const resp2 = try node.handleInstallSnapshot(.{
        .term = 1, .leader_id = 1, .last_included_index = 5, .last_included_term = 2,
        .offset = 0, .data = "first chunk", .done = false,
    });
    try std.testing.expect(resp2.success);
    try std.testing.expectEqual(@as(u64, 11), node.snapshot_recv_offset);
}

test "chunked snapshot resend from leader restarts receive" {
    const allocator = std.testing.allocator;
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType(mem_storage.MemoryStorage).init(allocator, &mstore, 4);
    defer log.deinit();
    var sm_impl = TestSM{};
    defer sm_impl.data.deinit(allocator);
    var node = try NodeType(TestSM, mem_storage.MemoryStorage).init(allocator, .{ .id = 2, .peers = &.{1, 3} }, &log, .{ .ptr = &sm_impl, .applyFn = TestSM.apply, .snapshotFn = TestSM.snapshot, .restoreFn = TestSM.restore }, mstore.toStorage(), 12345);
    defer node.deinit();

    // Send first chunk
    _ = try node.handleInstallSnapshot(.{
        .term = 1, .leader_id = 1, .last_included_index = 5, .last_included_term = 2,
        .offset = 0, .data = "first part ", .done = false,
    });
    try std.testing.expectEqual(@as(u64, 11), node.snapshot_recv_offset);

    // Leader resends from offset 0 (e.g., after timeout) — should restart
    _ = try node.handleInstallSnapshot(.{
        .term = 1, .leader_id = 1, .last_included_index = 5, .last_included_term = 2,
        .offset = 0, .data = "restarted ", .done = true,
    });

    // After done with the restarted snapshot, it should be applied
    try std.testing.expectEqual(@as(u64, 5), node.snapshot_index);
    // The snapshot should contain "restarted " (10 bytes)
    try std.testing.expectEqual(@as(u64, 5), node.last_applied);
}
