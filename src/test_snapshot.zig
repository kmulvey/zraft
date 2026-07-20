//! Snapshot and log compaction tests.

const std = @import("std");
const raft = @import("raft");

const mem_storage = raft.memory_storage;
const Log = raft.Log;
const StateMachine = raft.StateMachine;

const KvSM = struct {
    data: std.StringHashMap([]const u8),
    allocator: std.mem.Allocator,

    pub fn apply(_: *@This(), _: u64, _: []const u8) void {}

    pub fn snapshot(ptr: *@This(), allocator: std.mem.Allocator) ![]u8 {
        // Simple binary format: key_count(u32) + for each: key_len(u32)+key+val_len(u32)+val
        var buf: std.ArrayListUnmanaged(u8) = .empty;
        defer buf.deinit(allocator);

        var it = ptr.data.iterator();
        var count: u32 = 0;
        var keys_values: std.ArrayListUnmanaged(struct { k: []const u8, v: []const u8 }) = .empty;
        defer keys_values.deinit(allocator);
        while (it.next()) |entry| {
            try keys_values.append(allocator, .{ .k = entry.key_ptr.*, .v = entry.value_ptr.* });
            count += 1;
        }

        // Write count as 4 bytes LE
        var count_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &count_bytes, count, .little);
        try buf.appendSlice(allocator, &count_bytes);

        for (keys_values.items) |kv| {
            var kl: [4]u8 = undefined;
            std.mem.writeInt(u32, &kl, @as(u32, @intCast(kv.k.len)), .little);
            try buf.appendSlice(allocator, &kl);
            try buf.appendSlice(allocator, kv.k);

            var vl: [4]u8 = undefined;
            std.mem.writeInt(u32, &vl, @as(u32, @intCast(kv.v.len)), .little);
            try buf.appendSlice(allocator, &vl);
            try buf.appendSlice(allocator, kv.v);
        }

        return buf.toOwnedSlice(allocator);
    }

    pub fn restore(ptr: *@This(), data: []const u8) !void {
        ptr.data.clearRetainingCapacity();
        if (data.len < 4) return;
        const count = std.mem.readInt(u32, data[0..4], .little);
        var pos: usize = 4;
        var i: u32 = 0;
        while (i < count and pos + 8 <= data.len) : (i += 1) {
            const kl = std.mem.readInt(u32, data[pos..][0..4], .little);
            pos += 4;
            if (pos + kl > data.len) break;
            const key = try ptr.allocator.dupe(u8, data[pos..][0..kl]);
            pos += kl;
            if (pos + 4 > data.len) break;
            const vl = std.mem.readInt(u32, data[pos..][0..4], .little);
            pos += 4;
            if (pos + vl > data.len) break;
            const val = try ptr.allocator.dupe(u8, data[pos..][0..vl]);
            pos += vl;
            try ptr.data.put(key, val);
        }
    }
};

test "take snapshot compacts log" {
    const allocator = std.testing.allocator;

    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try Log(mem_storage.MemoryStorage).init(allocator, &mstore, 4);
    defer log.deinit();

    _ = try log.append(1, "entry-1");
    _ = try log.append(1, "entry-2");
    _ = try log.append(1, "entry-3");
    try std.testing.expectEqual(@as(u64, 3), log.lastIndex());

    var sm_impl = KvSM{ .data = std.StringHashMap([]const u8).init(allocator), .allocator = allocator };
    defer sm_impl.data.deinit();
    const sm = StateMachine(KvSM){ .ptr = &sm_impl, .applyFn = KvSM.apply, .snapshotFn = KvSM.snapshot, .restoreFn = KvSM.restore };

    var node = try raft.Node(KvSM, mem_storage.MemoryStorage).init(allocator, .{ .id = 1, .peers = &.{} }, &log, sm, mstore.toStorage(), 12345);
    defer node.deinit();
    node.role = .leader;
    node.current_term = 1;
    node.last_applied = 3;
    node.commit_index = 3;

    try node.takeSnapshot();

    // Log should be compacted
    try std.testing.expectEqual(@as(u64, 3), log.lastIndex());
    try std.testing.expectEqual(@as(usize, 1), log.len);
    try std.testing.expectEqual(@as(u64, 1), log.termAt(3));

    // Snapshot persisted
    const snap = mstore.snapshot orelse @panic("no snapshot stored");
    try std.testing.expectEqual(@as(u64, 3), snap.last_included_index);
    try std.testing.expectEqual(@as(u64, 1), snap.last_included_term);
    try std.testing.expect(snap.data.len > 0);
}

test "install snapshot restores state machine and compacts log" {
    const allocator = std.testing.allocator;

    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try Log(mem_storage.MemoryStorage).init(allocator, &mstore, 4);
    defer log.deinit();

    _ = try log.append(1, "old-data");

    var sm_impl = KvSM{ .data = std.StringHashMap([]const u8).init(allocator), .allocator = allocator };
    defer sm_impl.data.deinit();
    const sm = StateMachine(KvSM){ .ptr = &sm_impl, .applyFn = KvSM.apply, .snapshotFn = KvSM.snapshot, .restoreFn = KvSM.restore };

    var node = try raft.Node(KvSM, mem_storage.MemoryStorage).init(allocator, .{ .id = 2, .peers = &.{1} }, &log, sm, mstore.toStorage(), 67890);
    defer node.deinit();
    node.current_term = 1;

    const snap_data = "snapshot-state-data";
    const resp = try node.handleInstallSnapshot(.{
        .term = 1, .leader_id = 1,
        .last_included_index = 5, .last_included_term = 1,
        .offset = 0, .data = snap_data, .done = true,
    });

    try std.testing.expect(resp.success);
    try std.testing.expectEqual(@as(u64, 5), node.snapshot_index);
    try std.testing.expectEqual(@as(u64, 1), node.snapshot_term);
    try std.testing.expectEqual(@as(u64, 5), log.lastIndex());
    try std.testing.expectEqual(@as(usize, 1), log.len);
}
