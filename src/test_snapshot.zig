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
        // Free any existing entries before replacing state.
        var it = ptr.data.iterator();
        while (it.next()) |entry| {
            ptr.allocator.free(entry.key_ptr.*);
            ptr.allocator.free(entry.value_ptr.*);
        }
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
    }, 1_000_000);

    try std.testing.expect(resp.success);
    try std.testing.expectEqual(@as(u64, 5), node.snapshot_index);
    try std.testing.expectEqual(@as(u64, 1), node.snapshot_term);
    try std.testing.expectEqual(@as(u64, 5), log.lastIndex());
    try std.testing.expectEqual(@as(usize, 1), log.len);
}

test "append after snapshot preserves correct indexing" {
    const allocator = std.testing.allocator;

    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try Log(mem_storage.MemoryStorage).init(allocator, &mstore, 4);
    defer log.deinit();

    _ = try log.append(1, "e1");
    _ = try log.append(1, "e2");
    _ = try log.append(1, "e3");

    try log.replaceWithSnapshot(3, 1);
    try std.testing.expectEqual(@as(u64, 3), log.lastIndex());
    try std.testing.expectEqual(@as(usize, 1), log.len);

    const idx4 = try log.append(2, "e4");
    try std.testing.expectEqual(@as(u64, 4), idx4);
    try std.testing.expectEqual(@as(u64, 4), log.lastIndex());
    try std.testing.expectEqual(@as(u64, 2), log.termAt(4));
    try std.testing.expect(log.get(4) != null);

    const idx5 = try log.append(2, "e5");
    try std.testing.expectEqual(@as(u64, 5), idx5);
    try std.testing.expectEqual(@as(u64, 5), log.lastIndex());
    try std.testing.expectEqual(@as(u64, 2), log.termAt(5));
}

test "single node commits after snapshot" {
    const allocator = std.testing.allocator;

    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try Log(mem_storage.MemoryStorage).init(allocator, &mstore, 4);
    defer log.deinit();

    var sm_impl = KvSM{ .data = std.StringHashMap([]const u8).init(allocator), .allocator = allocator };
    defer sm_impl.data.deinit();
    const sm = StateMachine(KvSM){ .ptr = &sm_impl, .applyFn = KvSM.apply, .snapshotFn = KvSM.snapshot, .restoreFn = KvSM.restore };

    var node = try raft.Node(KvSM, mem_storage.MemoryStorage).init(allocator, .{ .id = 1, .peers = &.{}, .election_timeout_min_ns = 10_000_000, .election_timeout_max_ns = 20_000_000, .heartbeat_interval_ns = 5_000_000 }, &log, sm, mstore.toStorage(), 12345);
    defer node.deinit();

    // Become leader; no-op commits.
    try node.tick(30_000_000);
    try std.testing.expectEqual(raft.Role.leader, node.role);
    try std.testing.expect(node.commit_index > 0);

    const before_snap = try node.clientAppend("pre-snap");
    try std.testing.expect(before_snap > 0);
    try std.testing.expectEqual(before_snap, node.commit_index);

    node.last_applied = node.commit_index;
    try node.takeSnapshot();
    try std.testing.expectEqual(before_snap, log.lastIndex());
    try std.testing.expectEqual(@as(usize, 1), log.len);

    const after_snap = try node.clientAppend("post-snap");
    try std.testing.expectEqual(before_snap + 1, after_snap);
    try std.testing.expectEqual(after_snap, node.commit_index);
}

test "stale snapshot install is ignored" {
    const allocator = std.testing.allocator;

    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try Log(mem_storage.MemoryStorage).init(allocator, &mstore, 4);
    defer log.deinit();

    var sm_impl = KvSM{ .data = std.StringHashMap([]const u8).init(allocator), .allocator = allocator };
    defer sm_impl.data.deinit();
    const sm = StateMachine(KvSM){ .ptr = &sm_impl, .applyFn = KvSM.apply, .snapshotFn = KvSM.snapshot, .restoreFn = KvSM.restore };

    var node = try raft.Node(KvSM, mem_storage.MemoryStorage).init(allocator, .{ .id = 2, .peers = &.{1} }, &log, sm, mstore.toStorage(), 67890);
    defer node.deinit();
    node.current_term = 1;

    // Install snapshot at index 5.
    var resp = try node.handleInstallSnapshot(.{
        .term = 1, .leader_id = 1,
        .last_included_index = 5, .last_included_term = 1,
        .offset = 0, .data = "snap-5", .done = true,
    }, 1_000_000);
    try std.testing.expect(resp.success);
    try std.testing.expectEqual(@as(u64, 5), node.snapshot_index);

    // A later snapshot at index 3 must not regress the state.
    resp = try node.handleInstallSnapshot(.{
        .term = 1, .leader_id = 1,
        .last_included_index = 3, .last_included_term = 1,
        .offset = 0, .data = "snap-3", .done = true,
    }, 1_000_000);
    try std.testing.expect(resp.success);
    try std.testing.expectEqual(@as(u64, 5), node.snapshot_index);
}


test "boot restores state machine from persisted snapshot" {
    const allocator = std.testing.allocator;

    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try Log(mem_storage.MemoryStorage).init(allocator, &mstore, 4);
    defer log.deinit();

    var sm_impl = KvSM{ .data = std.StringHashMap([]const u8).init(allocator), .allocator = allocator };
    defer sm_impl.data.deinit();
    defer {
        var it = sm_impl.data.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
    }
    const sm = StateMachine(KvSM){ .ptr = &sm_impl, .applyFn = KvSM.apply, .snapshotFn = KvSM.snapshot, .restoreFn = KvSM.restore };

    try sm_impl.data.put(try allocator.dupe(u8, "k"), try allocator.dupe(u8, "v"));
    const snap_data = try sm_impl.snapshot(allocator);
    defer allocator.free(snap_data);
    try mstore.storeSnapshot(3, 1, snap_data);

    var node = try raft.Node(KvSM, mem_storage.MemoryStorage).init(allocator, .{ .id = 1, .peers = &.{} }, &log, sm, mstore.toStorage(), 12345);
    defer node.deinit();

    try std.testing.expectEqual(@as(u64, 3), node.snapshot_index);
    try std.testing.expectEqual(@as(u64, 3), node.last_applied);
    try std.testing.expectEqual(@as(u64, 3), node.commitIndex());
    try std.testing.expectEqualStrings("v", sm_impl.data.get("k").?);
}
