//! File-backed persistent storage tests.

const std = @import("std");
const raft = @import("raft");

const FileStorage = raft.FileStorage;

fn cleanTestDir(path: []const u8) void {
    std.Io.Dir.cwd().deleteTree(std.testing.io, path) catch {};
}

test "FileStorage init creates directories and files" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage";
    cleanTestDir(test_dir);
    defer cleanTestDir(test_dir);

    var st = try FileStorage.init(allocator, test_dir, 1);
    defer st.deinit();
    try std.testing.expectEqual(@as(raft.Term, 0), st.loadTerm());
    try std.testing.expectEqual(@as(?raft.ServerId, null), st.loadVotedFor());
    try std.testing.expectEqual(@as(raft.LogIndex, 0), st.loadLastLogIndex());
}

test "FileStorage round-trip term and votedFor" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage2";
    cleanTestDir(test_dir);
    defer cleanTestDir(test_dir);
    {
        var st = try FileStorage.init(allocator, test_dir, 2);
        defer st.deinit();
        try st.storeTerm(42);
        try st.storeVotedFor(7);
        try std.testing.expectEqual(@as(raft.Term, 42), st.loadTerm());
        try std.testing.expectEqual(@as(?raft.ServerId, 7), st.loadVotedFor());
    }
    {
        var st = try FileStorage.init(allocator, test_dir, 2);
        defer st.deinit();
        try std.testing.expectEqual(@as(raft.Term, 42), st.loadTerm());
        try std.testing.expectEqual(@as(?raft.ServerId, 7), st.loadVotedFor());
    }
}

test "FileStorage append and read log entry" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage3";
    cleanTestDir(test_dir);
    defer cleanTestDir(test_dir);

    var st = try FileStorage.init(allocator, test_dir, 3);
    defer st.deinit();
    const data = try allocator.dupe(u8, "hello-raft");
    defer allocator.free(data);
    try st.appendLogEntry(.{ .term = 1, .index = 1, .entry_type = .command, .data = data });
    try std.testing.expectEqual(@as(raft.LogIndex, 1), st.loadLastLogIndex());

    const loaded = st.loadLogEntry(1, allocator);
    try std.testing.expect(loaded != null);
    var entry = loaded.?;
    defer entry.deinit(allocator);
    try std.testing.expectEqualStrings("hello-raft", entry.data);
    try std.testing.expectEqual(@as(raft.Term, 1), entry.term);
}

test "FileStorage tail truncate keeps prefix" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage4";
    cleanTestDir(test_dir);
    defer cleanTestDir(test_dir);

    var st = try FileStorage.init(allocator, test_dir, 4);
    defer st.deinit();

    try st.appendLogEntry(.{ .term = 1, .index = 1, .entry_type = .command, .data = "a" });
    try st.appendLogEntry(.{ .term = 1, .index = 2, .entry_type = .command, .data = "b" });
    try st.appendLogEntry(.{ .term = 1, .index = 3, .entry_type = .command, .data = "c" });

    try st.truncateLog(1);
    try std.testing.expectEqual(@as(raft.LogIndex, 1), st.loadLastLogIndex());
    const e1 = st.loadLogEntry(1, allocator);
    try std.testing.expect(e1 != null);
    if (e1) |e| { var entry = e; defer entry.deinit(allocator); }
    try std.testing.expect(st.loadLogEntry(2, allocator) == null);
}

test "FileStorage prefix drop keeps suffix" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage5";
    cleanTestDir(test_dir);
    defer cleanTestDir(test_dir);

    var st = try FileStorage.init(allocator, test_dir, 5);
    defer st.deinit();

    try st.appendLogEntry(.{ .term = 1, .index = 1, .entry_type = .command, .data = "a" });
    try st.appendLogEntry(.{ .term = 1, .index = 2, .entry_type = .command, .data = "b" });
    try st.appendLogEntry(.{ .term = 1, .index = 3, .entry_type = .command, .data = "c" });

    try st.dropLogPrefix(2);
    try std.testing.expectEqual(@as(raft.LogIndex, 3), st.loadLastLogIndex());
    try std.testing.expect(st.loadLogEntry(1, allocator) == null);
    try std.testing.expect(st.loadLogEntry(2, allocator) == null);
    const loaded = st.loadLogEntry(3, allocator);
    try std.testing.expect(loaded != null);
    var entry = loaded.?;
    defer entry.deinit(allocator);
    try std.testing.expectEqualStrings("c", entry.data);
}

test "FileStorage snapshot round-trip and reload" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage6";
    cleanTestDir(test_dir);
    defer cleanTestDir(test_dir);

    {
        var st = try FileStorage.init(allocator, test_dir, 6);
        defer st.deinit();
        try st.storeSnapshot(10, 2, "snap-data");
        try std.testing.expectEqual(@as(raft.LogIndex, 10), st.loadLastSnapshotIndex());
        try std.testing.expectEqual(@as(raft.Term, 2), st.loadLastSnapshotTerm());
    }
    {
        var st = try FileStorage.init(allocator, test_dir, 6);
        defer st.deinit();
        try std.testing.expectEqual(@as(raft.LogIndex, 10), st.loadLastSnapshotIndex());
        try std.testing.expectEqual(@as(raft.Term, 2), st.loadLastSnapshotTerm());
        const snap = st.loadSnapshot(allocator);
        try std.testing.expect(snap != null);
        var snapshot = snap.?;
        defer snapshot.deinit(allocator);
        try std.testing.expectEqualStrings("snap-data", snapshot.data);
    }
}

test "FileStorage log reload after re-init" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage7";
    cleanTestDir(test_dir);
    defer cleanTestDir(test_dir);

    {
        var st = try FileStorage.init(allocator, test_dir, 7);
        defer st.deinit();
        try st.appendLogEntry(.{ .term = 1, .index = 1, .entry_type = .command, .data = "x" });
        try st.appendLogEntry(.{ .term = 1, .index = 2, .entry_type = .command, .data = "y" });
    }
    {
        var st = try FileStorage.init(allocator, test_dir, 7);
        defer st.deinit();
        try std.testing.expectEqual(@as(raft.LogIndex, 2), st.loadLastLogIndex());
        const e2 = st.loadLogEntry(2, allocator);
        try std.testing.expect(e2 != null);
        var entry = e2.?;
        defer entry.deinit(allocator);
        try std.testing.expectEqualStrings("y", entry.data);
    }
}

test "FileStorage prefix drop then reload starts after snapshot" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage8";
    cleanTestDir(test_dir);
    defer cleanTestDir(test_dir);

    {
        var st = try FileStorage.init(allocator, test_dir, 8);
        defer st.deinit();
        try st.appendLogEntry(.{ .term = 1, .index = 1, .entry_type = .command, .data = "old" });
        try st.appendLogEntry(.{ .term = 2, .index = 2, .entry_type = .command, .data = "new" });
        try st.dropLogPrefix(1);
    }
    {
        var st = try FileStorage.init(allocator, test_dir, 8);
        defer st.deinit();
        try std.testing.expect(st.loadLogEntry(1, allocator) == null);
        const e2 = st.loadLogEntry(2, allocator);
        try std.testing.expect(e2 != null);
        var entry = e2.?;
        defer entry.deinit(allocator);
        try std.testing.expectEqualStrings("new", entry.data);
    }
}
