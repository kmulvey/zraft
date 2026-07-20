//! File-backed persistent storage for Raft.
//!
//! On-disk layout for a server with ID `my_id`:
//!
//!   <base_dir>/raft-<my_id>/
//!     metadata.bin   — fixed-size: currentTerm (u8×8) + votedFor (u8×8, all-0xFF = null)
//!     wal.bin        — append-only log entries
//!     snapshot.bin   — snapshot: last_included_index (u64) + last_included_term (u64) + data_len (u64) + data

const std = @import("std");
const fs = std.fs;
const mem = std.mem;
const types = @import("types.zig");
const storage = @import("storage.zig");

const ServerId = types.ServerId;
const Term = types.Term;
const LogIndex = types.LogIndex;
const LogEntryOwned = storage.LogEntryOwned;
const SnapshotData = storage.SnapshotData;

const NULL_VOTED_FOR: u64 = 0xFFFF_FFFF_FFFF_FFFF;

pub const FileStorage = struct {
    allocator: std.mem.Allocator,
    dir: fs.Dir,
    server_id: ServerId,
    current_term: Term,
    voted_for: ?ServerId,
    last_log_index: LogIndex,
    max_wal_bytes: usize,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, base_dir: []const u8, server_id: ServerId, max_wal_bytes: usize) !Self {
        var buf: [256]u8 = undefined;
        const dirname = std.fmt.bufPrint(&buf, "raft-{}", .{server_id}) catch "raft";
        fs.cwd().makePath(base_dir) catch {};
        const base = try fs.cwd().openDir(base_dir, .{});
        errdefer base.close();
        base.makeDir(dirname) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => |e| return e,
        };
        const dir = try base.openDir(dirname, .{});
        errdefer dir.close();

        var self = Self{
            .allocator = allocator,
            .dir = dir,
            .server_id = server_id,
            .current_term = 0,
            .voted_for = null,
            .last_log_index = 0,
            .max_wal_bytes = max_wal_bytes,
        };
        try self.loadMetadata();
        try self.rebuildLastLogIndex();
        return self;
    }

    pub fn deinit(self: *Self) void { self.dir.close(); self.* = undefined; }

    // ------------------------------------------------------------------
    // Public interface
    // ------------------------------------------------------------------

    pub fn loadTerm(ptr: *Self) Term { return ptr.current_term; }
    pub fn storeTerm(ptr: *Self, term: Term) !void { ptr.current_term = term; try ptr.writeMetadata(); }
    pub fn loadVotedFor(ptr: *Self) ?ServerId { return ptr.voted_for; }
    pub fn storeVotedFor(ptr: *Self, voted_for: ?ServerId) !void { ptr.voted_for = voted_for; try ptr.writeMetadata(); }
    pub fn loadLastLogIndex(ptr: *Self) LogIndex { return ptr.last_log_index; }

    pub fn loadLogEntry(ptr: *Self, index: LogIndex, allocator: std.mem.Allocator) ?LogEntryOwned {
        return ptr.readEntry(index, allocator) catch null;
    }

    pub fn appendLogEntry(ptr: *Self, entry: LogEntryOwned) !void {
        try ptr.writeEntry(&entry);
        ptr.last_log_index = @max(ptr.last_log_index, entry.index);
    }

    pub fn truncateLog(ptr: *Self, last_kept_index: LogIndex) !void {
        try ptr.compactWal(last_kept_index);
        ptr.last_log_index = last_kept_index;
    }

    pub fn sync(ptr: *Self) !void { try ptr.dir.sync(); }

    // ------------------------------------------------------------------
    // Snapshot
    // ------------------------------------------------------------------

    pub fn storeSnapshot(ptr: *Self, last_included_index: LogIndex, last_included_term: Term, data: []const u8) !void {
        var buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&buf, "snapshot.bin", .{});
        const tmp_path = try std.fmt.bufPrint(&buf, "snapshot.tmp", .{});

        const tmp_file = try ptr.dir.createFile(tmp_path, .{ .truncate = true });
        defer tmp_file.close();

        var header: [24]u8 = undefined;
        mem.writeInt(u64, header[0..8], last_included_index, .little);
        mem.writeInt(u64, header[8..16], last_included_term, .little);
        mem.writeInt(u64, header[16..24], @as(u64, @intCast(data.len)), .little);
        try tmp_file.writeAll(&header);
        if (data.len > 0) try tmp_file.writeAll(data);
        try tmp_file.sync();
        try ptr.dir.rename(tmp_path, path);
    }

    pub fn loadSnapshot(ptr: *Self, allocator: std.mem.Allocator) ?SnapshotData {
        var buf: [256]u8 = undefined;
        const path = std.fmt.bufPrint(&buf, "snapshot.bin", .{}) catch return null;
        const file = ptr.dir.openFile(path, .{}) catch return null;
        defer file.close();

        var header: [24]u8 = undefined;
        const n = file.readAll(&header) catch return null;
        if (n < 24) return null;

        const last_index = mem.readInt(u64, header[0..8], .little);
        const last_term = mem.readInt(u64, header[8..16], .little);
        const data_len: usize = @as(usize, @intCast(mem.readInt(u64, header[16..24], .little)));

        const data = if (data_len > 0) allocator.alloc(u8, data_len) catch return null else &.{};
        if (data_len > 0) {
            file.readAll(data) catch { if (data.len > 0) allocator.free(data); return null; };
        }
        return SnapshotData{ .last_included_index = last_index, .last_included_term = last_term, .data = data };
    }

    pub fn loadLastSnapshotIndex(ptr: *Self) LogIndex {
        var buf: [256]u8 = undefined;
        const path = std.fmt.bufPrint(&buf, "snapshot.bin", .{}) catch return 0;
        const file = ptr.dir.openFile(path, .{}) catch return 0;
        defer file.close();
        var header: [8]u8 = undefined;
        if ((file.readAll(&header) catch 0) < 8) return 0;
        return mem.readInt(u64, &header, .little);
    }

    pub fn loadLastSnapshotTerm(ptr: *Self) Term {
        var buf: [256]u8 = undefined;
        const path = std.fmt.bufPrint(&buf, "snapshot.bin", .{}) catch return 0;
        const file = ptr.dir.openFile(path, .{}) catch return 0;
        defer file.close();
        var header: [16]u8 = undefined;
        if ((file.readAll(&header) catch 0) < 16) return 0;
        return mem.readInt(u64, header[8..16], .little);
    }

    // ------------------------------------------------------------------
    // Internal: metadata
    // ------------------------------------------------------------------

    fn writeMetadata(self: *Self) !void {
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "metadata.bin", .{});
        var file = try self.dir.createFile(path, .{ .truncate = true });
        defer file.close();
        var data: [16]u8 = undefined;
        mem.writeInt(u64, data[0..8], self.current_term, .little);
        mem.writeInt(u64, data[8..16], self.voted_for orelse NULL_VOTED_FOR, .little);
        try file.writeAll(&data);
        try file.sync();
    }

    fn loadMetadata(self: *Self) !void {
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "metadata.bin", .{});
        const file = self.dir.openFile(path, .{}) catch |err| switch (err) {
            error.FileNotFound => return,
            else => |e| return e,
        };
        defer file.close();
        var data: [16]u8 = undefined;
        if ((try file.readAll(&data)) < 16) return;
        self.current_term = mem.readInt(u64, data[0..8], .little);
        const vf = mem.readInt(u64, data[8..16], .little);
        self.voted_for = if (vf == NULL_VOTED_FOR) null else vf;
    }

    // ------------------------------------------------------------------
    // Internal: WAL
    // ------------------------------------------------------------------

    fn writeEntry(self: *Self, entry: *const LogEntryOwned) !void {
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "wal.bin", .{});
        const file = try self.dir.createFile(path, .{ .truncate = false });
        defer file.close();
        try file.seekFromEnd(0);
        var header: [21]u8 = undefined;
        mem.writeInt(u64, header[0..8], entry.index, .little);
        mem.writeInt(u64, header[8..16], entry.term, .little);
        header[16] = @intFromEnum(entry.entry_type);
        mem.writeInt(u32, header[17..21], @as(u32, @intCast(entry.data.len)), .little);
        try file.writeAll(&header);
        if (entry.data.len > 0) try file.writeAll(entry.data);
        try file.sync();
    }

    fn readEntry(self: *Self, index: LogIndex, allocator: std.mem.Allocator) !?LogEntryOwned {
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "wal.bin", .{});
        const file = try self.dir.openFile(path, .{});
        defer file.close();

        const file_size = try file.getEndPos();
        var pos: u64 = 0;
        while (pos < file_size) {
            var header: [21]u8 = undefined;
            if ((try file.preadAll(&header, pos)) < 21) return null;
            const entry_index = mem.readInt(u64, header[0..8], .little);
            const entry_term = mem.readInt(u64, header[8..16], .little);
            const entry_type = @as(types.EntryType, @enumFromInt(header[16]));
            const data_len: usize = @as(usize, @intCast(mem.readInt(u32, header[17..21], .little)));

            if (entry_index == index) {
                const data = if (data_len > 0) try allocator.alloc(u8, data_len) else &.{};
                if (data_len > 0) _ = try file.preadAll(data, pos + 21);
                return LogEntryOwned{ .term = entry_term, .index = entry_index, .entry_type = entry_type, .data = data };
            }
            pos += 21 + data_len;
        }
        return null;
    }

    fn rebuildLastLogIndex(self: *Self) !void {
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "wal.bin", .{});
        const file = self.dir.openFile(path, .{}) catch |err| switch (err) {
            error.FileNotFound => return,
            else => |e| return e,
        };
        defer file.close();

        const file_size = try file.getEndPos();
        var pos: u64 = 0;
        while (pos < file_size) {
            var header: [21]u8 = undefined;
            if ((try file.preadAll(&header, pos)) < 21) break;
            const entry_index = mem.readInt(u64, header[0..8], .little);
            const data_len: usize = @as(usize, @intCast(mem.readInt(u32, header[17..21], .little)));
            self.last_log_index = @max(self.last_log_index, entry_index);
            pos += 21 + data_len;
        }
    }

    // ------------------------------------------------------------------
    // Internal: compaction
    // ------------------------------------------------------------------

    fn compactWal(self: *Self, last_kept_index: LogIndex) !void {
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "wal.bin", .{});

        // Open source and temp files simultaneously.
        const src_file = self.dir.openFile(path, .{}) catch |err| switch (err) {
            error.FileNotFound => return,
            else => |e| return e,
        };
        defer src_file.close();

        const tmp_path = try std.fmt.bufPrint(&path_buf, "wal.tmp", .{});
        const tmp_file = try self.dir.createFile(tmp_path, .{ .truncate = true });
        defer tmp_file.close();

        // Stream entries: read from source, write matching ones to temp.
        const file_size = try src_file.getEndPos();
        var pos: u64 = 0;
        var highest_kept_index: LogIndex = 0;
        while (pos < file_size) {
            var header: [21]u8 = undefined;
            if ((try src_file.preadAll(&header, pos)) < 21) break;
            const entry_index = mem.readInt(u64, header[0..8], .little);
            const entry_term = mem.readInt(u64, header[8..16], .little);
            const entry_type = @as(types.EntryType, @enumFromInt(header[16]));
            const data_len: usize = @as(usize, @intCast(mem.readInt(u32, header[17..21], .little)));

            if (entry_index <= last_kept_index) {
                // Read entry data, write to temp, then free.
                const data = if (data_len > 0) try self.allocator.alloc(u8, data_len) else &.{};
                defer if (data.len > 0) self.allocator.free(data);
                if (data_len > 0) _ = try src_file.preadAll(data, pos + 21);

                var hdr: [21]u8 = undefined;
                mem.writeInt(u64, hdr[0..8], entry_index, .little);
                mem.writeInt(u64, hdr[8..16], entry_term, .little);
                hdr[16] = @intFromEnum(entry_type);
                mem.writeInt(u32, hdr[17..21], @as(u32, @intCast(data_len)), .little);
                try tmp_file.writeAll(&hdr);
                if (data.len > 0) try tmp_file.writeAll(data);

                highest_kept_index = entry_index;
            }
            pos += 21 + data_len;
        }

        try tmp_file.sync();

        // Atomically replace old WAL with compacted one.
        try self.dir.rename("wal.tmp", path);

        // Update last_log_index to the highest kept entry.
        self.last_log_index = highest_kept_index;
    }

    /// Wrap this FileStorage in a Storage(FileStorage) interface.
    pub fn toStorage(ptr: *FileStorage) storage.Storage(FileStorage) {
        return storage.Storage(FileStorage){
            .ptr = ptr,
            .loadTermFn = FileStorage.loadTerm,
            .storeTermFn = FileStorage.storeTerm,
            .loadVotedForFn = FileStorage.loadVotedFor,
            .storeVotedForFn = FileStorage.storeVotedFor,
            .loadLastLogIndexFn = FileStorage.loadLastLogIndex,
            .loadLogEntryFn = FileStorage.loadLogEntry,
            .appendLogEntryFn = FileStorage.appendLogEntry,
            .truncateLogFn = FileStorage.truncateLog,
            .syncFn = FileStorage.sync,
            .storeSnapshotFn = FileStorage.storeSnapshot,
            .loadSnapshotFn = FileStorage.loadSnapshot,
            .loadLastSnapshotIndexFn = FileStorage.loadLastSnapshotIndex,
            .loadLastSnapshotTermFn = FileStorage.loadLastSnapshotTerm,
        };
    }
};

test "FileStorage init creates directories and files" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage";
    fs.cwd().deleteTree(test_dir) catch {};
    var st = try FileStorage.init(allocator, test_dir, 1, 1024 * 1024);
    defer { st.deinit(); fs.cwd().deleteTree(test_dir) catch {}; }
    try std.testing.expectEqual(@as(Term, 0), st.loadTerm());
    try std.testing.expectEqual(@as(?ServerId, null), st.loadVotedFor());
    try std.testing.expectEqual(@as(LogIndex, 0), st.loadLastLogIndex());
}

test "FileStorage round-trip term and votedFor" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage2";
    fs.cwd().deleteTree(test_dir) catch {};
    {
        var st = try FileStorage.init(allocator, test_dir, 2, 1024 * 1024);
        defer st.deinit();
        try st.storeTerm(42);
        try st.storeVotedFor(7);
        try std.testing.expectEqual(@as(Term, 42), st.loadTerm());
        try std.testing.expectEqual(@as(?ServerId, 7), st.loadVotedFor());
    }
    {
        var st = try FileStorage.init(allocator, test_dir, 2, 1024 * 1024);
        defer st.deinit();
        try std.testing.expectEqual(@as(Term, 42), st.loadTerm());
        try std.testing.expectEqual(@as(?ServerId, 7), st.loadVotedFor());
    }
    fs.cwd().deleteTree(test_dir) catch {};
}

test "FileStorage append and read log entry" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage3";
    fs.cwd().deleteTree(test_dir) catch {};
    var st = try FileStorage.init(allocator, test_dir, 3, 1024 * 1024);
    defer { st.deinit(); fs.cwd().deleteTree(test_dir) catch {}; }
    const data = try allocator.dupe(u8, "hello-raft");
    defer allocator.free(data);
    try st.appendLogEntry(.{ .term = 1, .index = 1, .data = data });
    const loaded = st.loadLogEntry(1, allocator);
    try std.testing.expect(loaded != null);
    if (loaded) |e| { defer e.deinit(allocator); try std.testing.expectEqualStrings("hello-raft", e.data); }
}
