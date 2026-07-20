//! File-backed persistent storage for Raft.
//!
//! On-disk layout for a server with ID `my_id`:
//!
//!   <base_dir>/raft-<my_id>/
//!     metadata.bin   — fixed-size: currentTerm (u8×8) + votedFor (u8×8, all-0xFF = null)
//!     wal.bin        — append-only log entries
//!
//! WAL entry format (little-endian):
//!   [index: u64] [term: u64] [data_len: u32] [data: data_len bytes]
//!
//! On truncation: the WAL is compacted — entries after the keep-point
//! are dropped by rewriting the file, then atomically renaming.

const std = @import("std");
const fs = std.fs;
const types = @import("types.zig");
const storage = @import("storage.zig");

const ServerId = types.ServerId;
const Term = types.Term;
const LogIndex = types.LogIndex;
const LogEntryOwned = storage.LogEntryOwned;

const NULL_VOTED_FOR: u64 = 0xFFFF_FFFF_FFFF_FFFF;

/// File-backed Raft storage.
pub const FileStorage = struct {
    allocator: std.mem.Allocator,
    /// Directory containing metadata.bin and wal.bin.
    dir: fs.Dir,
    /// Server ID used to construct the directory path.
    server_id: ServerId,
    /// Cached values to avoid reading the metadata file on every access.
    current_term: Term,
    voted_for: ?ServerId,
    /// Cached last log index — rebuilt on init, updated on append.
    last_log_index: LogIndex,
    /// Maximum WAL size in bytes before compaction is triggered.
    max_wal_bytes: usize,

    const Self = @This();

    /// Open or create storage at `base_dir / raft-{server_id}`.
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

    pub fn deinit(self: *Self) void {
        self.dir.close();
        self.* = undefined;
    }

    // ------------------------------------------------------------------
    // Public interface — matches Storage(T) signature
    // ------------------------------------------------------------------

    pub fn loadTerm(ptr: *Self) Term {
        return ptr.current_term;
    }

    pub fn storeTerm(ptr: *Self, term: Term) void {
        ptr.current_term = term;
        ptr.writeMetadata() catch {}; // best-effort for now; caller should sync
    }

    pub fn loadVotedFor(ptr: *Self) ?ServerId {
        return ptr.voted_for;
    }

    pub fn storeVotedFor(ptr: *Self, voted_for: ?ServerId) void {
        ptr.voted_for = voted_for;
        ptr.writeMetadata() catch {};
    }

    pub fn loadLastLogIndex(ptr: *Self) LogIndex {
        return ptr.last_log_index;
    }

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

    pub fn sync(ptr: *Self) !void {
        ptr.dir.sync() catch {};
    }

    // ------------------------------------------------------------------
    // Metadata read/write
    // ------------------------------------------------------------------

    fn metadataPath(buf: []u8) ![]u8 {
        return std.fmt.bufPrint(buf, "metadata.bin", .{});
    }

    fn walPath(buf: []u8) ![]u8 {
        return std.fmt.bufPrint(buf, "wal.bin", .{});
    }

    fn writeMetadata(self: *Self) !void {
        var buf: [256]u8 = undefined;
        const path = try metadataPath(&buf);
        var file = try self.dir.createFile(path, .{ .truncate = true });
        defer file.close();

        var data: [16]u8 = undefined;
        std.mem.writeIntLittle(u64, data[0..8], self.current_term);
        const vf = self.voted_for orelse NULL_VOTED_FOR;
        std.mem.writeIntLittle(u64, data[8..16], vf);
        try file.writeAll(&data);
        try file.sync();
    }

    fn loadMetadata(self: *Self) !void {
        var buf: [256]u8 = undefined;
        const path = try metadataPath(&buf);
        const file = self.dir.openFile(path, .{}) catch |err| switch (err) {
            error.FileNotFound => return, // first run, use defaults
            else => |e| return e,
        };
        defer file.close();

        var data: [16]u8 = undefined;
        const n = try file.readAll(&data);
        if (n < 16) return; // corrupt file, use defaults

        self.current_term = std.mem.readIntLittle(u64, data[0..8]);
        const vf = std.mem.readIntLittle(u64, data[8..16]);
        self.voted_for = if (vf == NULL_VOTED_FOR) null else vf;
    }

    // ------------------------------------------------------------------
    // WAL read/write
    // ------------------------------------------------------------------

    fn writeEntry(self: *Self, entry: *const LogEntryOwned) !void {
        var buf: [256]u8 = undefined;
        const path = try walPath(&buf);
        const file = try self.dir.createFile(path, .{ .truncate = false });
        defer file.close();
        try file.seekFromEnd(0);

        var header: [20]u8 = undefined;
        std.mem.writeIntLittle(u64, header[0..8], entry.index);
        std.mem.writeIntLittle(u64, header[8..16], entry.term);
        std.mem.writeIntLittle(u32, header[16..20], @as(u32, @intCast(entry.data.len)));
        try file.writeAll(&header);
        if (entry.data.len > 0) {
            try file.writeAll(entry.data);
        }
        try file.sync();
    }

    fn readEntry(self: *Self, index: LogIndex, allocator: std.mem.Allocator) !?LogEntryOwned {
        var buf: [256]u8 = undefined;
        const path = try walPath(&buf);
        const file = try self.dir.openFile(path, .{});
        defer file.close();

        const file_size = try file.getEndPos();
        var pos: u64 = 0;
        while (pos < file_size) {
            var header: [20]u8 = undefined;
            const n = try file.preadAll(&header, pos);
            if (n < 20) return null;
            const entry_index = std.mem.readIntLittle(u64, header[0..8]);
            const entry_term = std.mem.readIntLittle(u64, header[8..16]);
            const data_len: usize = @as(usize, @intCast(std.mem.readIntLittle(u32, header[16..20])));

            if (entry_index == index) {
                const data = if (data_len > 0) try allocator.alloc(u8, data_len) else &.{};
                if (data_len > 0) {
                    _ = try file.preadAll(data, pos + 20);
                }
                return LogEntryOwned{ .term = entry_term, .index = entry_index, .data = data };
            }
            pos += 20 + data_len;
        }
        return null;
    }

    fn rebuildLastLogIndex(self: *Self) !void {
        var buf: [256]u8 = undefined;
        const path = try walPath(&buf);
        const file = self.dir.openFile(path, .{}) catch |err| switch (err) {
            error.FileNotFound => return, // empty WAL
            else => |e| return e,
        };
        defer file.close();

        const file_size = try file.getEndPos();
        var pos: u64 = 0;
        while (pos < file_size) {
            var header: [20]u8 = undefined;
            const n = try file.preadAll(&header, pos);
            if (n < 20) break;
            const entry_index = std.mem.readIntLittle(u64, header[0..8]);
            const data_len: usize = @as(usize, @intCast(std.mem.readIntLittle(u32, header[16..20])));
            self.last_log_index = @max(self.last_log_index, entry_index);
            pos += 20 + data_len;
        }
    }

    // ------------------------------------------------------------------
    // Compaction
    // ------------------------------------------------------------------

    fn compactWal(self: *Self, last_kept_index: LogIndex) !void {
        var buf: [256]u8 = undefined;
        const path = try walPath(&buf);

        // Build a temporary file with entries up to last_kept_index
        var tmp_path_buf: [256]u8 = undefined;
        const tmp_path = try std.fmt.bufPrint(&tmp_path_buf, "wal.tmp", .{});

        // Read all entries up to last_kept_index into memory
        var entries = std.ArrayListUnmanaged(LogEntryOwned){};
        defer {
            for (entries.items) |*e| e.deinit(self.allocator);
            entries.deinit(self.allocator);
        }

        {
            const file = self.dir.openFile(path, .{}) catch |err| switch (err) {
                error.FileNotFound => return, // no WAL
                else => |e| return e,
            };
            defer file.close();

            const file_size = try file.getEndPos();
            var pos: u64 = 0;
            while (pos < file_size) {
                var header: [20]u8 = undefined;
                const n = try file.preadAll(&header, pos);
                if (n < 20) break;
                const entry_index = std.mem.readIntLittle(u64, header[0..8]);
                const entry_term = std.mem.readIntLittle(u64, header[8..16]);
                const data_len: usize = @as(usize, @intCast(std.mem.readIntLittle(u32, header[16..20])));

                if (entry_index <= last_kept_index) {
                    const data = if (data_len > 0) try self.allocator.alloc(u8, data_len) else &.{};
                    if (data_len > 0) {
                        _ = try file.preadAll(data, pos + 20);
                    }
                    try entries.append(self.allocator, LogEntryOwned{ .term = entry_term, .index = entry_index, .data = data });
                }
                pos += 20 + data_len;
            }
        }

        // Write new WAL with only the kept entries
        {
            const tmp_file = try self.dir.createFile(tmp_path, .{ .truncate = true });
            defer tmp_file.close();

            for (entries.items) |*entry| {
                var hdr: [20]u8 = undefined;
                std.mem.writeIntLittle(u64, hdr[0..8], entry.index);
                std.mem.writeIntLittle(u64, hdr[8..16], entry.term);
                std.mem.writeIntLittle(u32, hdr[16..20], @as(u32, @intCast(entry.data.len)));
                try tmp_file.writeAll(&hdr);
                if (entry.data.len > 0) {
                    try tmp_file.writeAll(entry.data);
                }
            }
            try tmp_file.sync();
        }

        // Atomic rename
        try self.dir.rename(tmp_path, path);
    }
};

test "FileStorage init creates directories and files" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage";

    // Clean up from previous runs
    fs.cwd().deleteTree(test_dir) catch {};

    var storage_ = try FileStorage.init(allocator, test_dir, 1, 1024 * 1024);
    defer {
        storage_.deinit();
        fs.cwd().deleteTree(test_dir) catch {};
    }

    try std.testing.expectEqual(@as(Term, 0), storage_.loadTerm());
    try std.testing.expectEqual(@as(?ServerId, null), storage_.loadVotedFor());
    try std.testing.expectEqual(@as(LogIndex, 0), storage_.loadLastLogIndex());
}

test "FileStorage round-trip term and votedFor" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage2";

    fs.cwd().deleteTree(test_dir) catch {};

    {
        var storage_ = try FileStorage.init(allocator, test_dir, 2, 1024 * 1024);
        defer storage_.deinit();

        storage_.storeTerm(42);
        storage_.storeVotedFor(7);

        try std.testing.expectEqual(@as(Term, 42), storage_.loadTerm());
        try std.testing.expectEqual(@as(?ServerId, 7), storage_.loadVotedFor());
    }

    // Re-open and verify persistence
    {
        var storage_ = try FileStorage.init(allocator, test_dir, 2, 1024 * 1024);
        defer storage_.deinit();

        try std.testing.expectEqual(@as(Term, 42), storage_.loadTerm());
        try std.testing.expectEqual(@as(?ServerId, 7), storage_.loadVotedFor());
    }

    fs.cwd().deleteTree(test_dir) catch {};
}

test "FileStorage append and read log entry" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage3";

    fs.cwd().deleteTree(test_dir) catch {};

    var storage_ = try FileStorage.init(allocator, test_dir, 3, 1024 * 1024);
    defer {
        storage_.deinit();
        fs.cwd().deleteTree(test_dir) catch {};
    }

    const data = try allocator.dupe(u8, "hello-raft");
    defer allocator.free(data);

    try storage_.appendLogEntry(.{ .term = 1, .index = 1, .data = data });

    const loaded = storage_.loadLogEntry(1, allocator);
    try std.testing.expect(loaded != null);
    if (loaded) |entry| {
        defer entry.deinit(allocator);
        try std.testing.expectEqual(@as(Term, 1), entry.term);
        try std.testing.expectEqual(@as(LogIndex, 1), entry.index);
        try std.testing.expectEqualStrings("hello-raft", entry.data);
    }
}

test "FileStorage truncate log" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage4";

    fs.cwd().deleteTree(test_dir) catch {};

    var storage_ = try FileStorage.init(allocator, test_dir, 4, 1024 * 1024);
    defer {
        storage_.deinit();
        fs.cwd().deleteTree(test_dir) catch {};
    }

    for (0..3) |i| {
        const idx: LogIndex = @intCast(i + 1);
        const data = try allocator.dupe(u8, "entry-{d}");
        defer allocator.free(data);
        try storage_.appendLogEntry(.{ .term = 1, .index = idx, .data = data });
    }

    try std.testing.expectEqual(@as(LogIndex, 3), storage_.loadLastLogIndex());

    // Truncate to index 1
    try storage_.truncateLog(1);
    try std.testing.expectEqual(@as(LogIndex, 1), storage_.loadLastLogIndex());

    // Entry 1 should still exist
    const entry1 = storage_.loadLogEntry(1, allocator);
    try std.testing.expect(entry1 != null);
    if (entry1) |e| e.deinit(allocator);

    // Entry 2 should be gone
    const entry2 = storage_.loadLogEntry(2, allocator);
    try std.testing.expect(entry2 == null);
}
