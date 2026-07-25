//! File-backed persistent storage for Raft.
//!
//! On-disk layout for a server with ID `my_id`:
//!
//!   <base_dir>/raft-<my_id>/
//!     metadata.bin   — fixed-size: currentTerm (u8×8) + votedFor (u8×8, all-0xFF = null)
//!     wal.bin        — append-only log entries
//!     snapshot.bin   — snapshot: last_included_index (u64) + last_included_term (u64) + data_len (u64) + data

const std = @import("std");
const mem = std.mem;
const posix = std.posix;
const linux = std.os.linux;
const types = @import("types.zig");
const storage = @import("storage.zig");

const ServerId = types.ServerId;
const Term = types.Term;
const LogIndex = types.LogIndex;
const LogEntryOwned = storage.LogEntryOwned;
const SnapshotData = storage.SnapshotData;

const NULL_VOTED_FOR: u64 = 0xFFFF_FFFF_FFFF_FFFF;

/// Check a linux syscall return: 0 = success, negative = errno.
fn checkSyscall(ret: usize) !void {
    const signed = -@as(i32, @intCast(ret));
    if (signed >= 0) return;
    return posix.unexpectedErrno(posix.errno(signed));
}

/// Wrapper around linux.pread that returns the bytes read or a Zig error.
fn preadAll(fd: posix.fd_t, buf: []u8, offset: i64) !usize {
    const ret = linux.pread(fd, buf.ptr, buf.len, offset);
    const signed = -@as(i32, @intCast(ret));
    if (signed < 0) return posix.unexpectedErrno(posix.errno(signed));
    return ret;
}

/// Wrapper around linux.write that returns the bytes written or a Zig error.
fn writeAll(fd: posix.fd_t, buf: []const u8) !usize {
    const ret = linux.write(fd, buf.ptr, buf.len);
    const signed = -@as(i32, @intCast(ret));
    if (signed < 0) return posix.unexpectedErrno(posix.errno(signed));
    return ret;
}

pub const FileStorage = struct {
    allocator: std.mem.Allocator,
    dir_fd: posix.fd_t,
    server_id: ServerId,
    current_term: Term,
    voted_for: ?ServerId,
    last_log_index: LogIndex,
    max_wal_bytes: usize,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, base_dir: []const u8, server_id: ServerId, max_wal_bytes: usize) !Self {
        var buf: [256]u8 = undefined;
        const dirname = try std.fmt.bufPrintZ(&buf, "raft-{}", .{server_id});

        // Create base directory if needed.
        var bbuf: [256]u8 = undefined;
        const zbase = try std.fmt.bufPrintZ(&bbuf, "{s}", .{base_dir});
        _ = linux.mkdirat(posix.AT.FDCWD, zbase, 0o755);
        const base_fd = try posix.openat(posix.AT.FDCWD, base_dir, posix.O{ .DIRECTORY = true, .CLOEXEC = true }, 0);
        errdefer _ = linux.close(base_fd);

        // Create the raft-<id> subdirectory inside base_dir.
        _ = linux.mkdirat(base_fd, dirname, 0o755);
        const dir_fd = try posix.openat(base_fd, dirname, posix.O{ .DIRECTORY = true, .CLOEXEC = true }, 0);
        errdefer _ = linux.close(dir_fd);
        _ = linux.close(base_fd);

        var self = Self{
            .allocator = allocator,
            .dir_fd = dir_fd,
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
        _ = linux.close(self.dir_fd);
        self.* = undefined;
    }

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

    pub fn sync(ptr: *Self) !void {
        try checkSyscall(linux.fsync(ptr.dir_fd));
    }

    // ------------------------------------------------------------------
    // Snapshot
    // ------------------------------------------------------------------

    pub fn storeSnapshot(ptr: *Self, last_included_index: LogIndex, last_included_term: Term, data: []const u8) !void {
        const tmp_file = try posix.openat(ptr.dir_fd, "snapshot.tmp", posix.O{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true, .CLOEXEC = true }, 0o644);
        defer _ = linux.close(tmp_file);

        var header: [24]u8 = undefined;
        mem.writeInt(u64, header[0..8], last_included_index, .little);
        mem.writeInt(u64, header[8..16], last_included_term, .little);
        mem.writeInt(u64, header[16..24], @as(u64, @intCast(data.len)), .little);

        _ = try writeAll(tmp_file, &header);
        if (data.len > 0) _ = try writeAll(tmp_file, data);

        try checkSyscall(linux.fsync(tmp_file));
        try checkSyscall(linux.renameat(ptr.dir_fd, "snapshot.tmp", ptr.dir_fd, "snapshot.bin"));
    }

    pub fn loadSnapshot(ptr: *Self, allocator: std.mem.Allocator) ?SnapshotData {
        const file = posix.openat(ptr.dir_fd, "snapshot.bin", posix.O{ .CLOEXEC = true }, 0) catch return null;
        defer _ = linux.close(file);

        var header: [24]u8 = undefined;
        const n = posix.read(file, &header) catch return null;
        if (n < 24) return null;

        const last_index = mem.readInt(u64, header[0..8], .little);
        const last_term = mem.readInt(u64, header[8..16], .little);
        const data_len: usize = @intCast(mem.readInt(u64, header[16..24], .little));

        const data = if (data_len > 0) allocator.alloc(u8, data_len) catch return null else @as([]u8, &.{});
        if (data_len > 0) {
            _ = posix.read(file, data) catch {
                if (data.len > 0) allocator.free(data);
                return null;
            };
        }
        return SnapshotData{ .last_included_index = last_index, .last_included_term = last_term, .data = data };
    }

    pub fn loadLastSnapshotIndex(ptr: *Self) LogIndex {
        const file = posix.openat(ptr.dir_fd, "snapshot.bin", posix.O{ .CLOEXEC = true }, 0) catch return 0;
        defer _ = linux.close(file);
        var header: [8]u8 = undefined;
        if ((posix.read(file, &header) catch 0) < 8) return 0;
        return mem.readInt(u64, &header, .little);
    }

    pub fn loadLastSnapshotTerm(ptr: *Self) Term {
        const file = posix.openat(ptr.dir_fd, "snapshot.bin", posix.O{ .CLOEXEC = true }, 0) catch return 0;
        defer _ = linux.close(file);
        var header: [16]u8 = undefined;
        if ((posix.read(file, &header) catch 0) < 16) return 0;
        return mem.readInt(u64, header[8..16], .little);
    }

    // ------------------------------------------------------------------
    // Internal: metadata
    // ------------------------------------------------------------------

    fn writeMetadata(self: *Self) !void {
        const file = try posix.openat(self.dir_fd, "metadata.bin", posix.O{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true, .CLOEXEC = true }, 0o644);
        defer _ = linux.close(file);
        var data: [16]u8 = undefined;
        mem.writeInt(u64, data[0..8], self.current_term, .little);
        mem.writeInt(u64, data[8..16], self.voted_for orelse NULL_VOTED_FOR, .little);
        _ = try writeAll(file, &data);
        try checkSyscall(linux.fsync(file));
    }

    fn loadMetadata(self: *Self) !void {
        const file = posix.openat(self.dir_fd, "metadata.bin", posix.O{ .CLOEXEC = true }, 0) catch |err| switch (err) {
            error.FileNotFound => return,
            else => |e| return e,
        };
        defer _ = linux.close(file);
        var data: [16]u8 = undefined;
        if ((try posix.read(file, &data)) < 16) return;
        self.current_term = mem.readInt(u64, data[0..8], .little);
        const vf = mem.readInt(u64, data[8..16], .little);
        self.voted_for = if (vf == NULL_VOTED_FOR) null else vf;
    }

    // ------------------------------------------------------------------
    // Internal: WAL
    // ------------------------------------------------------------------

    fn writeEntry(self: *Self, entry: *const LogEntryOwned) !void {
        const file = try posix.openat(self.dir_fd, "wal.bin", posix.O{ .ACCMODE = .WRONLY, .CREAT = true, .CLOEXEC = true }, 0o644);
        defer _ = linux.close(file);

        // Seek to end for append.
        const seek_ret = linux.lseek(file, 0, @intCast(posix.SEEK.END));
        if (@as(i64, @bitCast(seek_ret)) < 0) {
            return posix.unexpectedErrno(posix.errno(-@as(i32, @intCast(seek_ret))));
        }

        var header: [21]u8 = undefined;
        mem.writeInt(u64, header[0..8], entry.index, .little);
        mem.writeInt(u64, header[8..16], entry.term, .little);
        header[16] = @intFromEnum(entry.entry_type);
        mem.writeInt(u32, header[17..21], @as(u32, @intCast(entry.data.len)), .little);
        _ = try writeAll(file, &header);
        if (entry.data.len > 0) _ = try writeAll(file, entry.data);

        try checkSyscall(linux.fsync(file));
    }

    fn readEntry(self: *Self, index: LogIndex, allocator: std.mem.Allocator) !?LogEntryOwned {
        const file = posix.openat(self.dir_fd, "wal.bin", posix.O{ .CLOEXEC = true }, 0) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => |e| return e,
        };
        defer _ = linux.close(file);

        // Get file size.
        const file_size = blk: {
            const sz = linux.lseek(file, 0, @intCast(posix.SEEK.END));
            if (@as(i64, @bitCast(sz)) < 0) return null;
            break :blk @as(u64, @intCast(sz));
        };

        var pos: u64 = 0;
        while (pos < file_size) {
            var header: [21]u8 = undefined;
            const n = try preadAll(file, &header, @intCast(pos));
            if (n < 21) return null;
            const entry_index = mem.readInt(u64, header[0..8], .little);
            const entry_term = mem.readInt(u64, header[8..16], .little);
            const entry_type: types.EntryType = @enumFromInt(header[16]);
            const data_len: usize = @intCast(mem.readInt(u32, header[17..21], .little));

            if (entry_index == index) {
                const data = if (data_len > 0) try allocator.alloc(u8, data_len) else @as([]u8, &.{});
                if (data_len > 0) _ = try preadAll(file, data, @intCast(pos + 21));
                return LogEntryOwned{ .term = entry_term, .index = entry_index, .entry_type = entry_type, .data = data };
            }
            pos += 21 + data_len;
        }
        return null;
    }

    fn rebuildLastLogIndex(self: *Self) !void {
        const file = posix.openat(self.dir_fd, "wal.bin", posix.O{ .CLOEXEC = true }, 0) catch |err| switch (err) {
            error.FileNotFound => return,
            else => |e| return e,
        };
        defer _ = linux.close(file);

        // Get file size.
        const file_size = blk: {
            const sz = linux.lseek(file, 0, @intCast(posix.SEEK.END));
            if (@as(i64, @bitCast(sz)) < 0) return;
            break :blk @as(u64, @intCast(sz));
        };

        var pos: u64 = 0;
        while (pos < file_size) {
            var header: [21]u8 = undefined;
            const n = preadAll(file, &header, @intCast(pos)) catch break;
            if (n < 21) break;
            const entry_index = mem.readInt(u64, header[0..8], .little);
            const data_len: usize = @intCast(mem.readInt(u32, header[17..21], .little));
            self.last_log_index = @max(self.last_log_index, entry_index);
            pos += 21 + data_len;
        }
    }

    // ------------------------------------------------------------------
    // Internal: compaction
    // ------------------------------------------------------------------

    fn compactWal(self: *Self, last_kept_index: LogIndex) !void {
        const src_file = posix.openat(self.dir_fd, "wal.bin", posix.O{ .CLOEXEC = true }, 0) catch |err| switch (err) {
            error.FileNotFound => return,
            else => |e| return e,
        };
        defer _ = linux.close(src_file);

        // Get source file size.
        const file_size = blk: {
            const sz = linux.lseek(src_file, 0, @intCast(posix.SEEK.END));
            if (@as(i64, @bitCast(sz)) < 0) return;
            break :blk @as(u64, @intCast(sz));
        };

        // Create temp file.
        const tmp_file = try posix.openat(self.dir_fd, "wal.tmp", posix.O{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true, .CLOEXEC = true }, 0o644);
        defer _ = linux.close(tmp_file);

        // Stream entries: read from source, write matching ones to temp.
        var pos: u64 = 0;
        var highest_kept_index: LogIndex = 0;
        while (pos < file_size) {
            var header: [21]u8 = undefined;
            const n = preadAll(src_file, &header, @intCast(pos)) catch break;
            if (n < 21) break;
            const entry_index = mem.readInt(u64, header[0..8], .little);
            const entry_term = mem.readInt(u64, header[8..16], .little);
            const entry_type: types.EntryType = @enumFromInt(header[16]);
            const data_len: usize = @intCast(mem.readInt(u32, header[17..21], .little));

            if (entry_index <= last_kept_index) {
                const data = if (data_len > 0) try self.allocator.alloc(u8, data_len) else @as([]u8, &.{});
                defer if (data.len > 0) self.allocator.free(data);
                if (data_len > 0) _ = try preadAll(src_file, data, @intCast(pos + 21));

                var hdr: [21]u8 = undefined;
                mem.writeInt(u64, hdr[0..8], entry_index, .little);
                mem.writeInt(u64, hdr[8..16], entry_term, .little);
                hdr[16] = @intFromEnum(entry_type);
                mem.writeInt(u32, hdr[17..21], @as(u32, @intCast(data_len)), .little);
                _ = try writeAll(tmp_file, &hdr);
                if (data.len > 0) _ = try writeAll(tmp_file, data);

                highest_kept_index = entry_index;
            }
            pos += 21 + data_len;
        }

        try checkSyscall(linux.fsync(tmp_file));
        try checkSyscall(linux.renameat(self.dir_fd, "wal.tmp", self.dir_fd, "wal.bin"));

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
    std.fs.deleteTreeAbsolute(test_dir) catch {};
    var st = try FileStorage.init(allocator, test_dir, 1, 1024 * 1024);
    defer { st.deinit(); std.fs.deleteTreeAbsolute(test_dir) catch {}; }
    try std.testing.expectEqual(@as(Term, 0), st.loadTerm());
    try std.testing.expectEqual(@as(?ServerId, null), st.loadVotedFor());
    try std.testing.expectEqual(@as(LogIndex, 0), st.loadLastLogIndex());
}

test "FileStorage round-trip term and votedFor" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage2";
    std.fs.deleteTreeAbsolute(test_dir) catch {};
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
    std.fs.deleteTreeAbsolute(test_dir) catch {};
}

test "FileStorage append and read log entry" {
    const allocator = std.testing.allocator;
    const test_dir = "tmp-test-filestorage3";
    std.fs.deleteTreeAbsolute(test_dir) catch {};
    var st = try FileStorage.init(allocator, test_dir, 3, 1024 * 1024);
    defer { st.deinit(); std.fs.deleteTreeAbsolute(test_dir) catch {}; }
    const data = try allocator.dupe(u8, "hello-raft");
    defer allocator.free(data);
    try st.appendLogEntry(.{ .term = 1, .index = 1, .data = data });
    const loaded = st.loadLogEntry(1, allocator);
    try std.testing.expect(loaded != null);
    if (loaded) |e| { defer e.deinit(allocator); try std.testing.expectEqualStrings("hello-raft", e.data); }
}
