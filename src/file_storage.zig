//! File-backed persistent storage for Raft.
//!
//! On-disk layout for a server with ID `my_id`:
//!
//!   <base_dir>/raft-<my_id>/
//!     metadata.bin   — fixed-size: currentTerm (u8×8) + votedFor (u8×8, all-0xFF = null)
//!     wal.bin        — append-only entry log
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
const WAL_ENTRY_HEADER_LEN: usize = 21;

fn checkErrno(ret: usize) !void {
    const e = linux.errno(ret);
    if (e == .SUCCESS) return;
    return error.Unexpected;
}

/// Loop until the whole buffer is written.
fn writeAll(fd: posix.fd_t, buf: []const u8) !void {
    var off: usize = 0;
    while (off < buf.len) {
        const ret = linux.write(fd, buf[off..].ptr, buf[off..].len);
        const e = linux.errno(ret);
        if (e != .SUCCESS) return error.Unexpected;
        if (ret == 0) return error.Unexpected;
        off += ret;
    }
}

/// Loop until the whole buffer is read from the current file offset.
fn readAll(fd: posix.fd_t, buf: []u8) !void {
    var off: usize = 0;
    while (off < buf.len) {
        const n = try posix.read(fd, buf[off..]);
        if (n == 0) return error.Unexpected;
        off += n;
    }
}

/// Loop until the whole buffer is pread from the given offset.
fn preadAll(fd: posix.fd_t, buf: []u8, offset: i64) !void {
    var off: usize = 0;
    while (off < buf.len) {
        const ret = linux.pread(fd, buf[off..].ptr, buf[off..].len, offset + @as(i64, @intCast(off)));
        const e = linux.errno(ret);
        if (e != .SUCCESS) return error.Unexpected;
        if (ret == 0) return error.Unexpected;
        off += ret;
    }
}

pub const FileStorage = struct {
    allocator: std.mem.Allocator,
    dir_fd: posix.fd_t,
    wal_fd: posix.fd_t,
    server_id: ServerId,
    current_term: Term,
    voted_for: ?ServerId,
    last_log_index: LogIndex,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, base_dir: []const u8, server_id: ServerId) !Self {
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

        // Open WAL once for appending and reading.
        const wal_fd = try posix.openat(dir_fd, "wal.bin", posix.O{
            .ACCMODE = .RDWR,
            .CREAT = true,
            .APPEND = true,
            .CLOEXEC = true,
        }, 0o644);
        errdefer _ = linux.close(wal_fd);

        var self = Self{
            .allocator = allocator,
            .dir_fd = dir_fd,
            .wal_fd = wal_fd,
            .server_id = server_id,
            .current_term = 0,
            .voted_for = null,
            .last_log_index = 0,
        };
        try self.loadMetadata();
        try self.rebuildLastLogIndex();
        return self;
    }

    pub fn deinit(self: *Self) void {
        _ = linux.close(self.wal_fd);
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
        try ptr.compactWalTail(last_kept_index);
        ptr.last_log_index = @min(ptr.last_log_index, last_kept_index);
    }

    pub fn dropLogPrefix(ptr: *Self, last_included_index: LogIndex) !void {
        try ptr.compactWalPrefix(last_included_index);
        // After dropping the prefix, indices <= last_included are gone.
        if (ptr.last_log_index <= last_included_index) {
            ptr.last_log_index = 0;
        }
    }

    pub fn sync(ptr: *Self) !void {
        try checkErrno(linux.fsync(ptr.wal_fd));
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

        try writeAll(tmp_file, &header);
        if (data.len > 0) try writeAll(tmp_file, data);

        try checkErrno(linux.fsync(tmp_file));
        try checkErrno(linux.renameat(ptr.dir_fd, "snapshot.tmp", ptr.dir_fd, "snapshot.bin"));
        try checkErrno(linux.fsync(ptr.dir_fd));
    }

    pub fn loadSnapshot(ptr: *Self, allocator: std.mem.Allocator) ?SnapshotData {
        const file = posix.openat(ptr.dir_fd, "snapshot.bin", posix.O{ .CLOEXEC = true }, 0) catch return null;
        defer _ = linux.close(file);

        var header: [24]u8 = undefined;
        readAll(file, &header) catch return null;

        const last_index = mem.readInt(u64, header[0..8], .little);
        const last_term = mem.readInt(u64, header[8..16], .little);
        const data_len: usize = @intCast(mem.readInt(u64, header[16..24], .little));

        const data = if (data_len > 0) allocator.alloc(u8, data_len) catch return null else @as([]u8, &.{});
        errdefer if (data.len > 0) allocator.free(data);
        if (data_len > 0) readAll(file, data) catch return null;
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
        try writeAll(file, &data);
        try checkErrno(linux.fsync(file));
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
        var header: [WAL_ENTRY_HEADER_LEN]u8 = undefined;
        mem.writeInt(u64, header[0..8], entry.index, .little);
        mem.writeInt(u64, header[8..16], entry.term, .little);
        header[16] = @intFromEnum(entry.entry_type);
        mem.writeInt(u32, header[17..21], @as(u32, @intCast(entry.data.len)), .little);
        try writeAll(self.wal_fd, &header);
        if (entry.data.len > 0) try writeAll(self.wal_fd, entry.data);
        try checkErrno(linux.fsync(self.wal_fd));
    }

    fn readEntry(self: *Self, index: LogIndex, allocator: std.mem.Allocator) !?LogEntryOwned {
        const file_size = blk: {
            const sz = linux.lseek(self.wal_fd, 0, @intCast(posix.SEEK.END));
            if (@as(i64, @bitCast(sz)) < 0) return null;
            break :blk @as(u64, @intCast(sz));
        };

        var pos: u64 = 0;
        while (pos < file_size) {
            var header: [WAL_ENTRY_HEADER_LEN]u8 = undefined;
            try preadAll(self.wal_fd, &header, @intCast(pos));
            const entry_index = mem.readInt(u64, header[0..8], .little);
            const entry_term = mem.readInt(u64, header[8..16], .little);
            const entry_type: types.EntryType = @enumFromInt(header[16]);
            const data_len: usize = @intCast(mem.readInt(u32, header[17..21], .little));

            if (entry_index == index) {
                const data = if (data_len > 0) try allocator.alloc(u8, data_len) else @as([]u8, &.{});
                if (data_len > 0) try preadAll(self.wal_fd, data, @intCast(pos + WAL_ENTRY_HEADER_LEN));
                return LogEntryOwned{ .term = entry_term, .index = entry_index, .entry_type = entry_type, .data = data };
            }
            pos += WAL_ENTRY_HEADER_LEN + data_len;
        }
        return null;
    }

    fn rebuildLastLogIndex(self: *Self) !void {
        const file_size = blk: {
            const sz = linux.lseek(self.wal_fd, 0, @intCast(posix.SEEK.END));
            if (@as(i64, @bitCast(sz)) < 0) return;
            break :blk @as(u64, @intCast(sz));
        };

        var pos: u64 = 0;
        while (pos < file_size) {
            var header: [WAL_ENTRY_HEADER_LEN]u8 = undefined;
            preadAll(self.wal_fd, &header, @intCast(pos)) catch break;
            const entry_index = mem.readInt(u64, header[0..8], .little);
            const data_len: usize = @intCast(mem.readInt(u32, header[17..21], .little));
            self.last_log_index = @max(self.last_log_index, entry_index);
            pos += WAL_ENTRY_HEADER_LEN + data_len;
        }
    }

    // ------------------------------------------------------------------
    // Internal: compaction
    // ------------------------------------------------------------------

    /// Stream WAL entries, keeping those with index <= last_kept_index (tail truncate).
    fn compactWalTail(self: *Self, last_kept_index: LogIndex) !void {
        try self.compactWal(last_kept_index, true);
    }

    /// Stream WAL entries, keeping those with index > last_included_index (prefix drop).
    fn compactWalPrefix(self: *Self, last_included_index: LogIndex) !void {
        try self.compactWal(last_included_index, false);
    }

    fn compactWal(self: *Self, boundary: LogIndex, keep_le: bool) !void {
        const file_size = blk: {
            const sz = linux.lseek(self.wal_fd, 0, @intCast(posix.SEEK.END));
            if (@as(i64, @bitCast(sz)) < 0) return;
            break :blk @as(u64, @intCast(sz));
        };
        if (file_size == 0) return;

        const tmp_file = try posix.openat(self.dir_fd, "wal.tmp", posix.O{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true, .CLOEXEC = true }, 0o644);
        defer _ = linux.close(tmp_file);

        var pos: u64 = 0;
        var highest_kept_index: LogIndex = 0;
        while (pos < file_size) {
            var header: [WAL_ENTRY_HEADER_LEN]u8 = undefined;
            preadAll(self.wal_fd, &header, @intCast(pos)) catch break;
            const entry_index = mem.readInt(u64, header[0..8], .little);
            const data_len: usize = @intCast(mem.readInt(u32, header[17..21], .little));

            const keep = if (keep_le) entry_index <= boundary else entry_index > boundary;
            if (keep) {
                const data = if (data_len > 0) try self.allocator.alloc(u8, data_len) else @as([]u8, &.{});
                defer if (data.len > 0) self.allocator.free(data);
                if (data_len > 0) try preadAll(self.wal_fd, data, @intCast(pos + WAL_ENTRY_HEADER_LEN));

                try writeAll(tmp_file, &header);
                if (data.len > 0) try writeAll(tmp_file, data);
                highest_kept_index = entry_index;
            }
            pos += WAL_ENTRY_HEADER_LEN + data_len;
        }

        try checkErrno(linux.fsync(tmp_file));
        try checkErrno(linux.renameat(self.dir_fd, "wal.tmp", self.dir_fd, "wal.bin"));
        try checkErrno(linux.fsync(self.dir_fd));

        // Re-open the renamed WAL.
        _ = linux.close(self.wal_fd);
        self.wal_fd = try posix.openat(self.dir_fd, "wal.bin", posix.O{
            .ACCMODE = .RDWR,
            .CREAT = true,
            .APPEND = true,
            .CLOEXEC = true,
        }, 0o644);

        if (keep_le) self.last_log_index = highest_kept_index;
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
            .dropLogPrefixFn = FileStorage.dropLogPrefix,
            .syncFn = FileStorage.sync,
            .storeSnapshotFn = FileStorage.storeSnapshot,
            .loadSnapshotFn = FileStorage.loadSnapshot,
            .loadLastSnapshotIndexFn = FileStorage.loadLastSnapshotIndex,
            .loadLastSnapshotTermFn = FileStorage.loadLastSnapshotTerm,
        };
    }
};
