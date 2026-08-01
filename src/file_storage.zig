//! File-backed persistent storage for Raft.
//!
//! Uses the cross-platform `std.Io` interface (Zig 0.16+), so it works on
//! Linux, macOS, Windows, etc. (the previous raw `std.os.linux` syscall
//! implementation crashed with SIGSYS on non-Linux platforms).
//!
//! On-disk layout for a server with ID `my_id`:
//!
//!   <base_dir>/raft-<my_id>/
//!     metadata.bin   — fixed-size: currentTerm (u8×8) + votedFor (u8×8, all-0xFF = null)
//!     wal.bin        — append-only entry log
//!     snapshot.bin   — snapshot: last_included_index (u64) + last_included_term (u64) + data_len (u64) + data

const std = @import("std");
const mem = std.mem;
const Io = std.Io;
const types = @import("types.zig");
const storage = @import("storage.zig");

const ServerId = types.ServerId;
const Term = types.Term;
const LogIndex = types.LogIndex;
const LogEntryOwned = storage.LogEntryOwned;
const SnapshotData = storage.SnapshotData;

const NULL_VOTED_FOR: u64 = 0xFFFF_FFFF_FFFF_FFFF;
const WAL_ENTRY_HEADER_LEN: usize = 21;

pub const FileStorage = struct {
    allocator: std.mem.Allocator,
    /// The I/O engine that backs all file operations.
    io_storage: Io.Threaded,
    io: Io,
    /// The `raft-<id>` data directory.
    dir: Io.Dir,
    /// The WAL file, opened once for reading and appending.
    wal: Io.File,
    /// Current WAL length in bytes (append position).
    wal_len: u64,
    server_id: ServerId,
    current_term: Term,
    voted_for: ?ServerId,
    last_log_index: LogIndex,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, base_dir: []const u8, server_id: ServerId) !Self {
        var io_storage = Io.Threaded.init(allocator, .{});
        errdefer io_storage.deinit();
        const io = io_storage.io();

        // Create <base_dir>/raft-<id> (mkdir -p style) and open it.
        var buf: [256]u8 = undefined;
        const dirname = try std.fmt.bufPrint(&buf, "{s}/raft-{}", .{ base_dir, server_id });
        const dir = try Io.Dir.createDirPathOpen(Io.Dir.cwd(), io, dirname, .{});
        errdefer dir.close(io);

        // Open (or create) the WAL without truncating existing data.
        const wal = try Io.Dir.createFile(dir, io, "wal.bin", .{ .read = true, .truncate = false });
        errdefer wal.close(io);
        const wal_len = try wal.length(io);

        var self = Self{
            .allocator = allocator,
            .io_storage = io_storage,
            .io = io,
            .dir = dir,
            .wal = wal,
            .wal_len = wal_len,
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
        self.wal.close(self.io);
        self.dir.close(self.io);
        self.io_storage.deinit();
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
        try ptr.wal.sync(ptr.io);
    }

    // ------------------------------------------------------------------
    // Snapshot
    // ------------------------------------------------------------------

    pub fn storeSnapshot(ptr: *Self, last_included_index: LogIndex, last_included_term: Term, data: []const u8) !void {
        const tmp_file = try Io.Dir.createFile(ptr.dir, ptr.io, "snapshot.tmp", .{});
        defer tmp_file.close(ptr.io);

        var header: [24]u8 = undefined;
        mem.writeInt(u64, header[0..8], last_included_index, .little);
        mem.writeInt(u64, header[8..16], last_included_term, .little);
        mem.writeInt(u64, header[16..24], @as(u64, @intCast(data.len)), .little);

        try tmp_file.writePositionalAll(ptr.io, &header, 0);
        if (data.len > 0) try tmp_file.writePositionalAll(ptr.io, data, header.len);
        try tmp_file.sync(ptr.io);
        try Io.Dir.rename(ptr.dir, "snapshot.tmp", ptr.dir, "snapshot.bin", ptr.io);
    }

    pub fn loadSnapshot(ptr: *Self, allocator: std.mem.Allocator) ?SnapshotData {
        const file = Io.Dir.openFile(ptr.dir, ptr.io, "snapshot.bin", .{}) catch return null;
        defer file.close(ptr.io);

        var header: [24]u8 = undefined;
        _ = file.readPositionalAll(ptr.io, &header, 0) catch return null;

        const last_index = mem.readInt(u64, header[0..8], .little);
        const last_term = mem.readInt(u64, header[8..16], .little);
        const data_len: usize = @intCast(mem.readInt(u64, header[16..24], .little));

        const data = if (data_len > 0) allocator.alloc(u8, data_len) catch return null else @as([]u8, &.{});
        errdefer if (data.len > 0) allocator.free(data);
        if (data_len > 0) _ = file.readPositionalAll(ptr.io, data, header.len) catch return null;
        return SnapshotData{ .last_included_index = last_index, .last_included_term = last_term, .data = data };
    }

    pub fn loadLastSnapshotIndex(ptr: *Self) LogIndex {
        const file = Io.Dir.openFile(ptr.dir, ptr.io, "snapshot.bin", .{}) catch return 0;
        defer file.close(ptr.io);
        var header: [8]u8 = undefined;
        if ((file.readPositionalAll(ptr.io, &header, 0) catch 0) < 8) return 0;
        return mem.readInt(u64, &header, .little);
    }

    pub fn loadLastSnapshotTerm(ptr: *Self) Term {
        const file = Io.Dir.openFile(ptr.dir, ptr.io, "snapshot.bin", .{}) catch return 0;
        defer file.close(ptr.io);
        var header: [16]u8 = undefined;
        if ((file.readPositionalAll(ptr.io, &header, 0) catch 0) < 16) return 0;
        return mem.readInt(u64, header[8..16], .little);
    }

    // ------------------------------------------------------------------
    // Internal: metadata
    // ------------------------------------------------------------------

    fn writeMetadata(self: *Self) !void {
        const file = try Io.Dir.createFile(self.dir, self.io, "metadata.bin", .{});
        defer file.close(self.io);
        var data: [16]u8 = undefined;
        mem.writeInt(u64, data[0..8], self.current_term, .little);
        mem.writeInt(u64, data[8..16], self.voted_for orelse NULL_VOTED_FOR, .little);
        try file.writePositionalAll(self.io, &data, 0);
        try file.sync(self.io);
    }

    fn loadMetadata(self: *Self) !void {
        const file = Io.Dir.openFile(self.dir, self.io, "metadata.bin", .{}) catch |err| switch (err) {
            error.FileNotFound => return,
            else => |e| return e,
        };
        defer file.close(self.io);
        var data: [16]u8 = undefined;
        if ((try file.readPositionalAll(self.io, &data, 0)) < 16) return;
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

        try self.wal.writePositionalAll(self.io, &header, self.wal_len);
        self.wal_len += header.len;
        if (entry.data.len > 0) {
            try self.wal.writePositionalAll(self.io, entry.data, self.wal_len);
            self.wal_len += entry.data.len;
        }
        try self.wal.sync(self.io);
    }

    fn readEntry(self: *Self, index: LogIndex, allocator: std.mem.Allocator) !?LogEntryOwned {
        const file_size = try self.wal.length(self.io);

        var pos: u64 = 0;
        while (pos < file_size) {
            var header: [WAL_ENTRY_HEADER_LEN]u8 = undefined;
            _ = try self.wal.readPositionalAll(self.io, &header, pos);
            const entry_index = mem.readInt(u64, header[0..8], .little);
            const entry_term = mem.readInt(u64, header[8..16], .little);
            const entry_type: types.EntryType = @enumFromInt(header[16]);
            const data_len: usize = @intCast(mem.readInt(u32, header[17..21], .little));

            if (entry_index == index) {
                const data = if (data_len > 0) try allocator.alloc(u8, data_len) else @as([]u8, &.{});
                if (data_len > 0) _ = try self.wal.readPositionalAll(self.io, data, pos + WAL_ENTRY_HEADER_LEN);
                return LogEntryOwned{ .term = entry_term, .index = entry_index, .entry_type = entry_type, .data = data };
            }
            pos += WAL_ENTRY_HEADER_LEN + data_len;
        }
        return null;
    }

    fn rebuildLastLogIndex(self: *Self) !void {
        const file_size = try self.wal.length(self.io);

        var pos: u64 = 0;
        while (pos < file_size) {
            var header: [WAL_ENTRY_HEADER_LEN]u8 = undefined;
            _ = self.wal.readPositionalAll(self.io, &header, pos) catch break;
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
        const file_size = try self.wal.length(self.io);
        if (file_size == 0) return;

        const tmp_file = try Io.Dir.createFile(self.dir, self.io, "wal.tmp", .{});
        defer tmp_file.close(self.io);

        var pos: u64 = 0;
        var highest_kept_index: LogIndex = 0;
        var out_pos: u64 = 0;
        while (pos < file_size) {
            var header: [WAL_ENTRY_HEADER_LEN]u8 = undefined;
            _ = self.wal.readPositionalAll(self.io, &header, pos) catch break;
            const entry_index = mem.readInt(u64, header[0..8], .little);
            const data_len: usize = @intCast(mem.readInt(u32, header[17..21], .little));

            const keep = if (keep_le) entry_index <= boundary else entry_index > boundary;
            if (keep) {
                const data = if (data_len > 0) try self.allocator.alloc(u8, data_len) else @as([]u8, &.{});
                defer if (data.len > 0) self.allocator.free(data);
                if (data_len > 0) _ = try self.wal.readPositionalAll(self.io, data, pos + WAL_ENTRY_HEADER_LEN);

                try tmp_file.writePositionalAll(self.io, &header, out_pos);
                out_pos += header.len;
                if (data.len > 0) {
                    try tmp_file.writePositionalAll(self.io, data, out_pos);
                    out_pos += data.len;
                }
                highest_kept_index = entry_index;
            }
            pos += WAL_ENTRY_HEADER_LEN + data_len;
        }

        try tmp_file.sync(self.io);
        try Io.Dir.rename(self.dir, "wal.tmp", self.dir, "wal.bin", self.io);

        // Re-open the renamed WAL and reset the append position.
        self.wal.close(self.io);
        self.wal = try Io.Dir.createFile(self.dir, self.io, "wal.bin", .{ .read = true, .truncate = false });
        self.wal_len = try self.wal.length(self.io);

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
