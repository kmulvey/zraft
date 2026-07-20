//! Core Raft consensus node.
//!
//! Implements the Raft consensus algorithm per the Ongaro & Ousterhout paper.
//!
//! Generic over:
//!   - `SM`: the state machine type (implements `StateMachine(SM)`)
//!   - `ST`: the storage type (implements `Storage(ST)`)
//!
//! # Design
//!
//! - **Callers handle networking**: incoming RPCs are delivered via `handle*` methods
//!   which return response structs. Outgoing RPCs are emitted through callback
//!   function pointers that the caller sets.
//! - **Explicit allocators**: no global state.
//! - **Deterministic time**: all timeouts use a monotonic timestamp (ns) passed
//!   into `tick()` and `handleAppendEntries`.

const std = @import("std");
const types = @import("types.zig");
const rpc = @import("rpc.zig");
const config_mod = @import("config.zig");
const log_mod = @import("log.zig");
const sm_iface = @import("state_machine.zig");
const storage_iface = @import("storage.zig");

const ServerId = types.ServerId;
const Term = types.Term;
const LogIndex = types.LogIndex;
const Role = types.Role;
const Config = config_mod.Config;
const Storage = storage_iface.Storage;
const StateMachine = sm_iface.StateMachine;

/// A Raft consensus node parameterised over state machine and storage types.
pub fn Node(comptime SM: type, comptime ST: type) type {
    return struct {
        const Self = @This();
        const LogType = log_mod.Log(ST);
        const StorageType = Storage(ST);

        allocator: std.mem.Allocator,
        config: Config,
        log: *LogType,
        state_machine: StateMachine(SM),
        storage: StorageType,

        // --- Persistent state (durable in storage) ---
        current_term: Term = 0,
        voted_for: ?ServerId = null,

        // --- Other volatile state ---
        commit_index: LogIndex = 0,
        last_applied: LogIndex = 0,
        role: Role = .follower,

        // --- Vote tracking during election ---
        votes_received: u32 = 0,

        // --- Leader volatile state ---
        next_index: []LogIndex = &.{},
        match_index: []LogIndex = &.{},

        // --- Timing (monotonic ns) ---
        last_heartbeat_ns: u64 = 0,
        election_start_ns: u64 = 0,
        election_timeout_ns: u64 = 0,

        rng: std.Random.DefaultPrng,

        // --- Outgoing message callbacks ---
        send_append_entries: ?*const fn (peer: ServerId, req: rpc.AppendEntriesRequest) void = null,
        send_request_vote: ?*const fn (peer: ServerId, req: rpc.RequestVoteRequest) void = null,
        send_install_snapshot: ?*const fn (peer: ServerId, req: rpc.InstallSnapshotRequest) void = null,

        // ===================================================================
        // Init / Deinit
        // ===================================================================

        pub fn init(
            allocator: std.mem.Allocator,
            config: Config,
            log: *LogType,
            state_machine: StateMachine(SM),
            storage: StorageType,
            rng_seed: u64,
        ) !Self {
            var self = Self{
                .allocator = allocator,
                .config = config,
                .log = log,
                .state_machine = state_machine,
                .storage = storage,
                .next_index = try allocator.alloc(LogIndex, config.peers.len),
                .match_index = try allocator.alloc(LogIndex, config.peers.len),
                .rng = std.Random.DefaultPrng.init(rng_seed),
            };
            self.current_term = self.storage.loadTerm();
            self.voted_for = self.storage.loadVotedFor();
            self.randomiseElectionTimeout();
            return self;
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.next_index);
            self.allocator.free(self.match_index);
        }

        // ===================================================================
        // Tick — periodic driver (call at ~50ms intervals)
        // ===================================================================

        pub fn tick(self: *Self, now_ns: u64) !void {
            switch (self.role) {
                .follower => try self.tickFollower(now_ns),
                .candidate => try self.tickCandidate(now_ns),
                .leader => try self.tickLeader(now_ns),
            }
        }

        fn tickFollower(self: *Self, now_ns: u64) !void {
            if (now_ns < self.election_start_ns + self.election_timeout_ns) return;
            try self.startElection(now_ns);
        }

        fn tickCandidate(self: *Self, now_ns: u64) !void {
            if (now_ns < self.election_start_ns + self.election_timeout_ns) return;
            try self.startElection(now_ns);
        }

        fn tickLeader(self: *Self, now_ns: u64) !void {
            if (now_ns < self.last_heartbeat_ns + self.config.heartbeat_interval_ns) return;
            self.last_heartbeat_ns = now_ns;
            try self.broadcastAppendEntries(now_ns);
        }

        // ===================================================================
        // Election
        // ===================================================================

        fn startElection(self: *Self, now_ns: u64) !void {
            self.current_term += 1;
            try self.storage.storeTerm(self.current_term);
            self.role = .candidate;
            self.voted_for = self.config.id;
            try self.storage.storeVotedFor(self.voted_for);
            self.votes_received = 1; // vote for self
            self.election_start_ns = now_ns;
            self.randomiseElectionTimeout();

            // Single-node cluster: become leader immediately
            if (self.haveQuorum()) {
                try self.becomeLeader();
                return;
            }

            const last_index = self.log.lastIndex();
            const last_term = self.log.termAt(last_index);
            const req = rpc.RequestVoteRequest{
                .term = self.current_term,
                .candidate_id = self.config.id,
                .last_log_index = last_index,
                .last_log_term = last_term,
            };
            if (self.send_request_vote) |send| {
                for (self.config.peers) |peer| send(peer, req);
            }
        }

        // ===================================================================
        // RPC Handlers (§5.1: persist state before responding)
        // ===================================================================

        pub fn handleRequestVote(self: *Self, req: rpc.RequestVoteRequest) !rpc.RequestVoteResponse {
            if (req.term < self.current_term) {
                return .{ .term = self.current_term, .vote_granted = false };
            }
            if (req.term > self.current_term) {
                try self.stepDown(req.term);
            }

            const granted = blk: {
                if (self.voted_for != null and self.voted_for != req.candidate_id) break :blk false;
                const my_last_idx = self.log.lastIndex();
                const my_last_term = self.log.termAt(my_last_idx);
                if (req.last_log_term < my_last_term) break :blk false;
                if (req.last_log_term == my_last_term and req.last_log_index < my_last_idx) break :blk false;

                self.voted_for = req.candidate_id;
                try self.storage.storeVotedFor(self.voted_for);
                break :blk true;
            };

            return .{ .term = self.current_term, .vote_granted = granted };
        }

        /// Handle an incoming AppendEntries RPC.
        /// `now_ns` is the current monotonic time — used to reset the election timer.
        pub fn handleAppendEntries(self: *Self, req: rpc.AppendEntriesRequest, now_ns: u64) !rpc.AppendEntriesResponse {
            if (req.term < self.current_term) {
                return .{ .term = self.current_term, .success = false };
            }

            if (req.term >= self.current_term) {
                if (self.role != .follower) try self.stepDown(req.term);
                self.current_term = req.term;
                try self.storage.storeTerm(self.current_term);
                self.voted_for = null;
                try self.storage.storeVotedFor(self.voted_for);
                self.election_start_ns = now_ns; // Reset election timer (§5.2)
            }

            // §5.3: Log consistency check
            if (req.prev_log_index > 0) {
                if (req.prev_log_index > self.log.lastIndex()) {
                    return .{ .term = self.current_term, .success = false, .conflict_index = self.log.lastIndex(), .conflict_term = 0 };
                }
                if (self.log.termAt(req.prev_log_index) != req.prev_log_term) {
                    const conflict_term = self.log.termAt(req.prev_log_index);
                    var conflict_idx = req.prev_log_index;
                    while (conflict_idx > 0 and self.log.termAt(conflict_idx) == conflict_term) {
                        conflict_idx -= 1;
                    }
                    return .{ .term = self.current_term, .success = false, .conflict_index = conflict_idx + 1, .conflict_term = conflict_term };
                }
            }

            // §5.3: Delete conflicting entries and append new ones
            for (req.entries) |wire_entry| {
                if (wire_entry.index < self.log.len) {
                    if (self.log.termAt(wire_entry.index) != wire_entry.term) {
                        try self.log.truncate(wire_entry.index - 1);
                        _ = try self.log.append(self.current_term, wire_entry.data);
                    }
                } else {
                    _ = try self.log.append(wire_entry.term, wire_entry.data);
                }
            }

            // §5.3: Update commit index
            if (req.leader_commit > self.commit_index) {
                const last_new = if (req.entries.len > 0) req.entries[req.entries.len - 1].index else req.prev_log_index;
                self.commit_index = @min(req.leader_commit, last_new);
            }

            self.applyCommittedEntries();
            return .{ .term = self.current_term, .success = true };
        }

        pub fn handleInstallSnapshot(self: *Self, req: rpc.InstallSnapshotRequest) !rpc.InstallSnapshotResponse {
            if (req.term < self.current_term) return .{ .term = self.current_term, .success = false };
            if (req.term > self.current_term) try self.stepDown(req.term);
            return .{ .term = self.current_term, .success = true };
        }

        // ===================================================================
        // Leader: client request handling
        // ===================================================================

        pub fn clientAppend(self: *Self, data: []const u8) !LogIndex {
            if (self.role != .leader) return error.NotLeader;
            return try self.log.append(self.current_term, data);
        }

        // ===================================================================
        // Internal helpers
        // ===================================================================

        fn stepDown(self: *Self, new_term: Term) !void {
            self.current_term = new_term;
            try self.storage.storeTerm(self.current_term);
            self.role = .follower;
            self.voted_for = null;
            try self.storage.storeVotedFor(self.voted_for);
            self.votes_received = 0;
            self.election_start_ns = 0;
        }

        fn randomiseElectionTimeout(self: *Self) void {
            const range = self.config.election_timeout_max_ns - self.config.election_timeout_min_ns;
            self.election_timeout_ns = self.config.election_timeout_min_ns + self.rng.random().uintAtMost(u64, range);
        }

        fn broadcastAppendEntries(self: *Self, _now_ns: u64) !void {
            _ = _now_ns;
            const last_idx = self.log.lastIndex();
            const ni = self.next_index;
            const send = self.send_append_entries;

            for (self.config.peers, 0..) |peer, i| {
                const next = if (i < ni.len) ni[i] else 1;
                if (next > last_idx) {
                    if (send) |s| s(peer, .{
                        .term = self.current_term,
                        .leader_id = self.config.id,
                        .prev_log_index = last_idx,
                        .prev_log_term = self.log.termAt(last_idx),
                        .entries = &.{},
                        .leader_commit = self.commit_index,
                    });
                } else {
                    const prev_idx = next - 1;
                    const entries_slice = self.log.sliceFrom(next);
                    var wire_entries: std.ArrayListUnmanaged(rpc.LogEntryWire) = .empty;
                    defer wire_entries.deinit(self.allocator);
                    try wire_entries.ensureTotalCapacity(self.allocator, entries_slice.len);
                    for (entries_slice) |e| wire_entries.appendAssumeCapacity(.{ .term = e.term, .index = e.index, .data = e.data });
                    if (send) |s| s(peer, .{
                        .term = self.current_term,
                        .leader_id = self.config.id,
                        .prev_log_index = prev_idx,
                        .prev_log_term = self.log.termAt(prev_idx),
                        .entries = wire_entries.items,
                        .leader_commit = self.commit_index,
                    });
                }
            }
        }

        fn applyCommittedEntries(self: *Self) void {
            while (self.last_applied < self.commit_index) {
                self.last_applied += 1;
                if (self.last_applied >= self.log.len) break;
                self.state_machine.apply(self.last_applied, self.log.entries[@as(usize, @intCast(self.last_applied))].data);
            }
        }

        // ===================================================================
        // Leader response handling
        // ===================================================================

        pub fn handleAppendEntriesResponse(self: *Self, peer: ServerId, resp: rpc.AppendEntriesResponse) !void {
            if (self.role != .leader) return;
            if (resp.term > self.current_term) {
                try self.stepDown(resp.term);
                return;
            }
            const peer_idx = for (self.config.peers, 0..) |p, i| { if (p == peer) break i; } else return;
            if (peer_idx >= self.next_index.len) return;

            if (resp.success) {
                self.match_index[peer_idx] = self.log.lastIndex();
                self.next_index[peer_idx] = self.match_index[peer_idx] + 1;
                self.advanceCommitIndex();
            } else if (self.next_index[peer_idx] > 1) {
                if (resp.conflict_term > 0) {
                    var new_next = self.next_index[peer_idx] - 1;
                    while (new_next > 0 and self.log.termAt(new_next) >= resp.conflict_term) new_next -= 1;
                    self.next_index[peer_idx] = new_next + 1;
                } else {
                    self.next_index[peer_idx] -= 1;
                }
            }
        }

        pub fn handleRequestVoteResponse(self: *Self, _: ServerId, resp: rpc.RequestVoteResponse) !void {
            if (self.role != .candidate) return;
            if (resp.term > self.current_term) {
                try self.stepDown(resp.term);
                return;
            }
            if (resp.vote_granted) {
                self.votes_received += 1;
                if (self.haveQuorum()) {
                    try self.becomeLeader();
                }
            }
        }

        pub fn becomeLeader(self: *Self) !void {
            self.role = .leader;
            self.voted_for = null;
            const last_idx = self.log.lastIndex() + 1;
            for (self.next_index, 0..) |*ni, i| {
                ni.* = last_idx;
                self.match_index[i] = 0;
            }
            try self.broadcastAppendEntries(0);
        }

        fn haveQuorum(self: *const Self) bool {
            // Majority of the cluster (self + peers)
            const needed = (self.config.peers.len + 2) / 2;
            return self.votes_received >= needed;
        }

        fn advanceCommitIndex(self: *Self) void {
            const peers = self.config.peers;
            const quorum = (peers.len + 2) / 2;
            var n = self.log.lastIndex();
            while (n > self.commit_index) {
                if (self.log.termAt(n) != self.current_term) { if (n == 0) break; n -= 1; continue; }
                var count: usize = 1;
                for (self.match_index) |mi| { if (mi >= n) count += 1; }
                if (count >= quorum) { self.commit_index = n; self.applyCommittedEntries(); break; }
                if (n == 0) break; n -= 1;
            }
        }
    };
}
