//! Core Raft consensus node.
//!
//! Implements the Raft consensus algorithm per the Ongaro & Ousterhout paper.
//!
//! # Design
//!
//! - **Callers handle networking**: incoming RPCs are delivered via `handle*` methods
//!   which return response structs. Outgoing RPCs are emitted through callback
//!   function pointers that the caller sets.
//! - **Explicit allocators**: no global state.
//! - **Deterministic time**: all timeouts use a monotonic timestamp (ns) passed
//!   into `tick()`.
//!
//! # Usage
//!
//! ```zig
//! var node = try raft.Node(MyStateMachine).init(allocator, config, log, sm);
//! defer node.deinit();
//!
//! // On each loop iteration (~50ms):
//! try node.tick(now_ns);
//!
//! // When an RPC arrives:
//! const resp = node.handleRequestVote(req);
//! // send resp back over the network
//! ```

const std = @import("std");
const types = @import("types.zig");
const rpc = @import("rpc.zig");
const config_mod = @import("config.zig");
const log_mod = @import("log.zig");
const sm = @import("state_machine.zig");

const ServerId = types.ServerId;
const Term = types.Term;
const LogIndex = types.LogIndex;
const Role = types.Role;
const Config = config_mod.Config;
const Log = log_mod.Log;
const LogEntry = log_mod.LogEntry;
const StateMachine = sm.StateMachine;

/// A Raft consensus node parameterised over a concrete state machine type.
pub fn Node(comptime SM: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        config: Config,
        log: *Log,
        state_machine: StateMachine(SM),

        // --- Persistent state (stable storage) ---
        current_term: Term = 0,
        voted_for: ?ServerId = null,

        // --- Volatile state ---
        commit_index: LogIndex = 0,
        last_applied: LogIndex = 0,
        role: Role = .follower,

        // --- Leader volatile state ---
        next_index: []LogIndex = &.{},
        match_index: []LogIndex = &.{},

        // --- Timing (monotonic ns) ---
        last_heartbeat_ns: u64 = 0,
        election_start_ns: u64 = 0,
        /// Randomised election timeout for the current term (ns).
        election_timeout_ns: u64 = 0,

        /// Random state for jitter (caller provides seed).
        rng: std.Random.DefaultPrng,

        // --- Outgoing message callbacks ---
        // The caller sets these after init. The node calls them when it needs
        // to send a message to a peer.

        /// Called when the leader needs to send an AppendEntries RPC.
        send_append_entries: ?*const fn (peer: ServerId, req: rpc.AppendEntriesRequest) void = null,
        /// Called when a candidate needs to send a RequestVote RPC.
        send_request_vote: ?*const fn (peer: ServerId, req: rpc.RequestVoteRequest) void = null,
        /// Called when the leader needs to send an InstallSnapshot RPC.
        send_install_snapshot: ?*const fn (peer: ServerId, req: rpc.InstallSnapshotRequest) void = null,

        // ===================================================================
        // Init / Deinit
        // ===================================================================

        pub fn init(
            allocator: std.mem.Allocator,
            config: Config,
            log: *Log,
            state_machine: StateMachine(SM),
            rng_seed: u64,        ) !Self {
            var self = Self{
                .allocator = allocator,
                .config = config,
                .log = log,
                .state_machine = state_machine,
                .next_index = try allocator.alloc(LogIndex, config.peers.len),
                .match_index = try allocator.alloc(LogIndex, config.peers.len),
                .rng = std.Random.DefaultPrng.init(rng_seed),
            };
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

        /// Advance time. `now_ns` is a monotonic timestamp in nanoseconds.
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
            // Check if we won (votes counted externally via becomeLeader)
            if (now_ns < self.election_start_ns + self.election_timeout_ns) return;
            // Timeout — start a new election
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
            self.role = .candidate;
            self.voted_for = self.config.id;
            self.election_start_ns = now_ns;
            self.randomiseElectionTimeout();

            // Send RequestVote to all peers
            const last_index = self.log.lastIndex();
            const last_term = self.log.termAt(last_index);
            const req = rpc.RequestVoteRequest{
                .term = self.current_term,
                .candidate_id = self.config.id,
                .last_log_index = last_index,
                .last_log_term = last_term,
            };
            if (self.send_request_vote) |send| {
                for (self.config.peers) |peer| {
                    send(peer, req);
                }
            }
        }

        // ===================================================================
        // RPC Handlers (called by the transport layer)
        // ===================================================================

        /// Handle an incoming RequestVote RPC. Returns the response to send back.
        pub fn handleRequestVote(self: *Self, req: rpc.RequestVoteRequest) rpc.RequestVoteResponse {
            // §5.2: Reply false if term < currentTerm
            if (req.term < self.current_term) {
                return .{ .term = self.current_term, .vote_granted = false };
            }

            // §5.1: If RPC term > currentTerm, step down
            if (req.term > self.current_term) {
                self.stepDown(req.term);
            }

            // §5.4.1: Voter must be at least as up-to-date
            const granted = blk: {
                // Already voted for another candidate this term?
                if (self.voted_for != null and self.voted_for != req.candidate_id) break :blk false;

                // Log must be at least as up-to-date
                const my_last_idx = self.log.lastIndex();
                const my_last_term = self.log.termAt(my_last_idx);

                if (req.last_log_term < my_last_term) break :blk false;
                if (req.last_log_term == my_last_term and req.last_log_index < my_last_idx) break :blk false;

                // Grant vote
                self.voted_for = req.candidate_id;
                // Reset election timer (we've heard from a valid candidate)
                // The caller should also reset via receiving this response
                break :blk true;
            };

            return .{ .term = self.current_term, .vote_granted = granted };
        }

        /// Handle an incoming AppendEntries RPC (heartbeat or replication).
        /// Returns the response to send back.
        pub fn handleAppendEntries(self: *Self, req: rpc.AppendEntriesRequest) rpc.AppendEntriesResponse {
            // §5.1: Reply false if term < currentTerm
            if (req.term < self.current_term) {
                return .{ .term = self.current_term, .success = false };
            }

            // §5.1: If RPC term >= currentTerm, recognise leader
            if (req.term >= self.current_term) {
                if (self.role != .follower) {
                    self.stepDown(req.term);
                }
                self.current_term = req.term;
                self.voted_for = null;
                // Reset election timer (caller should also manage this)
                self.election_start_ns = 0; // will be reset on next tick
            }

            // §5.3: Log consistency check
            // Reply false if log doesn't contain an entry at prev_log_index
            // whose term matches prev_log_term
            if (req.prev_log_index > 0) {
                if (req.prev_log_index > self.log.lastIndex()) {
                    // Follower's log is shorter — tell leader what we have
                    return .{
                        .term = self.current_term,
                        .success = false,
                        .conflict_index = self.log.lastIndex(),
                        .conflict_term = 0,
                    };
                }
                if (self.log.termAt(req.prev_log_index) != req.prev_log_term) {
                    // Conflict — send the term and first index where that term appears
                    const conflict_term = self.log.termAt(req.prev_log_index);
                    // Walk backward to find first index of this term
                    var conflict_idx = req.prev_log_index;
                    while (conflict_idx > 0 and self.log.termAt(conflict_idx) == conflict_term) {
                        conflict_idx -= 1;
                    }
                    return .{
                        .term = self.current_term,
                        .success = false,
                        .conflict_index = conflict_idx + 1,
                        .conflict_term = conflict_term,
                    };
                }
            }

            // §5.3: Delete conflicting entries and append new ones
            var new_entry_index = req.prev_log_index + 1;
            for (req.entries) |wire_entry| {
                if (wire_entry.index < self.log.len) {
                    // Entry exists — check for conflict
                    if (self.log.termAt(wire_entry.index) != wire_entry.term) {
                        // Conflict: delete from here onward and append leader's entry
                        self.log.truncate(wire_entry.index - 1);
                        // Duplicate data for our log
                        const data_copy = self.allocator.dupe(u8, wire_entry.data) catch {
                            return .{ .term = self.current_term, .success = false };
                        };
                        self.log.entries[self.log.len] = LogEntry{
                            .term = wire_entry.term,
                            .index = wire_entry.index,
                            .data = data_copy,
                        };
                        self.log.len += 1;
                    }
                    // else: already have this entry, skip
                } else {
                    // New entry beyond our log
                    const data_copy = self.allocator.dupe(u8, wire_entry.data) catch {
                        return .{ .term = self.current_term, .success = false };
                    };
                    // Ensure capacity
                    if (self.log.len >= self.log.capacity) {
                        const new_cap = self.log.capacity * 2;
                        self.log.entries = self.allocator.realloc(self.log.entries, new_cap) catch {
                            self.allocator.free(data_copy);
                            return .{ .term = self.current_term, .success = false };
                        };
                        self.log.capacity = new_cap;
                    }
                    self.log.entries[self.log.len] = LogEntry{
                        .term = wire_entry.term,
                        .index = wire_entry.index,
                        .data = data_copy,
                    };
                    self.log.len += 1;
                }
                new_entry_index = wire_entry.index + 1;
            }

            // §5.3: Update commit index
            if (req.leader_commit > self.commit_index) {
                const last_new = if (req.entries.len > 0)
                    req.entries[req.entries.len - 1].index
                else
                    req.prev_log_index;
                self.commit_index = @min(req.leader_commit, last_new);
            }

            // Apply newly committed entries
            self.applyCommittedEntries();

            return .{ .term = self.current_term, .success = true };
        }

        /// Handle an incoming InstallSnapshot RPC.
        pub fn handleInstallSnapshot(self: *Self, req: rpc.InstallSnapshotRequest) rpc.InstallSnapshotResponse {
            if (req.term < self.current_term) {
                return .{ .term = self.current_term, .success = false };
            }
            if (req.term > self.current_term) {
                self.stepDown(req.term);
            }
            // For a minimal implementation, just acknowledge.
            // A full implementation would stream the snapshot to storage.
            return .{ .term = self.current_term, .success = true };
        }

        // ===================================================================
        // Leader: client request handling
        // ===================================================================

        /// Append a command to the leader's log.
        /// Returns the index of the new entry, or error if not leader.
        pub fn clientAppend(self: *Self, data: []const u8) !LogIndex {
            if (self.role != .leader) return error.NotLeader;
            return try self.log.append(self.current_term, data);
        }

        // ===================================================================
        // Internal helpers
        // ===================================================================

        fn stepDown(self: *Self, new_term: Term) void {
            self.current_term = new_term;
            self.role = .follower;
            self.voted_for = null;
            self.election_start_ns = 0; // will be set on next tick
        }

        fn randomiseElectionTimeout(self: *Self) void {
            const range = self.config.election_timeout_max_ns - self.config.election_timeout_min_ns;
            const jitter = self.rng.random().uintAtMost(u64, range);
            self.election_timeout_ns = self.config.election_timeout_min_ns + jitter;
        }

        fn broadcastAppendEntries(self: *Self, _now_ns: u64) !void {
            _ = _now_ns;
            const last_idx = self.log.lastIndex();
            for (self.config.peers, 0..) |peer, i| {
                const next = if (i < self.next_index.len) self.next_index[i] else 1;
                if (next > last_idx) {
                    // Heartbeat only (all entries replicated)
                    const prev_idx = last_idx;
                    const prev_term = self.log.termAt(prev_idx);
                    const req = rpc.AppendEntriesRequest{
                        .term = self.current_term,
                        .leader_id = self.config.id,
                        .prev_log_index = prev_idx,
                        .prev_log_term = prev_term,
                        .entries = &.{},
                        .leader_commit = self.commit_index,
                    };
                    if (self.send_append_entries) |send| send(peer, req);
                } else {
                    // Replicate entries from next to end
                    const prev_idx = next - 1;
                    const prev_term = self.log.termAt(prev_idx);
                    const entries_slice = self.log.sliceFrom(next);
                    // Convert to wire entries
                    var wire_entries: std.ArrayListUnmanaged(rpc.LogEntryWire) = .empty;
                    defer wire_entries.deinit(self.allocator);
                    try wire_entries.ensureTotalCapacity(self.allocator, entries_slice.len);
                    for (entries_slice) |e| {
                        wire_entries.appendAssumeCapacity(.{
                            .term = e.term,
                            .index = e.index,
                            .data = e.data,
                        });
                    }
                    const req = rpc.AppendEntriesRequest{
                        .term = self.current_term,
                        .leader_id = self.config.id,
                        .prev_log_index = prev_idx,
                        .prev_log_term = prev_term,
                        .entries = wire_entries.items,
                        .leader_commit = self.commit_index,
                    };
                    if (self.send_append_entries) |send| send(peer, req);
                }
            }
        }

        fn applyCommittedEntries(self: *Self) void {
            while (self.last_applied < self.commit_index) {
                self.last_applied += 1;
                if (self.last_applied >= self.log.len) break;
                const entry = &self.log.entries[@as(usize, @intCast(self.last_applied))];
                self.state_machine.apply(entry.index, entry.data);
            }
        }

        // ===================================================================
        // Leader: handle AppendEntries responses (optimisation for conflict resolution)
        // ===================================================================

        /// Process an AppendEntries response from a peer.
        /// Call this when a response arrives so the leader can update its state.
        pub fn handleAppendEntriesResponse(self: *Self, peer: ServerId, resp: rpc.AppendEntriesResponse) void {
            if (self.role != .leader) return;
            if (resp.term > self.current_term) {
                self.stepDown(resp.term);
                return;
            }

            // Find the peer's index
            const peer_idx = for (self.config.peers, 0..) |p, i| {
                if (p == peer) break i;
            } else return;
            if (peer_idx >= self.next_index.len) return;

            if (resp.success) {
                // Successful replication — update match/next
                // The peer replicated up to what we sent; we need to know the
                // last entry we sent. A full impl would track this per RPC.
                // For now, optimistically advance.
                self.match_index[peer_idx] = self.log.lastIndex();
                self.next_index[peer_idx] = self.match_index[peer_idx] + 1;

                // §5.3: Advance commit index if a majority has replicated
                self.advanceCommitIndex();
            } else {
                // Rejected — back off next_index for this peer
                if (self.next_index[peer_idx] > 1) {
                    // Use conflict optimisation from the response
                    if (resp.conflict_term > 0) {
                        // Find the last index of the conflict term in our log
                        var new_next = self.next_index[peer_idx] - 1;
                        while (new_next > 0 and self.log.termAt(new_next) >= resp.conflict_term) {
                            new_next -= 1;
                        }
                        self.next_index[peer_idx] = new_next + 1;
                    } else {
                        self.next_index[peer_idx] -= 1;
                    }
                }
            }
        }

        /// Process a RequestVote response from a peer.
        pub fn handleRequestVoteResponse(self: *Self, _: ServerId, resp: rpc.RequestVoteResponse) void {
            if (self.role != .candidate) return;
            if (resp.term > self.current_term) {
                self.stepDown(resp.term);
                return;
            }
            if (!resp.vote_granted) return;

            // The candidate counts votes; this is tracked externally by the caller
            // who aggregates responses. For simplicity, the caller should detect
            // majority and call `becomeLeader`.
        }

        /// Transition to leader (call when majority of votes won).
        pub fn becomeLeader(self: *Self) !void {
            self.role = .leader;
            self.voted_for = null;

            const last_idx = self.log.lastIndex() + 1;
            for (self.next_index, 0..) |*ni, i| {
                ni.* = last_idx;
                self.match_index[i] = 0;
            }

            // Send initial heartbeat to establish authority
            try self.broadcastAppendEntries(0);
        }

        fn advanceCommitIndex(self: *Self) void {
            const peers = self.config.peers;
            const quorum = (peers.len + 2) / 2; // majority including leader

            // §5.3: only advance commit for entries from the current term
            var n = self.log.lastIndex();
            while (n > self.commit_index) {
                if (self.log.termAt(n) != self.current_term) {
                    if (n == 0) break;
                    n -= 1;
                    continue;
                }
                // Count replicas (including self) that have this entry
                var count: usize = 1; // self
                for (self.match_index) |mi| {
                    if (mi >= n) count += 1;
                }
                if (count >= quorum) {
                    self.commit_index = n;
                    self.applyCommittedEntries();
                    break;
                }
                if (n == 0) break;
                n -= 1;
            }
        }
    };
}
