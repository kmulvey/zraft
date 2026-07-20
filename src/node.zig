//! Core Raft consensus node with cluster membership changes (§6).

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
const EntryType = types.EntryType;
const ClusterConfig = types.ClusterConfig;
const Config = config_mod.Config;
const Storage = storage_iface.Storage;
const StateMachine = sm_iface.StateMachine;

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

        current_term: Term = 0,
        voted_for: ?ServerId = null,
        commit_index: LogIndex = 0,
        last_applied: LogIndex = 0,
        role: Role = .follower,

        /// Snapshot index/term — loaded from storage, updated on compact.
        snapshot_index: LogIndex = 0,
        snapshot_term: Term = 0,

        // ---- Cluster membership (§6) ----
        /// The currently effective (committed) cluster config.
        active_config: ClusterConfig = ClusterConfig{ .servers = &.{} },
        /// During joint consensus, the new config being phased in (C_new).
        joint_config: ?ClusterConfig = null,
        /// Log index at which `active_config` was committed.
        active_config_index: LogIndex = 0,

        votes_received: u32 = 0,
        next_index: []LogIndex = &.{},
        match_index: []LogIndex = &.{},
        /// Parallel to next_index/match_index: server IDs in current peer set.
        peer_ids: []ServerId = &.{},

        last_heartbeat_ns: u64 = 0,
        election_start_ns: u64 = 0,
        election_timeout_ns: u64 = 0,

        rng: std.Random.DefaultPrng,

        send_append_entries: ?*const fn (peer: ServerId, req: rpc.AppendEntriesRequest) void = null,
        send_request_vote: ?*const fn (peer: ServerId, req: rpc.RequestVoteRequest) void = null,
        send_install_snapshot: ?*const fn (peer: ServerId, req: rpc.InstallSnapshotRequest) void = null,

        // ===================================================================
        // Init
        // ===================================================================

        pub fn init(allocator: std.mem.Allocator, config: Config, log: *LogType, state_machine: StateMachine(SM), storage: StorageType, rng_seed: u64) !Self {
            // Build initial cluster config from config.peers + self
            const initial_peers = try allocator.alloc(ServerId, config.peers.len + 1);
            initial_peers[0] = config.id;
            @memcpy(initial_peers[1..], config.peers);
            const active_config = ClusterConfig{ .servers = initial_peers };

            const peer_ids = try allocator.dupe(ServerId, config.peers);
            const next_index = try allocator.alloc(LogIndex, config.peers.len);
            const match_index = try allocator.alloc(LogIndex, config.peers.len);

            var self = Self{
                .allocator = allocator,
                .config = config,
                .log = log,
                .state_machine = state_machine,
                .storage = storage,
                .active_config = active_config,
                .peer_ids = peer_ids,
                .next_index = next_index,
                .match_index = match_index,
                .rng = std.Random.DefaultPrng.init(rng_seed),
            };
            self.current_term = self.storage.loadTerm();
            self.voted_for = self.storage.loadVotedFor();
            self.snapshot_index = self.storage.loadLastSnapshotIndex();
            self.snapshot_term = self.storage.loadLastSnapshotTerm();
            self.randomiseElectionTimeout();
            return self;
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.next_index);
            self.allocator.free(self.match_index);
            self.allocator.free(self.peer_ids);
            self.active_config.deinit(self.allocator);
            if (self.joint_config) |*jc| jc.deinit(self.allocator);
        }

        // ===================================================================
        // Tick
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
        // Election (§5.2)
        // ===================================================================

        fn startElection(self: *Self, now_ns: u64) !void {
            self.current_term += 1;
            try self.storage.storeTerm(self.current_term);
            self.role = .candidate;
            self.voted_for = self.config.id;
            try self.storage.storeVotedFor(self.voted_for);
            self.votes_received = 1;
            self.election_start_ns = now_ns;
            self.randomiseElectionTimeout();

            if (self.haveQuorum()) { try self.becomeLeader(); return; }

            const last_index = self.log.lastIndex();
            const last_term = self.log.termAt(last_index);
            // §6: during joint consensus, candidate requests votes from C_old (active) only.
            // New servers in joint config do not vote until transition is complete.
            if (self.send_request_vote) |send| {
                for (self.active_config.servers) |srv| {
                    if (srv == self.config.id) continue;
                    send(srv, .{
                        .term = self.current_term, .candidate_id = self.config.id,
                        .last_log_index = last_index, .last_log_term = last_term,
                    });
                }
            }
        }

        // ===================================================================
        // RPC Handlers
        // ===================================================================

        pub fn handleRequestVote(self: *Self, req: rpc.RequestVoteRequest) !rpc.RequestVoteResponse {
            if (req.term < self.current_term) return .{ .term = self.current_term, .vote_granted = false };
            if (req.term > self.current_term) try self.stepDown(req.term);

            // §6: A server only grants a vote if the candidate is in its current config.
            if (!self.active_config.contains(req.candidate_id)) {
                return .{ .term = self.current_term, .vote_granted = false };
            }

            const granted = blk: {
                if (self.voted_for != null and self.voted_for != req.candidate_id) break :blk false;
                if (req.last_log_term < self.snapshot_term) break :blk false;
                if (req.last_log_term == self.snapshot_term and req.last_log_index < self.snapshot_index) break :blk false;
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

        pub fn handleAppendEntries(self: *Self, req: rpc.AppendEntriesRequest, now_ns: u64) !rpc.AppendEntriesResponse {
            if (req.term < self.current_term) return .{ .term = self.current_term, .success = false };

            if (req.term >= self.current_term) {
                if (self.role != .follower) try self.stepDown(req.term);
                self.current_term = req.term;
                try self.storage.storeTerm(self.current_term);
                self.voted_for = null;
                try self.storage.storeVotedFor(self.voted_for);
                self.election_start_ns = now_ns;
            }

            // §6: Follower learns the leader's config from AppendEntries.
            // If the leader has a config at a higher index than ours, update our config.
            if (req.leader_config_index > self.active_config_index and req.leader_config.len > 0) {
                const leader_config = try ClusterConfig.deserialize(req.leader_config, self.allocator);
                self.active_config.deinit(self.allocator);
                self.active_config = leader_config;
                self.active_config_index = req.leader_config_index;
                // Drop any stale joint config
                if (self.joint_config) |*jc| {
                    jc.deinit(self.allocator);
                    self.joint_config = null;
                }
            }

            if (req.prev_log_index > 0) {
                if (req.prev_log_index > self.log.lastIndex()) {
                    return .{ .term = self.current_term, .success = false, .conflict_index = self.log.lastIndex(), .conflict_term = 0 };
                }
                if (self.log.termAt(req.prev_log_index) != req.prev_log_term) {
                    const conflict_term = self.log.termAt(req.prev_log_index);
                    var conflict_idx = req.prev_log_index;
                    while (conflict_idx > 0 and self.log.termAt(conflict_idx) == conflict_term) conflict_idx -= 1;
                    return .{ .term = self.current_term, .success = false, .conflict_index = conflict_idx + 1, .conflict_term = conflict_term };
                }
            }

            for (req.entries) |wire_entry| {
                if (wire_entry.index < self.log.len) {
                    if (self.log.termAt(wire_entry.index) != wire_entry.term) {
                        try self.log.truncate(wire_entry.index - 1);
                        _ = try self.log.appendEntry(wire_entry.term, wire_entry.entry_type, wire_entry.data);
                    }
                } else {
                    _ = try self.log.appendEntry(wire_entry.term, wire_entry.entry_type, wire_entry.data);
                }
            }

            if (req.leader_commit > self.commit_index) {
                const last_new = if (req.entries.len > 0) req.entries[req.entries.len - 1].index else req.prev_log_index;
                self.commit_index = @min(req.leader_commit, last_new);
            }
            self.applyCommittedEntries();
            return .{ .term = self.current_term, .success = true, .last_confirmed_index = self.log.lastIndex() };
        }

        pub fn handleInstallSnapshot(self: *Self, req: rpc.InstallSnapshotRequest) !rpc.InstallSnapshotResponse {
            if (req.term < self.current_term) return .{ .term = self.current_term, .success = false };
            if (req.term > self.current_term) try self.stepDown(req.term);

            if (req.done) {
                try self.storage.storeSnapshot(req.last_included_index, req.last_included_term, req.data);
                try self.state_machine.restore(req.data);
                try self.log.replaceWithSnapshot(req.last_included_index, req.last_included_term);
                self.snapshot_index = req.last_included_index;
                self.snapshot_term = req.last_included_term;
                self.last_applied = req.last_included_index;
                self.commit_index = @max(self.commit_index, req.last_included_index);
                self.election_start_ns = 0;
            }
            return .{ .term = self.current_term, .success = true };
        }

        // ===================================================================
        // Snapshot (§7)
        // ===================================================================

        pub fn takeSnapshot(self: *Self) !void {
            if (self.last_applied <= self.snapshot_index) return;

            const snap_index = self.last_applied;
            const snap_term = self.log.termAt(snap_index);
            const data = try self.state_machine.snapshot(self.allocator);
            defer self.allocator.free(data);

            try self.storage.storeSnapshot(snap_index, snap_term, data);
            try self.log.replaceWithSnapshot(snap_index, snap_term);
            self.snapshot_index = snap_index;
            self.snapshot_term = snap_term;
        }

        // ===================================================================
        // Client requests
        // ===================================================================
        // ---

        pub fn clientAppend(self: *Self, data: []const u8) !LogIndex {
            if (self.role != .leader) return error.NotLeader;
            return try self.log.append(self.current_term, data);
        }

        // ---- Cluster membership change (§6) ----

        /// Initiate a cluster membership change as leader.
        /// Phase 1: append a C_old,new (joint consensus) entry.
        /// Once committed, phase 2 automatically appends a C_new entry.
        /// `new_servers` is the desired final set of voting members.
        pub fn clusterChangeRequest(self: *Self, new_servers: []const ServerId) !void {
            if (self.role != .leader) return error.NotLeader;
            if (self.joint_config != null) return error.AlreadyInJointConsensus;

            // Serialize the new config
            const new_config = ClusterConfig{ .servers = new_servers };
            const data = try new_config.serialize(self.allocator);
            defer self.allocator.free(data);

            // Append the C_old,new (joint) config entry
            _ = try self.log.appendEntry(self.current_term, .configuration, data);
        }

        /// Helper: get the union of all servers in the current effective configs.
        /// Caller must free the returned slice.
        fn allServerIds(self: *const Self, allocator: std.mem.Allocator) ![]ServerId {
            const active_servers = self.active_config.servers;
            const joint_servers = if (self.joint_config) |jc| jc.servers else &.{};

            // Count unique servers
            var unique = std.ArrayListUnmanaged(ServerId).empty;
            defer unique.deinit(allocator);
            for (active_servers) |s| try unique.append(allocator, s);
            for (joint_servers) |s| {
                var found = false;
                for (active_servers) |a| if (a == s) { found = true; break; };
                if (!found) try unique.append(allocator, s);
            }
            return unique.toOwnedSlice(allocator);
        }

        /// Get the peer IDs for the current peer set (excluding self).
        fn getPeerIds(self: *const Self) []const ServerId {
            return self.peer_ids;
        }

        /// Rebuild peer tracking arrays when membership changes.
        fn rebuildPeerTracking(self: *Self) !void {
            // Collect all unique peer IDs (union of active + joint, excluding self)
            const all = try self.allServerIds(self.allocator);
            defer self.allocator.free(all);

            var new_peers = std.ArrayListUnmanaged(ServerId).empty;
            defer new_peers.deinit(self.allocator);
            for (all) |s| if (s != self.config.id) try new_peers.append(self.allocator, s);

            const n = new_peers.items.len;

            // Allocate new arrays
            const new_peer_ids = try self.allocator.dupe(ServerId, new_peers.items);
            var new_next = try self.allocator.alloc(LogIndex, n);
            var new_match = try self.allocator.alloc(LogIndex, n);

            // Copy over existing next/match for servers that persist, init new ones
            const last_idx = self.log.lastIndex() + 1;
            for (new_peer_ids, 0..) |pid, i| {
                const old_idx = for (self.peer_ids, 0..) |op, j| { if (op == pid) break j; } else null;
                if (old_idx) |oi| {
                    new_next[i] = self.next_index[oi];
                    new_match[i] = self.match_index[oi];
                } else {
                    new_next[i] = last_idx;
                    new_match[i] = 0;
                }
            }

            // Replace
            self.allocator.free(self.peer_ids);
            self.allocator.free(self.next_index);
            self.allocator.free(self.match_index);
            self.peer_ids = new_peer_ids;
            self.next_index = new_next;
            self.match_index = new_match;
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
            const send_ae = self.send_append_entries;
            const send_snap = self.send_install_snapshot;

            // Serialize our current config to send to followers
            const config_data = try self.active_config.serialize(self.allocator);
            defer self.allocator.free(config_data);

            for (self.peer_ids, 0..) |peer, i| {
                const next = if (i < self.next_index.len) self.next_index[i] else 1;

                // §7: If follower needs a snapshot, send that instead
                if (next <= self.snapshot_index and next > 0) {
                    if (send_snap) |s| {
                        const snap = self.storage.loadSnapshot(self.allocator);
                        if (snap) |snapshot| {
                            var snap_mut = snapshot;
                            defer snap_mut.deinit(self.allocator);
                            s(peer, .{
                                .term = self.current_term,
                                .leader_id = self.config.id,
                                .last_included_index = snap_mut.last_included_index,
                                .last_included_term = snap_mut.last_included_term,
                                .offset = 0,
                                .data = snap_mut.data,
                                .done = true,
                            });
                            self.next_index[i] = snap_mut.last_included_index + 1;
                        }
                    }
                    continue;
                }

                if (next > last_idx) {
                    if (send_ae) |s| s(peer, .{
                        .term = self.current_term, .leader_id = self.config.id,
                        .prev_log_index = last_idx, .prev_log_term = self.log.termAt(last_idx),
                        .entries = &.{}, .leader_commit = self.commit_index,
                        .leader_config = config_data, .leader_config_index = self.active_config_index,
                    });
                } else {
                    const prev_idx = if (next > 0) next - 1 else @as(LogIndex, 0);
                    const entries_slice = self.log.sliceFrom(next);
                    var wire_entries: std.ArrayListUnmanaged(rpc.LogEntryWire) = .empty;
                    defer wire_entries.deinit(self.allocator);
                    try wire_entries.ensureTotalCapacity(self.allocator, entries_slice.len);
                    for (entries_slice) |e| wire_entries.appendAssumeCapacity(.{
                        .term = e.term, .index = e.index,
                        .entry_type = e.entry_type, .data = e.data,
                    });
                    if (send_ae) |s| s(peer, .{
                        .term = self.current_term, .leader_id = self.config.id,
                        .prev_log_index = prev_idx, .prev_log_term = self.log.termAt(prev_idx),
                        .entries = wire_entries.items, .leader_commit = self.commit_index,
                        .leader_config = config_data, .leader_config_index = self.active_config_index,
                    });
                }
            }
        }

        fn applyCommittedEntries(self: *Self) void {
            while (self.last_applied < self.commit_index) {
                self.last_applied += 1;
                if (self.last_applied >= self.log.len) break;
                const entry = &self.log.entries[@as(usize, @intCast(self.last_applied))];

                if (entry.entry_type == .configuration) {
                    // §6: Handle configuration entry
                    self.handleConfigEntry(entry.index, entry.data);
                } else {
                    self.state_machine.apply(self.last_applied, entry.data);
                }
            }
        }

        /// Process a committed configuration entry (§6).
        fn handleConfigEntry(self: *Self, index: LogIndex, data: []const u8) void {
            if (data.len < 1) return;

            _ = data[0]; // phase byte: 0 = joint, 1 = final (not currently used)
            const config_data = data[1..]; // remaining bytes are the serialized config

            if (self.joint_config != null) {
                // We are in joint consensus — this config entry is the final C_new.
                // Replace active config with C_new, drop joint config.
                const parsed = ClusterConfig.deserialize(config_data, self.allocator) catch return;
                self.active_config.deinit(self.allocator);
                if (self.joint_config) |*jc| jc.deinit(self.allocator);
                self.active_config = parsed;
                self.joint_config = null;
                self.active_config_index = index;

                // Rebuild peer tracking for the new config
                self.rebuildPeerTracking() catch {};
            } else {
                // Not in joint — this is a C_old,new (joint) entry.
                const parsed = ClusterConfig.deserialize(config_data, self.allocator) catch return;

                // During joint, active_config stays as C_old.
                // joint_config becomes C_new (the new desired config).
                // active_config_index stays at the C_old entry index.
                self.joint_config = parsed;
                // Note: we DON'T change active_config or active_config_index yet.

                // Rebuild peer tracking to include servers from both configs
                self.rebuildPeerTracking() catch {};
            }
        }

        // ===================================================================
        // Leader response handling
        // ===================================================================

        pub fn handleAppendEntriesResponse(self: *Self, peer: ServerId, resp: rpc.AppendEntriesResponse) !void {
            if (self.role != .leader) return;
            if (resp.term > self.current_term) { try self.stepDown(resp.term); return; }
            const peer_idx = for (self.peer_ids, 0..) |p, i| { if (p == peer) break i; } else return;
            if (peer_idx >= self.next_index.len) return;

            if (resp.success) {
                self.match_index[peer_idx] = resp.last_confirmed_index;
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
            if (resp.term > self.current_term) { try self.stepDown(resp.term); return; }
            if (resp.vote_granted) {
                self.votes_received += 1;
                if (self.haveQuorum()) try self.becomeLeader();
            }
        }

        pub fn becomeLeader(self: *Self) !void {
            self.role = .leader;
            self.voted_for = null;
            const last_idx = self.log.lastIndex() + 1;
            for (self.next_index, 0..) |*ni, i| { ni.* = last_idx; self.match_index[i] = 0; }

            // §8: Append a no-op entry to establish commitment authority for this term.
            _ = self.log.append(self.current_term, &.{}) catch {};
            try self.broadcastAppendEntries(0);
        }

        fn haveQuorum(self: *const Self) bool {
            return self.votes_received >= self.active_config.quorum();
        }

        fn advanceCommitIndex(self: *Self) void {
            const quorum = if (self.joint_config) |jc|
                @max(self.active_config.quorum(), jc.quorum())
            else
                self.active_config.quorum();

            var n = self.log.lastIndex();
            while (n > self.commit_index) {
                if (self.log.termAt(n) != self.current_term) { if (n == 0) break; n -= 1; continue; }
                var count: usize = 1;
                for (self.match_index) |mi| { if (mi >= n) count += 1; }
                if (count >= quorum) {
                    self.commit_index = n;
                    self.applyCommittedEntries();

                    // Check if we just committed a config entry as leader.
                    // If it was a C_old,new entry (joint), append the final C_new.
                    if (self.role == .leader and self.joint_config != null) {
                        // Check if the committed entry is a config entry
                        const committed = self.log.get(n);
                        if (committed) |ce| {
                            if (ce.entry_type == .configuration) {
                                // This was a joint config entry — append the final C_new.
                                // Serialize joint_config as a "final" entry (phase=1).
                                const data = self.joint_config.?.serialize(self.allocator) catch return;
                                defer self.allocator.free(data);
                                // Prepend phase byte
                                var buf = std.ArrayListUnmanaged(u8).empty;
                                buf.append(self.allocator, 1) catch return;
                                buf.appendSlice(self.allocator, data) catch return;
                                defer buf.deinit(self.allocator);
                                _ = self.log.appendEntry(self.current_term, .configuration, buf.items) catch {};
                            }
                        }
                    }
                    break;
                }
                if (n == 0) break;
                n -= 1;
            }
        }
    };
}
