//! RPC fuzz tests: hammer the public API with random inputs.
//! Verifies no crashes, no leaks (uses testing.allocator), and
//! that internal state consistency invariants hold.

const std = @import("std");
const raft = @import("raft");

const rpc = raft.rpc;
const mem_storage = raft.memory_storage;
const Log = raft.Log;
const StateMachine = raft.StateMachine;

const FuzzSM = struct {
    applied: std.ArrayListUnmanaged(u8) = .empty,

    pub fn apply(self: *@This(), _: u64, data: []const u8) void {
        self.applied.appendSlice(std.testing.allocator, data) catch {};
    }
    pub fn snapshot(_: *@This(), _: std.mem.Allocator) ![]u8 { return &.{}; }
    pub fn restore(self: *@This(), _: []const u8) !void { self.applied.clearRetainingCapacity(); }
};

const NodeType = raft.Node(FuzzSM, mem_storage.MemoryStorage);
const LogType = Log(mem_storage.MemoryStorage);

/// Check node state invariants that must always hold.
fn checkInvariants(node: *NodeType) !void {
    // Term must always be consistent with storage.
    try std.testing.expectEqual(node.current_term, node.storage.loadTerm());
    // voted_for consistency is checked loosely: if storage has a value,
    // node must agree (barring direct mutation by fuzz).
    const stored_vote = node.storage.loadVotedFor();
    if (stored_vote) |sv| {
        // Storage has a vote recorded; if node voted_for differs, it
        // should only be null (cleared by fuzz) or match.
        if (node.voted_for) |nv| {
            try std.testing.expectEqual(sv, nv);
        }
    }
    // commit_index should never exceed the log's last index (unless snapshot).
    const log_last = node.log.lastIndex();
    if (node.commit_index > log_last) {
        try std.testing.expect(node.snapshot_index >= node.commit_index);
    }
    // last_applied should not exceed commit_index.
    try std.testing.expect(node.last_applied <= node.commit_index);
    // Snapshot state is consistent: log base must cover the snapshot index,
    // and commit/applied indices must not lag behind the persisted snapshot.
    try std.testing.expect(node.log.baseIndex() <= node.snapshot_index);
    try std.testing.expect(node.commit_index >= node.snapshot_index);
    try std.testing.expect(node.last_applied >= node.snapshot_index);
    // Role consistency: candidate must have voted for self; leader must know
    // it is the current leader.
    if (node.role == .candidate) {
        try std.testing.expectEqual(node.config.id, node.voted_for);
    }
    if (node.role == .leader) {
        try std.testing.expectEqual(node.config.id, node.current_leader);
    }
    // ReadIndex round monotonicity: confirmed round never runs ahead of pending.
    try std.testing.expect(node.confirmed_read_round <= node.pending_read_round);
}

/// Fuzz a single node through random RPCs, ticks, and client operations.
fn fuzzNode(allocator: std.mem.Allocator, rng: std.Random) !void {
    var mstore = mem_storage.MemoryStorage.init(allocator);
    defer mstore.deinit();
    var log = try LogType.init(allocator, &mstore, 8);
    defer log.deinit();
    var sm_impl = FuzzSM{};
    defer sm_impl.applied.deinit(allocator);
    const sm = StateMachine(FuzzSM).init(&sm_impl);

    var node = try NodeType.init(allocator, .{
        .id = 1,
        .peers = &.{ 2, 3, 4, 5 },
        .election_timeout_min_ns = 10_000_000,
        .election_timeout_max_ns = 20_000_000,
        .heartbeat_interval_ns = 5_000_000,
    }, &log, sm, mstore.toStorage(), rng.int(u64));
    defer node.deinit();

    var clock: u64 = 0;

    for (0..2000) |_| {
        clock += rng.intRangeAtMost(u64, 1_000_000, 30_000_000);

        const op = rng.intRangeAtMost(u8, 0, 18);
        switch (op) {
            0 => _ = node.tick(clock) catch {},
            1 => {
                // handleRequestVote
                const resp = node.handleRequestVote(.{
                    .term = rng.intRangeAtMost(u64, 0, 20),
                    .candidate_id = rng.intRangeAtMost(u64, 1, 10),
                    .last_log_index = rng.intRangeAtMost(u64, 0, 50),
                    .last_log_term = rng.intRangeAtMost(u64, 0, 20),
                }) catch continue;
                // Response term >= our term (or we stepped down)
                _ = resp;
            },
            2 => {
                // handlePreVote
                const resp = node.handlePreVote(.{
                    .term = rng.intRangeAtMost(u64, 0, 20),
                    .candidate_id = rng.intRangeAtMost(u64, 1, 10),
                    .last_log_index = rng.intRangeAtMost(u64, 0, 50),
                    .last_log_term = rng.intRangeAtMost(u64, 0, 20),
                });
                _ = resp;
            },
            3 => {
                // handleAppendEntries (heartbeat variant mostly)
                const n_entries = rng.intRangeAtMost(usize, 0, 5);
                var wire_entries: [5]rpc.LogEntryWire = undefined;
                for (0..n_entries) |i| {
                    wire_entries[i] = .{
                        .term = rng.intRangeAtMost(u64, 0, 20),
                        .index = rng.intRangeAtMost(u64, 1, 50),
                        .entry_type = if (rng.boolean()) .command else .configuration,
                        .data = if (rng.boolean()) "fuzz-cmd" else &.{},
                    };
                }
                const resp = node.handleAppendEntries(.{
                    .term = rng.intRangeAtMost(u64, 0, 20),
                    .leader_id = rng.intRangeAtMost(u64, 1, 10),
                    .prev_log_index = rng.intRangeAtMost(u64, 0, 50),
                    .prev_log_term = rng.intRangeAtMost(u64, 0, 20),
                    .entries = wire_entries[0..n_entries],
                    .leader_commit = rng.intRangeAtMost(u64, 0, 50),
                }, clock) catch continue;
                _ = resp;
            },
            4 => {
                // handleInstallSnapshot
                const resp = node.handleInstallSnapshot(.{
                    .term = rng.intRangeAtMost(u64, 0, 20),
                    .leader_id = rng.intRangeAtMost(u64, 1, 10),
                    .last_included_index = rng.intRangeAtMost(u64, 1, 50),
                    .last_included_term = rng.intRangeAtMost(u64, 0, 20),
                    .offset = 0,
                    .data = if (rng.boolean()) "snap-data" else &.{},
                    .done = rng.boolean(),
                }, rng.int(u64)) catch continue;
                _ = resp;
            },
            5 => {
                // handleAppendEntriesResponse
                node.handleAppendEntriesResponse(
                    rng.intRangeAtMost(u64, 2, 6),
                    .{
                        .term = rng.intRangeAtMost(u64, 0, 20),
                        .success = rng.boolean(),
                        .last_confirmed_index = rng.intRangeAtMost(u64, 0, 50),
                        .conflict_index = rng.intRangeAtMost(u64, 0, 50),
                        .conflict_term = rng.intRangeAtMost(u64, 0, 20),
                    },
                ) catch continue;
            },
            6 => {
                // handleRequestVoteResponse
                node.handleRequestVoteResponse(
                    rng.intRangeAtMost(u64, 1, 10),
                    .{ .term = rng.intRangeAtMost(u64, 0, 20), .vote_granted = rng.boolean() },
                ) catch continue;
            },
            7 => {
                // handlePreVoteResponse
                node.handlePreVoteResponse(
                    rng.intRangeAtMost(u64, 1, 10),
                    .{ .term = rng.intRangeAtMost(u64, 0, 20), .vote_granted = rng.boolean() },
                ) catch continue;
            },
            8 => {
                // clientAppend
                _ = node.clientAppend(if (rng.boolean()) "client-data" else &.{}) catch {};
            },
            9 => {
                // clientAppendBatch
                const n = rng.intRangeAtMost(usize, 0, 4);
                var items: [4][]const u8 = undefined;
                for (0..n) |i| items[i] = if (rng.boolean()) "batch" else &.{};
                const indices = node.clientAppendBatch(items[0..n]) catch |err| blk: {
                    if (err == error.NotLeader) break :blk &.{};
                    // Other errors (e.g. OOM from fuzz stress) are ok to skip
                    break :blk &.{};
                };
                if (indices.len > 0) allocator.free(indices);
            },
            10 => {
                // readIndex
                _ = node.readIndex() catch {};
            },
            11 => {
                // isReadSafe with random commit
                _ = node.isReadSafe(rng.intRangeAtMost(u64, 0, 50));
            },
            12 => {
                // clusterChangeRequest
                const n = rng.intRangeAtMost(usize, 2, 6);
                var servers: [6]u64 = undefined;
                for (0..n) |i| servers[i] = @as(u64, @intCast(i + 1));
                _ = node.clusterChangeRequest(servers[0..n]) catch {};
            },
            13 => {
                // becomeLeader
                _ = node.becomeLeader() catch {};
            },
            14 => {
                // takeSnapshot
                _ = node.takeSnapshot() catch {};
            },
            15 => {
                // lastApplied / commitIndex getters
                _ = node.lastApplied();
                _ = node.commitIndex();
            },
            16 => {
                // Force-step to a specific role for more coverage
                if (rng.boolean()) {
                    node.role = .follower;
                    node.current_leader = null;
                } else if (rng.boolean()) {
                    node.role = .leader;
                    node.current_leader = node.config.id;
                    node.current_term = @max(node.current_term, 1);
                    _ = node.log.append(node.current_term, "force-leader-data") catch {};
                }
            },
            17 => {
                // Simulate a new term from a peer (step down)
                const t = rng.intRangeAtMost(u64, node.current_term, node.current_term + 10);
                if (t > node.current_term) {
                    node.current_term = t;
                    node.role = .follower;
                    node.voted_for = null;
                    _ = node.storage.storeTerm(t) catch {};
                    _ = node.storage.storeVotedFor(null) catch {};
                }
            },
            else => {},
        }
        try checkInvariants(&node);
    }
}

test "fuzz single node: random RPCs + ticks" {
    var prng = std.Random.DefaultPrng.init(0xDEADBEEF);
    const rng = prng.random();
    try fuzzNode(std.testing.allocator, rng);
}

test "fuzz single node: seed 1" {
    var prng = std.Random.DefaultPrng.init(1);
    const rng = prng.random();
    try fuzzNode(std.testing.allocator, rng);
}

test "fuzz single node: seed 42" {
    var prng = std.Random.DefaultPrng.init(42);
    const rng = prng.random();
    try fuzzNode(std.testing.allocator, rng);
}

test "fuzz single node: seed 12345" {
    var prng = std.Random.DefaultPrng.init(12345);
    const rng = prng.random();
    try fuzzNode(std.testing.allocator, rng);
}
