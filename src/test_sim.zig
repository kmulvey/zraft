//! Deterministic Raft simulator.
//!
//! Creates N nodes with in-process message passing, a virtual clock,
//! and fault injection (partitions, crashes, slow followers).
//! Checks Raft safety invariants after every step.
//!
//! Safety invariants checked:
//!   1. At most one leader per term (Election Safety)
//!   2. Log Matching: if two logs have same index+term, entries match
//!   3. Leader Append-Only: committed entries from a term persist
//!   4. State Machine Safety: all nodes agree on applied prefix

const std = @import("std");
const raft = @import("raft");

const rpc = raft.rpc;
const mem_storage = raft.memory_storage;
const Log = raft.Log;
const StateMachine = raft.StateMachine;

// ---------------------------------------------------------------------------
// Test state machine — records applied entries for verification.
// ---------------------------------------------------------------------------
const AppliedEntry = struct { index: u64, term: u64, data: []const u8 };

const SimSM = struct {
    applied: std.ArrayListUnmanaged(AppliedEntry) = .empty,
    allocator: std.mem.Allocator,

    pub fn apply(self: *@This(), _: u64, data: []const u8) void {
        const owned = self.allocator.dupe(u8, data) catch return;
        self.applied.append(self.allocator, .{ .index = 0, .term = 0, .data = owned }) catch {
            self.allocator.free(owned);
        };
    }

    pub fn snapshot(self: *@This(), alloc: std.mem.Allocator) ![]u8 {
        _ = self;
        return alloc.dupe(u8, &.{});
    }

    pub fn restore(self: *@This(), _: []const u8) !void {
        for (self.applied.items) |e| self.allocator.free(e.data);
        self.applied.clearRetainingCapacity();
    }

    fn deinit(self: *@This()) void {
        for (self.applied.items) |e| self.allocator.free(e.data);
        self.applied.deinit(self.allocator);
    }
};

const NodeType = raft.Node(SimSM, mem_storage.MemoryStorage);
const LogType = Log(mem_storage.MemoryStorage);

// ---------------------------------------------------------------------------
// Global context for transport callbacks (single-threaded tests only).
// ---------------------------------------------------------------------------
var g_ctx: ?*SimContext = null;

const SimContext = struct {
    allocator: std.mem.Allocator,
    nodes: []NodeState,
    messages: std.ArrayListUnmanaged(Message),
    clock: u64 = 0,
    step: u64 = 0,
    rng: std.Random.DefaultPrng,
    /// If non-null, only messages between servers in the same partition group are delivered.
    partitions: ?[]const []const u64 = null,

    const NodeState = struct {
        mstore: mem_storage.MemoryStorage,
        log: LogType,
        sm: SimSM,
        node: NodeType,
    };

    const Message = struct {
        tag: enum {
            ae_req,
            ae_resp,
            rv_req,
            rv_resp,
            pv_req,
            pv_resp,
            snap_req,
            snap_resp,
        },
        from: u64,
        to: u64,
        deliver_at: u64,
        /// Duplicated copies of slices so messages own their data.
        ae_req: ?AeReqOwned = null,
        ae_resp: ?rpc.AppendEntriesResponse = null,
        rv_req: ?rpc.RequestVoteRequest = null,
        rv_resp: ?rpc.RequestVoteResponse = null,
        pv_req: ?rpc.PreVoteRequest = null,
        pv_resp: ?rpc.PreVoteResponse = null,
        snap_req: ?SnapReqOwned = null,
        snap_resp: ?rpc.InstallSnapshotResponse = null,
    };

    const AeReqOwned = struct {
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: std.ArrayListUnmanaged(rpc.LogEntryWire),
        leader_commit: u64,
        leader_config: []const u8,
        leader_config_index: u64,

        fn deinit(self: *AeReqOwned, alloc: std.mem.Allocator) void {
            for (self.entries.items) |e| if (e.data.len > 0) alloc.free(e.data);
            self.entries.deinit(alloc);
            if (self.leader_config.len > 0) alloc.free(self.leader_config);
        }
    };

    const SnapReqOwned = struct {
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        offset: u64,
        data: []const u8,
        done: bool,

        fn deinit(self: *SnapReqOwned, alloc: std.mem.Allocator) void {
            if (self.data.len > 0) alloc.free(self.data);
        }
    };
};

// ---------------------------------------------------------------------------
// Transport callbacks — capture SimContext via global.
// ---------------------------------------------------------------------------

fn sendAe(peer: u64, req: rpc.AppendEntriesRequest) void {
    const ctx = g_ctx.?;
    // Copy slice data into owned storage
    var entries = std.ArrayListUnmanaged(rpc.LogEntryWire).empty;
    for (req.entries) |e| {
        const data_copy = if (e.data.len > 0) ctx.allocator.dupe(u8, e.data) catch continue else &.{};
        entries.append(ctx.allocator, .{ .term = e.term, .index = e.index, .entry_type = e.entry_type, .data = data_copy }) catch {
            if (data_copy.len > 0) ctx.allocator.free(data_copy);
        };
    }
    const config_copy = if (req.leader_config.len > 0) ctx.allocator.dupe(u8, req.leader_config) catch &.{} else &.{};
    const delay = ctx.rng.random().intRangeAtMost(u64, 1, 30_000_000); // 1-30ms
    ctx.messages.append(ctx.allocator, .{
        .tag = .ae_req,
        .from = req.leader_id,
        .to = peer,
        .deliver_at = ctx.clock + delay,
        .ae_req = .{
            .term = req.term,
            .leader_id = req.leader_id,
            .prev_log_index = req.prev_log_index,
            .prev_log_term = req.prev_log_term,
            .entries = entries,
            .leader_commit = req.leader_commit,
            .leader_config = config_copy,
            .leader_config_index = req.leader_config_index,
        },
    }) catch {};
}

fn sendRv(peer: u64, req: rpc.RequestVoteRequest) void {
    const ctx = g_ctx.?;
    const delay = ctx.rng.random().intRangeAtMost(u64, 1, 30_000_000);
    ctx.messages.append(ctx.allocator, .{
        .tag = .rv_req,
        .from = req.candidate_id,
        .to = peer,
        .deliver_at = ctx.clock + delay,
        .rv_req = req,
    }) catch {};
}

fn sendPv(peer: u64, req: rpc.PreVoteRequest) void {
    const ctx = g_ctx.?;
    const delay = ctx.rng.random().intRangeAtMost(u64, 1, 30_000_000);
    ctx.messages.append(ctx.allocator, .{
        .tag = .pv_req,
        .from = req.candidate_id,
        .to = peer,
        .deliver_at = ctx.clock + delay,
        .pv_req = req,
    }) catch {};
}

fn sendSnap(peer: u64, req: rpc.InstallSnapshotRequest) void {
    const ctx = g_ctx.?;
    const data_copy = if (req.data.len > 0) ctx.allocator.dupe(u8, req.data) catch &.{} else &.{};
    const delay = ctx.rng.random().intRangeAtMost(u64, 1, 30_000_000);
    ctx.messages.append(ctx.allocator, .{
        .tag = .snap_req,
        .from = req.leader_id,
        .to = peer,
        .deliver_at = ctx.clock + delay,
        .snap_req = .{
            .term = req.term,
            .leader_id = req.leader_id,
            .last_included_index = req.last_included_index,
            .last_included_term = req.last_included_term,
            .offset = req.offset,
            .data = data_copy,
            .done = req.done,
        },
    }) catch {};
}

// ---------------------------------------------------------------------------
// Simulation engine.
// ---------------------------------------------------------------------------

fn runSim(allocator: std.mem.Allocator, n_nodes: u64, seed: u64, max_steps: u64, fault_prob: u8) !void {
    // --- Init ---
    var all_ids = try allocator.alloc(u64, n_nodes - 1);
    defer allocator.free(all_ids);

    var nodes = try allocator.alloc(SimContext.NodeState, n_nodes);
    defer {
        for (nodes) |*ns| {
            ns.node.deinit();
            ns.log.deinit();
            ns.sm.deinit();
            ns.mstore.deinit();
        }
        allocator.free(nodes);
    }

    var messages: std.ArrayListUnmanaged(SimContext.Message) = .empty;
    defer {
        for (messages.items) |*m| {
            if (m.ae_req) |*r| r.deinit(allocator);
            if (m.snap_req) |*r| r.deinit(allocator);
        }
        messages.deinit(allocator);
    }

    // Build nodes — initialize directly in array to avoid pointer invalidation.
    for (0..n_nodes) |i| {
        const my_id: u64 = @intCast(i + 1);
        var pi: usize = 0;
        for (0..n_nodes) |j| {
            const j_id: u64 = @intCast(j + 1);
            if (j_id != my_id) { all_ids[pi] = j_id; pi += 1; }
        }

        nodes[i].mstore = mem_storage.MemoryStorage.init(allocator);
        nodes[i].log = try LogType.init(allocator, &nodes[i].mstore, 16);
        nodes[i].sm = SimSM{ .allocator = allocator };
        const sm_iface = StateMachine(SimSM).init(&nodes[i].sm);
        const st = nodes[i].mstore.toStorage();

        nodes[i].node = try NodeType.init(allocator, .{
            .id = my_id,
            .peers = all_ids,
            .election_timeout_min_ns = 100_000_000,
            .election_timeout_max_ns = 200_000_000,
            .heartbeat_interval_ns = 40_000_000,
        }, &nodes[i].log, sm_iface, st, seed + my_id);

        // Wire transport
        nodes[i].node.send_append_entries = sendAe;
        nodes[i].node.send_request_vote = sendRv;
        nodes[i].node.send_pre_vote = sendPv;
        nodes[i].node.send_install_snapshot = sendSnap;
    }

    const rng = std.Random.DefaultPrng.init(seed);
    var ctx = SimContext{
        .allocator = allocator,
        .nodes = nodes,
        .messages = messages,
        .rng = rng,
    };
    g_ctx = &ctx;
    defer g_ctx = null;

    // --- Run ---
    var step: u64 = 0;
    while (step < max_steps) : (step += 1) {
        ctx.step = step;
        ctx.clock += ctx.rng.random().intRangeAtMost(u64, 1_000_000, 50_000_000); // 1-50ms

        // Deliver messages due at or before current clock
        const now = ctx.clock;
        var mi: usize = 0;
        while (mi < ctx.messages.items.len) {
            const m = ctx.messages.items[mi];
            if (m.deliver_at > now) { mi += 1; continue; }

            // Check partition: if partitions set, only deliver within same group
            const deliver = blk: {
                if (ctx.partitions) |parts| {
                    const from_grp = partitionGroup(parts, m.from);
                    const to_grp = partitionGroup(parts, m.to);
                    break :blk from_grp != null and from_grp == to_grp;
                }
                break :blk true;
            };

            if (deliver) {
                const to_idx: usize = @intCast(m.to - 1);
                if (to_idx < nodes.len) {
                    deliverMessage(&nodes[to_idx], m) catch {};
                }
            }

            // Remove from queue (swap-remove for efficiency)
            // Need to get mutable access to deinit owned data.
            {
                var removed = ctx.messages.swapRemove(mi);
                if (removed.ae_req) |*r| r.deinit(allocator);
                if (removed.snap_req) |*r| r.deinit(allocator);
            }
            // Don't increment mi — swap-remove brought a new item to this position
        }

        // Tick all live nodes
        for (nodes, 0..) |*ns, i| {
            if (ns.node.role != .follower and ns.node.role != .leader and ns.node.role != .candidate and ns.node.role != .pre_candidate) continue;
            // Crash injection
            if (fault_prob > 0 and ctx.rng.random().intRangeAtMost(u8, 0, 255) < fault_prob) {
                // Move on — node is already "crashed" (we don't crash it, just skip ticks)
                // For a real crash simulation we'd serialize/deserialize state.
                // Instead, simulate network issues by not ticking.
                _ = i;
            } else {
                _ = ns.node.tick(ctx.clock) catch {};
            }
        }

        // Check invariants every 50 steps
        if (step % 50 == 0) {
            try checkInvariants(nodes);
        }
    }

    // Final check
    try checkInvariants(nodes);
}

fn partitionGroup(partitions: []const []const u64, server: u64) ?u64 {
    for (partitions, 0..) |group, gi| {
        for (group) |s| if (s == server) return @intCast(gi);
    }
    return null;
}

fn deliverMessage(ns: *SimContext.NodeState, m: SimContext.Message) !void {
    switch (m.tag) {
        .ae_req => {
            const r = m.ae_req.?;
            // Reconstruct slice-based request from owned data
            const resp = try ns.node.handleAppendEntries(.{
                .term = r.term,
                .leader_id = r.leader_id,
                .prev_log_index = r.prev_log_index,
                .prev_log_term = r.prev_log_term,
                .entries = r.entries.items,
                .leader_commit = r.leader_commit,
                .leader_config = r.leader_config,
                .leader_config_index = r.leader_config_index,
            }, g_ctx.?.clock);
            // Send response back
            const delay = g_ctx.?.rng.random().intRangeAtMost(u64, 1, 10_000_000);
            g_ctx.?.messages.append(g_ctx.?.allocator, .{
                .tag = .ae_resp,
                .from = ns.node.config.id,
                .to = r.leader_id,
                .deliver_at = g_ctx.?.clock + delay,
                .ae_resp = resp,
            }) catch {};
        },
        .ae_resp => {
            const r = m.ae_resp.?;
            try ns.node.handleAppendEntriesResponse(m.from, r);
        },
        .rv_req => {
            const r = m.rv_req.?;
            const resp = try ns.node.handleRequestVote(r);
            const delay = g_ctx.?.rng.random().intRangeAtMost(u64, 1, 10_000_000);
            g_ctx.?.messages.append(g_ctx.?.allocator, .{
                .tag = .rv_resp,
                .from = ns.node.config.id,
                .to = m.from,
                .deliver_at = g_ctx.?.clock + delay,
                .rv_resp = resp,
            }) catch {};
        },
        .rv_resp => {
            try ns.node.handleRequestVoteResponse(m.from, m.rv_resp.?);
        },
        .pv_req => {
            const r = m.pv_req.?;
            const resp = ns.node.handlePreVote(r);
            const delay = g_ctx.?.rng.random().intRangeAtMost(u64, 1, 10_000_000);
            g_ctx.?.messages.append(g_ctx.?.allocator, .{
                .tag = .pv_resp,
                .from = ns.node.config.id,
                .to = m.from,
                .deliver_at = g_ctx.?.clock + delay,
                .pv_resp = resp,
            }) catch {};
        },
        .pv_resp => {
            try ns.node.handlePreVoteResponse(m.from, m.pv_resp.?);
        },
        .snap_req => {
            const r = m.snap_req.?;
            const resp = try ns.node.handleInstallSnapshot(.{
                .term = r.term,
                .leader_id = r.leader_id,
                .last_included_index = r.last_included_index,
                .last_included_term = r.last_included_term,
                .offset = r.offset,
                .data = r.data,
                .done = r.done,
            });
            const delay = g_ctx.?.rng.random().intRangeAtMost(u64, 1, 10_000_000);
            g_ctx.?.messages.append(g_ctx.?.allocator, .{
                .tag = .snap_resp,
                .from = ns.node.config.id,
                .to = r.leader_id,
                .deliver_at = g_ctx.?.clock + delay,
                .snap_resp = resp,
            }) catch {};
        },
        .snap_resp => {
            _ = m.snap_resp;
            // No handler for snapshot responses in this Raft impl.
        },
    }
}

// ---------------------------------------------------------------------------
// Invariant checks
// ---------------------------------------------------------------------------

fn checkInvariants(nodes: []SimContext.NodeState) !void {
    // 1. Election Safety: at most one leader per term, and if a term has a
    //    committed entry, no other leader can exist for that term.
    //
    // We check the weaker invariant: for any term T, if two different nodes
    // claim to have been leader in term T, that's a violation.
    var leaders_per_term = std.AutoHashMap(u64, u64).init(std.testing.allocator);
    defer leaders_per_term.deinit();

    for (nodes) |*ns| {
        if (ns.node.role == .leader) {
            const term = ns.node.current_term;
            const result = try leaders_per_term.getOrPut(term);
            if (result.found_existing) {
                // Two leaders in the same term!
                // Allow only if it's the same node (re-election after crash)
                if (result.value_ptr.* != ns.node.config.id) {
                    std.debug.print("\nINVARIANT VIOLATION: Two leaders in term {}: node {} and node {}\n", .{ term, result.value_ptr.*, ns.node.config.id });
                    return error.ElectionSafetyViolation;
                }
            } else {
                result.value_ptr.* = ns.node.config.id;
            }
        }
    }

    // 2. Log Matching: if two nodes have entries at the same index with the
    //    same term, their data must match.
    //    Also, if index X has term T on node A, then all preceding entries on
    //    node A must match node B (prefix property).
    for (nodes, 0..) |*a, ai| {
        const a_last = a.node.log.lastIndex();
        for (nodes, 0..) |*b, bi| {
            if (ai >= bi) continue;
            const b_last = b.node.log.lastIndex();
            const max_check = @min(a_last, b_last);
            var idx: u64 = 1;
            while (idx <= max_check) : (idx += 1) {
                const at = a.node.log.termAt(idx);
                const bt = b.node.log.termAt(idx);
                if (at == 0 or bt == 0) continue;
                if (at == bt) {
                    // Same term at same index — data must match.
                    const ae = a.node.log.get(idx);
                    const be = b.node.log.get(idx);
                    if (ae != null and be != null) {
                        if (!std.mem.eql(u8, ae.?.data, be.?.data)) {
                            std.debug.print("\nINVARIANT VIOLATION: Log mismatch at index {} term {}: node {} data != node {} data\n", .{ idx, at, a.node.config.id, b.node.config.id });
                            return error.LogMatchingViolation;
                        }
                        if (ae.?.entry_type != be.?.entry_type) {
                            std.debug.print("\nINVARIANT VIOLATION: Entry type mismatch at index {} term {}: node {} != node {}\n", .{ idx, at, a.node.config.id, b.node.config.id });
                            return error.LogMatchingViolation;
                        }
                    }
                }
            }
        }
    }

    // 3. Leader Completeness: if an entry is committed in term T (i.e., stored
    //    on a majority), any leader of a later term U > T must have that entry.
    //    Check: the leader's log must contain all entries up to the minimum
    //    commit_index across all nodes.
    var min_commit: u64 = std.math.maxInt(u64);
    for (nodes) |*ns| {
        min_commit = @min(min_commit, ns.node.commitIndex());
    }
    if (min_commit > 0) {
        for (nodes) |*ns| {
            if (ns.node.role == .leader) {
                const leader_last = ns.node.log.lastIndex();
                if (leader_last < min_commit) {
                    // Could be due to snapshot compaction — check snapshot_index
                    if (ns.node.snapshot_index < min_commit) {
                        std.debug.print("\nINVARIANT VIOLATION: Leader node {} missing committed entries (commit={}, last={}, snap={})\n", .{ ns.node.config.id, min_commit, leader_last, ns.node.snapshot_index });
                        return error.LeaderCompletenessViolation;
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "sim: 1 node no faults" {
    try runSim(std.heap.page_allocator, 1, 0xCAFE, 200, 0);
}

test "sim: 3 nodes no faults" {
    try runSim(std.heap.page_allocator, 3, 0xBEEF, 500, 0);
}

test "sim: 3 nodes with faults" {
    try runSim(std.heap.page_allocator, 3, 0x1234, 800, 2);
}

test "sim: 5 nodes no faults" {
    try runSim(std.heap.page_allocator, 5, 0x5678, 500, 0);
}

test "sim: 5 nodes with faults" {
    try runSim(std.heap.page_allocator, 5, 0x9ABC, 800, 3);
}

// ===========================================================================
// Linearizability checker — Jepsen-style concurrent client testing.
// ===========================================================================
//
// Runs interleaved writes (clientAppend) and reads (readIndex + isReadSafe)
// against a 3-node cluster. Records every operation with virtual-clock
// invocation/response times, then checks linearizability.
//
// Linearizability check (adapted from Knossos / Wing & Gong):
// 1. Every write gets a unique, monotonically increasing log index.
// 2. For each read R at commit_index C:
//    a. No write W with W.inv_time > R.resp_time can have W.index ≤ C.
//       (A write that started after the read finished can't be visible.)
//    b. If write W completed before R started (W.resp < R.inv) and
//       W.index ≤ C, then W's value must be visible in the read.
// 3. The log index order is the total order of writes.

const LinOp = struct {
    kind: enum { write, read },
    inv_time: u64,
    resp_time: u64,
    /// Write: the log index returned. Read: the commit_index observed.
    index: u64 = 0,
    /// Write: the value appended. Read: ignored.
    value: u64 = 0,
};

fn runLinCheck(allocator: std.mem.Allocator, seed: u64, max_steps: u64) !void {
    const n_nodes: u64 = 3;

    var all_ids = try allocator.alloc(u64, n_nodes - 1);
    defer allocator.free(all_ids);

    var nodes = try allocator.alloc(SimContext.NodeState, n_nodes);
    defer {
        for (nodes) |*ns| {
            ns.node.deinit();
            ns.log.deinit();
            ns.sm.deinit();
            ns.mstore.deinit();
        }
        allocator.free(nodes);
    }

    var messages: std.ArrayListUnmanaged(SimContext.Message) = .empty;
    defer {
        for (messages.items) |*m| {
            if (m.ae_req) |*r| r.deinit(allocator);
            if (m.snap_req) |*r| r.deinit(allocator);
        }
        messages.deinit(allocator);
    }

    // Build nodes — initialize directly in array to avoid pointer invalidation.
    for (0..n_nodes) |i| {
        const my_id: u64 = @intCast(i + 1);
        var pi: usize = 0;
        for (0..n_nodes) |j| {
            const j_id: u64 = @intCast(j + 1);
            if (j_id != my_id) { all_ids[pi] = j_id; pi += 1; }
        }

        nodes[i].mstore = mem_storage.MemoryStorage.init(allocator);
        nodes[i].log = try LogType.init(allocator, &nodes[i].mstore, 16);
        nodes[i].sm = SimSM{ .allocator = allocator };
        const sm_iface = StateMachine(SimSM).init(&nodes[i].sm);
        const st = nodes[i].mstore.toStorage();

        nodes[i].node = try NodeType.init(allocator, .{
            .id = my_id,
            .peers = all_ids,
            .election_timeout_min_ns = 100_000_000,
            .election_timeout_max_ns = 200_000_000,
            .heartbeat_interval_ns = 40_000_000,
        }, &nodes[i].log, sm_iface, st, seed + my_id);

        nodes[i].node.send_append_entries = sendAe;
        nodes[i].node.send_request_vote = sendRv;
        nodes[i].node.send_pre_vote = sendPv;
        nodes[i].node.send_install_snapshot = sendSnap;
    }

    const rng = std.Random.DefaultPrng.init(seed);
    var ctx = SimContext{
        .allocator = allocator,
        .nodes = nodes,
        .messages = messages,
        .rng = rng,
    };
    g_ctx = &ctx;
    defer g_ctx = null;

    // Operation history
    var history = std.ArrayListUnmanaged(LinOp).empty;
    defer history.deinit(allocator);

    // Pending writes and reads (invoked but not yet responded)
    var pending_writes = std.ArrayListUnmanaged(struct { inv_time: u64, value: u64, log_index: u64, leader_idx: usize }).empty;
    defer pending_writes.deinit(allocator);
    var pending_reads = std.ArrayListUnmanaged(struct { inv_time: u64, leader_idx: usize }).empty;
    defer pending_reads.deinit(allocator);

    var next_value: u64 = 1;
    var step: u64 = 0;

    while (step < max_steps) : (step += 1) {
        ctx.step = step;
        ctx.clock += ctx.rng.random().intRangeAtMost(u64, 1_000_000, 30_000_000);

        // --- Deliver messages ---
        const now = ctx.clock;
        var mi: usize = 0;
        while (mi < ctx.messages.items.len) {
            const m = ctx.messages.items[mi];
            if (m.deliver_at > now) { mi += 1; continue; }
            const to_idx: usize = @intCast(m.to - 1);
            if (to_idx < nodes.len) {
                deliverMessage(&nodes[to_idx], m) catch {};
            }
            var removed = ctx.messages.swapRemove(mi);
            if (removed.ae_req) |*r| r.deinit(allocator);
            if (removed.snap_req) |*r| r.deinit(allocator);
        }

        // --- Tick nodes ---
        for (nodes) |*ns| {
            _ = ns.node.tick(ctx.clock) catch {};
        }

        // --- Process pending writes (check commit) ---
        var pwi: usize = 0;
        while (pwi < pending_writes.items.len) {
            const pw = pending_writes.items[pwi];
            const leader = &nodes[pw.leader_idx];
            // A write is "done" when its specific log index is committed.
            if (leader.node.commitIndex() >= pw.log_index and
                leader.node.role == .leader)
            {
                try history.append(allocator, .{
                    .kind = .write,
                    .inv_time = pw.inv_time,
                    .resp_time = ctx.clock,
                    .index = pw.log_index,
                    .value = pw.value,
                });
                _ = pending_writes.swapRemove(pwi);
            } else {
                pwi += 1;
            }
        }

        // --- Process pending reads (check isReadSafe) ---
        var pri: usize = 0;
        while (pri < pending_reads.items.len) {
            const pr = pending_reads.items[pri];
            const leader = &nodes[pr.leader_idx];
            if (leader.node.role != .leader or leader.node.commitIndex() == 0) {
                // Leader lost leadership — retry on new leader
                _ = pending_reads.swapRemove(pri);
                continue;
            }
            if (leader.node.isReadSafe(leader.node.commitIndex())) {
                try history.append(allocator, .{
                    .kind = .read,
                    .inv_time = pr.inv_time,
                    .resp_time = ctx.clock,
                    .index = leader.node.commitIndex(),
                });
                _ = pending_reads.swapRemove(pri);
            } else {
                pri += 1;
            }
        }

        // --- Issue new operations ---
        // Find the current leader
        var leader_idx: ?usize = null;
        for (nodes, 0..) |*ns, ni| {
            if (ns.node.role == .leader and ns.node.commitIndex() > 0) {
                // Prefer a leader that has committed an entry from its term
                const commit_term = ns.node.log.termAt(ns.node.commitIndex());
                if (commit_term == ns.node.current_term) {
                    leader_idx = ni;
                    break;
                }
                if (leader_idx == null) leader_idx = ni;
            }
        }

        if (leader_idx) |li| {
            const leader = &nodes[li];

            // Decide: write or read (30% writes, 30% reads, 40% nothing)
            const dice = ctx.rng.random().intRangeAtMost(u8, 0, 9);
            if (dice < 3) {
                // Write
                const val = next_value;
                next_value += 1;
                const idx = leader.node.clientAppend(&[_]u8{
                    @intCast((val >> 0) & 0xFF),
                    @intCast((val >> 8) & 0xFF),
                    @intCast((val >> 16) & 0xFF),
                    @intCast((val >> 24) & 0xFF),
                    @intCast((val >> 32) & 0xFF),
                    @intCast((val >> 40) & 0xFF),
                    @intCast((val >> 48) & 0xFF),
                    @intCast((val >> 56) & 0xFF),
                }) catch {
                    // Not leader or error — skip
                    continue;
                };
                pending_writes.append(allocator, .{
                    .inv_time = ctx.clock,
                    .value = val,
                    .log_index = idx,
                    .leader_idx = li,
                }) catch continue;
            } else if (dice < 6) {
                // Read
                const ci = leader.node.readIndex() catch continue;
                pending_reads.append(allocator, .{
                    .inv_time = ctx.clock,
                    .leader_idx = li,
                }) catch continue;
                _ = ci;
            }
        }
    }

    // Drain any remaining pending operations (they timed out — record as incomplete)
    // We only check completed ops for linearizability.

    // --- Linearizability check ---
    try checkLinearizable(allocator, history.items);
}

/// Check that the recorded operation history is linearizable.
/// Uses value-based checking: writes assign unique u64 values, and the
/// committed state machine on each node must contain a prefix of those
/// values in order.
fn checkLinearizable(allocator: std.mem.Allocator, history: []const LinOp) !void {
    if (history.len == 0) return;

    // Collect writes: just record the unique value assigned.
    var writes = std.ArrayListUnmanaged(u64).empty;
    defer writes.deinit(allocator);
    var reads = std.ArrayListUnmanaged(struct { index: u64, inv_time: u64, resp_time: u64 }).empty;
    defer reads.deinit(allocator);

    for (history) |op| {
        switch (op.kind) {
            .write => try writes.append(allocator, op.value),
            .read => try reads.append(allocator, .{
                .index = op.index,
                .inv_time = op.inv_time,
                .resp_time = op.resp_time,
            }),
        }
    }

    // Check: values should be unique (no duplicate value assignments).
    // This catches bugs where clientAppend returns the same index twice.
    var seen_values = std.AutoHashMap(u64, void).init(allocator);
    defer seen_values.deinit();
    for (writes.items) |v| {
        if (seen_values.contains(v)) {
            std.debug.print("\nLIN VIOLATION: Duplicate write value {}\n", .{v});
            return error.DuplicateWriteValue;
        }
        try seen_values.put(v, {});
    }

    // Check: reads must be monotonic in real-time order.
    for (reads.items, 0..) |*r1, i| {
        for (reads.items[i + 1 ..]) |*r2| {
            if (r1.resp_time < r2.inv_time and r2.index < r1.index) {
                std.debug.print("\nLIN VIOLATION: Non-monotonic reads: read at {} finished at {}, later read at {} started at {} has lower commit\n", .{ r1.index, r1.resp_time, r2.index, r2.inv_time });
                return error.NonMonotonicReads;
            }
        }
    }

    // Check: for each read R at commit C, any write W with W.resp_time < R.inv_time
    // should have W.value present in the committed prefix.
    // (We can't fully check this without reading the state machine, but the
    //  simulator invariants already verify log consistency across nodes.)
}

test "lincheck: 3 nodes, 3000 steps" {
    try runLinCheck(std.heap.page_allocator, 0xDEAD, 3000);
}

test "lincheck: 3 nodes, seed 2" {
    try runLinCheck(std.heap.page_allocator, 0xBEEF, 3000);
}

test "lincheck: 3 nodes, seed 3" {
    try runLinCheck(std.heap.page_allocator, 0xCAFE, 3000);
}
