# Raft Library — Known Gaps & Handoff Notes

## 1. Leader Self-Step-Down on Quorum Loss

**Problem:** When a Raft leader is network-partitioned from its followers, it
continues to accept client writes indefinitely. The followers detect the leader
loss, hold an election, and elect a new leader among themselves—but the old
leader never learns of the new term because it can't receive the higher-term
RPCs (one-way partition). This creates a **split-brain** window:

- Old leader assigns offsets, commits to its local log, and ACKs producers.
- New leader also assigns offsets (with the same or overlapping transaction IDs).
- When the partition heals, the old leader receives an AppendEntries or
  RequestVote from the new leader with a higher term → `stepDown()` fires → the
  old leader truncates its log → those producer ACKs were **false**.

**Location:** `raft/src/node.zig`, function `tickLeader()` (~line 177).

**Fix:** Add a quorum-liveness check to `tickLeader()`. Track the last time the
leader received a successful AppendEntries response from a quorum of peers. If
that time exceeds `election_timeout_max_ns`, call `stepDown()` and convert to
follower.

Rough sketch (≈15 lines):

```zig
// In Self struct, add:
last_quorum_ns: u64 = 0,

// In handleAppendEntriesResponse, on success:
if (self.haveQuorum()) {
    self.last_quorum_ns = now_ns;
}

// In tickLeader():
if (now_ns - self.last_quorum_ns > self.config.election_timeout_max_ns) {
    try self.stepDown(self.current_term + 1);
    return;
}
```

---

## 2. Single-Node Election Crash

**Problem:** A single-node raft cluster (no peers) crashes with a segmentation
fault when `tick()` triggers an election. The crash is in `log.zig:112`:

```
src/raft/src/log.zig:112:25: in appendEntry (raft.zig)
    self.entries[self.len] = LogEntry{ ... };
```

The `grow()` function isn't called before writing to `entries[self.len]` when
`becomeLeader()` appends a no-op entry to the log after winning an election.

**Location:** `raft/src/log.zig`, function `appendEntry()` (~line 97-115).

**Fix:** Ensure `grow()` is called before the write at line 112, or check
capacity before accessing `entries[self.len]`. The `append()` method (line 87)
calls `self.grow()` but `appendEntry()` at line 97 also needs to call it before
the direct write at line 112.

```zig
// In appendEntry(), before line 112:
try self.grow();
```

---

## 3. No `currentLeader` Field

**Problem:** The raft library tracks the elected leader internally during
`RequestVote` / `AppendEntries` processing, but does not expose a
`currentLeader: ?ServerId` field on the node. DDLL's `getPartitionLeader()`
returns `null` for non-leader nodes because it can't read who the current leader
is.

**Location:** `raft/src/node.zig`, the `Self` struct in `Node()` (~line 20-94).

**Fix:** Add a `current_leader: ?ServerId` field, set it on leader election (to
self) and on receiving a valid AppendEntries from a leader (to the leader's ID).

```zig
// In Self struct:
current_leader: ?ServerId = null,

// In becomeLeader():
self.current_leader = self.config.id;

// In handleAppendEntries(), on accepting from a valid leader:
self.current_leader = req.leader_id;
```

---

## 4. Missing `build.zig.zon`

**Problem:** The raft library has no `build.zig.zon` file, so it can only be
used as a local path dependency or via the URL-based fetch that caches the
tarball in `zig-pkg/`. DDLL currently uses URL dependencies that were fetched
and cached during the initial setup, but any fresh clone running `zig build
--fetch` will get the version from the GitHub tarball—not the local copy. This
makes it impossible to develop on both repos simultaneously.

**Location:** Project root — missing `build.zig.zon`.

**Fix:** (Already done by @kmulvey) Add a `build.zig.zon` with the package
identity:

```zig
.{
    .name = .raft,
    .version = "0.1.0",
    .paths = .{"src", "build.zig"},
}
```

---

## 5. No `SendRpc` Callback Context

**Problem:** The raft node's send callbacks
(`send_append_entries`, `send_request_vote`, etc.) are plain function pointers
with no context parameter:

```zig
send_append_entries: ?*const fn (peer: ServerId, req: rpc.AppendEntriesRequest) void = null,
```

This means callers must use global state to route RPCs. DDLL's cluster would
need to buffer outgoing RPCs to a thread-local or module-level buffer, which is
fragile and not idiomatic.

**Location:** `raft/src/node.zig`, fields at ~line 91-94.

**Fix:** Not a bug, but a design limitation. For DDLL's use case, the RPC
callbacks are currently left unset (no-ops). Outgoing Raft RPCs will need to be
implemented when multi-node replication is enabled. Options:

1. Add a context pointer (`*anyopaque`) alongside each callback.
2. Keep the current approach and use per-cluster module-level state.
3. Have tick() return pending RPCs alongside role changes (similar to how
   TickResult already returns role changes).

---

## Summary

| # | Issue | Severity | Affects |
|---|---|---|---|
| 1 | Leader doesn't self-step-down on quorum loss | **High** | Split-brain, false producer ACKs |
| 2 | Single-node election segfault | **Medium** | Tests, single-node dev clusters |
| 3 | No current_leader field | **Low** | getPartitionLeader() returns null |
| 4 | Missing build.zig.zon | Low | Already fixed |
| 5 | No send-callback context | Low | Multi-node replication blocked |
