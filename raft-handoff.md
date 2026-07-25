# Raft Library — Known Gaps & Handoff Notes

## Status Summary

Most of the issues captured in the original handoff have been resolved during
the recent correctness pass:

| # | Issue | Status |
|---|---|---|
| 1 | Leader doesn't self-step-down on quorum loss | **Fixed** — `tickLeader()` tracks quorum responses and steps down after `election_timeout_max_ns` |
| 2 | Single-node election segfault | **Fixed** — `Log.appendEntry()` grows the entries array before writing |
| 3 | No `current_leader` field | **Fixed** — `Node.current_leader` is set on election and on valid AppendEntries |
| 4 | Missing `build.zig.zon` | Already present |
| 5 | No send-callback context | Still a design limitation (see below) |

Additional fixes completed:

- Log indexing after snapshots (base-index offset in `Log`).
- Election safety: voter sets, duplicate-vote deduplication, active/joint config
  aware voting, monotonic `match_index`.
- FileStorage rewrite with correct errno handling, looping I/O, WAL fsync, and
  directory fsync after rename.
- Snapshot install/recovery: timer reset, stale-snapshot rejection, chunked
  validation, boot-time state-machine restore.
- §6 membership correctness: vote/request to union of configs, joint-majority
  quorums, leader step-down on self-removal, config replay at boot.
- ReadIndex hardening: per-round responder bitmask prevents duplicate responses
  from spuriously confirming a read quorum.

---

## 1. Leader Self-Step-Down on Quorum Loss

**Status:** Fixed.

`tickLeader()` now tracks the last time a quorum of peers responded via
`handleAppendEntriesResponse()`. If `now_ns - last_quorum_ns >
election_timeout_max_ns`, the leader calls `stepDown(current_term + 1)` and
reverts to follower, preventing split-brain during asymmetric partitions.

---

## 2. Single-Node Election Crash

**Status:** Fixed.

`Log.appendEntry()` now calls `grow()` before writing to `entries[self.len]`,
so single-node clusters (and any election that appends a no-op) no longer
segfault.

---

## 3. `currentLeader` Field

**Status:** Fixed.

`Node` exposes `current_leader: ?ServerId`. It is set to `self.config.id` in
`becomeLeader()` and to `req.leader_id` when a valid `AppendEntries` is
received.

---

## 4. `build.zig.zon`

**Status:** Present.

```zig
.{
    .name = .raft,
    .version = "0.1.0",
    .paths = .{"src", "build.zig"},
}
```

---

## 5. No `SendRpc` Callback Context

**Status:** Unchanged design limitation.

The transport callbacks remain plain function pointers:

```zig
send_append_entries: ?*const fn (peer: ServerId, req: rpc.AppendEntriesRequest) void = null,
```

Callers still need global or module-level state to route RPCs. For DDLL this is
acceptable while multi-node replication is not yet enabled; when it is, the
recommended options are still:

1. Add a context pointer (`*anyopaque`) alongside each callback.
2. Keep module-level routing state.
3. Have `tick()` return pending outbound RPCs.

---

## Remaining Known Limitations

- **Snapshot config persistence:** Snapshots currently store only state-machine
  data. If a membership-change log entry is compacted into a snapshot, the
  configuration is not recovered from the snapshot on restart (it is recovered
  from later log entries if they exist). A future change should embed the
  cluster configuration in the snapshot.
- **Restart simulation:** The deterministic simulator can restart a single node
  in isolation, but full-cluster restart injection during a live simulation
  triggered a memory-corruption crash under `page_allocator` that was not fully
  root-caused. The crash is reproducible only when restarts are interleaved
  with in-flight messages; the isolated restart path is clean.
- **Send-callback context:** As noted above, no per-callback context parameter.
