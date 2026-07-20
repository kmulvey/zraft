# Raft — A Raft Consensus Library for Zig

A library implementation of the [Raft consensus algorithm](https://raft.github.io/) (Ongaro & Ousterhout, USENIX ATC '14) written in Zig. Explicit allocators throughout — no global state. Callers provide networking, storage, and state machine implementations.

> **Requires Zig 0.16+**

---

## Features

| Feature | Status |
|---|---|
| Leader election (§5.2) | ✅ |
| Log replication (§5.3) | ✅ |
| Safety + commitment (§5.4, §5.5) | ✅ |
| Persistent storage (WAL + metadata + snapshots) | ✅ |
| Log compaction / snapshots (§7) | ✅ |
| Cluster membership changes (§6) | ✅ Two-phase joint consensus |
| Pre-vote protocol (§9.6) | ✅ Prevents disruptive elections |
| Linearizable reads / ReadIndex (§8) | ✅ |
| Pipelined replication | ✅ Immediate broadcast on append, pipeline on response |
| Client request batching | ✅ `clientAppendBatch()` |
| Config validation | ✅ 7 invariant checks at init |
| No-op entry on leader election (§8) | ✅ Establishes commitment authority |
| Conflict-term optimisation (§5.3) | ✅ Fast log backtracking |

---

## Project Structure

```
src/
├── raft.zig              — Public API module (re-exports everything)
├── config.zig            — Node configuration + validation
├── types.zig             — ServerId, Term, LogIndex, Role, EntryType, ClusterConfig
├── node.zig              — Core Raft node (state machine, RPC handlers, timers)
├── log.zig               — In-memory replicated log backed by persistent storage
├── rpc.zig               — Wire-format message types
├── storage.zig           — Storage trait + LogEntryOwned + SnapshotData
├── state_machine.zig     — StateMachine trait
├── memory_storage.zig    — In-memory storage (tests / reference)
├── file_storage.zig      — File-backed persistent storage
├── test_election.zig     — Election unit tests
├── test_replication.zig  — Replication unit tests
├── test_integration.zig  — Multi-node integration tests
├── test_state.zig        — State initialisation tests
├── test_protocol.zig     — RPC wire format tests
├── test_snapshot.zig     — Snapshot/compaction tests
├── test_membership.zig   — Cluster membership (§6) tests
├── test_pre_vote.zig     — Pre-vote (§9.6) tests
├── test_read_index.zig   — ReadIndex (§8) tests
├── test_pipeline.zig     — Pipelining / batching tests
└── test_validation.zig   — Config validation tests
```

---

## Quick Start

### 1. Implement the State Machine interface

Three methods required:

```zig
const MyStateMachine = struct {
    // Apply a committed log entry. Called synchronously, in order.
    pub fn apply(self: *@This(), index: u64, data: []const u8) void {
        // e.g., apply a state machine command
    }

    // Serialize application state to a byte slice. Caller frees the result.
    pub fn snapshot(self: *@This(), allocator: std.mem.Allocator) ![]u8 {
        return allocator.dupe(u8, self.serialized_state);
    }

    // Restore application state from a byte slice.
    pub fn restore(self: *@This(), data: []const u8) !void {
        // e.g., deserialize and replace application state
    }
};
```

### 2. Implement the Storage interface

Persistent storage for Raft's durable state. Two implementations included:

- **`raft.MemoryStorage`** — in-memory, suitable for tests.
- **`raft.FileStorage`** — file-backed with this on-disk layout:
  ```
  <base_dir>/raft-<my_id>/
      metadata.bin    — 16 bytes: currentTerm (u64 LE) + votedFor (u64 LE, 0xFF… = null)
      wal.bin         — append-only entry log
      snapshot.bin    — 24-byte header + snapshot data
  ```

For production, implement the `raft.Storage(T)` trait:

```zig
const Storage = raft.Storage;

// Your storage type must provide these function signatures
pub fn loadTerm(ptr: *MyStorage) u64;
pub fn storeTerm(ptr: *MyStorage, term: u64) !void;
pub fn loadVotedFor(ptr: *MyStorage) ?u64;
pub fn storeVotedFor(ptr: *MyStorage, voted_for: ?u64) !void;
pub fn loadLastLogIndex(ptr: *MyStorage) u64;
pub fn loadLogEntry(ptr: *MyStorage, index: u64, allocator: std.mem.Allocator) ?storage.LogEntryOwned;
pub fn appendLogEntry(ptr: *MyStorage, entry: storage.LogEntryOwned) !void;
pub fn truncateLog(ptr: *MyStorage, last_kept_index: u64) !void;
pub fn sync(ptr: *MyStorage) !void;
pub fn storeSnapshot(ptr: *MyStorage, last_included_index: u64, last_included_term: u64, data: []const u8) !void;
pub fn loadSnapshot(ptr: *MyStorage, allocator: std.mem.Allocator) ?storage.SnapshotData;
pub fn loadLastSnapshotIndex(ptr: *MyStorage) u64;
pub fn loadLastSnapshotTerm(ptr: *MyStorage) u64;
```

### 3. Build and run

```zig
const std = @import("std");
const raft = @import("raft");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    // Configuration
    const config = raft.Config{
        .id = 1,
        .peers = &.{ 2, 3, 4, 5 },
        .election_timeout_min_ns = 150_000_000,   // 150 ms
        .election_timeout_max_ns = 300_000_000,   // 300 ms
        .heartbeat_interval_ns = 50_000_000,       // 50 ms
    };

    // Storage
    var storage_impl = raft.MemoryStorage.init(allocator);
    defer storage_impl.deinit();
    const storage = storage_impl.toStorage();

    // Log
    var log = try raft.Log(raft.MemoryStorage).init(allocator, &storage_impl, 1024);
    defer log.deinit();

    // State machine
    var sm_impl = MyStateMachine{};
    const sm = raft.StateMachine(MyStateMachine){
        .ptr = &sm_impl,
        .applyFn = MyStateMachine.apply,
        .snapshotFn = MyStateMachine.snapshot,
        .restoreFn = MyStateMachine.restore,
    };

    // Node
    var node = try raft.Node(MyStateMachine, raft.MemoryStorage).init(
        allocator, config, &log, sm, storage, @intCast(std.time.timestamp()),
    );
    defer node.deinit();

    // Wire up transport callbacks
    node.send_append_entries = mySendAppendEntries;
    node.send_request_vote = mySendRequestVote;
    node.send_pre_vote = mySendPreVote;
    node.send_install_snapshot = mySendInstallSnapshot;

    // Main loop
    while (true) {
        try node.tick(std.time.nanoTimestamp());
        // handle incoming RPCs, sleep, etc.
        std.time.sleep(10_000_000); // 10 ms
    }
}
```

### 4. Send callbacks

The transport layer is caller-provided. You must deliver RPC messages to peers:

```zig
fn mySendAppendEntries(peer: raft.ServerId, req: raft.AppendEntriesRequest) void {
    // Serialize req and send to peer over your network
}

fn mySendRequestVote(peer: raft.ServerId, req: raft.RequestVoteRequest) void {
    // ...
}

fn mySendPreVote(peer: raft.ServerId, req: raft.PreVoteRequest) void {
    // ...
}

fn mySendInstallSnapshot(peer: raft.ServerId, req: raft.InstallSnapshotRequest) void {
    // ...
}
```

When you receive a response, call the corresponding handler:

```zig
// Upon receiving an AppendEntriesResponse from peer `2`:
try node.handleAppendEntriesResponse(2, response);
```

---

## API Reference

### `raft.Config`

| Field | Default | Description |
|---|---|---|
| `id: u64` | *(required)* | Unique server ID |
| `peers: []const u64` | *(required)* | IDs of all peers (excluding self) |
| `election_timeout_min_ns` | 150 ms | Base election timeout |
| `election_timeout_max_ns` | 300 ms | Max jitter for election timeout |
| `heartbeat_interval_ns` | 50 ms | Must be strictly < `election_timeout_min_ns` |
| `initial_log_capacity` | 1024 | Pre-allocated log entry slots |

`config.validate() !void` checks all invariants and is called automatically by `Node.init()`.

### `raft.Node(SM, ST)`

| Method | Description |
|---|---|
| `init(allocator, config, log, state_machine, storage, rng_seed) !Self` | Create a node |
| `deinit(self) void` | Free resources |
| `tick(now_ns: u64) !void` | Drive timers (call periodically) |
| `handleRequestVote(req) !ResponseVoteResponse` | §5.2 — incoming vote request |
| `handlePreVote(req) PreVoteResponse` | §9.6 — incoming pre-vote request |
| `handleAppendEntries(req, now_ns) !AppendEntriesResponse` | §5.3 — incoming log replication |
| `handleInstallSnapshot(req) !InstallSnapshotResponse` | §7 — incoming snapshot install |
| `handleAppendEntriesResponse(peer, resp) !void` | Leader: process replication response |
| `handleRequestVoteResponse(peer, resp) !void` | Candidate: process vote response |
| `handlePreVoteResponse(peer, resp) !void` | Pre-candidate: process pre-vote response |
| `clientAppend(data) !LogIndex` | Leader: append and replicate a command |
| `clientAppendBatch(data_items) ![]LogIndex` | Leader: batch-append multiple commands |
| `readIndex() !LogIndex` | §8 — linearizable read; wait for `lastApplied() ≥ result` before reading SM |
| `clusterChangeRequest(new_servers) !void` | §6 — initiate membership change |
| `becomeLeader() !void` | Transition to leader (appends no-op) |
| `takeSnapshot() !void` | §7 — compact log via snapshot |
| `lastApplied() LogIndex` | Highest log index applied to SM |
| `commitIndex() LogIndex` | Highest committed log index |

### `raft.Log(ST)`

| Method | Description |
|---|---|
| `init(allocator, storage, initial_capacity) !Self` | Create log, load from storage |
| `deinit(self) void` | Free entries |
| `lastIndex() LogIndex` | Last entry index (0 if empty) |
| `termAt(index) Term` | Term at index (0 for sentinel / invalid) |
| `append(term, data) !LogIndex` | Append a command entry |
| `appendConfig(term, data) !LogIndex` | Append a config entry |
| `sliceFrom(start) []const LogEntry` | Slice of entries from `start` |
| `get(index) ?*const LogEntry` | Entry at index (null if 0 / out-of-range) |
| `truncate(last_kept) !void` | Remove entries after `last_kept` |
| `replaceWithSnapshot(index, term) !void` | Replace log with compact sentinel |

---

## Raft Paper References

| Section | Feature |
|---|---|
| §5.1 | Raft basics: leader, log, safety |
| §5.2 | Leader election |
| §5.3 | Log replication, conflict-term optimisation |
| §5.4, §5.5 | Safety, commitment rules |
| §6 | Cluster membership changes (joint consensus) |
| §7 | Log compaction / snapshots |
| §8 | Client interaction, linearizable reads (ReadIndex) |
| §9.6 | Pre-vote protocol |

---

## Testing

```bash
zig build test          # Run all 48 tests across 12 suites
zig build test-snapshot # Run a specific suite
```

Test suites: `test-protocol`, `test-state`, `test-election`, `test-replication`, `test-integration`, `test-snapshot`, `test-membership`, `test-pre-vote`, `test-read-index`, `test-pipeline`, `test-validation`.

---

## License

MIT
