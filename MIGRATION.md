# Migration Guide — Raft Correctness Overhaul

This guide covers the public API changes introduced by the six-phase correctness
overhaul. If you are upgrading from a version before this change, update the
following call sites.

---

## 1. `Storage(T)` now requires `dropLogPrefix`

### What changed

The storage trait gained a new required method:

```zig
pub fn dropLogPrefix(ptr: *MyStorage, last_included_index: LogIndex) !void;
```

This method is called when the log is compacted into a snapshot. It must delete
all persisted log entries with index `<= last_included_index` while keeping any
entries with index `> last_included_index`.

### Why

Previously the log kept all entries in storage even after a snapshot was taken.
The new `dropLogPrefix` method lets storage backends reclaim disk space and
keeps the in-memory and on-disk log prefixes consistent.

### How to update

Add `dropLogPrefix` to your storage implementation. The in-memory reference
implementation shows the expected behavior:

```zig
pub fn dropLogPrefix(ptr: *MyStorage, last_included_index: LogIndex) !void {
    // Remove all entries whose index <= last_included_index.
    var i: usize = 0;
    while (i < ptr.entries.items.len) {
        if (ptr.entries.items[i].index <= last_included_index) {
            _ = ptr.entries.orderedRemove(i);
        } else {
            i += 1;
        }
    }
}
```

For a file-backed implementation, rewrite the WAL so that only entries after
`last_included_index` remain, and persist the snapshot metadata.

### Example diff

```zig
 const Storage = raft.Storage(MyStorage);

 pub fn loadLastSnapshotTerm(ptr: *MyStorage) Term { ... }
+
+pub fn dropLogPrefix(ptr: *MyStorage, last_included_index: LogIndex) !void {
+    // ...implementation...
+}
```

---

## 2. `handleInstallSnapshot` now takes `now_ns`

### What changed

The signature of `handleInstallSnapshot` changed from:

```zig
pub fn handleInstallSnapshot(self: *Self, req: rpc.InstallSnapshotRequest) !rpc.InstallSnapshotResponse
```

to:

```zig
pub fn handleInstallSnapshot(self: *Self, req: rpc.InstallSnapshotRequest, now_ns: u64) !rpc.InstallSnapshotResponse
```

### Why

A valid `InstallSnapshot` RPC from the leader must reset the follower's election
timer. The node needs the current monotonic timestamp to do that safely.

### How to update

Pass the same timestamp you use for `handleAppendEntries` and `tick()`:

```zig
const now = std.time.nanoTimestamp();

// Before:
const resp = try node.handleInstallSnapshot(req);

// After:
const resp = try node.handleInstallSnapshot(req, now);
```

---

## 3. `applyCommittedEntries` is now public

### What changed

`Node.applyCommittedEntries()` is now a public method. You do not need to call
it manually in normal operation — it is invoked automatically by `tick()`,
`handleAppendEntries()`, and the client append methods. It is exposed only for
advanced use cases (e.g., deterministic simulators or custom event loops).

### How to update

No changes are required unless you previously worked around the private method.
If you did, you can now call it directly:

```zig
try node.applyCommittedEntries();
```

---

## 4. Summary checklist

- [ ] Add `dropLogPrefix` to your `Storage(T)` implementation.
- [ ] Update all `handleInstallSnapshot(req)` calls to `handleInstallSnapshot(req, now_ns)`.
- [ ] (Optional) Review any custom event loops and consider whether `applyCommittedEntries()` should be called explicitly.

After making these changes, run your tests. If you use the included test suites,
`zig build test` and `zig build test-sim` should both pass.
