//! Raft consensus library for Zig.
//!
//! A library implementation of the Raft consensus algorithm (Ongaro & Ousterhout, USENIX ATC '14).
//! Explicit allocators throughout — no global state. Callers provide networking and I/O.
//!
//! Usage:
//! ```zig
//! const raft = @import("raft");
//! var node = try raft.Node(MyStateMachine, MyStorage).init(allocator, config, log, sm, storage, seed);
//! ```

pub const types = @import("types.zig");
pub const rpc = @import("rpc.zig");
pub const config = @import("config.zig");
pub const log = @import("log.zig");
pub const node = @import("node.zig");
pub const state_machine = @import("state_machine.zig");
pub const storage = @import("storage.zig");
pub const memory_storage = @import("memory_storage.zig");
pub const file_storage = @import("file_storage.zig");

pub const Config = config.Config;
pub const LogEntry = log.LogEntry;
pub const Log = log.Log;
pub const Node = node.Node;
pub const StateMachine = state_machine.StateMachine;
pub const Storage = storage.Storage;
pub const MemoryStorage = memory_storage.MemoryStorage;
pub const FileStorage = file_storage.FileStorage;
pub const Role = types.Role;
pub const ServerId = types.ServerId;
pub const Term = types.Term;
pub const LogIndex = types.LogIndex;
pub const EntryType = types.EntryType;
pub const ClusterConfig = types.ClusterConfig;
pub const RequestVoteRequest = rpc.RequestVoteRequest;
pub const RequestVoteResponse = rpc.RequestVoteResponse;
pub const AppendEntriesRequest = rpc.AppendEntriesRequest;
pub const AppendEntriesResponse = rpc.AppendEntriesResponse;
pub const InstallSnapshotRequest = rpc.InstallSnapshotRequest;
pub const InstallSnapshotResponse = rpc.InstallSnapshotResponse;

test {
    std.testing.refAllDecls(@This());
}

const std = @import("std");
