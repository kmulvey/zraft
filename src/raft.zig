//! Raft consensus library for Zig.
//!
//! A library implementation of the Raft consensus algorithm (Ongaro & Ousterhout, USENIX ATC '14).
//! Explicit allocators throughout — no global state. Callers provide networking and I/O.
//!
//! Usage:
//! ```zig
//! const raft = @import("raft");
//! var node = try raft.Node(YourStateMachine).init(allocator, config, log, sm);
//! ```

pub const types = @import("types.zig");
pub const rpc = @import("rpc.zig");
pub const config = @import("config.zig");
pub const log = @import("log.zig");
pub const node = @import("node.zig");
pub const state_machine = @import("state_machine.zig");

pub const Config = config.Config;
pub const LogEntry = log.LogEntry;
pub const Log = log.Log;
pub const Node = node.Node;
pub const StateMachine = state_machine.StateMachine;
pub const Role = types.Role;
pub const ServerId = types.ServerId;
pub const Term = types.Term;
pub const LogIndex = types.LogIndex;
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
