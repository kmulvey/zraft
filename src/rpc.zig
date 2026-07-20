//! RPC message types for the Raft protocol.
//! Wire-format structures sent between nodes over a caller-provided transport.

const types = @import("types.zig");
const ServerId = types.ServerId;
const Term = types.Term;
const LogIndex = types.LogIndex;

// ---------------------------------------------------------------------------
// RequestVote
// ---------------------------------------------------------------------------

pub const RequestVoteRequest = struct {
    term: Term,
    candidate_id: ServerId,
    last_log_index: LogIndex,
    last_log_term: Term,
};

pub const RequestVoteResponse = struct {
    term: Term,
    vote_granted: bool,
};

// ---------------------------------------------------------------------------
// AppendEntries (also used for heartbeats when entries is empty)
// ---------------------------------------------------------------------------

pub const AppendEntriesRequest = struct {
    term: Term,
    leader_id: ServerId,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: []const LogEntryWire,
    leader_commit: LogIndex,
};

/// Wire representation of a log entry for RPC transport.
/// The data slice is owned by the caller and must be copied if needed.
pub const LogEntryWire = struct {
    term: Term,
    index: LogIndex,
    data: []const u8,
};

pub const AppendEntriesResponse = struct {
    term: Term,
    success: bool,
    /// Optimisation: the conflicting index and term the follower suggests
    /// the leader skip past on rejection (raft paper §5.3, last paragraph).
    conflict_index: LogIndex = 0,
    conflict_term: Term = 0,
};

// ---------------------------------------------------------------------------
// InstallSnapshot
// ---------------------------------------------------------------------------

pub const InstallSnapshotRequest = struct {
    term: Term,
    leader_id: ServerId,
    last_included_index: LogIndex,
    last_included_term: Term,
    /// Offset of the first byte in `data` within the snapshot blob.
    offset: u64,
    data: []const u8,
    done: bool,
};

pub const InstallSnapshotResponse = struct {
    term: Term,
    success: bool,
};
