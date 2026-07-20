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
// PreVote (§9.6) — identical wire format to RequestVote, but responders
// do NOT persist votedFor or step down.
// ---------------------------------------------------------------------------

pub const PreVoteRequest = struct {
    /// Proposed next term (current_term + 1 of the candidate).
    term: Term,
    candidate_id: ServerId,
    last_log_index: LogIndex,
    last_log_term: Term,
};

pub const PreVoteResponse = struct {
    /// The responder's current term (so the candidate can detect staleness).
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
    /// The leader's current cluster configuration (§6). Sent as raw serialized bytes
    /// (little-endian u64 server IDs). Follower uses this to learn of pending config changes.
    leader_config: []const u8 = &.{},
    /// The log index at which the leader's current config was committed.
    leader_config_index: LogIndex = 0,
};

/// Wire representation of a log entry for RPC transport.
/// The data slice is owned by the caller and must be copied if needed.
pub const LogEntryWire = struct {
    term: Term,
    index: LogIndex,
    entry_type: types.EntryType,
    data: []const u8,
};

pub const AppendEntriesResponse = struct {
    term: Term,
    success: bool,
    /// The follower's last log index after processing this request.
    /// The leader uses this to update matchIndex correctly.
    last_confirmed_index: LogIndex = 0,
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
