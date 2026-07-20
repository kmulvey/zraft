//! Configuration for a Raft node.
//!
//! All durations are in nanoseconds.

pub const Config = struct {
    /// Unique server ID in the cluster.
    id: u64,
    /// IDs of all peers (excluding self).
    peers: []const u64,

    /// Minimum election timeout in ns (base for randomisation).
    election_timeout_min_ns: u64 = 150_000_000, // 150 ms
    /// Maximum election timeout jitter added to the minimum.
    election_timeout_max_ns: u64 = 300_000_000, // 300 ms

    /// Heartbeat interval in ns.
    heartbeat_interval_ns: u64 = 50_000_000, // 50 ms

    /// Initial log capacity (pre-allocated entry slots).
    initial_log_capacity: usize = 1024,
};
