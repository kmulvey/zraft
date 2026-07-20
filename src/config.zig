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

    /// Heartbeat interval in ns. Must be strictly less than `election_timeout_min_ns`.
    heartbeat_interval_ns: u64 = 50_000_000, // 50 ms

    /// Initial log capacity (pre-allocated entry slots).
    initial_log_capacity: usize = 1024,

    /// Validate all configuration invariants. Returns the first error found.
    pub fn validate(self: Config) !void {
        if (self.election_timeout_min_ns == 0) return error.ElectionTimeoutMinZero;
        if (self.election_timeout_max_ns < self.election_timeout_min_ns) return error.ElectionTimeoutMaxLessThanMin;
        if (self.heartbeat_interval_ns == 0) return error.HeartbeatIntervalZero;
        if (self.heartbeat_interval_ns >= self.election_timeout_min_ns) return error.HeartbeatNotLessThanElectionTimeout;
        if (self.initial_log_capacity == 0) return error.InitialLogCapacityZero;
        // Peers must not include self
        for (self.peers) |p| if (p == self.id) return error.PeerEqualsSelf;
        // Peers must have no duplicates
        for (self.peers, 0..) |p, i| {
            for (self.peers[i + 1 ..]) |q| if (q == p) return error.DuplicatePeer;
        }
    }
};
