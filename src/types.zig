//! Core type definitions for the Raft library.

const std = @import("std");

/// Unique identifier for a server in the cluster.
pub const ServerId = u64;

/// Raft term number (monotonically increasing).
pub const Term = u64;

/// Index position in the replicated log (1-based; index 0 is a sentinel).
pub const LogIndex = u64;

/// Node role in the cluster.
pub const Role = enum(u8) {
    follower,
    candidate,
    leader,

    pub fn label(self: Role) []const u8 {
        return switch (self) {
            .follower => "FOLLOWER",
            .candidate => "CANDIDATE",
            .leader => "LEADER",
        };
    }
};

test "Role label strings" {
    const stdtesting = std.testing;
    try stdtesting.expectEqualStrings("FOLLOWER", Role.follower.label());
    try stdtesting.expectEqualStrings("CANDIDATE", Role.candidate.label());
    try stdtesting.expectEqualStrings("LEADER", Role.leader.label());
}
