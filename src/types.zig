//! Core type definitions for the Raft library.
//!
//! Also defines the cluster configuration (§6) and log entry type discriminant.

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
    pre_candidate,
    candidate,
    leader,

    pub fn label(self: Role) []const u8 {
        return switch (self) {
            .follower => "FOLLOWER",
            .pre_candidate => "PRE-CANDIDATE",
            .candidate => "CANDIDATE",
            .leader => "LEADER",
        };
    }
};

test "Role label strings" {
    const stdtesting = std.testing;
    try stdtesting.expectEqualStrings("FOLLOWER", Role.follower.label());
    try stdtesting.expectEqualStrings("PRE-CANDIDATE", Role.pre_candidate.label());
    try stdtesting.expectEqualStrings("CANDIDATE", Role.candidate.label());
    try stdtesting.expectEqualStrings("LEADER", Role.leader.label());
}

/// Discriminates log entries — regular state-machine commands vs configuration entries (§6).
pub const EntryType = enum(u8) {
    command,
    configuration,
};

/// A cluster configuration — the set of server IDs that are Raft voting members.
///
/// Serialized on the wire / in log entries as little-endian u64s.
pub const ClusterConfig = struct {
    /// All voting members in this config (unsorted, must include self).
    servers: []const ServerId,

    pub fn contains(self: ClusterConfig, id: ServerId) bool {
        for (self.servers) |s| if (s == id) return true;
        return false;
    }

    pub fn len(self: ClusterConfig) usize {
        return self.servers.len;
    }

    /// Majority quorum size for this config.
    pub fn quorum(self: ClusterConfig) usize {
        return (self.servers.len + 2) / 2;
    }

    /// Serialize this config to an allocator-owned byte slice.
    pub fn serialize(self: ClusterConfig, allocator: std.mem.Allocator) ![]u8 {
        const buf = try allocator.alloc(u8, self.servers.len * 8);
        for (self.servers, 0..) |s, i| {
            std.mem.writeInt(u64, buf[i * 8 ..][0..8], s, .little);
        }
        return buf;
    }

    /// Deserialize from a byte slice. Caller owns the returned slice.
    pub fn deserialize(data: []const u8, allocator: std.mem.Allocator) !ClusterConfig {
        if (data.len % 8 != 0) return error.InvalidConfigData;
        const count = data.len / 8;
        const servers = try allocator.alloc(ServerId, count);
        for (servers, 0..) |*s, i| {
            s.* = std.mem.readInt(u64, data[i * 8 ..][0..8], .little);
        }
        return ClusterConfig{ .servers = servers };
    }

    /// Deep-copy the config. Caller owns the returned slice.
    pub fn clone(self: ClusterConfig, allocator: std.mem.Allocator) !ClusterConfig {
        const servers = try allocator.dupe(ServerId, self.servers);
        return ClusterConfig{ .servers = servers };
    }

    /// Free the servers slice.
    pub fn deinit(self: *ClusterConfig, allocator: std.mem.Allocator) void {
        allocator.free(self.servers);
        self.* = undefined;
    }
};

test "ClusterConfig serialization round-trip" {
    const allocator = std.testing.allocator;
    const original = ClusterConfig{ .servers = &.{ 1, 2, 3, 5 } };
    const data = try original.serialize(allocator);
    defer allocator.free(data);

    const restored = try ClusterConfig.deserialize(data, allocator);
    defer restored.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 4), restored.len());
    try std.testing.expect(restored.contains(1));
    try std.testing.expect(restored.contains(5));
    try std.testing.expect(!restored.contains(4));
    try std.testing.expectEqual(@as(usize, 3), restored.quorum());
}

test "ClusterConfig quorum sizes" {
    const c1 = ClusterConfig{ .servers = &.{1} };
    const c2 = ClusterConfig{ .servers = &.{ 1, 2 } };
    const c3 = ClusterConfig{ .servers = &.{ 1, 2, 3 } };
    const c4 = ClusterConfig{ .servers = &.{ 1, 2, 3, 4 } };
    const c5 = ClusterConfig{ .servers = &.{ 1, 2, 3, 4, 5 } };
    try std.testing.expectEqual(@as(usize, 1), c1.quorum());
    try std.testing.expectEqual(@as(usize, 2), c2.quorum());
    try std.testing.expectEqual(@as(usize, 2), c3.quorum());
    try std.testing.expectEqual(@as(usize, 3), c4.quorum());
    try std.testing.expectEqual(@as(usize, 3), c5.quorum());
}
