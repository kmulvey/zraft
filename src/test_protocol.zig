//! Protocol RPC tests.

const std = @import("std");
const raft = @import("raft");

test "RequestVote round-trip" {
    const rpc = raft.rpc;
    const req = rpc.RequestVoteRequest{
        .term = 5,
        .candidate_id = 3,
        .last_log_index = 42,
        .last_log_term = 4,
    };
    try std.testing.expectEqual(@as(u64, 5), req.term);
    try std.testing.expectEqual(@as(u64, 3), req.candidate_id);
    try std.testing.expectEqual(@as(u64, 42), req.last_log_index);

    const resp = rpc.RequestVoteResponse{ .term = 5, .vote_granted = true };
    try std.testing.expect(resp.vote_granted);
}

test "AppendEntries wire format" {
    const rpc = raft.rpc;
    const wire_entry = rpc.LogEntryWire{ .term = 1, .index = 1, .data = "hello" };
    try std.testing.expectEqual(@as(u64, 1), wire_entry.term);
    try std.testing.expectEqualStrings("hello", wire_entry.data);
}
