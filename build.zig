const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Library module
    const raft_mod = b.addModule("raft", .{
        .root_source_file = b.path("src/raft.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Test definitions: name, source path
    const test_suites = [_]struct { name: []const u8, path: []const u8 }{
        .{ .name = "test-protocol", .path = "src/test_protocol.zig" },
        .{ .name = "test-state", .path = "src/test_state.zig" },
        .{ .name = "test-election", .path = "src/test_election.zig" },
        .{ .name = "test-replication", .path = "src/test_replication.zig" },
        .{ .name = "test-integration", .path = "src/test_integration.zig" },
        .{ .name = "test-snapshot", .path = "src/test_snapshot.zig" },
        .{ .name = "test-membership", .path = "src/test_membership.zig" },
        .{ .name = "test-pre-vote", .path = "src/test_pre_vote.zig" },
        .{ .name = "test-read-index", .path = "src/test_read_index.zig" },
        .{ .name = "test-pipeline", .path = "src/test_pipeline.zig" },
    };

    // Top-level "test" step that depends on all suites
    const all_tests = b.step("test", "Run all test suites");

    for (test_suites) |t| {
        const test_mod = b.createModule(.{
            .root_source_file = b.path(t.path),
            .target = target,
            .optimize = optimize,
        });
        test_mod.addImport("raft", raft_mod);

        const test_exe = b.addTest(.{
            .name = t.name,
            .root_module = test_mod,
        });
        const run_test = b.addRunArtifact(test_exe);

        // Register as individual step
        _ = b.step(t.name, t.name);
        // Make top-level test depend on this suite
        all_tests.dependOn(&run_test.step);
    }

    // Top-level "zig build" = build the library module
    const lib_step = b.step("lib", "Build the raft library");
    _ = lib_step;
}
