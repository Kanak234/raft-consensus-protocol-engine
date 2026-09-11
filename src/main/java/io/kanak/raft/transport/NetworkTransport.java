package io.kanak.raft.transport;

import io.kanak.raft.rpc.AppendEntriesArgs;
import io.kanak.raft.rpc.AppendEntriesReply;
import io.kanak.raft.rpc.InstallSnapshotArgs;
import io.kanak.raft.rpc.InstallSnapshotReply;
import io.kanak.raft.rpc.RequestVoteArgs;
import io.kanak.raft.rpc.RequestVoteReply;

import java.util.concurrent.CompletableFuture;

/**
 * Outbound RPC transport abstraction.
 */
public interface NetworkTransport {

    CompletableFuture<RequestVoteReply> sendRequestVote(String sender, String target, RequestVoteArgs args);

    CompletableFuture<AppendEntriesReply> sendAppendEntries(String sender, String target, AppendEntriesArgs args);

    CompletableFuture<InstallSnapshotReply> sendInstallSnapshot(String sender, String target, InstallSnapshotArgs args);
}
