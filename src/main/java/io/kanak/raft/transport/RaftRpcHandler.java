package io.kanak.raft.transport;

import io.kanak.raft.rpc.AppendEntriesArgs;
import io.kanak.raft.rpc.AppendEntriesReply;
import io.kanak.raft.rpc.InstallSnapshotArgs;
import io.kanak.raft.rpc.InstallSnapshotReply;
import io.kanak.raft.rpc.RequestVoteArgs;
import io.kanak.raft.rpc.RequestVoteReply;

/**
 * Inbound RPC handler implemented by each RaftNode.
 */
public interface RaftRpcHandler {

    RequestVoteReply handleRequestVote(RequestVoteArgs args);

    AppendEntriesReply handleAppendEntries(AppendEntriesArgs args);

    InstallSnapshotReply handleInstallSnapshot(InstallSnapshotArgs args);
}
