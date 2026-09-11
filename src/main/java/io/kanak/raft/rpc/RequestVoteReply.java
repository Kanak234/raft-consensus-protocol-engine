package io.kanak.raft.rpc;

import java.io.Serializable;

/**
 * Result of RequestVote RPC (§5.2).
 */
public record RequestVoteReply(
        long term,
        boolean voteGranted
) implements Serializable {
}
