package io.kanak.raft.rpc;

import java.io.Serializable;

/**
 * Invoked by candidates to gather votes (§5.2).
 */
public record RequestVoteArgs(
        long term,
        String candidateId,
        long lastLogIndex,
        long lastLogTerm,
        boolean isPreVote
) implements Serializable {

    public RequestVoteArgs(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        this(term, candidateId, lastLogIndex, lastLogTerm, false);
    }
}
