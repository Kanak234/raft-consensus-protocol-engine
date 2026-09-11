package io.kanak.raft.rpc;

import java.io.Serializable;

/**
 * Result of AppendEntries RPC (§5.3).
 */
public record AppendEntriesReply(
        long term,
        boolean success,
        long matchIndex,
        long conflictIndex,
        long conflictTerm
) implements Serializable {

    public static AppendEntriesReply success(long term, long matchIndex) {
        return new AppendEntriesReply(term, true, matchIndex, 0, 0);
    }

    public static AppendEntriesReply failure(long term, long conflictIndex, long conflictTerm) {
        return new AppendEntriesReply(term, false, 0, conflictIndex, conflictTerm);
    }
}
