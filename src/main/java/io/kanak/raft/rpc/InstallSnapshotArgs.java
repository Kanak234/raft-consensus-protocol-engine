package io.kanak.raft.rpc;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Invoked by leader to send chunks of a snapshot to a follower (§7).
 */
public record InstallSnapshotArgs(
        long term,
        String leaderId,
        long lastIncludedIndex,
        long lastIncludedTerm,
        byte[] data
) implements Serializable {

    public InstallSnapshotArgs {
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        Objects.requireNonNull(data, "data must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InstallSnapshotArgs that)) return false;
        return term == that.term &&
                lastIncludedIndex == that.lastIncludedIndex &&
                lastIncludedTerm == that.lastIncludedTerm &&
                Objects.equals(leaderId, that.leaderId) &&
                Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(term, leaderId, lastIncludedIndex, lastIncludedTerm);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}
