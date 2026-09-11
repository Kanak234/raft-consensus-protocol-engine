package io.kanak.raft.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable snapshot containing compacted state machine data and metadata.
 */
public record Snapshot(
        long lastIncludedIndex,
        long lastIncludedTerm,
        byte[] data
) implements Serializable {

    public Snapshot {
        if (lastIncludedIndex < 0) {
            throw new IllegalArgumentException("lastIncludedIndex must be >= 0");
        }
        if (lastIncludedTerm < 0) {
            throw new IllegalArgumentException("lastIncludedTerm must be >= 0");
        }
        Objects.requireNonNull(data, "data must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Snapshot snapshot)) return false;
        return lastIncludedIndex == snapshot.lastIncludedIndex &&
                lastIncludedTerm == snapshot.lastIncludedTerm &&
                Arrays.equals(data, snapshot.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(lastIncludedIndex, lastIncludedTerm);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}
