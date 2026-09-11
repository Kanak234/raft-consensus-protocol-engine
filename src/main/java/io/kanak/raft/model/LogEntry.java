package io.kanak.raft.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * An individual log entry containing an index, the election term when it was created,
 * and the replicated state machine command.
 */
public record LogEntry(
        long index,
        long term,
        Command command
) implements Serializable {

    public LogEntry {
        if (index < 1) {
            throw new IllegalArgumentException("Log index must be >= 1, got " + index);
        }
        if (term < 1) {
            throw new IllegalArgumentException("Log term must be >= 1, got " + term);
        }
        Objects.requireNonNull(command, "command must not be null");
    }
}
