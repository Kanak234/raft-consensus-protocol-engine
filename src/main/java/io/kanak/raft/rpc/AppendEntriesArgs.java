package io.kanak.raft.rpc;

import io.kanak.raft.model.LogEntry;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
 */
public record AppendEntriesArgs(
        long term,
        String leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,
        long leaderCommit
) implements Serializable {

    public AppendEntriesArgs {
        Objects.requireNonNull(leaderId, "leaderId must not be null");
        entries = (entries == null) ? Collections.emptyList() : List.copyOf(entries);
    }

    public boolean isHeartbeat() {
        return entries.isEmpty();
    }
}
