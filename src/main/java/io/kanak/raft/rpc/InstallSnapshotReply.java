package io.kanak.raft.rpc;

import java.io.Serializable;

/**
 * Result of InstallSnapshot RPC (§7).
 */
public record InstallSnapshotReply(
        long term
) implements Serializable {
}
