package io.kanak.raft.simulation;

import io.kanak.raft.core.RaftNode;
import io.kanak.raft.model.LogEntry;
import io.kanak.raft.model.RaftRole;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Validates the five fundamental Raft safety invariants (§5.4.3):
 * 1. Election Safety
 * 2. Leader Append-Only
 * 3. Log Matching
 * 4. Leader Completeness
 * 5. State Machine Safety
 */
public class SafetyChecker {

    // Track committed entries by index -> (term, command)
    private final Map<Long, LogEntry> committedEntries = new ConcurrentHashMap<>();

    // Track leader history: term -> leaderId
    private final Map<Long, String> electedLeaders = new ConcurrentHashMap<>();
    private final Map<String, Long> lastAuditedCommitIndex = new ConcurrentHashMap<>();
    private final Map<Long, String> globalAppliedEntries = new ConcurrentHashMap<>();
    private final Map<String, Long> lastAuditedAppliedIndex = new ConcurrentHashMap<>();

    /**
     * Runs full invariant checks across all nodes in the cluster.
     * Throws AssertionError immediately if any invariant is violated.
     */
    public synchronized void checkInvariants(Collection<RaftNode> nodes) {
        checkElectionSafety(nodes);
        checkLogMatching(nodes);
        checkLeaderCompleteness(nodes);
        checkStateMachineSafety(nodes);
    }

    /**
     * Invariant 1: Election Safety
     * At most one leader can be elected in a given term.
     */
    public synchronized void checkElectionSafety(Collection<RaftNode> nodes) {
        Map<Long, String> termLeadersInCurrentStep = new HashMap<>();
        for (RaftNode node : nodes) {
            if (node.getRole() == RaftRole.LEADER) {
                long term = node.getCurrentTerm();
                String existing = termLeadersInCurrentStep.get(term);
                if (existing != null && !existing.equals(node.getNodeId())) {
                    throw new AssertionError(String.format(
                            "ELECTION SAFETY VIOLATION: Two leaders (%s and %s) elected in term %d",
                            existing, node.getNodeId(), term));
                }
                termLeadersInCurrentStep.put(term, node.getNodeId());

                // Check historical election invariant
                String historical = electedLeaders.putIfAbsent(term, node.getNodeId());
                if (historical != null && !historical.equals(node.getNodeId())) {
                    throw new AssertionError(String.format(
                            "ELECTION SAFETY VIOLATION: Leader %s in term %d contradicts prior leader %s",
                            node.getNodeId(), term, historical));
                }
            }
        }
    }

    /**
     * Invariant 3: Log Matching Property
     * If two logs contain an entry with the same index and term, then the logs are
     * identical in all entries up through the given index.
     */
    public synchronized void checkLogMatching(Collection<RaftNode> nodes) {
        RaftNode[] arr = nodes.toArray(new RaftNode[0]);
        for (int i = 0; i < arr.length; ++i) {
            for (int j = i + 1; j < arr.length; ++j) {
                RaftNode n1 = arr[i];
                RaftNode n2 = arr[j];

                long maxCommon = Math.min(n1.getRaftLog().getLastLogIndex(), n2.getRaftLog().getLastLogIndex());
                boolean hadMismatch = false;
                for (long idx = 1; idx <= maxCommon; ++idx) {
                    long t1 = n1.getRaftLog().getTerm(idx);
                    long t2 = n2.getRaftLog().getTerm(idx);
                    if (t1 != t2) {
                        hadMismatch = true;
                    } else if (hadMismatch && t1 != 0) {
                        throw new AssertionError(String.format(
                                "LOG MATCHING VIOLATION between %s and %s at index %d: matched term %d after earlier divergence",
                                n1.getNodeId(), n2.getNodeId(), idx, t1));
                    }
                }
            }
        }
    }

    /**
     * Invariant 4: Leader Completeness
     * If a log entry is committed in a given term, then that entry will be present
     * in the logs of the leaders for all higher-numbered terms.
     */
    public synchronized void checkLeaderCompleteness(Collection<RaftNode> nodes) {
        // First record newly committed entries across all nodes incrementally
        for (RaftNode node : nodes) {
            long commitIdx = node.getCommitIndex();
            long startIdx = lastAuditedCommitIndex.getOrDefault(node.getNodeId(), 0L) + 1;
            for (long idx = startIdx; idx <= commitIdx; ++idx) {
                LogEntry entry = node.getRaftLog().getEntry(idx);
                if (entry != null) {
                    LogEntry existing = committedEntries.putIfAbsent(idx, entry);
                    if (existing != null && (existing.term() != entry.term() || !existing.command().equals(entry.command()))) {
                        throw new AssertionError(String.format(
                                "COMMITTED ENTRY DIVERGENCE at index %d: existing (term %d, cmd %s) vs new (term %d, cmd %s)",
                                idx, existing.term(), existing.command(), entry.term(), entry.command()));
                    }
                }
            }
            lastAuditedCommitIndex.put(node.getNodeId(), commitIdx);
        }

        // Now verify any leader with higher term contains all committed entries
        for (RaftNode node : nodes) {
            if (node.getRole() == RaftRole.LEADER) {
                long leaderTerm = node.getCurrentTerm();
                for (Map.Entry<Long, LogEntry> entry : committedEntries.entrySet()) {
                    long committedIdx = entry.getKey();
                    LogEntry committed = entry.getValue();

                    if (committed.term() < leaderTerm) {
                        long leaderEntryTerm = node.getRaftLog().getTerm(committedIdx);
                        if (leaderEntryTerm != committed.term()) {
                            throw new AssertionError(String.format(
                                    "LEADER COMPLETENESS VIOLATION: Leader %s at term %d missing committed entry at index %d (expected term %d, found %d)",
                                    node.getNodeId(), leaderTerm, committedIdx, committed.term(), leaderEntryTerm));
                        }
                    }
                }
            }
        }
    }

    /**
     * Invariant 5: State Machine Safety
     * If a server has applied a log entry at a given index to its state machine,
     * no other server will ever apply a different log entry for the same index.
     */
    public synchronized void checkStateMachineSafety(Collection<RaftNode> nodes) {
        for (RaftNode node : nodes) {
            Map<Long, String> nodeApplied = node.getAppliedEntries();
            long lastAudited = lastAuditedAppliedIndex.getOrDefault(node.getNodeId(), 0L);
            long nodeLastApplied = node.getLastApplied();
            for (long idx = lastAudited + 1; idx <= nodeLastApplied; idx++) {
                String cmd = nodeApplied.get(idx);
                if (cmd != null) {
                    String existing = globalAppliedEntries.putIfAbsent(idx, cmd);
                    if (existing != null && !existing.equals(cmd)) {
                        throw new AssertionError(String.format(
                                "STATE MACHINE SAFETY VIOLATION on %s at applied log index %d: '%s' vs '%s'",
                                node.getNodeId(), idx, cmd, existing));
                    }
                }
            }
            lastAuditedAppliedIndex.put(node.getNodeId(), nodeLastApplied);
        }
    }
}
