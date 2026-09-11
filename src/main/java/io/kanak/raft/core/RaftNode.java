package io.kanak.raft.core;

import io.kanak.raft.model.Command;
import io.kanak.raft.model.LogEntry;
import io.kanak.raft.model.RaftRole;
import io.kanak.raft.model.Snapshot;
import io.kanak.raft.rpc.AppendEntriesArgs;
import io.kanak.raft.rpc.AppendEntriesReply;
import io.kanak.raft.rpc.InstallSnapshotArgs;
import io.kanak.raft.rpc.InstallSnapshotReply;
import io.kanak.raft.rpc.RequestVoteArgs;
import io.kanak.raft.rpc.RequestVoteReply;
import io.kanak.raft.statemachine.StateMachine;
import io.kanak.raft.storage.RaftLog;
import io.kanak.raft.transport.NetworkTransport;
import io.kanak.raft.transport.RaftRpcHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Complete Raft consensus node implementing election, log replication,
 * heartbeats, snapshotting, and state machine application.
 */
public class RaftNode implements RaftRpcHandler {

    private final String nodeId;
    private final List<String> peers;
    private final NetworkTransport transport;
    private final StateMachine stateMachine;
    private final RaftLog raftLog;

    // Persistent state on all servers
    private long currentTerm = 0;
    private String votedFor = null;

    // Volatile state on all servers
    private RaftRole role = RaftRole.FOLLOWER;
    private String currentLeader = null;
    private long commitIndex = 0;
    private long lastApplied = 0;

    // Volatile state on leaders
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    // Client execution callbacks: logIndex -> CompletableFuture<String>
    private final Map<Long, CompletableFuture<String>> pendingClientRequests = new ConcurrentHashMap<>();
    private final Map<Long, String> appliedEntries = new ConcurrentHashMap<>();

    // Timing
    private final long minElectionTimeoutMs;
    private final long maxElectionTimeoutMs;
    private final long heartbeatIntervalMs;
    private long electionDeadlineMs = 0;
    private long nextHeartbeatDueMs = 0;
    private long currentTimeMs = 0;
    private final Random rng;

    public RaftNode(String nodeId, List<String> peers, NetworkTransport transport,
                    StateMachine stateMachine, RaftLog raftLog,
                    long minElectionTimeoutMs, long maxElectionTimeoutMs, long heartbeatIntervalMs,
                    long seed) {
        this.nodeId = nodeId;
        this.peers = new ArrayList<>(peers);
        this.transport = transport;
        this.stateMachine = stateMachine;
        this.raftLog = raftLog;
        this.minElectionTimeoutMs = minElectionTimeoutMs;
        this.maxElectionTimeoutMs = maxElectionTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.rng = new Random(seed);

        resetElectionTimeout();
    }

    public synchronized String getNodeId() { return nodeId; }
    public synchronized RaftRole getRole() { return role; }
    public synchronized long getCurrentTerm() { return currentTerm; }
    public synchronized String getVotedFor() { return votedFor; }
    public synchronized String getCurrentLeader() { return currentLeader; }
    public synchronized long getCommitIndex() { return commitIndex; }
    public synchronized long getLastApplied() { return lastApplied; }
    public synchronized RaftLog getRaftLog() { return raftLog; }
    public synchronized StateMachine getStateMachine() { return stateMachine; }
    public Map<Long, String> getAppliedEntries() { return new ConcurrentHashMap<>(appliedEntries); }
    public List<String> getAppliedCommandHistory() {
        List<String> list = new ArrayList<>();
        long max = appliedEntries.keySet().stream().mapToLong(k -> k).max().orElse(0L);
        for (long i = 1; i <= max; i++) {
            if (appliedEntries.containsKey(i)) {
                list.add(appliedEntries.get(i));
            }
        }
        return list;
    }

    /**
     * Discrete-event simulation clock tick.
     */
    public synchronized void tick(long deltaMs) {
        currentTimeMs += deltaMs;

        if (role == RaftRole.LEADER) {
            if (currentTimeMs >= nextHeartbeatDueMs) {
                broadcastAppendEntries(true);
                nextHeartbeatDueMs = currentTimeMs + heartbeatIntervalMs;
            }
        } else {
            if (currentTimeMs >= electionDeadlineMs) {
                startPreVote();
            }
        }
    }

    private void resetElectionTimeout() {
        long duration = minElectionTimeoutMs + (Math.abs(rng.nextLong()) % (maxElectionTimeoutMs - minElectionTimeoutMs + 1));
        electionDeadlineMs = currentTimeMs + duration;
    }

    private void startPreVote() {
        resetElectionTimeout();
        long nextTerm = currentTerm + 1;
        long lastLogIndex = raftLog.getLastLogIndex();
        long lastLogTerm = raftLog.getLastLogTerm();

        int quorum = (peers.size() + 1) / 2 + 1;
        if (quorum <= 1) {
            startElection();
            return;
        }

        int[] preVotesReceived = new int[]{1}; // Vote for self

        for (String peer : peers) {
            RequestVoteArgs args = new RequestVoteArgs(nextTerm, nodeId, lastLogIndex, lastLogTerm, true);
            transport.sendRequestVote(nodeId, peer, args).thenAccept(reply -> {
                synchronized (RaftNode.this) {
                    if (role == RaftRole.LEADER || currentTerm >= nextTerm) {
                        return;
                    }
                    if (reply.term() > currentTerm) {
                        becomeFollower(reply.term());
                        return;
                    }
                    if (reply.voteGranted()) {
                        preVotesReceived[0]++;
                        if (preVotesReceived[0] >= quorum && role != RaftRole.LEADER && role != RaftRole.CANDIDATE) {
                            startElection();
                        }
                    }
                }
            });
        }
    }

    private void startElection() {
        role = RaftRole.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        currentLeader = null;
        resetElectionTimeout();

        long electionTerm = currentTerm;
        long lastLogIndex = raftLog.getLastLogIndex();
        long lastLogTerm = raftLog.getLastLogTerm();

        int quorum = (peers.size() + 1) / 2 + 1;
        if (quorum <= 1) {
            becomeLeader();
            return;
        }

        int[] votesReceived = new int[]{1}; // Vote for self

        for (String peer : peers) {
            RequestVoteArgs args = new RequestVoteArgs(electionTerm, nodeId, lastLogIndex, lastLogTerm, false);
            transport.sendRequestVote(nodeId, peer, args).thenAccept(reply -> {
                synchronized (RaftNode.this) {
                    if (role != RaftRole.CANDIDATE || currentTerm != electionTerm) {
                        return;
                    }
                    if (reply.term() > currentTerm) {
                        becomeFollower(reply.term());
                        return;
                    }
                    if (reply.voteGranted()) {
                        votesReceived[0]++;
                        if (votesReceived[0] >= quorum) {
                            becomeLeader();
                        }
                    }
                }
            });
        }
    }

    private void becomeLeader() {
        role = RaftRole.LEADER;
        currentLeader = nodeId;

        for (String peer : peers) {
            nextIndex.put(peer, raftLog.getLastLogIndex() + 1);
            matchIndex.put(peer, 0L);
        }

        nextHeartbeatDueMs = currentTimeMs + heartbeatIntervalMs;
        broadcastAppendEntries(true);
    }

    private void becomeFollower(long newTerm) {
        role = RaftRole.FOLLOWER;
        currentTerm = newTerm;
        votedFor = null;
        currentLeader = null;
        resetElectionTimeout();

        // Fail pending client requests
        for (var future : pendingClientRequests.values()) {
            future.completeExceptionally(new RuntimeException("Node stepped down from leader"));
        }
        pendingClientRequests.clear();
    }

    public synchronized CompletableFuture<String> submitCommand(Command command) {
        CompletableFuture<String> future = new CompletableFuture<>();
        if (role != RaftRole.LEADER) {
            future.completeExceptionally(new IllegalStateException(
                    "Not leader (current leader: " + currentLeader + ")"));
            return future;
        }

        long index = raftLog.append(currentTerm, command);
        pendingClientRequests.put(index, future);
        broadcastAppendEntries(false);
        return future;
    }

    private void broadcastAppendEntries(boolean heartbeatOnly) {
        if (role != RaftRole.LEADER) return;

        long leaderTerm = currentTerm;
        long leaderCommitIndex = commitIndex;

        for (String peer : peers) {
            long peerNextIndex = nextIndex.getOrDefault(peer, raftLog.getLastLogIndex() + 1);

            if (peerNextIndex <= raftLog.getLastIncludedIndex()) {
                // Follower lags behind snapshot: send InstallSnapshot
                Snapshot snap = raftLog.getLatestSnapshot();
                if (snap != null) {
                    InstallSnapshotArgs snapArgs = new InstallSnapshotArgs(
                            leaderTerm, nodeId, snap.lastIncludedIndex(), snap.lastIncludedTerm(), snap.data());
                    transport.sendInstallSnapshot(nodeId, peer, snapArgs).thenAccept(reply -> {
                        synchronized (RaftNode.this) {
                            if (role != RaftRole.LEADER || currentTerm != leaderTerm) return;
                            if (reply.term() > currentTerm) {
                                becomeFollower(reply.term());
                                return;
                            }
                            nextIndex.put(peer, snap.lastIncludedIndex() + 1);
                            matchIndex.put(peer, snap.lastIncludedIndex());
                        }
                    });
                }
                continue;
            }

            long prevLogIndex = peerNextIndex - 1;
            long prevLogTerm = raftLog.getTerm(prevLogIndex);

            List<LogEntry> entries = raftLog.getEntriesFrom(peerNextIndex, 100);

            AppendEntriesArgs args = new AppendEntriesArgs(
                    leaderTerm, nodeId, prevLogIndex, prevLogTerm, entries, leaderCommitIndex);

            transport.sendAppendEntries(nodeId, peer, args).thenAccept(reply -> {
                synchronized (RaftNode.this) {
                    if (role != RaftRole.LEADER || currentTerm != leaderTerm) return;
                    if (reply.term() > currentTerm) {
                        becomeFollower(reply.term());
                        return;
                    }

                    if (reply.success()) {
                        long match = reply.matchIndex();
                        matchIndex.put(peer, Math.max(matchIndex.getOrDefault(peer, 0L), match));
                        nextIndex.put(peer, match + 1);
                        checkAndUpdateCommitIndex();
                    } else {
                        // Fast log decrement using conflict index
                        long decr = reply.conflictIndex() > 0
                                ? reply.conflictIndex()
                                : Math.max(1, peerNextIndex - 1);
                        nextIndex.put(peer, Math.max(1, Math.min(peerNextIndex - 1, decr)));
                    }
                }
            });
        }
    }

    private void checkAndUpdateCommitIndex() {
        if (role != RaftRole.LEADER) return;

        long medianIndex = commitIndex;
        List<Long> matches = new ArrayList<>();
        matches.add(raftLog.getLastLogIndex()); // Leader's own match index
        for (String peer : peers) {
            matches.add(matchIndex.getOrDefault(peer, 0L));
        }
        Collections.sort(matches);

        // Find median match index that has quorum support
        int quorumIdx = (matches.size() - 1) / 2;
        long quorumMatch = matches.get(quorumIdx);

        if (quorumMatch > commitIndex && raftLog.getTerm(quorumMatch) == currentTerm) {
            commitIndex = quorumMatch;
            applyEntries();
        }
    }

    private void applyEntries() {
        while (commitIndex > lastApplied) {
            lastApplied++;
            LogEntry entry = raftLog.getEntry(lastApplied);
            if (entry != null) {
                String result = stateMachine.apply(entry.command());
                appliedEntries.put(lastApplied, entry.command().action() + ":" + entry.command().key());

                CompletableFuture<String> future = pendingClientRequests.remove(lastApplied);
                if (future != null) {
                    future.complete(result);
                }
            }
        }
    }

    public synchronized Snapshot compactLog(long snapshotIndex) {
        if (snapshotIndex <= commitIndex) {
            byte[] state = stateMachine.takeSnapshot();
            return raftLog.takeSnapshot(snapshotIndex, state);
        }
        return null;
    }

    @Override
    public synchronized RequestVoteReply handleRequestVote(RequestVoteArgs args) {
        if (args.isPreVote()) {
            if (args.term() <= currentTerm) {
                return new RequestVoteReply(currentTerm, false);
            }
            long myLastTerm = raftLog.getLastLogTerm();
            long myLastIndex = raftLog.getLastLogIndex();
            boolean logUpToDate = (args.lastLogTerm() > myLastTerm) ||
                    (args.lastLogTerm() == myLastTerm && args.lastLogIndex() >= myLastIndex);

            return new RequestVoteReply(currentTerm, logUpToDate);
        }

        if (args.term() > currentTerm) {
            becomeFollower(args.term());
        }

        boolean canVote = (votedFor == null || votedFor.equals(args.candidateId()));
        boolean termOk = (args.term() == currentTerm);

        // Candidate's log must be at least as up-to-date as receiver's log (§5.4.1)
        long myLastTerm = raftLog.getLastLogTerm();
        long myLastIndex = raftLog.getLastLogIndex();
        boolean logUpToDate = (args.lastLogTerm() > myLastTerm) ||
                (args.lastLogTerm() == myLastTerm && args.lastLogIndex() >= myLastIndex);

        if (termOk && canVote && logUpToDate) {
            votedFor = args.candidateId();
            resetElectionTimeout();
            return new RequestVoteReply(currentTerm, true);
        }

        return new RequestVoteReply(currentTerm, false);
    }

    @Override
    public synchronized AppendEntriesReply handleAppendEntries(AppendEntriesArgs args) {
        if (args.term() > currentTerm) {
            becomeFollower(args.term());
        }

        if (args.term() < currentTerm) {
            return AppendEntriesReply.failure(currentTerm, raftLog.getLastLogIndex() + 1, 0);
        }

        // Valid leader recognized
        if (role == RaftRole.CANDIDATE) {
            role = RaftRole.FOLLOWER;
        }
        currentLeader = args.leaderId();
        resetElectionTimeout();

        // Check log matching property (§5.3)
        if (args.prevLogIndex() > 0) {
            if (raftLog.getLastLogIndex() < args.prevLogIndex()) {
                return AppendEntriesReply.failure(currentTerm, raftLog.getLastLogIndex() + 1, 0);
            }
            long localTerm = raftLog.getTerm(args.prevLogIndex());
            if (localTerm != args.prevLogTerm()) {
                // Find first index of conflict term
                long conflictIndex = args.prevLogIndex();
                while (conflictIndex > 1 && raftLog.getTerm(conflictIndex - 1) == localTerm) {
                    --conflictIndex;
                }
                return AppendEntriesReply.failure(currentTerm, conflictIndex, localTerm);
            }
        }

        // Append new entries and resolve conflicts
        raftLog.appendEntries(args.entries());

        // Advance commit index if leaderCommit > commitIndex (§5.3)
        if (args.leaderCommit() > commitIndex) {
            long maxSafeCommit = !args.entries().isEmpty()
                    ? args.entries().get(args.entries().size() - 1).index()
                    : args.prevLogIndex();
            if (maxSafeCommit > commitIndex) {
                commitIndex = Math.min(args.leaderCommit(), maxSafeCommit);
                applyEntries();
            }
        }

        long matchIndex = args.prevLogIndex() + args.entries().size();
        return AppendEntriesReply.success(currentTerm, matchIndex);
    }

    @Override
    public synchronized InstallSnapshotReply handleInstallSnapshot(InstallSnapshotArgs args) {
        if (args.term() > currentTerm) {
            becomeFollower(args.term());
        }

        if (args.term() < currentTerm) {
            return new InstallSnapshotReply(currentTerm);
        }

        if (role == RaftRole.CANDIDATE) {
            role = RaftRole.FOLLOWER;
        }
        currentLeader = args.leaderId();
        resetElectionTimeout();

        if (args.lastIncludedIndex() > commitIndex) {
            Snapshot snap = new Snapshot(args.lastIncludedIndex(), args.lastIncludedTerm(), args.data());
            raftLog.installSnapshot(snap);
            stateMachine.restoreSnapshot(args.data());
            commitIndex = Math.max(commitIndex, args.lastIncludedIndex());
            lastApplied = Math.max(lastApplied, args.lastIncludedIndex());
        }

        return new InstallSnapshotReply(currentTerm);
    }
}
