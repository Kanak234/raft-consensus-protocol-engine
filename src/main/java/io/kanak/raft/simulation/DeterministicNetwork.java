package io.kanak.raft.simulation;

import io.kanak.raft.rpc.AppendEntriesArgs;
import io.kanak.raft.rpc.AppendEntriesReply;
import io.kanak.raft.rpc.InstallSnapshotArgs;
import io.kanak.raft.rpc.InstallSnapshotReply;
import io.kanak.raft.rpc.RequestVoteArgs;
import io.kanak.raft.rpc.RequestVoteReply;
import io.kanak.raft.transport.NetworkTransport;
import io.kanak.raft.transport.RaftRpcHandler;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Deterministic discrete-event simulated network.
 * Supports programmable asymmetric network partitions, bounded latency,
 * packet loss, and monotonically advancing virtual time.
 */
public class DeterministicNetwork implements NetworkTransport {

    private final Map<String, RaftRpcHandler> handlers = new ConcurrentHashMap<>();
    private final Set<String> blockedDirectedLinks = ConcurrentHashMap.newKeySet();
    private final PriorityQueue<SimulatedEvent> eventQueue = new PriorityQueue<>(
            Comparator.comparingLong(SimulatedEvent::deliverAtTime)
                    .thenComparingLong(SimulatedEvent::sequenceNumber)
    );

    private final AtomicLong eventSequence = new AtomicLong(0);
    private long virtualTimeMs = 0;
    private long minLatencyMs = 5;
    private long maxLatencyMs = 15;
    private double dropRate = 0.0;
    private final Random rng = new Random(42);

    private record SimulatedEvent(
            long deliverAtTime,
            long sequenceNumber,
            Runnable task
    ) {}

    public synchronized void registerNode(String nodeId, RaftRpcHandler handler) {
        handlers.put(nodeId, handler);
    }

    public synchronized void unregisterNode(String nodeId) {
        handlers.remove(nodeId);
    }

    public synchronized void setLatency(long minMs, long maxMs) {
        this.minLatencyMs = minMs;
        this.maxLatencyMs = maxMs;
    }

    public synchronized void setDropRate(double rate) {
        this.dropRate = rate;
    }

    public synchronized void partition(Set<String> groupA, Set<String> groupB) {
        for (String a : groupA) {
            for (String b : groupB) {
                blockedDirectedLinks.add(linkKey(a, b));
                blockedDirectedLinks.add(linkKey(b, a));
            }
        }
    }

    public synchronized void isolate(String node) {
        for (String other : handlers.keySet()) {
            if (!other.equals(node)) {
                blockedDirectedLinks.add(linkKey(node, other));
                blockedDirectedLinks.add(linkKey(other, node));
            }
        }
    }

    public synchronized void heal() {
        blockedDirectedLinks.clear();
    }

    public synchronized boolean isConnected(String from, String to) {
        if (!handlers.containsKey(from) || !handlers.containsKey(to)) {
            return false;
        }
        return !blockedDirectedLinks.contains(linkKey(from, to));
    }

    public synchronized long getVirtualTime() {
        return virtualTimeMs;
    }

    /**
     * Advances virtual time and dispatches all scheduled events due by that time.
     */
    public synchronized void advanceTime(long millis) {
        long targetTime = virtualTimeMs + millis;
        while (!eventQueue.isEmpty() && eventQueue.peek().deliverAtTime() <= targetTime) {
            SimulatedEvent event = eventQueue.poll();
            virtualTimeMs = event.deliverAtTime();
            event.task().run();
        }
        virtualTimeMs = targetTime;
    }

    /**
     * Dispatches all pending queued events up to current virtual time.
     */
    public synchronized void dispatchAllPending() {
        while (!eventQueue.isEmpty() && eventQueue.peek().deliverAtTime() <= virtualTimeMs) {
            SimulatedEvent event = eventQueue.poll();
            event.task().run();
        }
    }

    public synchronized int pendingEventCount() {
        return eventQueue.size();
    }

    private synchronized void schedule(long delayMs, Runnable task) {
        long deliverAt = virtualTimeMs + delayMs;
        eventQueue.add(new SimulatedEvent(deliverAt, eventSequence.incrementAndGet(), task));
    }

    private long sampleLatency() {
        if (minLatencyMs >= maxLatencyMs) return minLatencyMs;
        return minLatencyMs + (Math.abs(rng.nextLong()) % (maxLatencyMs - minLatencyMs + 1));
    }

    private String linkKey(String from, String to) {
        return from + "->" + to;
    }

    @Override
    public CompletableFuture<RequestVoteReply> sendRequestVote(String sender, String target, RequestVoteArgs args) {
        CompletableFuture<RequestVoteReply> future = new CompletableFuture<>();
        synchronized (this) {
            if (!isConnected(sender, target) || (dropRate > 0 && rng.nextDouble() < dropRate)) {
                // Drop packet: complete exceptionally on timeout
                schedule(300, () -> future.completeExceptionally(new TimeoutException("RequestVote dropped/timeout")));
                return future;
            }

            long oneWayLatency = sampleLatency();
            schedule(oneWayLatency, () -> {
                synchronized (DeterministicNetwork.this) {
                    RaftRpcHandler handler = handlers.get(target);
                    if (handler == null || !isConnected(sender, target)) {
                        future.completeExceptionally(new TimeoutException("Target unreachable"));
                        return;
                    }
                    RequestVoteReply reply = handler.handleRequestVote(args);
                    long returnLatency = sampleLatency();
                    schedule(returnLatency, () -> {
                        synchronized (DeterministicNetwork.this) {
                            if (isConnected(target, sender)) {
                                future.complete(reply);
                            } else {
                                future.completeExceptionally(new TimeoutException("Reply dropped"));
                            }
                        }
                    });
                }
            });
        }
        return future;
    }

    @Override
    public CompletableFuture<AppendEntriesReply> sendAppendEntries(String sender, String target, AppendEntriesArgs args) {
        CompletableFuture<AppendEntriesReply> future = new CompletableFuture<>();
        synchronized (this) {
            if (!isConnected(sender, target) || (dropRate > 0 && rng.nextDouble() < dropRate)) {
                schedule(300, () -> future.completeExceptionally(new TimeoutException("AppendEntries dropped/timeout")));
                return future;
            }

            long oneWayLatency = sampleLatency();
            schedule(oneWayLatency, () -> {
                synchronized (DeterministicNetwork.this) {
                    RaftRpcHandler handler = handlers.get(target);
                    if (handler == null || !isConnected(sender, target)) {
                        future.completeExceptionally(new TimeoutException("Target unreachable"));
                        return;
                    }
                    AppendEntriesReply reply = handler.handleAppendEntries(args);
                    long returnLatency = sampleLatency();
                    schedule(returnLatency, () -> {
                        synchronized (DeterministicNetwork.this) {
                            if (isConnected(target, sender)) {
                                future.complete(reply);
                            } else {
                                future.completeExceptionally(new TimeoutException("Reply dropped"));
                            }
                        }
                    });
                }
            });
        }
        return future;
    }

    @Override
    public CompletableFuture<InstallSnapshotReply> sendInstallSnapshot(String sender, String target, InstallSnapshotArgs args) {
        CompletableFuture<InstallSnapshotReply> future = new CompletableFuture<>();
        synchronized (this) {
            if (!isConnected(sender, target) || (dropRate > 0 && rng.nextDouble() < dropRate)) {
                schedule(300, () -> future.completeExceptionally(new TimeoutException("InstallSnapshot dropped/timeout")));
                return future;
            }

            long oneWayLatency = sampleLatency();
            schedule(oneWayLatency, () -> {
                synchronized (DeterministicNetwork.this) {
                    RaftRpcHandler handler = handlers.get(target);
                    if (handler == null || !isConnected(sender, target)) {
                        future.completeExceptionally(new TimeoutException("Target unreachable"));
                        return;
                    }
                    InstallSnapshotReply reply = handler.handleInstallSnapshot(args);
                    long returnLatency = sampleLatency();
                    schedule(returnLatency, () -> {
                        synchronized (DeterministicNetwork.this) {
                            if (isConnected(target, sender)) {
                                future.complete(reply);
                            } else {
                                future.completeExceptionally(new TimeoutException("Reply dropped"));
                            }
                        }
                    });
                }
            });
        }
        return future;
    }
}
