package io.kanak.raft.core;

import io.kanak.raft.model.Command;
import io.kanak.raft.model.RaftRole;
import io.kanak.raft.simulation.DeterministicNetwork;
import io.kanak.raft.simulation.SafetyChecker;
import io.kanak.raft.statemachine.KeyValueStateMachine;
import io.kanak.raft.storage.RaftLog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/**
 * Manages an in-process simulated cluster of Raft nodes with unified clock advancement
 * and continuous safety invariant auditing.
 */
public class RaftCluster {

    private final DeterministicNetwork network;
    private final Map<String, RaftNode> nodes = new HashMap<>();
    private final List<String> nodeIds = new ArrayList<>();
    private final SafetyChecker safetyChecker = new SafetyChecker();

    public RaftCluster(DeterministicNetwork network) {
        this.network = network;
    }

    public static RaftCluster create(int numNodes, long seed) {
        return create(numNodes, seed, 150, 300, 50);
    }

    public static RaftCluster create(int numNodes, long seed,
                                     long minElectionTimeoutMs, long maxElectionTimeoutMs,
                                     long heartbeatIntervalMs) {
        if (numNodes <= 0) {
            throw new IllegalArgumentException("numNodes must be positive: " + numNodes);
        }
        DeterministicNetwork net = new DeterministicNetwork();
        RaftCluster cluster = new RaftCluster(net);

        List<String> allIds = new ArrayList<>();
        for (int i = 1; i <= numNodes; ++i) {
            allIds.add("node" + i);
        }

        for (int i = 0; i < numNodes; ++i) {
            String id = allIds.get(i);
            List<String> peers = new ArrayList<>(allIds);
            peers.remove(id);

            RaftNode node = new RaftNode(
                    id, peers, net,
                    new KeyValueStateMachine(),
                    new RaftLog(),
                    minElectionTimeoutMs, maxElectionTimeoutMs, heartbeatIntervalMs,
                    seed + i * 1000L
            );
            net.registerNode(id, node);
            cluster.nodes.put(id, node);
            cluster.nodeIds.add(id);
        }

        return cluster;
    }

    public DeterministicNetwork getNetwork() { return network; }
    public Map<String, RaftNode> getNodes() { return Collections.unmodifiableMap(nodes); }
    public List<String> getNodeIds() { return Collections.unmodifiableList(nodeIds); }
    public RaftNode getNode(String id) { return nodes.get(id); }
    public SafetyChecker getSafetyChecker() { return safetyChecker; }

    public void verifyInvariants() {
        safetyChecker.checkInvariants(nodes.values());
    }

    public RaftNode getLeader() {
        for (RaftNode node : nodes.values()) {
            if (node.getRole() == RaftRole.LEADER) {
                return node;
            }
        }
        return null;
    }

    public List<RaftNode> getAllLeaders() {
        List<RaftNode> leaders = new ArrayList<>();
        for (RaftNode node : nodes.values()) {
            if (node.getRole() == RaftRole.LEADER) {
                leaders.add(node);
            }
        }
        return leaders;
    }

    /**
     * Ticks all nodes by deltaMs and processes network messages up to new virtual time.
     * Slices intervals into fine-grained sub-steps to allow discrete-event message
     * round-trips to deliver naturally in simulated time.
     */
    public void tick(long deltaMs) {
        long step = 10;
        for (long elapsed = 0; elapsed < deltaMs; elapsed += step) {
            long d = Math.min(step, deltaMs - elapsed);
            for (RaftNode node : nodes.values()) {
                node.tick(d);
            }
            network.advanceTime(d);
            network.dispatchAllPending();
        }
        verifyInvariants();
    }

    /**
     * Repeatedly steps simulation until condition is met or timeout expires.
     */
    public boolean tickUntil(Predicate<RaftCluster> condition, long maxTimeMs, long stepMs) {
        long elapsed = 0;
        while (elapsed < maxTimeMs) {
            if (condition.test(this)) {
                return true;
            }
            tick(stepMs);
            elapsed += stepMs;
        }
        return condition.test(this);
    }

    public void partition(Set<String> groupA, Set<String> groupB) {
        network.partition(groupA, groupB);
    }

    public void isolate(String node) {
        network.isolate(node);
    }

    public void heal() {
        network.heal();
    }

    public CompletableFuture<String> submit(Command command) {
        RaftNode leader = getLeader();
        if (leader == null) {
            CompletableFuture<String> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("No elected leader currently in cluster"));
            return failed;
        }
        return leader.submitCommand(command);
    }
}
