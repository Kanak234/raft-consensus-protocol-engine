package io.kanak.raft;

import io.kanak.raft.core.RaftCluster;
import io.kanak.raft.core.RaftNode;
import io.kanak.raft.model.Command;
import io.kanak.raft.model.RaftRole;
import io.kanak.raft.statemachine.KeyValueStateMachine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class NetworkPartitionTest {

    @Test
    @DisplayName("Should maintain safety under 3v2 asymmetric network partition and heal cleanly")
    void testNetworkPartitionSafetyAndRecovery() {
        RaftCluster cluster = RaftCluster.create(5, 4004);

        // 1. Initial election
        assertTrue(cluster.tickUntil(c -> c.getLeader() != null, 3000, 20));
        RaftNode oldLeader = cluster.getLeader();
        assertNotNull(oldLeader);

        // Commit initial entry
        CompletableFuture<String> initFuture = cluster.submit(Command.put("initKey", "initVal"));
        assertTrue(cluster.tickUntil(c -> initFuture.isDone(), 2000, 10));
        assertEquals("OK", initFuture.join());

        // 2. Create asymmetric partition:
        // Minority: {oldLeader, peer1}
        // Majority: {peer2, peer3, peer4}
        String ldrId = oldLeader.getNodeId();
        String minorPeer = cluster.getNodeIds().stream()
                .filter(id -> !id.equals(ldrId)).findFirst().orElseThrow();

        Set<String> minority = Set.of(ldrId, minorPeer);
        Set<String> majority = cluster.getNodeIds().stream()
                .filter(id -> !minority.contains(id))
                .collect(java.util.stream.Collectors.toSet());

        cluster.partition(minority, majority);

        // 3. Attempt write to minority leader -> cannot commit without quorum
        CompletableFuture<String> minorityFuture = oldLeader.submitCommand(Command.put("minorityKey", "staleVal"));
        cluster.tick(500);
        assertFalse(minorityFuture.isDone(), "Minority write must not commit without majority quorum");

        // 4. Majority partition elects new leader
        boolean majorityElected = cluster.tickUntil(c -> {
            for (String id : majority) {
                if (c.getNode(id).getRole() == RaftRole.LEADER) {
                    return true;
                }
            }
            return false;
        }, 4000, 20);
        assertTrue(majorityElected, "Majority partition must elect a new leader");

        RaftNode majorityLeader = majority.stream()
                .map(cluster::getNode)
                .filter(n -> n.getRole() == RaftRole.LEADER)
                .findFirst().orElseThrow();

        // 5. Submit write to majority leader -> commits successfully
        CompletableFuture<String> majorityFuture = majorityLeader.submitCommand(Command.put("majKey", "majVal"));
        assertTrue(cluster.tickUntil(c -> majorityFuture.isDone(), 2000, 10));
        assertEquals("OK", majorityFuture.join());

        // 6. Heal network partition
        cluster.heal();
        cluster.tick(500);

        // Old leader must step down
        assertEquals(RaftRole.FOLLOWER, oldLeader.getRole());

        // All nodes must converge to majority state
        for (String id : cluster.getNodeIds()) {
            RaftNode node = cluster.getNode(id);
            var sm = (KeyValueStateMachine) node.getStateMachine();
            assertEquals("initVal", sm.get("initKey"));
            assertEquals("majVal", sm.get("majKey"));
            assertNull(sm.get("minorityKey"), "Uncommitted minority write must have been rolled back");
        }

        // All 5 Raft invariants must hold
        cluster.verifyInvariants();
    }
}
