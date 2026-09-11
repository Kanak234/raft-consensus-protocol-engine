package io.kanak.raft;

import io.kanak.raft.core.RaftCluster;
import io.kanak.raft.core.RaftNode;
import io.kanak.raft.model.RaftRole;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class LeaderElectionTest {

    @Test
    @DisplayName("Should elect exactly one leader in a 5-node cluster")
    void testInitialLeaderElection() {
        RaftCluster cluster = RaftCluster.create(5, 1001);

        boolean elected = cluster.tickUntil(c -> c.getLeader() != null, 3000, 20);
        assertTrue(elected, "Leader should be elected within timeout");

        RaftNode leader = cluster.getLeader();
        assertNotNull(leader);
        assertEquals(RaftRole.LEADER, leader.getRole());
        assertTrue(leader.getCurrentTerm() >= 1);

        // Verify only 1 leader exists in the cluster
        assertEquals(1, cluster.getAllLeaders().size());

        // Invariants must hold
        cluster.verifyInvariants();
    }

    @Test
    @DisplayName("Should elect a new leader when the current leader is isolated")
    void testReElectionOnLeaderFailure() {
        RaftCluster cluster = RaftCluster.create(5, 2002);

        // 1. Elect initial leader
        assertTrue(cluster.tickUntil(c -> c.getLeader() != null, 3000, 20));
        RaftNode oldLeader = cluster.getLeader();
        long oldTerm = oldLeader.getCurrentTerm();
        String oldLeaderId = oldLeader.getNodeId();

        // 2. Isolate current leader
        cluster.isolate(oldLeaderId);

        // 3. Cluster should elect a new leader with a higher term
        boolean newLeaderElected = cluster.tickUntil(c -> {
            RaftNode ldr = c.getLeader();
            return ldr != null && !ldr.getNodeId().equals(oldLeaderId) && ldr.getCurrentTerm() > oldTerm;
        }, 4000, 20);

        assertTrue(newLeaderElected, "New leader should be elected from remaining majority nodes");

        RaftNode newLeader = cluster.getLeader();
        assertNotEquals(oldLeaderId, newLeader.getNodeId());
        assertTrue(newLeader.getCurrentTerm() > oldTerm);

        // 4. Heal the old leader and advance time
        cluster.heal();
        cluster.tick(300);

        // Old leader must have stepped down to FOLLOWER upon hearing higher term
        assertEquals(RaftRole.FOLLOWER, oldLeader.getRole());
        assertEquals(newLeader.getCurrentTerm(), oldLeader.getCurrentTerm());

        cluster.verifyInvariants();
    }
}
