package io.kanak.raft;

import io.kanak.raft.core.RaftCluster;
import io.kanak.raft.core.RaftNode;
import io.kanak.raft.model.Command;
import io.kanak.raft.statemachine.KeyValueStateMachine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class LogCompactionSnapshotTest {

    @Test
    @DisplayName("Should compact log and install snapshot on lagging follower")
    void testLogCompactionAndSnapshotInstallation() {
        RaftCluster cluster = RaftCluster.create(5, 5005);

        // 1. Initial leader election
        assertTrue(cluster.tickUntil(c -> c.getLeader() != null, 3000, 20));
        RaftNode leader = cluster.getLeader();
        assertNotNull(leader);

        // 2. Commit 20 entries
        for (int i = 1; i <= 20; ++i) {
            CompletableFuture<String> f = cluster.submit(Command.put("k" + i, "v" + i));
            assertTrue(cluster.tickUntil(c -> f.isDone(), 1000, 10));
        }

        // 3. Isolate node5
        String laggingNodeId = "node5";
        cluster.isolate(laggingNodeId);

        // 4. Commit 30 more entries (entries 21..50) with remaining 4 nodes
        for (int i = 21; i <= 50; ++i) {
            CompletableFuture<String> f = cluster.submit(Command.put("k" + i, "v" + i));
            assertTrue(cluster.tickUntil(c -> f.isDone(), 1000, 10));
        }

        // 5. Leader takes snapshot at index 40, discarding entries 1..40
        assertNotNull(leader.compactLog(40), "Leader should take snapshot at index 40");
        assertTrue(leader.getRaftLog().getLastIncludedIndex() >= 40);

        // 6. Reconnect node5
        cluster.heal();
        // Allow leader to send InstallSnapshot and catch up
        cluster.tick(600);

        // 7. Verify node5 has caught up
        RaftNode node5 = cluster.getNode(laggingNodeId);
        assertEquals(50, node5.getCommitIndex());

        var sm = (KeyValueStateMachine) node5.getStateMachine();
        for (int i = 1; i <= 50; ++i) {
            assertEquals("v" + i, sm.get("k" + i));
        }

        cluster.verifyInvariants();
    }
}
