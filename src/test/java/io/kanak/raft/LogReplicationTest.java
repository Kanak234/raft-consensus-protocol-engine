package io.kanak.raft;

import io.kanak.raft.core.RaftCluster;
import io.kanak.raft.core.RaftNode;
import io.kanak.raft.model.Command;
import io.kanak.raft.statemachine.KeyValueStateMachine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class LogReplicationTest {

    @Test
    @DisplayName("Should replicate and commit entries across all cluster nodes")
    void testBasicLogReplication() {
        RaftCluster cluster = RaftCluster.create(5, 3003);

        // Elect leader
        assertTrue(cluster.tickUntil(c -> c.getLeader() != null, 3000, 20));
        RaftNode leader = cluster.getLeader();
        assertNotNull(leader);

        // Submit 25 sequential commands
        List<CompletableFuture<String>> futures = new ArrayList<>();
        for (int i = 1; i <= 25; ++i) {
            Command cmd = Command.put("key_" + i, "val_" + i);
            futures.add(cluster.submit(cmd));
        }

        // Advance simulation until all futures complete
        boolean allDone = cluster.tickUntil(c -> futures.stream().allMatch(CompletableFuture::isDone), 3000, 10);
        assertTrue(allDone, "All commands should be committed within quorum timeout");

        for (var f : futures) {
            assertTrue(f.isDone());
            assertFalse(f.isCompletedExceptionally());
            assertEquals("OK", f.join());
        }

        // Allow trailing heartbeats to synchronize commitIndex on followers
        cluster.tick(200);

        // Verify state machine values match across all nodes
        for (String id : cluster.getNodeIds()) {
            RaftNode node = cluster.getNode(id);
            assertEquals(25, node.getCommitIndex());
            var sm = (KeyValueStateMachine) node.getStateMachine();
            for (int i = 1; i <= 25; ++i) {
                assertEquals("val_" + i, sm.get("key_" + i));
            }
        }

        cluster.verifyInvariants();
    }
}
