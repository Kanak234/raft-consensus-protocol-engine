package io.kanak.raft;

import io.kanak.raft.core.RaftCluster;
import io.kanak.raft.core.RaftNode;
import io.kanak.raft.model.Command;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SafetyInvariantsFuzzTest {

    @Test
    @DisplayName("Fuzz simulation: verify all 5 Raft safety invariants under random partitions and drops")
    void testSafetyInvariantsUnderFuzz() {
        RaftCluster cluster = RaftCluster.create(5, 6006);
        cluster.getNetwork().setDropRate(0.05); // 5% packet drop rate
        Random rng = new Random(98765);

        // Initial election
        assertTrue(cluster.tickUntil(c -> c.getLeader() != null, 3000, 20));

        int clientCommandCounter = 0;
        List<String> nodeIds = new ArrayList<>(cluster.getNodeIds());

        // Run 200 simulation cycles with random dynamic perturbations
        for (int cycle = 0; cycle < 200; ++cycle) {
            int action = rng.nextInt(10);

            if (action == 0) {
                // Partition into random 3v2 or 4v1
                Collections.shuffle(nodeIds, rng);
                int split = 2 + rng.nextInt(2);
                Set<String> groupA = new HashSet<>(nodeIds.subList(0, split));
                Set<String> groupB = new HashSet<>(nodeIds.subList(split, nodeIds.size()));
                cluster.partition(groupA, groupB);
            } else if (action == 1) {
                // Heal partitions
                cluster.heal();
            } else if (action == 2) {
                // Isolate random node
                String target = nodeIds.get(rng.nextInt(nodeIds.size()));
                cluster.isolate(target);
            } else {
                // Submit client command if leader is present
                RaftNode leader = cluster.getLeader();
                if (leader != null) {
                    clientCommandCounter++;
                    Command cmd = Command.put("fuzz_key_" + clientCommandCounter, "fuzz_val_" + clientCommandCounter);
                    cluster.submit(cmd);
                }
            }

            // Step clock and verify invariants
            cluster.tick(30);
            cluster.verifyInvariants();
        }

        // Final healing and stabilization
        cluster.heal();
        cluster.getNetwork().setDropRate(0.0);
        cluster.tick(1000);
        cluster.verifyInvariants();
    }
}
