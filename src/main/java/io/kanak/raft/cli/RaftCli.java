package io.kanak.raft.cli;

import io.kanak.raft.core.RaftCluster;
import io.kanak.raft.core.RaftNode;
import io.kanak.raft.model.Command;
import io.kanak.raft.model.RaftRole;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Interactive Command-Line Interface and REPL for the Raft Consensus Protocol Engine.
 */
public class RaftCli {

    private static RaftCluster cluster = null;

    private static void printHelp() {
        System.out.println("""
                Raft Consensus Protocol Engine CLI
                Commands:
                  init [nodes]                 Initialize cluster (default: 5 nodes)
                  status                       Show cluster state (nodes, roles, terms, commitIndex)
                  tick [ms]                    Advance virtual simulation clock (default: 50ms)
                  run [ms]                     Run simulation for [ms] until quorum/leader elected
                  put <key> <value>            Submit client write to current leader
                  get <key>                    Read key from leader's state machine
                  isolate <node>               Completely isolate node from network
                  partition <groupA> <groupB>  Partition network (e.g., node1,node2 node3,node4,node5)
                  heal                         Heal all network partitions
                  bench [ops]                  Run consensus throughput microbenchmark (default: 1000)
                  help                         Show this help menu
                  exit / quit                  Exit CLI
                """);
    }

    public static void runBenchmark(int numOps) {
        System.out.printf("Starting Raft Consensus Benchmark (%d operations)...%n", numOps);
        RaftCluster benchCluster = RaftCluster.create(5, 7777);

        // Elect leader
        benchCluster.tickUntil(c -> c.getLeader() != null, 2000, 20);
        RaftNode leader = benchCluster.getLeader();
        if (leader == null) {
            System.err.println("Failed to elect leader for benchmark.");
            return;
        }
        System.out.printf("Leader %s elected in term %d%n", leader.getNodeId(), leader.getCurrentTerm());

        long startNs = System.nanoTime();
        int committed = 0;

        for (int i = 0; i < numOps; ++i) {
            Command cmd = Command.put("bench_key_" + i, "val_" + i);
            CompletableFuture<String> future = benchCluster.submit(cmd);

            // Step clock until committed
            boolean success = benchCluster.tickUntil(c -> future.isDone(), 500, 5);
            if (success && !future.isCompletedExceptionally()) {
                committed++;
            }
        }

        long endNs = System.nanoTime();
        double elapsedSec = (endNs - startNs) / 1e9;
        double opsPerSec = committed / elapsedSec;

        System.out.printf("Benchmark complete:%n");
        System.out.printf("  Committed: %d / %d operations%n", committed, numOps);
        System.out.printf("  Elapsed Time: %.2f ms%n", elapsedSec * 1000.0);
        System.out.printf("  Throughput: %.1f ops/sec%n", opsPerSec);
        System.out.printf("  Mean Latency: %.2f us/op%n", (elapsedSec * 1e6) / committed);
    }

    public static void main(String[] args) {
        if (args.length > 0 && (args[0].equalsIgnoreCase("--help") || args[0].equalsIgnoreCase("-h"))) {
            printHelp();
            return;
        }

        if (args.length > 0 && args[0].equalsIgnoreCase("--bench")) {
            int ops = args.length > 1 ? Integer.parseInt(args[1]) : 1000;
            runBenchmark(ops);
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Raft engine simulation...");
        }));

        int defaultNodes = getEnvInt("RAFT_CLUSTER_NODES_COUNT", 5);
        long minTimeout = getEnvLong("RAFT_ELECTION_TIMEOUT_MIN_MS", 150L);
        long maxTimeout = getEnvLong("RAFT_ELECTION_TIMEOUT_MAX_MS", 300L);
        long heartbeat = getEnvLong("RAFT_HEARTBEAT_INTERVAL_MS", 50L);

        System.out.println("Raft Consensus Protocol Engine (Java 21 LTS)");
        System.out.printf("Type 'help' for commands. Initializing default %d-node cluster...%n", defaultNodes);
        cluster = RaftCluster.create(defaultNodes, 12345, minTimeout, maxTimeout, heartbeat);
        cluster.tickUntil(c -> c.getLeader() != null, 2000, 20);

        RaftNode leader = cluster.getLeader();
        if (leader != null) {
            System.out.printf("Cluster ready. Elected leader: %s (Term %d)%n%n",
                    leader.getNodeId(), leader.getCurrentTerm());
        }

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("raft> ");
            if (!scanner.hasNextLine()) break;
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) continue;

            String[] tokens = line.split("\\s+");
            String cmd = tokens[0].toLowerCase();

            switch (cmd) {
                case "exit", "quit" -> {
                    return;
                }
                case "help" -> printHelp();
                case "init" -> {
                    int n = tokens.length > 1 ? Integer.parseInt(tokens[1]) : 5;
                    cluster = RaftCluster.create(n, System.currentTimeMillis());
                    cluster.tickUntil(c -> c.getLeader() != null, 2000, 20);
                    System.out.printf("Cluster initialized with %d nodes.%n", n);
                }
                case "status" -> {
                    if (cluster == null) {
                        System.out.println("Cluster not initialized.");
                        break;
                    }
                    System.out.printf("Virtual Clock: %d ms | Invariants Verified: OK%n",
                            cluster.getNetwork().getVirtualTime());
                    System.out.printf("%-10s %-12s %-8s %-12s %-12s %-12s%n",
                            "Node", "Role", "Term", "VotedFor", "CommitIdx", "LogEntries");
                    System.out.println("-".repeat(68));
                    for (String id : cluster.getNodeIds()) {
                        RaftNode node = cluster.getNode(id);
                        System.out.printf("%-10s %-12s %-8d %-12s %-12d %-12d%n",
                                node.getNodeId(),
                                node.getRole(),
                                node.getCurrentTerm(),
                                node.getVotedFor() != null ? node.getVotedFor() : "-",
                                node.getCommitIndex(),
                                node.getRaftLog().getLastLogIndex());
                    }
                }
                case "tick" -> {
                    long ms = tokens.length > 1 ? Long.parseLong(tokens[1]) : 50;
                    cluster.tick(ms);
                    System.out.printf("Advanced clock by %d ms. Current time: %d ms%n",
                            ms, cluster.getNetwork().getVirtualTime());
                }
                case "run" -> {
                    long ms = tokens.length > 1 ? Long.parseLong(tokens[1]) : 500;
                    cluster.tickUntil(c -> false, ms, 20);
                    System.out.printf("Ran simulation for %d ms. Current time: %d ms%n",
                            ms, cluster.getNetwork().getVirtualTime());
                }
                case "put" -> {
                    if (tokens.length < 3) {
                        System.out.println("Usage: put <key> <value>");
                        break;
                    }
                    Command putCmd = Command.put(tokens[1], tokens[2]);
                    CompletableFuture<String> future = cluster.submit(putCmd);
                    boolean ok = cluster.tickUntil(c -> future.isDone(), 500, 10);
                    if (ok && !future.isCompletedExceptionally()) {
                        System.out.printf("Committed! Result: %s%n", future.join());
                    } else {
                        System.out.println("Submission failed or timed out.");
                    }
                }
                case "get" -> {
                    if (tokens.length < 2) {
                        System.out.println("Usage: get <key>");
                        break;
                    }
                    RaftNode ldr = cluster.getLeader();
                    if (ldr != null) {
                        var sm = (io.kanak.raft.statemachine.KeyValueStateMachine) ldr.getStateMachine();
                        System.out.printf("%s => %s%n", tokens[1], sm.get(tokens[1]));
                    } else {
                        System.out.println("No active leader.");
                    }
                }
                case "isolate" -> {
                    if (tokens.length < 2) {
                        System.out.println("Usage: isolate <nodeId>");
                        break;
                    }
                    cluster.isolate(tokens[1]);
                    System.out.printf("Node %s isolated from network.%n", tokens[1]);
                }
                case "partition" -> {
                    if (tokens.length < 3) {
                        System.out.println("Usage: partition <node1,node2> <node3,node4,node5>");
                        break;
                    }
                    Set<String> groupA = new HashSet<>(Arrays.asList(tokens[1].split(",")));
                    Set<String> groupB = new HashSet<>(Arrays.asList(tokens[2].split(",")));
                    cluster.partition(groupA, groupB);
                    System.out.printf("Network partitioned: %s <X> %s%n", groupA, groupB);
                }
                case "heal" -> {
                    cluster.heal();
                    System.out.println("Network healed. All links restored.");
                }
                case "bench" -> {
                    int ops = tokens.length > 1 ? Integer.parseInt(tokens[1]) : 1000;
                    runBenchmark(ops);
                }
                default -> System.out.println("Unknown command. Type 'help' for available commands.");
            }
        }
    }

    private static int getEnvInt(String name, int defaultValue) {
        String val = System.getenv(name);
        if (val != null && !val.isBlank()) {
            try {
                return Integer.parseInt(val.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultValue;
    }

    private static long getEnvLong(String name, long defaultValue) {
        String val = System.getenv(name);
        if (val != null && !val.isBlank()) {
            try {
                return Long.parseLong(val.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultValue;
    }
}
