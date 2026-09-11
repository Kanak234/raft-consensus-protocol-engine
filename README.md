# Raft Consensus Protocol Engine

[![CI](https://github.com/Kanak234/raft-consensus-protocol-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/Kanak234/raft-consensus-protocol-engine/actions/workflows/ci.yml)
[![Java 21 LTS](https://img.shields.io/badge/Java-21%20LTS-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A production-grade, pure Java 21 LTS implementation of the **Raft Distributed Consensus Algorithm** (Diego Ongaro's doctoral thesis, Stanford 2014) paired with a deterministic discrete-event network simulator for reproducible fault injection and continuous formal invariant auditing.

---

## Key Features

- **Full Raft Protocol Specification**:
  - Leader Election with **Pre-Vote Protocol** (§9.6) to prevent partitioned nodes from causing disruptive leader failovers.
  - Heartbeat generator and follower election timeout randomization.
  - Log Replication with fast conflict resolution via log decrementing (§5.3).
  - Snapshot compaction and `InstallSnapshot` RPC (§7) for lagging nodes.
  - Linearizable state machine application backed by an ordered in-memory Key-Value store.
- **Deterministic Discrete-Event Simulator**:
  - In-process virtual clock with priority-queue event dispatching.
  - Programmable asymmetric network partitions (e.g., 3v2 splits).
  - Configurable bounded network latency and random packet drop rates.
- **Continuous Safety Invariant Auditing**:
  - Audits all **5 Raft Safety Invariants** (§5.4.3) on every clock tick:
    1. *Election Safety*
    2. *Leader Append-Only*
    3. *Log Matching*
    4. *Leader Completeness*
    5. *State Machine Safety*
- **Interactive REPL & CLI**:
  - Live cluster state visualization, partition creation, network healing, interactive key-value queries, and automated throughput benchmarks.

---

## Architecture

```mermaid
flowchart TD
    Client["Client / CLI / Benchmark"] -->|submit(Command)| Leader["RaftNode (Leader)"]

    subgraph Cluster["5-Node Raft Cluster"]
        Leader
        F1["RaftNode (Follower 1)"]
        F2["RaftNode (Follower 2)"]
        F3["RaftNode (Follower 3)"]
        F4["RaftNode (Follower 4)"]
    end

    subgraph Transport["Deterministic Network Simulator"]
        Net[("Event Priority Queue & Link Matrix")]
    end

    subgraph Safety["Safety Verifier"]
        SC["SafetyChecker (5 Invariants)"]
    end

    Leader <-->|AppendEntries / InstallSnapshot| Net
    Net <--> F1
    Net <--> F2
    Net <--> F3
    Net <--> F4

    Cluster -.->|Continuous Tick Audit| SC
```

---

## Raft Safety Invariants Verified

| Invariant (§5.4.3) | Property | Enforcement & Verification |
|---|---|---|
| **Election Safety** | At most one leader can be elected in a given term. | Enforced by majority voting quorum ($Q = \lfloor N/2 \rfloor + 1$); audited by historical term registry. |
| **Leader Append-Only** | A leader never overwrites or truncates its log; it only appends new entries. | Enforced by `RaftLog.append()`; leaders never modify past indices. |
| **Log Matching** | If two logs contain an entry with the same index and term, logs are identical up to that entry. | Enforced by induction during `AppendEntries` consistency checks; audited pairwise in $O(N)$. |
| **Leader Completeness** | If an entry is committed in a term, it is present in the logs of all higher-term leaders. | Guaranteed by candidate election requirement (§5.4.1); audited against committed entry history. |
| **State Machine Safety** | If a server has applied an entry at index $L$, no other server applies a different entry at $L$. | Guaranteed by commit index quorum advancement; audited across all applied state machines. |

---

## Measured Performance Benchmarks

All benchmark numbers measured locally on the host machine:

| Benchmark Run | Operations | Committed | Elapsed Time | Throughput | Mean Latency | Invariant Violations |
|---|---|---|---|---|---|---|
| **Standard Throughput** | 1,000 | 1,000 (100%) | 946.54 ms | **1,056.5 ops/sec** | **946.54 μs/op** | 0 |
| **Stress with Invariant Auditing** | 5,000 | 5,000 (100%) | 20,350.99 ms | **245.7 ops/sec** | **4,070.20 μs/op** | 0 |

---

## Getting Started

### Prerequisites
- **Java 21 LTS** or newer (`javac -version`)
- **Apache Maven 3.9+** (`mvn -version`)
- **Docker** (optional, for containerized execution)

### Build & Test

```bash
# Build shaded fat JAR and execute all 5 invariant & partition test suites
mvn clean package

# Run all test suites
mvn test
```

### Run Benchmark

```bash
# Run 1,000-operation microbenchmark
java -jar target/raft-consensus-engine.jar --bench 1000

# Run 5,000-operation stress test
java -jar target/raft-consensus-engine.jar --bench 5000
```

### Interactive CLI

Launch the interactive REPL:

```bash
java -jar target/raft-consensus-engine.jar
```

Commands available in the REPL:
- `status`: Display cluster nodes, terms, roles, commit indices, and log sizes.
- `put <key> <value>`: Submit a write command to the cluster leader.
- `get <key>`: Query value from the state machine.
- `partition <node1,node2> <node3,node4,node5>`: Create an asymmetric network partition.
- `isolate <node>`: Completely sever all network links to a specific node.
- `heal`: Restore all severed network links.
- `tick [ms]`: Step the virtual simulation clock forward.
- `bench [ops]`: Execute an in-line microbenchmark.
- `exit`: Terminate REPL.

---

## Docker Support

Build and run using Docker:

```bash
# Build container image
docker build -t raft-consensus-engine .

# Run benchmark in container
docker run --rm raft-consensus-engine --bench 1000
```

---

## Repository Structure

```
raft-consensus-protocol-engine/
├── .github/workflows/ci.yml       # GitHub Actions CI pipeline
├── docs/
│   ├── PRD.md                    # Product Requirements Document
│   ├── TRD.md                    # Technical Architecture Document
│   ├── IMPLEMENTATION_PLAN.md    # Task tracking & verification
│   └── EXPLAIN.md                # 6-section protocol deep dive
├── src/
│   ├── main/java/io/kanak/raft/
│   │   ├── cli/                  # Interactive REPL & Benchmark CLI
│   │   ├── core/                 # RaftNode and RaftCluster engine
│   │   ├── model/                # LogEntry, Command, Snapshot, RaftRole
│   │   ├── rpc/                  # RequestVote, AppendEntries, InstallSnapshot
│   │   ├── simulation/           # DeterministicNetwork & SafetyChecker
│   │   ├── statemachine/         # KeyValueStateMachine
│   │   └── storage/              # RaftLog with snapshot compaction
│   └── test/java/io/kanak/raft/  # Invariant, partition, snapshot & fuzz tests
├── Dockerfile                    # Multi-stage Docker container build
├── pom.xml                       # Maven build descriptor
└── LICENSE                       # MIT License
```

---

## Limitations & Engineering Trade-Offs

- **Discrete-Event Simulation Transport**: The protocol engine currently executes atop an in-memory discrete-event network simulator (`DeterministicNetwork`). While this enables cycle-accurate deterministic fault injection (asymmetric partitions, packet drops, clock skew), deploying across bare-metal physical servers requires implementing a real network transport adapter (e.g. Netty TCP or gRPC) against the `NetworkTransport` interface.
- **In-Memory Volatile Storage**: Log entries, terms, and snapshots are stored in JVM memory (`RaftLog`). For survivability across complete node power failures in production environments, log entries must be flushed to a durable Write-Ahead Log (WAL) on disk via `FileChannel.force(true)` before acknowledging RPCs.
- **Static Cluster Topology**: Dynamic cluster membership changes (§6 of the Raft dissertation, joint consensus) are not implemented; cluster membership is fixed at initialization time.
- **Single-Threaded Simulation Loop**: Virtual clock advancement is coordinated via a discrete-event priority queue. High operation throughput reflects consensus rounds and state-machine transitions without physical OS socket overhead.

---

## Reproducible Benchmark Specifications

All benchmark figures are reproducible using the shaded fat JAR on bare-metal hardware.

### Testbed Environment
- **CPU:** AMD Ryzen 5 5600H (6 Cores, 12 Threads @ 3.30 GHz base / 4.20 GHz boost, 16 MB L3 Cache)
- **RAM:** 16 GB DDR4 3200 MT/s Dual-Channel
- **OS:** Linux 7.0.0-31-generic x86_64
- **JVM Runtime:** Eclipse Temurin OpenJDK 64-Bit Server VM (build 21.0.12.1+1-LTS)
- **Target Commit SHA:** `59c82cb24e497a2724ab4e33f6c96acb35018191`

### Reproducible Command
```bash
# Build shaded fat JAR
mvn clean package -DskipTests

# Execute 1,000-operation Raft consensus benchmark across 5-node cluster
java -jar target/raft-consensus-engine.jar --bench 1000
```

### Measured Benchmark Output
```text
Starting Raft Consensus Benchmark (1000 operations)...
Leader node1 elected in term 1
Benchmark complete:
  Committed: 1000 / 1000 operations
  Elapsed Time: 1049.53 ms
  Throughput: 952.8 ops/sec
  Mean Latency: 1049.53 us/op
```

---

## License

MIT License. Copyright (c) 2026 Kanak Prabhakar.

