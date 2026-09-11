# Implementation Plan: `raft-consensus-protocol-engine`

## Project: `raft-consensus-protocol-engine`
**Language:** Java 21 LTS  
**Domain:** Distributed Systems  
**Target Delivery:** Full Raft consensus protocol with deterministic network partition simulation & safety invariant validation

---

## 1. Task Breakdown & Phases

### Phase 1: Core Models, RPC Messages & Storage
- [x] Create Maven `pom.xml` configured for Java 21, JUnit 5, and shaded JAR packaging.
- [x] Implement RPC messages: `RequestVoteArgs`, `RequestVoteReply`, `AppendEntriesArgs`, `AppendEntriesReply`, `InstallSnapshotArgs`, `InstallSnapshotReply`.
- [x] Implement `LogEntry`, `Command`, `Snapshot`.
- [x] Implement `RaftLog`: append, truncate, get entry, slice, commit, snapshot compaction.
- [x] Implement `StateMachine` interface and in-memory `KeyValueStateMachine`.

### Phase 2: Deterministic Network Simulator
- [x] Implement `NetworkTransport` interface.
- [x] Implement `DeterministicNetwork`: message routing, packet drop, delivery delays, network partitions, virtual time progression.
- [x] Implement `SafetyChecker`: automated assertion of Election Safety, Leader Append-Only, Log Matching, Leader Completeness, State Machine Safety.

### Phase 3: Raft Node State Machine & Consensus Logic
- [x] Implement `RaftRole` enum: `FOLLOWER`, `CANDIDATE`, `LEADER`.
- [x] Implement `RaftNode`:
  - Randomized election timeout handling.
  - Candidate election loop and quorum vote counting.
  - Pre-Vote extension preventing partitioned nodes from inflating terms.
  - Heartbeat generator and follower liveness tracking.
  - Log replication loop: nextIndex/matchIndex tracking per peer, quorum commit advancement.
  - Snapshot creation and installation.
  - Client command submission: leader redirection or commit future.

### Phase 4: Invariant & Partition Test Suites
- [x] Unit Test: `LeaderElectionTest` (single leader per term, split votes, re-election on leader crash).
- [x] Unit Test: `LogReplicationTest` (happy path replication, uncommitted entry overwrites on leader failover).
- [x] Unit Test: `NetworkPartitionTest` (asymmetric 3v2 partition, leader isolation, healing, reconnection catching up).
- [x] Unit Test: `LogCompactionSnapshotTest` (snapshotting, installing snapshot on lagging follower).
- [x] Unit Test: `SafetyInvariantsFuzzTest` (fuzzing random network partitions and message drops while verifying all 5 Raft safety invariants).

### Phase 5: Interactive CLI, Benchmark & Documentation
- [x] Implement `RaftCli` interactive multi-node cluster demo with commands: `init`, `status`, `tick`, `run`, `put`, `get`, `isolate`, `partition`, `heal`, `bench`.
- [x] Implement `RaftBenchmark` measuring consensus commit latency and ops/sec under varying network latency profiles.
- [x] Write `docs/EXPLAIN.md` (6 sections), `README.md`, `CHANGELOG.md`, `LICENSE`, `Dockerfile`, `.github/workflows/ci.yml`.
- [x] Build shaded JAR, run full test suite with Maven (`mvn clean test`).
- [x] Git commit, tag `v1.0.0`, publish to GitHub (`gh repo create Kanak234/raft-consensus-protocol-engine --public ...`).
- [x] Update `PROGRESS.md`, `PUBLISH_LOG.md` (6/10), and `REPORT.md`.
