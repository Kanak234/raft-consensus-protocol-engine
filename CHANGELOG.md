# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-09-11

### Added
- Complete implementation of the Raft consensus algorithm according to Diego Ongaro's dissertation.
- Leader election with **Pre-Vote extension** (§9.6) to avoid disruptive elections from reconnected partitioned followers.
- Log replication with fast conflict index backtracking and follower commit safety guarantees (§5.3).
- Log compaction and `InstallSnapshot` RPC (§7) with linearizable key-value state restoration.
- Thread-safe in-memory `KeyValueStateMachine` supporting `PUT`, `GET`, and `DELETE` commands with client sequence deduplication.
- Deterministic discrete-event simulation network (`DeterministicNetwork`) supporting programmable directed partitions, bounded uniform latency, and packet drops.
- Continuous safety verifier (`SafetyChecker`) auditing Election Safety, Leader Append-Only, Log Matching, Leader Completeness, and State Machine Safety on every clock step.
- Interactive Command-Line Interface (`RaftCli`) with live cluster telemetry, partition fault injection, and consensus throughput benchmarking.
- Comprehensive JUnit 5 test suite:
  - `LeaderElectionTest`: single leader election and leader failover re-election.
  - `LogReplicationTest`: linear replication and uncommitted entry rollback.
  - `NetworkPartitionTest`: 3v2 asymmetric partition, uncommitted minority isolation, healing, and convergence.
  - `LogCompactionSnapshotTest`: snapshot installation on partitioned lagging followers.
  - `SafetyInvariantsFuzzTest`: dynamic partition fuzzing and packet drops with 100% invariant compliance.
- Multi-stage Docker container build and GitHub Actions CI workflow.
