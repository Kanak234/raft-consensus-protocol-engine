# Product Requirements Document (PRD)

## Project: `raft-consensus-protocol-engine`
**Repository:** `Kanak234/raft-consensus-protocol-engine`  
**Author:** Kanak Prabhakar (`Kanak234`)  
**Domain:** Distributed Systems / Consensus Protocols  
**Language:** Java 21 LTS  
**Build System:** Apache Maven  
**License:** MIT  

---

## 1. Product Overview & Problem Statement

Building fault-tolerant distributed services (distributed key-value stores, distributed databases, cluster schedulers) requires replicated state machines to maintain a consistent state across a cluster of independent nodes despite arbitrary network delays, message reordering, packet drops, and node crashes (non-Byzantine failures).

The Paxos consensus algorithm has historically been notoriously difficult to understand, implement, and verify correctly. The Raft consensus algorithm (introduced by Diego Ongaro and John Ousterhout in 2014) addresses this by decomposing consensus into three orthogonal subproblems:
1. **Leader Election:** Electing a distinguished leader safely using randomized election timeouts.
2. **Log Replication:** Replicating an append-only sequence of log entries from the leader to followers, forcing follower logs to match the leader's.
3. **Safety & Invariants:** Enforcing strict election rules ensuring any elected leader has all committed entries from prior terms.
4. **Log Compaction:** Discarding committed log history through snapshotting (`InstallSnapshot`) without blocking ongoing client operations.

`raft-consensus-protocol-engine` is a pure Java 21, production-grade Raft consensus implementation featuring zero external framework dependencies, strict verification of all Raft safety invariants, and a deterministic in-process discrete-event network partition simulator capable of reproducing network split-brain, message drop/delay queues, and healing.

---

## 2. Goals & Non-Goals

### 2.1 Goals
- **Full Raft Protocol Specification:** Complete implementation of Leader Election, Log Replication, Heartbeats, Commit Index advancement, Log Compaction (Snapshots), and State Machine Application.
- **Safety Invariant Verification:** Programmatic, automated assertion of all Raft safety properties:
  1. *Election Safety:* At most one leader elected per term.
  2. *Leader Append-Only:* Leaders never overwrite or truncate their own logs.
  3. *Log Matching Property:* If two logs contain an entry with the same index and term, they are identical across all previous entries.
  4. *Leader Completeness:* Any entry committed in a term is present in all future leaders' logs.
  5. *State Machine Safety:* Replicated state machines apply identical entries in identical order across all nodes.
- **Deterministic Network-Partition Simulation:** In-process discrete-event simulation harness capable of introducing:
  - Symmetric and asymmetric network partitions (e.g. $\{1, 2\}$ vs $\{3, 4, 5\}$).
  - Packet drops, message duplication, and bounded delays.
  - Virtual deterministic clock (no timing flakiness or dependence on wall-clock sleep).
- **Linearizable Client Interface:** Support for client read and write commands with futures/callbacks, leader redirection, and linearizable reads.
- **Zero Third-Party Runtime Dependencies:** 100% pure Java 21 (`java.base`), zero Netty/gRPC/Spring bloat.

### 2.2 Non-Goals
- **Byzantine Fault Tolerance (BFT):** Raft assumes non-Byzantine crash-fault tolerance. Malicious or compromised nodes are out of scope.
- **Dynamic Cluster Membership Changes (Joint Consensus):** Focused on fixed-size clusters ($N=3, 5, 7$ nodes), which covers $>99\%$ of operational Raft deployments.

---

## 3. Target Users & Use Cases

- **Distributed Systems Engineers:** Embedding consensus coordination into custom databases, coordination services, or message brokers.
- **Cloud Infrastructure Architects:** Validating failover scenarios and quorum behaviors under network degradation.
- **Academic & Industry Researchers:** Rigorous model-checking and deterministic simulation of Raft edge cases.

---

## 4. Key Functional Features & Architecture

### 4.1 Raft Node Roles & Transitions
- **Follower:** Responds to incoming RPCs from candidates and leaders. Resets election timer upon receiving valid `AppendEntries` or granting vote. Transitions to Candidate upon election timeout.
- **Candidate:** Increments current term, votes for self, broadcasts `RequestVote` RPCs. Transitions to Leader upon receiving votes from a quorum ($\lfloor N/2 \rfloor + 1$). Reverts to Follower if a leader with term $\ge$ candidate's term is discovered.
- **Leader:** Dispatches periodic empty `AppendEntries` (heartbeats). Appends client commands to local log, replicates to followers via `AppendEntries`, advances `commitIndex` once an entry is replicated to a quorum, and applies committed entries to the State Machine.

### 4.2 RPC Specification
1. **`RequestVote(term, candidateId, lastLogIndex, lastLogTerm)`** $\to$ **`{term, voteGranted}`**
   - Rules: Grant vote if `term >= currentTerm`, `(votedFor == null || votedFor == candidateId)`, and candidate's log is at least as up-to-date as receiver's log.
2. **`AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)`** $\to$ **`{term, success, matchIndex}`**
   - Rules: Reply false if `term < currentTerm`. Reply false if log doesn't contain entry at `prevLogIndex` matching `prevLogTerm`. If an existing entry conflicts with a new one, delete existing entry and all that follow it. Append any new entries not already in log. Update `commitIndex = min(leaderCommit, indexOfLastNewEntry)`.
3. **`InstallSnapshot(term, leaderId, lastIncludedIndex, lastIncludedTerm, data)`** $\to$ **`{term}`**
   - Compacts follower log and installs snapshot state when follower lags behind leader's compacted log window.

### 4.3 Deterministic Network Simulator
- Explicit `NetworkSimulator` routing messages through configurable virtual links:
  - `partition(Set<NodeId> groupA, Set<NodeId> groupB)`: isolates groups.
  - `heal()`: restores all connectivity.
  - `dropPacket(Predicate<Message>)`: simulates packet loss.
  - `step()` / `runUntilQuorum()`: executes event loop deterministically.

---

## 5. Non-Functional Requirements

- **Code Quality:** Java 21 record classes, pattern matching, sealed interfaces where appropriate. Clean modular architecture.
- **Reliability:** 100% deterministic test suite execution with 0 flaky tests.
- **Packaging:** Apache Maven build producing executable shaded JAR, Docker container for interactive multi-node cluster demo.
