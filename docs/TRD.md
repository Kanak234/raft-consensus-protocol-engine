# Technical Requirements Document (TRD)

## Project: `raft-consensus-protocol-engine`
**Language:** Java 21 LTS  
**Runtime:** Java Virtual Machine (OpenJDK 21+)  
**Build System:** Apache Maven (maven-compiler-plugin, maven-shade-plugin, maven-surefire-plugin)  
**Dependencies:** Zero external runtime dependencies (`java.base` only). JUnit 5 for testing.  

---

## 1. System Architecture

```
+-------------------------------------------------------------------------+
|                        Raft Node (StateMachine)                         |
|     [Role: Follower / Candidate / Leader]    [Term: uint64]             |
+-------------------------------------------------------------------------+
     |                    |                      |                  ^
     | (Logs)             | (State Machine)      | (RPC Handler)    | (Apply)
     v                    v                      v                  |
+-------------+   +-------------------+   +--------------------+    |
| RaftLog     |   | StateMachine (KV) |   | RaftRpcDispatcher  |----+
| (Entries &  |   | (Apply committed  |   | RequestVote        |
|  Snapshots) |   |  commands)        |   | AppendEntries      |
+-------------+   +-------------------+   | InstallSnapshot    |
                                          +--------------------+
                                                    ^
                                                    | (Virtual Messages)
                                                    v
                                          +--------------------+
                                          | Deterministic      |
                                          | Network Simulator  |
                                          | (Partitions, Drop, |
                                          |  Delay, Reorder)   |
                                          +--------------------+
```

---

## 2. Core Components & State Machines

### 2.1 Persistent & Volatile State on All Nodes
```java
public class RaftState {
    // Persistent state on all servers (must be updated before replying to RPCs)
    private long currentTerm = 0;
    private String votedFor = null;
    private final RaftLog log;

    // Volatile state on all servers
    private long commitIndex = 0;
    private long lastApplied = 0;

    // Volatile state on leaders (re-initialized after election)
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();
}
```

### 2.2 Invariant Verification Engine (`SafetyChecker`)
Evaluates consensus safety invariants continuously after every state mutation or clock tick:
1. **Election Safety:**
   $$\forall \text{term } t, \quad \left| \{ s \in \text{Servers} \mid s.\text{role} == \text{LEADER} \land s.\text{term} == t \} \right| \le 1$$
2. **Log Matching:**
   If $s_1.\text{log}[i].\text{term} == s_2.\text{log}[i].\text{term}$, then $\forall k \le i, s_1.\text{log}[k] == s_2.\text{log}[k]$.
3. **Leader Completeness:**
   If entry $e$ at index $i$ is committed in term $t$, then in all future terms $t' > t$, the leader in term $t'$ contains $e$ at index $i$.
4. **State Machine Safety:**
   $$\forall i, \quad \text{applied}(s_1, i) == \text{applied}(s_2, i)$$

### 2.3 Deterministic Network Simulator (`DeterministicNetwork`)
- Pure discrete-event message bus.
- Network partitions defined as directed or undirected connectivity matrices:
  - `void partition(Set<String> partitionA, Set<String> partitionB);`
  - `void isolate(String node);`
  - `void heal();`
- Deterministic event queue ordered by `(deliveryTime, messageId)`.
- Virtual clock advancing monotonically on `tick(long millis)`.

---

## 3. Technology Stack & Packaging

- **Compiler:** `javac` 21 (`--release 21 -Xlint:all -Werror`)
- **Build Plugin:** `maven-shade-plugin` creating an executable standalone JAR with `RaftCli` as main-class.
- **Testing:** `maven-surefire-plugin` executing comprehensive safety and partition tests.
