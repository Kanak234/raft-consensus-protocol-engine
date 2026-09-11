# Contributing to Raft Consensus Protocol Engine

Thank you for contributing!

## Branching & Protection Rules

The default branch (`main`) is protected with the following requirements:
- **Pull Requests Required**: Direct pushes to `main` are restricted. All contributions must arrive through feature/fix branches via pull requests.
- **Passing Status Checks**: All PRs must pass the `Build, Lint & Test (Java 21)` CI pipeline before merging.
- **Linear History & Integrity**: Force pushes and branch deletions are disabled on `main`.
- **Code Coverage Standards**: Maintain $\ge 80\%$ line coverage on core protocol classes (`io.kanak.raft.core.*`), enforced by JaCoCo.

## Local Development Workflow

1. Branch off `main`:
   ```bash
   git checkout -b feat/your-feature
   ```
2. Run test suites and verify JaCoCo coverage:
   ```bash
   mvn clean verify
   ```
3. Run microbenchmarks locally:
   ```bash
   java -jar target/raft-consensus-engine.jar --bench 1000
   ```
4. Push and submit a Pull Request against `main`.
