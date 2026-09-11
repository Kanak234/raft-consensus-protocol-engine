# Security Policy

## Supported Versions

Security updates and vulnerability patches are provided for the following releases:

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |
| < 1.0.0 | :x:                |

## Reporting a Vulnerability

If you discover a security vulnerability in `raft-consensus-protocol-engine`, please report it responsibly rather than opening a public issue.

### Disclosure Process

1. **Private Disclosure**: Report vulnerabilities via GitHub Private Vulnerability Reporting or contact the maintainer at `security@kanak.dev` (or via Kanak234 profile contact).
2. **Details to Include**:
   - Detailed description of the vulnerability and attack vector (e.g. state machine desync, log truncation bypass, forged RPC deserialization).
   - Deterministic seed or reproduction test case demonstrating the failure mode.
   - Any suggested mitigations.
3. **Response Timeline**:
   - Initial acknowledgement within 48 hours.
   - Severity assessment and remediation timeline within 5 business days.
   - Coordinated disclosure upon verification and patch release.
