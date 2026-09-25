# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.2.x   | Yes |
| < 0.2   | No (use 0.2.x) |

Security fixes land on the latest release line on `main`.

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report privately via
[GitHub Security Advisories / Private Vulnerability Reporting](https://github.com/shu0819-sjy/byo-redis/security/advisories/new)
for this repository (repository → *Security* → *Report a vulnerability*).

Include:

- Affected version or commit
- Description of the issue and impact
- Steps to reproduce with **placeholder** credentials only
- Any suggested fix if you have one

We aim to acknowledge reports within **7 days** and to agree a fix or mitigation
timeline after triage. Credit is optional and anonymous by default.

## Scope

In scope:

- The `byo_redis` package (protocol parser bounds, AUTH, bind policy, persistence
  path handling, replication handshake)
- Crashes or memory blow-ups reachable via crafted RESP from an untrusted client
  when the server is intentionally exposed
- Bypass of the non-loopback bind guard when AUTH is not configured

Out of scope:

- Using BYO-Redis as a drop-in production Redis (no TLS, no ACL users, subset
  commands only — see README limitations)
- Operator misconfiguration (binding publicly without an encrypted network
  boundary, weak shared passwords, shared SQLite-style mistakes N/A here)
- Clients or tools outside this repository

## Design posture

- Default bind is loopback; non-loopback bind requires AUTH or an explicit unsafe
  override.
- Protocol parser enforces buffer / nesting limits.
- Single shared password `AUTH` only — not a multi-user ACL system.
- No TLS in-process: remote exposure still needs an encrypted network boundary.

## Secrets hygiene

Never commit real `AUTH` passwords or operator credentials. If a secret was ever
committed, **rotate it first**; history cleanup is secondary to rotation.
