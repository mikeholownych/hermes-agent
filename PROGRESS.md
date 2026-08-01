# Charterforge Progress Report — 2026-07-28

## Release Status

| Version | Status | Description |
|---------|--------|-------------|
| **v0.20.0** | ✅ Released | Defensible autonomous company with crash recovery proof |
| **v0.21.0-rc.1** | 🔶 RC | Payment rails (Strip, Nevermined, Circle) |
| **v0.22.0** | 🔨 In Progress | Postgres authority + DB CLI |

## Test Coverage

```
Core Tests (22 passing):
  - test_worker_fault_injection.py:     9 tests (governed worker)
  - test_supervisor_lifecycle.py:       5 tests (real subprocess)
  - test_db_commands.py:               8 tests (CLI commands)

Optional Tests (23 when deps installed):
  - test_postgres_authority.py:        16 tests (requires psycopg)
  - test_stripe_rail.py:                7 tests (requires package)
```

## Architecture

```
Charterforge v0.21+ Architecture
├── Core (hermes_cli)
│   ├── governed_worker/ — Crash recovery invariants
│   ├── postgres_authority.py — Production authority store
│   └── db_commands.py — Database management CLI
│
├── Payment Rails (packages/)
│   ├── charterforge-stripe-rail — Cards, global coverage
│   ├── charterforge-nevermined-rail — Agent-to-agent USDC
│   └── charterforge-circle-rail — Native USDC, CCTP
│
└── Authority Store
    ├── SQLite (dev/testing) — Default
    └── Postgres (production) — Requires charterforge[postgres]
```

## OTA Pilots

Trialing [ota-run/ota](https://github.com/ota-run/ota) — a Rust CLI that declares a repo's setup/task truth (toolchains, tasks, dependency ordering) in one `ota.yaml` contract instead of scattering it across loose scripts and duplicated `npm ci`/`uv sync` invocations across separate CI workflow files.

| Pilot | Workflow | PR | Status | What it tests |
|-------|----------|----|--------|----------------|
| #1 | `docs-site-checks.yml` | #6 | ✅ Merged | Node/Docusaurus task chain: npm ci → skill extraction/regen → diagram lint → build |
| #2 | `payment-rails.yml` | #7 | ✅ Merged | Python wheel build + fresh-venv install + entry-point verification, across all 3 rails (Stripe, Nevermined, Circle) |

Both scoped narrowly (single, isolated workflow each) and verified end-to-end — locally, in PR CI, and post-merge on `main` — before merging. Each collapses several bespoke workflow steps into one `ota run <task>` call, with the task graph declared once in the shared root `ota.yaml`.

**Investigated and declined:** expanding to `js-tests.yml`. That workflow dynamically discovers npm workspace packages + their check scripts at runtime and fans them into a matrix (currently 10 package/script pairs across 6 packages). OTA has no construct for "dynamically enumerate N things and run each" — `variants` is a conditional-selection mechanism, not a matrix, and task `inputs` don't thread into the executed command through any tested binding (env var, templating, positional args). The only path would be hardcoding today's 10 tasks, which would silently stop covering any future new package or script — a real regression on a workflow that gates every PR. Left as-is by deliberate decision.

## Next Actions

1. Test v0.21.0-rc.1 in real environment
2. Complete v0.22.0: alembic migrations + health check
3. Start v0.23.0: multi-tenant isolation

## Commits This Session (2026-07-27 to 2026-07-28)

```
bf557fc52 - Supervisor lifecycle tests (5 tests)
bd58f3acd - Nevermined + Circle payment rails
1b4ee49db - Roadmap v0.21.0 in-progress
b9a10404f - Stripe rail integration tests (7 tests)
cd56c5fd7 - CI workflow for payment rails
acbb3b8e0 - v0.21.0 ready for RC
00197ac59 - Postgres authority store (16 tests)
84968b8d5 - Roadmap v0.22.0 in-progress
e3f53552a - Postgres CI workflow
fd0485cab - Database CLI commands (8 tests)
6f152f5eb - Roadmap update
392cf7186 - Payment rails setup guide
```

## Evidence

- **v0.20.0 Release**: https://github.com/mikeholownych/charterforge/releases/tag/v0.20.0
- **v0.21.0-rc.1 Release**: https://github.com/mikeholownych/charterforge/releases/tag/v0.21.0-rc.1
- **CI Passes**: All core tests passing locally and in CI
- **Proof**: Real subprocess fault injection + Postgres authority operations
- **OTA Pilot #1 (docs-site-checks.yml)**: https://github.com/mikeholownych/charterforge/pull/6
- **OTA Pilot #2 (payment-rails.yml)**: https://github.com/mikeholownych/charterforge/pull/7
