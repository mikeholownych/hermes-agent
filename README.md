# Charterforge

Charterforge is a governed autonomous-company runtime. Its initial autonomous
agent is the company’s Founder and Chief Executive Officer. The human operator
is an advisor, stakeholder, and legal principal where unavoidable—not the
routine planner, dispatcher, project manager, or source of every next action.

Charterforge is independently developed from the
[Hermes Agent](https://github.com/NousResearch/hermes-agent) foundation. It is
not an official Hermes Agent distribution. See [ATTRIBUTION.md](ATTRIBUTION.md)
and [LICENSE](LICENSE).

## Operating premise

The Founder/CEO owns mission interpretation, strategy, objective creation,
prioritization, organizational design, delegation, bounded resource
allocation, execution oversight, evidence review, operating cadence,
adaptation, reporting, and escalation.

Human silence does not stop ordinary authorized operations. The CEO proceeds
when the next action follows from company mission, active objectives,
authoritative state, existing decisions, constraints, evidence, delegated
authority, and success criteria. It escalates only when required authority or
evidence is absent.

Open advisor interventions are a hard human boundary: only explicitly
identified human advisor/owner/approver identities may resolve them. CEO,
worker, and runtime identities cannot self-approve an escalation.

The company begins as a solo-founder organization. A specialist worker is
considered only when evidence shows a durable capability or capacity gap.
Staffing policy evaluates whether bounded contract work or an FTE is warranted
and records the reporting relationship in an enterprise hierarchy.

## Implemented runtime

The independent runtime currently includes:

- durable organizations, Founder/CEO mandates, objectives, immutable plan
  versions, candidate actions, permits, execution results, and verification
  records;
- an event-driven operational loop with claims, retries, resource leases,
  circuit breakers, recovery, expiry, stop, and escalation semantics;
- deterministic policy evaluation against setup-time authority, system,
  risk, spend, and resource limits;
- solo-founder self-dispatch and employee delegation bound to exact immutable
  capabilities, systems, toolsets, skills, budgets, and expiry;
- evidence-based contractor-versus-FTE evaluation and hierarchical employee
  provisioning;
- append-only accounting, treasury reservations, budgets, payment intents,
  fiscal periods, tax assessment records, filing receipts, and payment
  evidence;
- non-custodial payment-rail contracts with independent read-back
  verification;
- compliance inventory, deadlines, evidence, exceptions, and audit export;
- business status and decision-memo projections.

The implementation is not a legal entity, bank, accountant, tax professional,
payment processor, or substitute for legal advice. Payment-provider settlement,
government filings, and jurisdiction-specific compliance remain dependent on
configured external systems and legally authorized humans.

## Canonical names

| Surface | Canonical value |
|---|---|
| CLI | `charterforge` |
| Python distribution and namespace | `charterforge` |
| Environment prefix | `CHARTERFORGE_` |
| POSIX state root | `~/.charterforge` |
| Windows state root | `%LOCALAPPDATA%\charterforge` |
| Container/image | `charterforge` |
| Service prefix | `charterforge-` |

Legacy `hermes`, `HERMES_*`, `~/.hermes`, and inherited Python modules remain
temporarily available only for migration compatibility. New automation should
not introduce them.

## Source installation

This repository is under active independent development. There is not yet a
published Charterforge installer or container registry release.

```bash
git clone git@github.com:mikeholownych/hermes-agent.git charterforge
cd charterforge
uv sync
uv run charterforge --help
uv run charterforge setup
```

Local artifact installation is also supported after building the package; see
[packaging](docs/packaging.md) for the verified wheel, CLI, and independent
installer smoke commands.

On Windows, use the PowerShell installer instead: `scripts/install.ps1`.

For a persistent container deployment, use the documented Compose profile in
[agentic bootstrap](docs/agentic-bootstrap.md). The `agentic` profile runs the
standalone Founder/CEO supervisor against the mounted Charterforge state; it
does not establish production provider or compliance readiness.

Do not use the upstream Hermes installer and assume it installs Charterforge.
That installer targets the upstream distribution.

## Governed business setup

The following commands exist in this checkout:

```bash
uv run charterforge setup
uv run charterforge business --help
uv run charterforge business readiness
uv run charterforge objectives --help
uv run charterforge gateway --help
uv run charterforge backup --help
uv run charterforge doctor
```

The persistent standalone CEO worker can be supervised independently when the
charter admits `runtime_host: "standalone"` or `"either"`:

```bash
uv run charterforge objectives worker
uv run charterforge objectives worker-status
```

It stops and records a durable failure/escalation state when authority,
provider evidence, or runtime readiness is insufficient.

The setup flow must explicitly enable governed operation and establish the
standing charter. Empty solo-founder toolsets mean the CEO may govern but may
not launch general-purpose execution. Exact capabilities, systems, toolsets,
skills, budgets, prohibited actions, and resource ceilings should be selected
before unattended operation.

To run the complete current-tree install/bootstrap/readiness/worker/restart
acceptance proof:

```bash
scripts/run_agentic_acceptance.sh
```

## Development validation

Release status is explicit: the controlled Founder/CEO runtime acceptance
passed at the tagged evidence commit
`4f7d585b8d0eda6f0d7646c843f9ddd43c4d7afe` (`v0.19.0-agentic-foundation`).
Current `main` contains additional unreleased commits and has a separate
post-boundary focused evidence run recorded in [READINESS.md](READINESS.md);
that run does not turn current `main` into the tagged release or establish
production autonomous-business readiness.

Commands actually completed successfully during the autonomous-runtime work:

```bash
source .venv/bin/activate
pytest -q tests/hermes_cli/test_workforce_delegation.py \
  tests/hermes_cli/test_objective_policy.py \
  tests/hermes_cli/test_organization_db.py \
  tests/hermes_cli/test_setup_agentic.py \
  tests/hermes_cli/test_agentic_business_e2e.py \
  tests/hermes_cli/test_objective_adapters.py \
  tests/hermes_cli/test_kanban_db.py
# 281 passed

python -m compileall -q cli.py hermes_cli
git diff --check
uv run charterforge --help
uv run charterforge --version
```

The focused lint command did **not** run because `ruff` is not installed in
the current `.venv`; neither `ruff` nor `python -m ruff` was available. This is
recorded as a limitation, not a passing check.

## Documentation

- [Readiness determination and release evidence](READINESS.md)
- [Agentic Business OS guide](website/docs/guides/agentic-business-os.md)
- [Non-interactive agentic bootstrap](docs/agentic-bootstrap.md)
- [Architecture](docs/architecture.md)
- [Company operating model](docs/company-operating-model.md)
- [Security and threat model](docs/security.md)
- [Operations, backup, and recovery](docs/operations-runbook.md)
- [Rebranding and state migration](docs/rebranding-and-migration.md)
- [Implementation status and limitations](docs/implementation-status.md)
- [Upstream attribution](ATTRIBUTION.md)
- [Development guide](AGENTS.md)

Some linked Charterforge documents are being added as part of the active
rebrand. A missing document is not evidence that its subject is implemented.

## License

Charterforge retains the upstream MIT license and required Nous Research
copyright notice. Rebranding does not remove upstream or third-party
obligations.
