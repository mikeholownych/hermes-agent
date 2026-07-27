# Changelog

## 0.20.0 — 2026-07-27

**Governed autonomous company runtime with proven crash recovery.**

This release establishes the authority-bound supervised worker lifecycle with
deterministic crash recovery proof. The autonomous business can be installed,
bootstrapped from a charter, and funded via payment rails.

### Governance and Authority

- Added comprehensive fault-injection acceptance tests proving crash recovery
  invariants for supervised workers: claim-before-execute redelivery,
  effect-before-evidence readback, evidence-before-complete idempotency,
  complete-before-CEO-wake sync, stale-worker fencing, master-stop blocking
  mid-execution, exclusive claim CAS, run-id CAS preventing double-complete,
  and idempotent re-entry after crash. All 9 tests pass.
- Orchestrator-only Kanban list and unblock actions now require exact permits
  bound to filters/limits/tenant/archive mode or task/board target,
  respectively.
- Kanban full-task inspection and attachment listing now require exact
  task/board read permits, preventing cross-task disclosure of bodies,
  comments, runs, events, and artifact metadata.
- Kanban completion and block transitions now require exact lifecycle permits
  bound to task, board, handoff/reason payload, artifacts or block kind, and
  expected worker run identity before objective state can advance or pause.
- Kanban heartbeat lease extensions now require an exact permit bound to task,
  board, note, claim lock, and expected worker run identity.
- Kanban inline and URL attachment writes now require exact artifact permits
  bound to task, board, filename, content identity/source, and content type
  before durable storage or remote download.
- Kanban comments now require an exact `kanban.comment` permit bound to the
  task, board, runtime author, and redacted body before durable handoff
  evidence is written.
- Kanban task creation now requires an exact contract-bound `kanban.create`
  permit covering task identity, assignment, dependencies, tenant, workspace,
  project, skills, model/provider, goal settings, status, session, and board.
- Kanban task linking now requires a `kanban.link` permit bound to the exact
  parent task, child task, board, and operation before durable state changes.
- `execute_code` permits now bind the script, task identity, effective sandbox
  tool allow-list, and backend environment; a script grant cannot be replayed
  with broader RPC capabilities or a different execution target.
- Remote image and video ingestion now requires URL-bound `vision.read` and
  `video.read` permits before any download; local media grants do not imply
  authority to fetch arbitrary external media.
- Web search and extraction permits now bind the complete provider request,
  including query or URL plus result limit, output format, and character limit;
  a worker cannot broaden quota or change returned content under one grant.
- Terminal execution permits now bind the exact command, resolved working
  directory, backend, timeout, background/PTY mode, task identity, and
  notification/watch settings; a command grant cannot be replayed with a
  different execution context.
- Browser navigation permits now bind to the exact browser session as well as
  the destination URL, preventing a URL grant from being replayed in another
  worker's browser context.
- Outbound `send_message` and reaction actions now require permits bound to
  the resolved platform, chat, thread, exact message or emoji, media list, and
  delivery mode; target aliases and content changes cannot reuse a grant.
- Feishu document comment replies and additions now require separate,
  operation-specific permits bound to the exact document, comment (when
  applicable), content, and file type before any provider POST.

### Payment Rails

- Added `business payment-rails --check`, a read-only machine-check contract
  that returns non-zero when a discovered inbound or outbound rail is
  unavailable, without attempting money movement.
- Added `charterforge-stripe-rail` package with webhook authentication,
  signature verification, and event routing into objectives.
- Documented payment rail options: Stripe, Nevermined, Circle, Crossmint,
  Wise, Payoneer, PayPal for geographic flexibility.

### Readiness and Deployment

- Added `business readiness --check`, a non-mutating exit-status contract for
  supervisors and container healthchecks; blocked or unconfigured readiness
  returns status 1 while preserving the diagnostic JSON projection.
- Added an independent GitHub Actions artifact workflow that builds and
  isolated-installs the Charterforge wheel/sdist on pull requests, `main`, and
  version-tag pushes without publishing to an index automatically.
- Added Docker Compose profile for standalone Founder/CEO supervisor.
- Added `examples/autonomous-business-charter.json` with 4-phase execution
  strategy (ideation → validation → build → scale) and success criteria.

### Delegation and Process Separation

- Governed employee worker launches now carry and verify exact capabilities
  and systems in addition to toolsets and skills; a subprocess with a broader
  semantic surface fails closed before it can perform task work.
- Permit records now derive and persist the exact target resource from the
  immutable action payload; callers attempting to retarget a permit are
  rejected, and delegator budget enforcement remains cumulative across active
  grants.
- Added a process-separated delegation acceptance gate: the Founder/CEO creates
  a grant-bound Kanban task, a fresh subordinate Python process validates the
  exact profile/mandate/toolset/skill grant, records evidence in the task run,
  and a fresh CEO runtime consumes the completion event and verifies the parent
  objective. The proof uses deterministic local databases and providers.

### Release Gates (Remaining)

- Corporate formation, legal personhood, banking, and human legal-principal
  actions remain outside the software's authority.
- Production deployment, high availability, disaster-recovery drills, and
  external payment-provider credentials are deployment work.
- SQLite is the implemented authority store; Postgres and an external broker
  remain deployment work.
- PCI DSS, SOC 2, SOX, GDPR, EU AI Act, CASL, CAN-SPAM, and jurisdiction-specific
  tax or payments applicability remain deployment and legal work.

## Unreleased
  the subordinate mandate and objective scope.
- Non-root managers must now hold an active parent grant before sub-delegating;
  child grants are bounded by that parent grant's capabilities, systems,
  toolsets, skills, exact resource, budget, and expiry, preventing transitive
  privilege amplification through the reporting hierarchy.
- Non-root managers can no longer manufacture delegation authority by
  self-dispatching from a standing mandate; self-assigned grants also require
  an active parent grant from the manager above them.
- Browser worker interactions now require operation-specific capabilities and
  an exact browser-session resource. Navigation remains URL-scoped; click,
  type, scroll, history, keypress, console, evaluation, image, snapshot, and
  vision operations cannot be inferred from a generic browser toolset.
- Raw `browser_cdp` escape-hatch calls now require an exact `browser.cdp`
  permit bound to the worker session, CDP method, and optional target/frame;
  direct CDP access cannot bypass the governed browser surface.
- Desktop `computer_use` actions now require exact `computer.<operation>`
  permits bound to a desktop session. Governed workers use the permit as their
  execution authority instead of relying on an ambient interactive approval.
- Dynamically discovered MCP tools and MCP resource/prompt utilities now
  require `mcp.call` permits bound to the exact configured server and tool
  (plus URI or prompt name where applicable); MCP cannot bypass the authority
  plane merely because a server advertises a tool.
- Cron scheduler mutations (`create`, `update`, `pause`, `resume`, `remove`,
  and immediate run) now require operation-specific permits bound to the exact
  scheduled job, or the exact creation identity before a job exists.
- Governed workers can no longer use the legacy conversational `delegate_task`
  fan-out path without a workforce permit bound to the exact delegation
  payload; ordinary interactive delegation remains unchanged.
- Discord and Discord-admin REST actions now require operation-specific
  permits bound to a hash of the exact action payload before any external API
  request is sent.
- Home Assistant service calls now require a `homeassistant.call_service`
  permit bound to the exact HA instance, service domain/name, entity, and data
  payload before device state can be changed.
- Grant admission now requires the delegator employee and mandate to be
  active; suspended or expired managers cannot issue new authority.
- Delegated grants now persist an immutable `parent_grant_id`; authority
  verification can prove the exact parent authorization chain instead of
  inferring it from aggregate mandate fields.
- Parent-grant delegation is now bounded to the same objective and action
  type, with cumulative child-budget accounting so repeated sub-delegation
  cannot exceed the parent grant ceiling.
- Delegated worker launch and result handoff now require an independent full
  grant-chain integrity verification in addition to live mandate checks.
- Result handoff now revalidates the exact Kanban task contract and accepts an
  explicit board identity, preventing a tampered or cross-board task from
  waking CEO planning.
- Governed completed Kanban task results are now append-only; post-handoff CLI
  edits are rejected instead of mutating evidence used by CEO verification.
- Governed task completion is now authorized again at the database write
  boundary: direct callers must present the exact worker task and execution
  contract, and the live grant/mandate projection is revalidated before the
  task can transition to `done`; rejected attempts are audited.
- Delegation grants now bind any explicit subordinate contract embedded in the
  action payload: capabilities, systems, toolsets, skills, budget, and expiry
  must match exactly, so a manager cannot broaden or substitute the worker's
  authorized surface while remaining within a broad mandate.
- Governed file-worker operations now revalidate the live grant at execution
  time and require exact `file.read` or `file.write` capability, `localhost`
  system, and canonical target-resource equality before opening the path.
- Employee grants now persist worker resource scope separately from the
  Kanban binding scope, allowing an exact local file or service resource to be
  authorized without confusing it with the task board used for coordination.
- Governed terminal workers now require `terminal.exec` plus an exact
  `command:<shell text>` resource at execution time; a worker with only a
  broad terminal/toolset grant cannot run an unlisted command.
- Governed web search and extraction now require execution-time `web.search`
  or `web.read` authorization with exact query/URL resources; web toolsets no
  longer imply unrestricted external destinations.
- Governed browser navigation now requires `browser.navigate` and an exact URL
  resource before any page is opened.
- Governed cross-channel messaging and reactions now require exact
  `message.send` or `message.react` capability plus platform/target resource
  equality before an outbound action is attempted.
- Added a direct delegated-worker boundary regression proving that an exact
  read target is allowed only for the granted resource and operation; sibling
  resources and write capabilities are rejected at the live authorization
  chokepoint.
- The delegation regression now exercises the concrete local-file contract:
  `file.read` on `localhost:/home/mike/ceofile.txt` cannot read
  `/home/mike/notceofile.txt` or write the granted file.
- The same regression now invokes the real `read_file_tool` and
  `write_file_tool` entry points, proving the exact grant is enforced before
  filesystem access rather than only by a direct policy-unit call.
- The installed process-separated delegation acceptance now executes the real
  file tools in the subordinate process, records a `file_tool_boundary` pass,
  and wakes the CEO only after that evidence-bearing task completion.
- Canonical toolset ordering is now persisted in delegation grants, preventing
  valid multi-toolset grants from failing integrity verification after restart.
- Governed `patch_tool` writes now pass through the same exact `file.write`
  authorization boundary as `write_file_tool`; empty or unresolved patch
  targets fail closed for contracted workers.
- The process-separated acceptance now proves that a read-only worker cannot
  bypass write authority through patch replacement.
- Governed `search_tool` access now requires an explicit `file.search` grant
  bound to the exact canonical search root; `file.read` no longer implies
  directory enumeration or content search.
- The process-separated acceptance now records rejection of unauthorized file
  search in addition to direct writes and patch replacement.
- Governed `execute_code` now requires an explicit `code.execute` capability
  bound to the exact SHA-256 script resource before any child process or
  sandbox dispatch; file and terminal grants do not imply arbitrary Python.
- The process-separated acceptance now proves ungranted code execution is
  rejected before it can open a host file.
- Local vision and video reads now require explicit `vision.read` or
  `video.read` authority bound to the canonical media path; media tools cannot
  bypass file authorization by opening a path directly.
- The process-separated acceptance now proves an ordinary file grant cannot
  invoke the vision resolver on a host path.
- Bootstrapped organization actor authorization now rejects unknown
  `employee:<id>` identities; the compatibility `employee:ceo` alias resolves
  only to the active CEO in the objective's organization, while legacy
  `__unscoped__` objective stores retain their explicit compatibility path.
- Organization-bound permits now require a known employee executor; control
  or arbitrary service labels cannot receive execution authority implicitly.
- CEO compatibility alias resolution now fails closed when active CEO state is
  missing or ambiguous instead of selecting the first matching row.
- Revocation checks now traverse the parent-grant chain and fail closed when
  any ancestor is revoked or the chain is cyclic, fencing descendant workers
  and result handoffs immediately.
- The independent package-artifact workflow now exercises
  `scripts/install-charterforge.sh` against the built wheel in a fresh isolated
  environment before uploading artifacts.
- Independent container CI now validates both Linux and Windows Compose
  deployment definitions before building the Charterforge image.
- Authority-integrity verification now independently checks parent-grant
  expiry, budget, capabilities, systems, toolsets, skills, and exact resource
  equality, detecting persisted hierarchy expansion or stale parent evidence.
- Intervention resolution now requires an explicitly identified human advisor
  identity; CEO, worker, and runtime identities cannot self-resolve an open
  escalation even with evidence-shaped payloads.
- Organization-scoped intervention resolution now fails closed when the caller
  omits organization scope; only explicitly unscoped control records may use
  the legacy scope-free path.
- Permit actor-scope checks now tolerate legacy objective-only databases without
  crashing during migration; fully bootstrapped stores continue to enforce the
  employee organization/status boundary.
- Master pause/manual mode now revokes active employee grants and blocks both
  subordinate launch and result handoff, preventing workers already in flight
  from continuing after an autonomy stop.
- Delegation grants now persist the exact action resource scope and reject a
  Kanban binding to a different board; legacy grants without a scope fail closed
  until reissued under a current action contract.
- Tax-rate records now use immutable supersession lineage; amended rates replace
  the current rule explicitly, branch-from-old-rule attempts are rejected, and
  tax calculation ignores superseded rules.
- Lifecycle maintenance now requeues authorized and executing objectives that
  have no pending or processing wake event, allowing a fresh worker to resume
  after a crash between action execution and event scheduling.
- Readiness now blocks a charter that grants `email.send` until its declared
  AgentMail inbox and API key are actually configured; unavailable company
  email is reported as a deterministic blocker instead of failing later at
  execution time.
- Compliance supersession now rejects branching from an already superseded
  assessment or control record, and current-authority projections fail closed
  on ambiguous legacy branches instead of silently selecting one interpretation.
- Payment-provider assessments now use the same immutable supersession lineage:
  revoked screening or registry evidence cannot remain implicitly authorized,
  and readiness ignores superseded provider records.
- `business provider-verify` now exposes bounded `--supersedes-id` and
  `--supersession-reason` options so advisor/legal updates use the governed
  replacement path rather than direct database access.
- Reran the complete current-tree install-to-master-stop acceptance on the
  current branch, including scheduled replanning, uncertain provider
  read-back, inbound tax-bearing settlement, and durable autonomy revocation;
  the deterministic local-provider boundary passed with zero duplicate effects.
- Interrupted idempotent action recovery now has an explicit authority-store
  restart regression: after a worker crash with an uncertain provider effect,
  a fresh runtime connection resumes reconciliation without replanning or
  duplicating the provider call.
- Model-backed planner compute reservations now reconcile as an immutable
  `released` record when an LLM call, response, JSON parse, or typed action
  contract fails before any billable result exists. This prevents rate-limit
  and crash paths from stranding budget holds while preserving the failure
  inference and durable retry boundary.
- The provider recovery acceptance now includes a verified tax rule, proving a
  tax-bearing inbound receipt splits revenue and tax liability correctly while
  preserving idempotent settlement.
- The unified current-tree acceptance now ends with a master autonomy stop,
  proving the worker exits in paused mode without producing another provider
  effect.
- The provider recovery acceptance now covers inbound receivable creation and
  read-back settlement alongside outbound uncertain-action recovery, proving
  idempotent bi-directional money movement at the deterministic rail boundary.
- The current-tree acceptance now asserts that event-driven CEO progress creates
  at least two durable plan versions, proving replanning rather than repeated
  execution of one in-memory plan.
- The current-tree acceptance now admits its initial objective through an
  authenticated, freshness-checked external event subscription with durable
  idempotent routing before CEO execution begins.
- The workforce E2E now proves a subordinate employee worker can launch under
  an exact CEO-issued mandate, return a completed task, and feed that result
  back into the CEO objective inbox for governed planning.
- The current-tree acceptance now includes a durable scheduled wake event,
  proving the CEO worker can initiate the next governed cycle without a human
  dispatch.
- The unified current-tree acceptance now includes an interrupted provider
  effect, a real container restart, read-back reconciliation, and a zero
  duplicate-provider-call assertion after the normal CEO restart path.
- Objective runtime now classifies LLM/provider rate-limit failures, persists a
  durable retry marker and backoff, honors provider retry hints, and resumes
  the claimed event without replaying an external action.
- Added `scripts/run_agentic_acceptance.sh`, a current-tree install-to-restart
  Founder/CEO acceptance scenario with durable recovery and zero duplicate
  provider effects; evidence is recorded in `docs/agentic-bootstrap.md` and
  remains distinct from the tagged `0.19.0-agentic-foundation` boundary.
- Payment readiness now requires the current screened provider assessment to
  match the credential-ready rail discovered for the declared direction.
- The authenticated Business dashboard now displays the authoritative readiness
  state, exact blockers, and separate CEO worker-liveness status.
- The current-tree acceptance harness now exercises the image's real supervised
  container entrypoint before the worker/restart assertions.
- The example solo-founder charter now explicitly grants bounded successor
  objective authority (`objectives.create` on the `objectives` system).
- Added an interrupted-provider acceptance scenario proving uncertain intent
  recovery, read-back settlement, and zero duplicate provider calls after a
  container restart.
- `business payment-rails` now performs credential-safe, read-only payment-rail
  discovery and reports unavailable optional providers without implying
  settlement readiness.
- Added an opt-in `docker-compose.yml` `agentic` profile for a standalone
  Founder/CEO supervisor sharing the durable Charterforge state volume with
  the gateway.
- Rebuilt-image container smoke now covers bootstrap, a standalone CEO worker
  tick, and cross-container durable stop evidence for an expected security
  block.
- Added the read-only `business readiness` projection with explicit blocker
  codes and no implicit autonomy or provider side effects.
- Readiness now fails closed when a charter declares inbound or outbound
  payment capabilities without a credential-ready rail in that direction.
- Readiness now surfaces the worker's deterministic security-readiness
  violations directly, before a supervisor is started.
- Standalone CEO Compose supervision now bounds unexpected crash retries at
  five while leaving successful governed stops stopped.
- Payment readiness now also requires a durable non-custodial compliance profile
  and current AML/sanctions-screened provider assessment per declared direction.
- Recorded a cross-process authority snapshot restore smoke that preserves
  accounting integrity, pauses autonomy, and opens reconciliation handoff.
- Split control-plane readiness from worker liveness: readiness may pass before
  startup and reports `runtime_active` separately for the supervised CEO.

## 0.19.0-agentic-foundation — 2026-07-27

This is the consolidated release-evidence boundary for the independent
Charterforge agentic runtime. It is a controlled-runtime foundation boundary,
not a claim of production autonomous-business readiness.

### Included

- Founder/CEO solo-founder bootstrap with advisor-by-default human posture.
- Durable objectives, immutable plans, admissible permits, event-driven
  replanning, independent verification, audit lineage, and bounded worker
  coordination.
- Fail-closed authority, budget, payment, accounting, compliance, recovery,
  lease, and advisor-intervention controls.
- Explicit end-to-end acceptance evidence in
  `tests/hermes_cli/test_agentic_business_e2e.py::test_founder_ceo_operating_loop_acceptance`.

### Evidence boundary

- Controlled Founder/CEO runtime acceptance: **PASS** when the exact commands
  in [READINESS.md](READINESS.md) pass at the recorded evidence commit.
- Production autonomous business operation: **NOT READY**.
- Universal legal, tax, payment, and compliance operation: **NOT PROVEN**.
- Evidence commit SHA: `4f7d585b8d0eda6f0d7646c843f9ddd43c4d7afe`, with exact
  commands and results recorded in [READINESS.md](READINESS.md). The evidence
  documentation itself was recorded in commit
  `757408d82884afd60651762715c3ef00446bc0c0`.
- Immutable annotated release tag: `v0.19.0-agentic-foundation` points at the
  evidence commit above.
- This SHA is the designated `0.19.0-agentic-foundation` release boundary.
  `main` is ahead of it: the stale-spend-hold escalation
  (`172a515c6b5d7efeef1a4e222c5c35ca46246a0b`) and evidence-bound spend-hold
  resolution (`5c92744e42878929ed981c81f34b1239c00d992a`) are post-boundary
  commits. Those changes have focused regression evidence, but they are not
  included in this release acceptance determination and remain under
  Unreleased.

### Capability inventory present at the designated boundary

The immutable release tree at tag `v0.19.0-agentic-foundation` is the
authoritative source for this included capability inventory. The entries below
describe capabilities present in that tagged tree and are part of
`0.19.0-agentic-foundation`, not Unreleased. The commit comparison is used
only to classify changes introduced after the tagged boundary; it is not the
source of the complete inventory.

- Added the optional standalone `charterforge-stripe-rail` package for
  idempotent inbound Checkout Sessions, provider read-back verification, and
  narrowly scoped Connected Account outbound payments. The core runtime does
  not install or enable it automatically.
- Enforced objective-level cumulative spend ceilings inside the atomic treasury
  reservation transaction, including concurrent workers and released-budget
  reuse.
- Paused/manual autonomy now causes the supervised objective worker to exit with
  a durable `autonomy_paused` stop reason instead of polling indefinitely.
- Worker exception handling now checks the persisted autonomy mode, so a
  provider failure caused by an in-flight emergency stop cannot trigger retry
  loops.
- Durable audit and planner-lineage records now redact credential-like fields
  before persistence while preserving ordinary response evidence unchanged.
- External event receipts now redact credential-like payload and authentication
  fields before durable storage and CEO-planner routing.
- Added an optional immutable runtime baseline that pauses autonomous cycles on
  charter, schema, package, or Python-runtime drift until a human rebaselines
  explicitly.
- External-content ingestion and authenticated event fan-out now collapse
  concurrent duplicate deliveries into one durable receipt and wakeup.
- Portfolio and workforce authority admission now serialize local concurrent
  budget checks; external handlers perform a final autonomy-state check before
  side effects.
- Stripe webhook ingress now rejects missing or malformed positive
  amount/currency evidence before routing.
- Worker supervision now persists expired heartbeat leases as `stale` workers
  with an explicit stop reason during supervisor startup and emits a
  deduplicated advisor intervention for each stale worker.
- Gateway-hosted objective supervision now reconciles stale gateway workers
  before registering a replacement worker.
- Supervised workers now fence each cycle on their durable heartbeat lease and
  stop when that lease is revoked.
- Autonomous readiness stops now persist advisor handoffs for missing CEO
  authority, unavailable governed capabilities, and unreachable objectives;
  objectives without admissible verifiers are blocked before execution.
- Outbound payment velocity controls now reserve daily spend atomically per
  tokenized instrument until provider read-back settles or releases the hold.
- Payment and metered-billing schema checks no longer release active authority
  transactions on already-initialized stores.
- Outcome attribution reconciliation now uses the same guarded payment schema
  initialization path.
- Runtime-host mismatches and empty action-contract charters now create durable
  advisor handoffs before autonomous operation stops.
- Hardened durable runtime ledgers so schema checks preserve active authority
  transactions across finance, accounting, payments, compliance, approvals,
  event ingress, billing, commitments, metrics, worker state, and audit
  lineage; focused regressions cover rollback preservation.

### Additional capability inventory at the designated boundary

- Immutable usage metering and governed metered-invoice actions. Prices are
  captured when usage occurs; invoice actions reference exact event IDs and
  immutable allocations prevent duplicate billing.
- Metered-invoice recovery now permits only same-intent allocation replay and
  rejects idempotency-key amount drift.
- Standalone objective workers now stop durably on disabled autonomy and
  fail-closed runtime, security, configuration, integrity, or drift gates.
- Metered-invoice verification now requires independent allocation-ledger
  read-back of the exact event set and total amount.
- External objective-event routing now rejects evidence without an adapter
  validation marker.
- Permit issuance now rejects actions from superseded plan versions.
- Permit consumption now rejects unexpired permits issued under a stale policy
  version.
- Hiring materialization now rejects stale positive decisions when intervening
  headcount or payroll use exhausts the current organization limits.
- Permit consumption now rechecks objective lifecycle state and rejects stale
  permits after cancellation or expiry.
- Metered invoices now calculate optional tax only from an active,
  organization-owned jurisdiction-matched tax rule and record the gross intent.
- External subscriptions now skip terminal objectives, preventing stale goals
  from being reactivated by late events.
- Durable inbox claims now apply the same terminal-objective fence to internal
  worker, compliance, and maintenance events.
- Authority-store connections now explicitly use full synchronous durability
  and a bounded busy timeout alongside WAL and foreign-key enforcement.
- Charterforge independent identity, canonical package/CLI/namespace, state
  root, environment prefix, container/service naming, attribution, and
  migration documentation.
- Governed autonomous-company runtime with durable objectives, event-driven
  progression, deterministic permits, independent verification, recovery, and
  audit evidence.
- Founder/CEO organization model with advisor-by-default human role.
- Solo-founder self-dispatch bound to exact toolset and skill grants.
- Evidence-based contractor-versus-FTE staffing and enterprise hierarchy.
- Treasury, accounting, tax-record, compliance, commitment, procurement, and
  non-custodial payment-rail control surfaces.

#### Changed

- Product-specific development is independent and is not intended for
  submission to the upstream Hermes Agent repository.
- Legacy Hermes commands, environment variables, paths, and Python modules are
  migration compatibility surfaces rather than project branding.

#### Security

- Governed worker launch and result handoff fail closed when task, mandate,
  profile, authority, toolset, skill, budget, or expiry evidence differs from
  the immutable grant.
- Housekeeping repairs lost wake events for active accepted/planned objectives
  using versioned idempotency fences, without reviving blocked objectives.

All notable independent Charterforge changes are documented here. Upstream
Hermes Agent history remains available in Git.

## Post-boundary commit comparison

This section is derived from `4f7d585b8d0eda6f0d7646c843f9ddd43c4d7afe..HEAD`.
The current branch is ahead of the validated boundary; these changes do not
inherit the boundary's PASS determination until a newer evidence commit is
recorded.

### Deployability evidence

- Local Docker image build and supervised-entrypoint bootstrap/status
  persistence smoke passed at the current main; registry publication and
  production deployment remain unreleased.
- The complete Docker restart regression now passes against the built image,
  including stale PID cleanup and live gateway auto-start after restart.
- Live Docker sandbox-provider integration now passes the container-only read
  and host-secret non-exfiltration checks; other providers remain unvalidated.
- Added an independent local artifact installer with Python-version and
  destination safety checks; package-index publication remains unreleased.
- Unconfigured business status now exposes a structured advisor handoff to the
  explicit charter bootstrap command and confirms that autonomy has not begun.
- The sample charter now explicitly admits gateway or standalone supervision;
  a real standalone `worker --once` smoke records `security_blocked` and stops
  without fabricating an external outcome when provider evidence is absent.
- Blocked worker cycles now retain the exact readiness reason in durable worker
  health evidence for advisor diagnosis.
- Current-main focused acceptance evidence was rerun at
  `56e2c1a1ccf2a4a5c5409b9d7187816e2ecf7b98`: 98 tests
  passed across 10 files; this does not move the immutable 0.19.0 release
  boundary.
- Security readiness failures now open a deduplicated organization-scoped
  advisor intervention with exact violations and an explicit no-action boundary.

- `757408d82884afd60651762715c3ef00446bc0c0` — recorded the release
  readiness evidence documentation.
- `172a515c6b5d7efeef1a4e222c5c35ca46246a0b` — escalated stale outbound spend
  holds without automatically releasing uncertain provider commitments.
- `5c92744e42878929ed981c81f34b1239c00d992a` — bound spend-hold resolutions to
  durable provider read-back or failed/cancelled settlement evidence.
- `e5ee81ded0b18b53453fc15570fafce231bdef75` — clarified release evidence,
  coverage scope, and the test-provider boundary.
- `1bf543e12f7b14bd4d808fbb1438690a594f53e9` — stopped the supervised
  objective worker on global `recovery_blocked` results so unavailable
  authority recovery cannot leave the runtime polling for new work.
- `a4ae9be834e76548c96556fd2ecf41cbd2b1c4e1` — shared the fail-closed
  supervisor status contract with the gateway-hosted objective loop.
- `8e415f4ed85339b6bb3dc83e39b93b938028f6c3` — made proposed-objective
  acceptance an explicit evidence-bearing advisor handoff that wakes the
  governed runtime after acceptance.
- `62866e01cb3ec1bf672038653fcd413a5e3a3f21` — blocked stale objective intent
  until evidence-bearing reaffirmation refreshes the standing objective.
- `1720b01ade8543326029676e4d142d914ee4bc9f` — required a substantive
  decision basis when resolving stale-intent reaffirmation.
- `33ca4eae20cd4291381e053de7838b2a107138a3` — rejected future-dated payment
  provider assessments before rail authorization.
- `d38f119ee35f9f0998f220f2883720f32a1d006c` — rejected provider assessments
  that were already expired at admission.
- `a498499a0273adb8d849474e5c461bdf0dd27443` — enforced standalone worker
  deployment-role and live-host checks even for injected callbacks.
- `2caf2de7ed8941d6a542cb38bfbee1afee338bb4` — required auditable provider,
  jurisdiction, and registry-reference fields for payment assessments.
- `4a129bcbb3d09606ce83b577e63a6e601303331b` — revalidated standalone worker
  deployment authority before every cycle to fence dynamic host changes.
- Current-main focused acceptance rerun at baseline
  `c4662689660f1e2447a2479f1844fae28bd2a57c`: 6 Founder/CEO E2E tests, 48
  objective service/runtime/worker tests, and 21 finance/attribution tests
  passed; compilation and diff checks passed. This is post-boundary evidence,
  not a release-tag move.
- The containing documentation/control-boundary commit also adds proposal-time
  validation of registered action payload schemas and temporal preconditions;
  its SHA is the final commit that contains this changelog entry.
- Runtime construction now rejects a verifier that shares the planner or
  executor identity, preserving an explicit independent-verification boundary.
- The supervised worker now checks the durable autonomy kill switch before
  invoking any tick callback, including alternate worker integrations.
- `29df862970a7caa3e34e3b4f27c98218ab0efce2` — rejected malformed,
  future-dated, and stale authenticated external-event evidence before
  objective wake-up; the focused ingress regression passed 21 tests.
- `c4662689660f1e2447a2479f1844fae28bd2a57c` — provider-authenticated ingress
  now requires signed freshness evidence for Stripe and Svix schemes; the
  focused ingress regression passed 23 tests.
- `247617ca47102af87c968d303897fff45dcaa2ee` — Founder/CEO-originated
  objectives now enter the operating portfolio under standing authority
  without requiring a routine advisor dispatch; externally originated
  proposals still require evidence-bearing acceptance.
- `a497763a54767bc5c577e535e4bfafd531981cd7` — bound automatic objective
  acceptance to the active CEO's canonical employee identity and organization,
  preventing a forgeable `employee:ceo` label from granting standing authority.
- `71330197889f03c0ae67ba8424038aaed4bda54c` — rejected explicit objective
  organization IDs that are not present in the enterprise tenant authority
  store.
- `dee2818df7f12949911e87419b33545c350080ce` — required provider payment
  read-backs to carry a non-empty reference/status and valid amount/currency
  fields before settlement or receipt recording.
- `1e82c8b137b7830534cd0dbb7e5a144877a8be5c` — bound payment idempotency
  retries to the original intent parameters, rejecting amount, party, tenant,
  direction, or purpose drift.
- `9c2bdd63a3360c222046f5bf490dc7cca620f103` — bound treasury ledger
  idempotency retries to exact entry parameters, rejecting duplicate-key
  amount, account, action, or external-reference drift.
- `15dd8224cadf9c46e2b16ea7d6bef2175420cd0b` — bound accounting journal replays to exact description,
  currency, and line parameters, rejecting source-key drift while permitting
  evidence-only retries.
- `a54de88bd08749bf7e1542805aba16f78ee2f179` — bound procurement decision
  retries to the original tenant, objective, sourcing case, and evidence,
  rejecting idempotency-key input drift.
- `614a74b5714a37146a01b95fc50abe60bf5b2a7b` — bound metered usage-event
  retries to the original meter, customer, quantity, supplied timestamp, and evidence,
  rejecting duplicate-key event drift.
- `4bd0ccefb7be7c87c6c57479ce323be382c25e0f` — bound child and successor
  objective relationship retries to immutable request fingerprints, failing
  closed for unbound legacy rows or changed decomposition inputs.
- `0e22898814e8b2d61345e6c1088031a0856d6481` — bound FTE/contractor hiring
  decision retries to the original organization, staffing case, policy, and
  evaluator identity, rejecting idempotency-key drift.
- `c9a151ab9ec3588336aecd0ff99ccc1d773bb3fc` — bound budget reservation
  retries to the original account, objective, action, amount, and currency,
  rejecting rebinding of an existing spend authorization.
- `f1ec869d5280f9ba33d5822d4f23e1d655432237` — scoped advisor-intervention
  action and dedupe replays to their organization, rejecting cross-tenant
  escalation collisions.
- `a1e6e1630229e429c43dabdc3ed2bd890cc7c1de` — required approval-artifact
  issuance to name the expected organization, preventing direct callers from
  issuing authority against another tenant's intervention.
- `3f91ce6344150885a0e74ec961565c7190fa56c2` — made company-email send
  recording explicitly idempotent and payload-bound, returning the original
  operation for true retries and rejecting changed recipients or content.
- `4523d7c42844473497d12deae9679e3153230dec` — bound compute-cost
  reconciliation retries to provider, model, reference, amount, status, and
  exact evidence, rejecting changed settlement identity.
- `cb0e8b7cdf43aa3aa2383821eb2a91794da8965c` — converged identical payment
  provider readbacks to one immutable observation while preserving distinct
  rows for actual provider-state changes.
- `ac8b60da5ea510c6bcf6a4c68b73787d6c7cf0d9` — bound quarantined external-content release to the intervention
  organization, with cross-tenant release regression coverage.
- `484634193b508c3fa735eab0dd3d8041e431ac84` — bound business-commitment
  fulfillment to the organization executing the governed action.
- `8585dd47dd030f0ad225fc0ceab6093617008828` — bound tax filing and payment
  mutations to the organization executing the governed accounting action.
- `2853b1e45660d00be27c50b4f22c0caf7eabd49b` — bound fiscal-period closure to
  the organization executing the governed accounting action.
- `42fd303f13d617759ab48fc0ffef97dada7aad4e` — bound approval-artifact
  validation to the organization executing the governed action.
- `5175dfd8294fdeae725a7b800fa42ddd698d45f6` — made organization binding
  mandatory when consuming execution permits.
- `70261890ea87714194914b3dd04bff8e0a73ef5b` — bound execution-result
  recording to the organization executing the governed action.
- `e860e36534bf323f61a4ee4f0ab4c940020fd97a` — bound durable verification
  recording to the organization executing the governed action.
- `1799f65aebbc28e195e631a4f1dd9200cfd73ecb` — completed organization
  propagation across all governed verification paths.
- `70ad7a0bbca351573c4b202e5ee80665866c1d3d` — bound external and scheduled
  objective wakeups to their organization at enqueue time.
- `b8b9285e6573097a327daf1bdd30bb64e1e22cae` — scoped employee actors to their
  organization for objective lifecycle mutations.
- `2a45edea8560c084c09b3ef62de82bdc904b192b` — extended employee actor scope
  checks to plan creation and action proposals.
- `9836000c9a21a4f2c87f007e2c1547e66ea128c1` — scoped employee identities at
  permit issuance before execution authority is minted.
- `bf5202b9bca5c2cb098bb9bc38d27342a91b2e4a` — refreshed current-main release
  evidence and semantic-release documentation posture.
- `a36c4c746260fc4add0115c93f5deffc3d175a88` — rejected applicability and
  obligation records for unknown or retired compliance regimes.
- `540b1f1054461929f6d749d1420290204e740c3e` — rejected already-expired
  compliance control evidence at admission time.
- `f8b09c922f523079e0099a5e48efae83bfaf008b` — made compliance applicability,
  obligation, and control-evidence records append-only.
- `7fabe92dd94c0aeedfb5193cad505c2861ed4ed9` — serialized circuit-breaker
  recovery probes with a durable half-open lease and safe expiry reclamation.
- `971ea3dfb4a49d92191acee57dbc0308d343828e` — preserved active authority
  transactions during circuit-breaker schema checks.
- `9442db2154771cc84298c8d984050e471d5c2554` — preserved active authority
  transactions during compliance schema checks.
- `6e955798d2fd3f7844628d7e9b571fb6c3e42c33` — preserved active authority
  transactions during company-email schema checks.
- `9b21ea30ce7cd579a71ed46fdef77ca6557e3037` — added explicit append-only
  compliance supersession lineage and current-record selection.
- `24204f0149eb48036c11ffa2504bae1d99a130c8` — enabled independently
  installable Charterforge wheel and source-distribution artifacts.
- `21c6e85282fe57fb957732b55141fa4f70f40f68` — added the non-interactive
  agentic bootstrap contract, example charter, and operator runbook.
