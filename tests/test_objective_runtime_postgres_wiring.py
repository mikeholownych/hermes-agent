"""End-to-end proof that ObjectiveRuntime.tick() actually drives the
Postgres AuthorityBridge through claim -> issue_permit -> consume_permit ->
record_effect -> complete, not just claim() as before.

Prior to this test, AuthorityBridge.issue_permit()/consume_permit()/
record_effect()/complete() were only exercised directly by
tests/test_authority_bridge.py and tests/test_postgres_runtime_integration.py
— never through the real tick() cycle. tick() itself only ever called
bridge.claim(), wrapped in a bare `except Exception: pass`, meaning the
entire Postgres permit/effect/completion mirror (and the fail-closed
executor/capability/target_resource/policy_version/objective-lifecycle/
autonomy checks in consume_permit) was unreachable from production code.

This test proves the full chain is now reachable and correct.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

POSTGRES_URL = os.environ.get("AUTHORITY_POSTGRES_TEST_URL", "")


@pytest.fixture
def pg_env(monkeypatch):
    if not POSTGRES_URL:
        pytest.skip("AUTHORITY_POSTGRES_TEST_URL not set")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError:
        pytest.skip("psycopg not installed")

    schema_name = f"tickwire_{uuid.uuid4().hex[:12]}"
    conn = psycopg.connect(POSTGRES_URL, row_factory=dict_row, autocommit=False)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema_name}")
    conn.commit()
    conn.close()

    if POSTGRES_URL.startswith("postgresql://") or POSTGRES_URL.startswith("postgres://"):
        sep = "&" if "?" in POSTGRES_URL else "?"
        modified_url = f"{POSTGRES_URL}{sep}options=-csearch_path%3D{schema_name}"
    else:
        modified_url = f"{POSTGRES_URL} options=-csearch_path={schema_name}"
    monkeypatch.setenv("AUTHORITY_POSTGRES_URL", modified_url)
    monkeypatch.delenv("HERMES_TENANT_ID", raising=False)

    yield schema_name

    cleanup = psycopg.connect(POSTGRES_URL, autocommit=True)
    with cleanup.cursor() as cur:
        cur.execute(f"DROP SCHEMA {schema_name} CASCADE")
    cleanup.close()


class Planner:
    identity = "employee:ceo"

    def __init__(self, actions):
        self.actions = actions

    def propose(self, snapshot, event):
        from hermes_cli import objective_runtime as runtime

        return runtime.PlanProposal(
            assumptions=[f"event:{event['event_type']}"],
            tasks=[{"step": "update CRM"}],
            dependencies=[],
            risks=["stale data"],
            actions=self.actions,
            objective_complete_when_verified=True,
        )


class Executor:
    identity = "employee:revenue-ops"

    def __init__(self, status="succeeded"):
        self.status = status
        self.calls = []

    def execute(self, action_type, payload):
        from hermes_cli import objective_runtime as runtime

        self.calls.append((action_type, payload))
        return runtime.ExecutionOutcome(
            status=self.status,
            result={"read_back": payload},
            external_reference="crm-event-123",
        )


class Verifier:
    identity = "employee:internal-audit"

    def __init__(self, verdict="pass"):
        self.verdict = verdict

    def verify(self, action, execution):
        from hermes_cli import objective_runtime as runtime
        from hermes_cli import verification_evidence

        return runtime.VerificationOutcome(
            verdict=self.verdict,
            evidence=verification_evidence.build(
                observer=self.identity,
                source_kind="authoritative_database_readback",
                source_reference=str(execution.external_reference),
                facts={"read_back": execution.result["read_back"]},
            ),
        )

    def verify_objective(self, snapshot, plan, action_verifications):
        from hermes_cli import objective_runtime as runtime
        from hermes_cli import verification_evidence

        return runtime.VerificationOutcome(
            verdict=self.verdict,
            evidence=verification_evidence.build(
                observer=self.identity,
                source_kind="deterministic_check",
                source_reference=f"objective:{snapshot['id']}",
                facts={
                    "success_criteria": snapshot["success_criteria"],
                    "action_verdicts": [
                        item.verdict for item in action_verifications
                    ],
                },
            ),
        )


def _charter():
    return {
        "enabled": True,
        "operating_cadence": {"enabled": False},
        "operating_mode": "autonomous",
        "allowed_capabilities": ["crm.write"],
        "forbidden_capabilities": [],
        "allowed_systems": ["crm"],
        "approval_required_capabilities": [],
        "max_autonomous_risk": "medium",
        "allow_irreversible": False,
        "max_action_spend_minor": 100,
        "permit_ttl_seconds": 300,
    }


def _action():
    from hermes_cli import objective_runtime as runtime

    return runtime.ActionProposal(
        action_type="crm.update",
        payload={"system": "crm", "target_resource": "lead:123", "stage": "qualified"},
        expected_outcome="lead is qualified",
        required_capability="crm.write",
        verification_method="crm read-back",
        risk_class="low",
        reversible=True,
    )


def test_tick_wires_bridge_through_permit_effect_complete(tmp_path, pg_env):
    """The decisive check: a full tick() cycle for an unscoped (single-org)
    objective must leave a real Postgres claim, a consumed permit with the
    exact executor/capability/target_resource recorded, and a recorded
    execution effect — proving these are no longer only reachable via
    direct AuthorityBridge API calls in other tests."""
    from hermes_cli import objective_runtime as runtime
    from hermes_cli import objectives_db as db
    from hermes_cli.postgres_authority import connect as pg_connect, init_schema

    # Ensure the schema exists before the bridge (constructed lazily inside
    # tick()) or this test's own verification queries touch it.
    setup_conn = pg_connect()
    init_schema(setup_conn)
    setup_conn.close()

    conn = db.connect(tmp_path / "authority.db")
    objective = db.create_objective(
        conn,
        desired_outcome="Keep qualified leads current",
        originator="setup:user",
        permitted_systems=["crm"],
        success_criteria=["CRM read-back matches expected stage"],
    )
    objective = db.transition_objective(conn, objective.id, "accepted", actor="setup:user")
    event_id = db.enqueue_objective_event(
        conn,
        objective_id=objective.id,
        event_type="crm.lead.changed",
        payload={"lead_id": "123"},
        dedupe_key="crm:lead:123:pg-wiring-test",
    )

    loop = runtime.ObjectiveRuntime(
        conn,
        planner=Planner([_action()]),
        executor=Executor(),
        verifier=Verifier(),
        charter=_charter(),
        policy_version="charter-v1",
        runtime_id="runtime-pg-wiring-test",
    )

    outcome = loop.tick()

    assert outcome.event_id == event_id
    assert outcome.status == "verified"
    assert db.get_objective(conn, objective.id).status == "verified"

    # Now prove the Postgres side actually recorded the mirrored permit
    # consumption and effect — not just the claim.
    pg_conn = pg_connect()
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT executor, capability, target_resource, consumed_at "
                "FROM task_permits WHERE task_id = %s",
                (objective.id,),
            )
            permits = cur.fetchall()
        assert len(permits) == 1, "exactly one Postgres permit must have been issued"
        permit = permits[0]
        assert permit["executor"] == "employee:revenue-ops"
        assert permit["capability"] == "crm.write"
        assert permit["target_resource"] == "lead:123"
        assert permit["consumed_at"] is not None, "the permit must have been consumed"

        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT effect_type, provider_ref FROM execution_effects "
                "WHERE task_id = %s",
                (objective.id,),
            )
            effects = cur.fetchall()
        assert len(effects) == 1, "exactly one effect must have been recorded"
        assert effects[0]["effect_type"] == "crm.update"
        assert effects[0]["provider_ref"] == "crm-event-123"

        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM task_runs WHERE task_id = %s "
                "ORDER BY lease_generation DESC LIMIT 1",
                (objective.id,),
            )
            run = cur.fetchone()
        assert run["status"] == "completed", "the Postgres claim must have been completed"
    finally:
        pg_conn.close()


def test_tick_lost_claim_race_fails_closed_and_retries(tmp_path, pg_env, monkeypatch):
    """If another worker already holds the Postgres claim for this
    objective, tick() must NOT proceed to execute the SQLite-side action at
    all — it must fail closed (retry-scheduled), not silently continue
    without exclusive authority."""
    from hermes_cli import objective_runtime as runtime
    from hermes_cli import objectives_db as db
    from hermes_cli.postgres_authority import claim_task, connect as pg_connect, init_schema

    setup_conn = pg_connect()
    init_schema(setup_conn)
    setup_conn.close()

    conn = db.connect(tmp_path / "authority.db")
    objective = db.create_objective(
        conn,
        desired_outcome="Keep qualified leads current",
        originator="setup:user",
        permitted_systems=["crm"],
        success_criteria=["CRM read-back matches expected stage"],
    )
    objective = db.transition_objective(conn, objective.id, "accepted", actor="setup:user")
    db.enqueue_objective_event(
        conn,
        objective_id=objective.id,
        event_type="crm.lead.changed",
        payload={"lead_id": "123"},
    )

    # Simulate another worker already holding the Postgres claim for this
    # exact task_id (objective_id) under a real, unexpired claim.
    pg_conn = pg_connect()
    try:
        gen = claim_task(
            pg_conn,
            task_id=objective.id,
            claim_token="rival-worker-token",
            organization_id="__unscoped__",
            worker_id="rival-worker",
            claim_scope_url="",
            expires_at=time.time() + 3600,
        )
        assert gen == 1
    finally:
        pg_conn.close()

    executor = Executor()
    loop = runtime.ObjectiveRuntime(
        conn,
        planner=Planner([_action()]),
        executor=executor,
        verifier=Verifier(),
        charter=_charter(),
        policy_version="charter-v1",
        runtime_id="runtime-pg-wiring-loser",
    )

    outcome = loop.tick()

    assert outcome.status == "retry_scheduled"
    assert len(executor.calls) == 0, (
        "the SQLite-side action must never execute when the Postgres claim was lost"
    )
    assert db.get_objective(conn, objective.id).status == "accepted", (
        "the objective must not have progressed toward execution"
    )
