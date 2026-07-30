from __future__ import annotations

import sqlite3
import time

import pytest

from hermes_cli import (
    business_metrics,
    objective_adapters,
    objectives_db,
    organization_db,
    outcome_attribution,
    payments,
    verification_evidence,
)


def test_attribution_schema_read_preserves_active_transaction(tmp_path):
    conn = objectives_db.connect(tmp_path / "authority.db")
    outcome_attribution.ensure_schema(conn)
    conn.execute("BEGIN IMMEDIATE")
    outcome_attribution.ensure_schema(conn)
    assert conn.in_transaction is True
    conn.rollback()


def test_sync_schema_reads_preserve_active_transaction(tmp_path):
    conn, organization_id, _ = _company(tmp_path)
    payments.ensure_schema(conn)
    business_metrics.ensure_schema(conn)
    outcome_attribution.ensure_schema(conn)
    conn.execute("BEGIN IMMEDIATE")
    outcome_attribution.sync_authoritative_links(conn, organization_id)
    assert conn.in_transaction is True
    conn.rollback()


def _company(tmp_path):
    conn = objectives_db.connect(tmp_path / "authority.db")
    organization_id, _ = organization_db.bootstrap_solo_founder(
        conn,
        organization_name="Learning Company",
        purpose="Learn only from defensible outcomes",
        profile_name="learning",
        charter={},
    )
    objective = objectives_db.create_objective(
        conn,
        organization_id=organization_id,
        desired_outcome="Find a repeatable source of revenue",
        originator="employee:ceo",
        permitted_systems=["crm", "payments", "strategy"],
        max_spend_minor=10_000,
        currency="USD",
        expires_at=int(time.time()) + 86_400,
    )
    objectives_db.transition_objective(
        conn, objective.id, "accepted", actor="employee:ceo"
    )
    return conn, organization_id, objective.id


def _verified_action(conn, objective_id: str) -> tuple[str, str]:
    organization_id = conn.execute(
        "SELECT organization_id FROM objectives WHERE id=?", (objective_id,)
    ).fetchone()["organization_id"]
    plan_id = objectives_db.create_plan(
        conn,
        objective_id,
        assumptions=[],
        tasks=[{"id": "change"}],
        dependencies=[],
        risks=[],
        created_by="planner",
    )
    objectives_db.transition_objective(
        conn, objective_id, "planned", actor="control"
    )
    action_id = objectives_db.propose_action(
        conn,
        objective_id=objective_id,
            plan_id=plan_id,
        action_type="customer.update",
        payload={"resource": "customer:1", "value": "active"},
        expected_outcome="customer is active",
        required_capability="crm.write",
        verification_method="crm.readback",
        risk_class="low",
        reversible=True,
        proposed_by="planner",
    )
    permit_id = objectives_db.issue_permit(
        conn,
        action_id,
        capability="crm.write",
        issued_to="employee:ceo",
        policy_version="v1",
        expires_at=int(time.time()) + 60,
    )
    objectives_db.consume_permit(
        conn,
        permit_id,
        action_id=action_id,
        organization_id=organization_id,
        payload={"resource": "customer:1", "value": "active"},
        executor="employee:ceo",
    )
    result_id = objectives_db.record_execution_result(
        conn,
        action_id=action_id,
        permit_id=permit_id,
        executor="employee:ceo",
        organization_id=organization_id,
        status="succeeded",
        result={"provider_id": "change-1"},
        started_at=int(time.time()),
    )
    verification_id = objectives_db.record_verification(
        conn,
        objective_id=objective_id,
        organization_id=organization_id,
        plan_id=plan_id,
        action_id=action_id,
        execution_result_id=result_id,
        verifier="crm-reader",
        method="crm.readback",
        verdict="pass",
        evidence=verification_evidence.build(
            observer="crm-reader",
            source_kind="provider_readback",
            source_reference="customer:1",
            facts={"status": "active"},
        ),
    )
    return action_id, verification_id


def test_action_attribution_requires_independent_pass_and_is_immutable(tmp_path):
    conn, organization_id, objective_id = _company(tmp_path)
    action_id, verification_id = _verified_action(conn, objective_id)

    attribution_id, created = outcome_attribution.link_action_verification(
        conn, verification_id
    )
    duplicate_id, duplicate_created = (
        outcome_attribution.link_action_verification(conn, verification_id)
    )

    assert created is True
    assert duplicate_created is False
    assert duplicate_id == attribution_id
    row = conn.execute(
        "SELECT * FROM outcome_attributions WHERE id=?", (attribution_id,)
    ).fetchone()
    assert row["subject_id"] == action_id
    assert row["evidence_strength"] == "independently_verified"
    assert outcome_attribution.verify_attributions(conn, organization_id) is True
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        conn.execute(
            "UPDATE outcome_attributions SET verdict='invented' WHERE id=?",
            (attribution_id,),
        )


def test_provider_readback_is_counted_once_and_never_from_pending_state(tmp_path):
    conn, organization_id, objective_id = _company(tmp_path)
    conn.executescript(payments.SCHEMA_SQL)
    now = int(time.time())
    conn.execute(
        """INSERT INTO payment_intents (
             id,organization_id,account_id,objective_id,direction,provider,
             party_json,amount_minor,currency,purpose,status,provider_reference,
             idempotency_key,metadata_json,tax_minor,created_at,updated_at
           ) VALUES (
             'payment_1',?, 'treasury_1',?,'incoming','fake','{}',1200,'USD',
             'sale','succeeded','provider_1','payment-key-00000001','{}',200,?,?
           )""",
        (organization_id, objective_id, now, now),
    )
    for readback_id, status in (("readback_pending", "pending"), ("readback_1", "succeeded")):
        conn.execute(
            """INSERT INTO payment_provider_readbacks (
                 id,payment_intent_id,provider,provider_reference,status,
                 amount_minor,currency,evidence_json,observed_at
               ) VALUES (?,'payment_1','fake','provider_1',?,1200,'USD','{}',?)""",
            (readback_id, status, now),
        )

    with pytest.raises(
        outcome_attribution.AttributionError, match="succeeded"
    ):
        outcome_attribution.link_payment_readback(conn, "readback_pending")
    attribution_id, created = outcome_attribution.link_payment_readback(
        conn, "readback_1"
    )
    assert created is True
    row = conn.execute(
        "SELECT value_minor FROM outcome_attributions WHERE id=?",
        (attribution_id,),
    ).fetchone()
    assert row["value_minor"] == 1000
    snapshot = outcome_attribution.planning_snapshot(conn, organization_id)
    assert snapshot["basis"] == "authoritative_links_not_inferred_causality"
    assert snapshot["outcomes"][0]["net_value_minor"] == 1000
    conn.execute(
        """INSERT INTO payment_provider_readbacks (
             id,payment_intent_id,provider,provider_reference,status,
             amount_minor,currency,evidence_json,observed_at
           ) VALUES (
             'readback_reversed','payment_1','fake','provider_1','reversed',
             1200,'USD','{"reversal":"confirmed"}',?
           )""",
        (now + 10,),
    )
    result = outcome_attribution.sync_authoritative_links(conn, organization_id)
    assert result["payments"] == 0
    assert result["contradictions"] == 1
    assert outcome_attribution.planning_snapshot(
        conn, organization_id
    )["outcomes"] == []
    assert outcome_attribution.verify_attributions(conn, organization_id) is True


def test_controlled_experiment_link_and_tenant_isolation(tmp_path):
    conn, organization_id, objective_id = _company(tmp_path)
    metric_id = business_metrics.register_metric(
        conn,
        organization_id=organization_id,
        metric_key="activation",
        name="Activation",
        unit="ratio",
        preferred_direction="increase",
        source_system="analytics",
        verifier="analytics:readback",
        idempotency_key="metric-activation-0000001",
        created_by="employee:ceo",
    )[0]
    now = int(time.time())
    experiment_id = business_metrics.start_experiment(
        conn,
        organization_id=organization_id,
        objective_id=objective_id,
        name="Short onboarding",
        hypothesis="Short onboarding raises activation",
        metric_id=metric_id,
        comparator="gte",
        success_threshold_scaled=300_000,
        starts_at=now - 100,
        ends_at=now + 1,
        max_spend_minor=100,
        currency="USD",
        idempotency_key="experiment-short-onboarding-0001",
        created_by="employee:ceo",
    )[0]
    business_metrics.record_observation(
        conn,
        organization_id=organization_id,
        metric_id=metric_id,
        value_scaled=350_000,
        observed_at=now,
        source_reference="analytics:cohort:1",
        verifier="analytics:readback",
        evidence={"signed": True},
    )
    business_metrics.dispatch_reviews(
        conn, organization_id=organization_id, now=now + 1
    )
    evaluation_id = str(
        conn.execute(
            """SELECT id FROM strategy_experiment_evaluations
                WHERE experiment_id=?""",
            (experiment_id,),
        ).fetchone()["id"]
    )

    outcome_attribution.sync_authoritative_links(conn, organization_id)
    row = conn.execute(
        """SELECT * FROM outcome_attributions
            WHERE outcome_kind='experiment_evaluation' AND outcome_id=?""",
        (evaluation_id,),
    ).fetchone()
    assert row["subject_id"] == experiment_id
    assert row["evidence_strength"] == "experimental_evidence"
    foreign_id = organization_db.create_organization(
        conn, name="Foreign", purpose="Stay isolated"
    )
    assert outcome_attribution.planning_snapshot(conn, foreign_id)["outcomes"] == []
    context = objective_adapters.organization_planning_context(
        conn, organization_id
    )
    assert context["outcome_attribution"]["outcomes"][0]["subject_id"] == experiment_id
