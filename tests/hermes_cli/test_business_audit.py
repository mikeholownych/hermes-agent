import sqlite3

import pytest

from hermes_cli import (
    accounting_db,
    business_audit,
    objectives_db,
    organization_db,
    planner_inferences,
    resource_budget,
)


def test_audit_schema_read_preserves_active_transaction(tmp_path):
    conn = objectives_db.connect(tmp_path / "authority.db")
    business_audit.ensure_schema(conn)
    conn.execute("BEGIN IMMEDIATE")
    business_audit.ensure_schema(conn)
    assert conn.in_transaction is True
    conn.rollback()


def test_audit_chain_rejects_database_tampering():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    business_audit.append(
        conn, organization_id="org_1", event_type="plan.proposed",
        objective_id="obj_1", plan_id="plan_1", payload={"model": "planner"},
    )
    business_audit.append(
        conn, organization_id="org_1", event_type="payment.settled",
        objective_id="obj_1", plan_id="plan_1", action_id="act_1",
        permit_id="permit_1", execution_result_id="result_1",
        verification_id="verify_1", payload={"amount_minor": 100},
    )
    assert business_audit.verify_chain(conn, "org_1")
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        conn.execute(
            "UPDATE business_audit_events SET payload_json='{}' WHERE sequence=2"
        )
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        conn.execute("DELETE FROM business_audit_events WHERE sequence=2")
    assert business_audit.verify_chain(conn, "org_1")


def test_durable_audit_redacts_credential_like_fields():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    business_audit.append(
        conn,
        organization_id="org_1",
        event_type="action.proposed",
        payload={"api_key": "sk_live_secret", "nested": {"token": "abc"}},
    )
    payload = conn.execute(
        "SELECT payload_json FROM business_audit_events"
    ).fetchone()["payload_json"]
    assert "sk_live_secret" not in payload
    assert "[REDACTED]" in payload
    assert business_audit.verify_chain(conn, "org_1")


def test_audit_export_is_tenant_scoped_and_self_verifying(tmp_path):
    conn = objectives_db.connect(tmp_path / "authority.db")
    org_1, _ = organization_db.bootstrap_solo_founder(
        conn,
        organization_name="One",
        purpose="First",
        profile_name="default",
        charter={},
    )
    org_2 = organization_db.create_organization(
        conn, name="Two", purpose="Second"
    )
    objective = objectives_db.create_objective(
        conn, desired_outcome="Earn revenue", originator="owner",
        organization_id=org_1,
    )
    objectives_db.create_objective(
        conn, desired_outcome="Other tenant", originator="owner",
        organization_id=org_2,
    )
    objectives_db.transition_objective(
        conn, objective.id, "accepted", actor="owner"
    )
    business_audit.append(
        conn, organization_id=org_1, event_type="objective.created",
        objective_id=objective.id, payload={"outcome": "Earn revenue"},
    )
    inference_id = planner_inferences.record(
        conn,
        objective_id=objective.id,
        inbox_event_id=None,
        planner_identity="employee:ceo",
        task="objective_planner",
        model="audit-model",
        request={"messages": [{"role": "user", "content": "plan"}]},
        response_text='{"actions":[]}',
        parse_status="parsed",
        error=None,
        input_tokens=10,
        output_tokens=3,
        started_at=1,
        finished_at=2,
    )
    plan_id = objectives_db.create_plan(
        conn,
        objective.id,
        assumptions=[],
        tasks=[],
        dependencies=[],
        risks=[],
        created_by="employee:ceo",
        inference_id=inference_id,
    )
    reservation_id = resource_budget.reserve_planner_call(
        conn,
        objective_id=objective.id,
        limits=resource_budget.DEFAULT_LIMITS,
        input_tokens=100,
        output_tokens=50,
        estimated_compute_cost_minor=10,
        enforce_treasury=False,
    )
    period_id = accounting_db.open_fiscal_period(
        conn,
        organization_id=org_1,
        name="2026",
        starts_at=100,
        ends_at=199,
        evidence={"calendar": "org-1"},
    )
    accounting_db.open_fiscal_period(
        conn,
        organization_id=org_2,
        name="2026",
        starts_at=100,
        ends_at=199,
        evidence={"calendar": "org-2"},
    )
    registration_id = accounting_db.configure_tax_registration(
        conn,
        organization_id=org_1,
        jurisdiction="CA-ON",
        tax_type="sales",
        filing_frequency="annual",
        effective_from=100,
        evidence={"authority": "CRA"},
    )
    obligation_id = accounting_db.record_tax_obligation(
        conn,
        organization_id=org_1,
        registration_id=registration_id,
        period_start=100,
        period_end=199,
        due_at=250,
        amount_minor=0,
        currency="CAD",
        evidence={"workpaper": "org-1"},
    )
    package = business_audit.export_audit_package(conn, org_1)
    assert business_audit.verify_audit_package(package)
    assert package["format"] == "charterforge-business-audit-v1"
    assert package["compatibility_format"] == "hermes-business-audit-v1"
    assert [row["organization_id"] for row in package["objectives"]] == [org_1]
    assert [row["id"] for row in package["planner_inferences"]] == [inference_id]
    assert package["plans"][0]["id"] == plan_id
    assert package["plans"][0]["inference_id"] == inference_id
    assert len(package["objective_resource_usage"]) == 1
    assert package["objective_resource_usage"][0]["objective_id"] == objective.id
    assert (
        package["objective_resource_usage"][0][
            "estimated_compute_cost_minor"
        ]
        == 10
    )
    assert [
        row["id"] for row in package["planner_compute_reservations"]
    ] == [reservation_id]
    assert [row["id"] for row in package["fiscal_periods"]] == [period_id]
    assert len(package["fiscal_period_events"]) == 1
    assert [row["id"] for row in package["tax_registrations"]] == [
        registration_id
    ]
    assert [row["id"] for row in package["tax_obligations"]] == [obligation_id]
    assert len(package["tax_obligation_events"]) == 1
    package["objectives"][0]["desired_outcome"] = "tampered"
    assert not business_audit.verify_audit_package(package)
