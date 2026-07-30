from __future__ import annotations

import sqlite3

import pytest

from hermes_cli import objective_event_policy
from hermes_cli import objectives_db
from hermes_cli import organization_db


def _objective(conn, organization_id: str, name: str):
    objective = objectives_db.create_objective(
        conn,
        organization_id=organization_id,
        desired_outcome=name,
        originator="employee:ceo",
    )
    objectives_db.transition_objective(
        conn, objective.id, "accepted", actor="employee:ceo"
    )
    return objective.id


def test_admission_policy_ignores_payload_self_promotion():
    generic = objective_event_policy.classify(
        "provider.data.changed",
        {"priority": 100, "priority_class": "critical"},
    )
    assert generic.priority_class == "normal"
    assert generic.priority == 50
    overdue = objective_event_policy.classify(
        "compliance.deadline.approaching",
        {"overdue": True, "due_at": 100},
    )
    assert overdue == objective_event_policy.EventAdmission("critical", 100, 100)


def test_overdue_deadline_preempts_routine_and_high_events(tmp_path, monkeypatch):
    conn = objectives_db.connect(tmp_path / "authority.db")
    organization_id, _ = organization_db.bootstrap_solo_founder(
        conn,
        organization_name="Priority Company",
        purpose="Handle urgent work first",
        profile_name="default",
        charter={},
    )
    objective_id = _objective(conn, organization_id, "Operate safely")
    monkeypatch.setattr(objectives_db, "_now", lambda: 1_000)
    routine_id = objectives_db.enqueue_objective_event(
        conn,
        objective_id=objective_id,
        event_type="ceo.operating_review",
        payload={},
    )
    high_id = objectives_db.enqueue_objective_event(
        conn,
        objective_id=objective_id,
        event_type="strategy.metric_target.reviewed",
        payload={"verdict": "off_track"},
    )
    overdue_id = objectives_db.enqueue_objective_event(
        conn,
        objective_id=objective_id,
        event_type="compliance.deadline.approaching",
        payload={"kind": "tax_obligation", "overdue": True, "due_at": 999},
    )

    claimed = objectives_db.claim_objective_event(
        conn, runtime_id="runtime", organization_id=organization_id
    )

    assert claimed["id"] == overdue_id
    assert claimed["priority_class"] == "critical"
    assert claimed["priority"] == 100
    assert claimed["deadline_at"] == 999
    assert {routine_id, high_id}


def test_business_commitment_breach_is_critical_and_cannot_self_promote():
    breached = objective_event_policy.classify(
        "commitment.breached",
        {"due_at": 100, "priority": 999999},
    )
    upcoming = objective_event_policy.classify(
        "commitment.deadline.approaching",
        {"due_at": 200, "overdue": False, "priority": 999999},
    )
    assert (breached.priority_class, breached.priority) == ("critical", 98)
    assert (upcoming.priority_class, upcoming.priority) == ("high", 82)


def test_aging_prevents_old_routine_event_starvation(tmp_path, monkeypatch):
    conn = objectives_db.connect(tmp_path / "authority.db")
    organization_id, _ = organization_db.bootstrap_solo_founder(
        conn,
        organization_name="Aging Company",
        purpose="Avoid starvation",
        profile_name="default",
        charter={},
    )
    old_objective = _objective(conn, organization_id, "Old routine work")
    recent_objective = _objective(conn, organization_id, "Recent high work")
    monkeypatch.setattr(objectives_db, "_now", lambda: 100)
    old_id = objectives_db.enqueue_objective_event(
        conn,
        objective_id=old_objective,
        event_type="ceo.operating_review",
        payload={},
    )
    now = 100 + 20 * 3_600
    monkeypatch.setattr(objectives_db, "_now", lambda: now)
    objectives_db.enqueue_objective_event(
        conn,
        objective_id=recent_objective,
        event_type="strategy.metric_target.reviewed",
        payload={"verdict": "off_track"},
    )

    claimed = objectives_db.claim_objective_event(
        conn, runtime_id="runtime", organization_id=organization_id
    )

    assert claimed["id"] == old_id
    assert claimed["priority_class"] == "routine"


def test_priority_claim_is_tenant_scoped_and_admission_is_immutable(
    tmp_path, monkeypatch
):
    conn = objectives_db.connect(tmp_path / "authority.db")
    active_org, _ = organization_db.bootstrap_solo_founder(
        conn,
        organization_name="Active",
        purpose="Active tenant",
        profile_name="active-ceo",
        charter={},
    )
    foreign_org, _ = organization_db.bootstrap_solo_founder(
        conn,
        organization_name="Foreign",
        purpose="Foreign tenant",
        profile_name="foreign-ceo",
        charter={},
    )
    active_objective = _objective(conn, active_org, "Active routine")
    foreign_objective = _objective(conn, foreign_org, "Foreign emergency")
    monkeypatch.setattr(objectives_db, "_now", lambda: 1_000)
    active_id = objectives_db.enqueue_objective_event(
        conn,
        objective_id=active_objective,
        event_type="ceo.operating_review",
        payload={},
    )
    foreign_id = objectives_db.enqueue_objective_event(
        conn,
        objective_id=foreign_objective,
        event_type="compensation.required",
        payload={},
    )

    claimed = objectives_db.claim_objective_event(
        conn, runtime_id="runtime", organization_id=active_org
    )

    assert claimed["id"] == active_id
    assert conn.execute(
        "SELECT status FROM objective_inbox WHERE id=?", (foreign_id,)
    ).fetchone()["status"] == "pending"
    with pytest.raises(sqlite3.IntegrityError, match="admission is immutable"):
        conn.execute(
            "UPDATE objective_inbox SET priority=100 WHERE id=?", (active_id,)
        )


def test_existing_fifo_inbox_is_backfilled_with_deterministic_admission(tmp_path):
    path = tmp_path / "legacy.db"
    legacy = sqlite3.connect(path)
    legacy.execute(
        """CREATE TABLE objective_inbox (
             id TEXT PRIMARY KEY, objective_id TEXT NOT NULL,
             event_type TEXT NOT NULL, payload_json TEXT NOT NULL,
             dedupe_key TEXT UNIQUE, status TEXT NOT NULL,
             available_at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
             claimed_by TEXT, claim_expires INTEGER, last_error TEXT,
             created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
           )"""
    )
    legacy.execute(
        """INSERT INTO objective_inbox (
             id,objective_id,event_type,payload_json,dedupe_key,status,
             available_at,created_at,updated_at
           ) VALUES (
             'evt_legacy','obj_legacy','compliance.deadline.approaching',
             '{"due_at":99,"overdue":true}',NULL,'pending',100,100,100
           )"""
    )
    legacy.commit()
    legacy.close()

    conn = objectives_db.connect(path)
    row = conn.execute(
        """SELECT priority_class,priority,deadline_at
            FROM objective_inbox WHERE id='evt_legacy'"""
    ).fetchone()

    assert tuple(row) == ("critical", 100, 99)
