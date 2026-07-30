"""Durable authority state for governed, long-running objectives.

This module is deliberately model-free.  Models may propose plans and actions,
but only this state machine may commit lifecycle transitions.  Completion is
fail-closed: an objective cannot become ``verified`` until a verifier has
recorded independent evidence for its current plan.

The store is profile-aware and local by default.  A future backend interface
may provide Postgres for shared deployments without making it a core runtime
dependency.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional

from hermes_constants import get_hermes_home


OBJECTIVE_STATUSES = frozenset(
    {
        "proposed",
        "accepted",
        "planned",
        "authorized",
        "executing",
        "blocked",
        "completed",
        "verified",
        "closed",
        "cancelled",
        "expired",
        "abandoned",
        "superseded",
    }
)

TERMINAL_OBJECTIVE_STATUSES = (
    "closed", "cancelled", "expired", "abandoned", "superseded", "verified",
)

_TRANSITIONS = {
    "proposed": {"accepted", "cancelled", "expired", "superseded"},
    "accepted": {"planned", "blocked", "cancelled", "expired", "superseded"},
    "planned": {
        "authorized",
        "blocked",
        "completed",
        "cancelled",
        "expired",
        "superseded",
    },
    "authorized": {"executing", "blocked", "cancelled", "expired"},
    "executing": {"planned", "blocked", "completed", "cancelled", "expired"},
    "blocked": {"planned", "authorized", "cancelled", "expired", "abandoned"},
    "completed": {"executing", "verified", "blocked"},
    "verified": {"closed"},
    "closed": set(),
    "cancelled": set(),
    "expired": set(),
    "abandoned": set(),
    "superseded": set(),
}

ACTION_STATUSES = frozenset(
    {"proposed", "permitted", "denied", "executing", "succeeded", "failed", "expired"}
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS objectives (
    id                    TEXT PRIMARY KEY,
    organization_id       TEXT NOT NULL DEFAULT '__unscoped__',
    desired_outcome       TEXT NOT NULL,
    status                TEXT NOT NULL,
    originator            TEXT NOT NULL,
    owner                 TEXT,
    constraints_json      TEXT NOT NULL,
    authority_scope_json  TEXT NOT NULL,
    success_criteria_json TEXT NOT NULL,
    termination_json      TEXT NOT NULL,
    permitted_systems_json TEXT NOT NULL,
    prohibited_actions_json TEXT NOT NULL,
    max_spend_minor       INTEGER,
    currency              TEXT,
    expires_at            INTEGER,
    reaffirmed_at         INTEGER NOT NULL,
    created_at            INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL,
    version               INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS plans (
    id               TEXT PRIMARY KEY,
    objective_id     TEXT NOT NULL,
    version          INTEGER NOT NULL,
    assumptions_json TEXT NOT NULL,
    tasks_json       TEXT NOT NULL,
    dependencies_json TEXT NOT NULL,
    risks_json       TEXT NOT NULL,
    created_by       TEXT NOT NULL,
    created_at       INTEGER NOT NULL,
    supersedes_id    TEXT,
    inference_id     TEXT,
    UNIQUE(objective_id, version),
    FOREIGN KEY(objective_id) REFERENCES objectives(id)
);

CREATE TABLE IF NOT EXISTS candidate_actions (
    id                  TEXT PRIMARY KEY,
    objective_id        TEXT NOT NULL,
    plan_id             TEXT NOT NULL,
    action_type         TEXT NOT NULL,
    payload_json        TEXT NOT NULL,
    payload_sha256      TEXT NOT NULL,
    rationale           TEXT,
    expected_outcome    TEXT NOT NULL,
    required_capability TEXT NOT NULL,
    verification_method TEXT NOT NULL,
    risk_class          TEXT NOT NULL,
    reversible          INTEGER NOT NULL,
    compensation_action_type TEXT,
    compensation_payload_json TEXT,
    compensation_capability TEXT,
    compensation_verification_method TEXT,
    estimated_cost_minor INTEGER,
    status              TEXT NOT NULL,
    proposed_by         TEXT NOT NULL,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    FOREIGN KEY(objective_id) REFERENCES objectives(id),
    FOREIGN KEY(plan_id) REFERENCES plans(id)
);

CREATE TABLE IF NOT EXISTS permits (
    id                    TEXT PRIMARY KEY,
    action_id             TEXT NOT NULL,
    capability            TEXT NOT NULL,
    payload_sha256        TEXT NOT NULL,
    target_resource       TEXT,
    policy_version        TEXT NOT NULL,
    approval_artifact_id  TEXT,
    constraints_json      TEXT NOT NULL,
    issued_to             TEXT NOT NULL,
    issued_at             INTEGER NOT NULL,
    expires_at            INTEGER NOT NULL,
    consumed_at           INTEGER,
    revoked_at            INTEGER,
    FOREIGN KEY(action_id) REFERENCES candidate_actions(id)
);

CREATE TABLE IF NOT EXISTS execution_results (
    id                 TEXT PRIMARY KEY,
    action_id          TEXT NOT NULL,
    permit_id          TEXT NOT NULL,
    executor           TEXT NOT NULL,
    status             TEXT NOT NULL,
    external_reference TEXT,
    result_json        TEXT NOT NULL,
    actual_cost_minor  INTEGER,
    preserve_reservation INTEGER NOT NULL DEFAULT 0,
    started_at         INTEGER NOT NULL,
    finished_at        INTEGER NOT NULL,
    FOREIGN KEY(action_id) REFERENCES candidate_actions(id),
    FOREIGN KEY(permit_id) REFERENCES permits(id)
);

CREATE TABLE IF NOT EXISTS verification_records (
    id                  TEXT PRIMARY KEY,
    objective_id        TEXT NOT NULL,
    plan_id             TEXT NOT NULL,
    action_id           TEXT,
    execution_result_id TEXT,
    verifier            TEXT NOT NULL,
    method              TEXT NOT NULL,
    verdict             TEXT NOT NULL,
    evidence_json       TEXT NOT NULL,
    evidence_sha256     TEXT NOT NULL,
    created_at          INTEGER NOT NULL,
    FOREIGN KEY(objective_id) REFERENCES objectives(id),
    FOREIGN KEY(plan_id) REFERENCES plans(id)
);

CREATE TABLE IF NOT EXISTS objective_events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    objective_id   TEXT NOT NULL,
    kind           TEXT NOT NULL,
    actor          TEXT NOT NULL,
    payload_json   TEXT NOT NULL,
    previous_status TEXT,
    next_status    TEXT,
    created_at     INTEGER NOT NULL,
    FOREIGN KEY(objective_id) REFERENCES objectives(id)
);

CREATE TABLE IF NOT EXISTS objective_inbox (
    id             TEXT PRIMARY KEY,
    objective_id   TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    payload_json   TEXT NOT NULL,
    dedupe_key     TEXT UNIQUE,
    priority_class TEXT NOT NULL DEFAULT 'normal',
    priority       INTEGER NOT NULL DEFAULT 50,
    deadline_at    INTEGER,
    status         TEXT NOT NULL,
    available_at   INTEGER NOT NULL,
    attempts       INTEGER NOT NULL DEFAULT 0,
    claimed_by     TEXT,
    claim_expires  INTEGER,
    last_error     TEXT,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL,
    FOREIGN KEY(objective_id) REFERENCES objectives(id)
);

CREATE TABLE IF NOT EXISTS objective_cycles (
    id             TEXT PRIMARY KEY,
    objective_id   TEXT NOT NULL,
    inbox_event_id TEXT NOT NULL,
    runtime_id     TEXT NOT NULL,
    status         TEXT NOT NULL,
    summary_json   TEXT NOT NULL,
    started_at     INTEGER NOT NULL,
    finished_at    INTEGER,
    FOREIGN KEY(objective_id) REFERENCES objectives(id),
    FOREIGN KEY(inbox_event_id) REFERENCES objective_inbox(id)
);

CREATE INDEX IF NOT EXISTS idx_objectives_status ON objectives(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_plans_objective ON plans(objective_id, version);
CREATE INDEX IF NOT EXISTS idx_actions_objective ON candidate_actions(objective_id, status);
CREATE INDEX IF NOT EXISTS idx_permits_action ON permits(action_id, expires_at);
CREATE INDEX IF NOT EXISTS idx_verifications_objective
    ON verification_records(objective_id, created_at);
CREATE INDEX IF NOT EXISTS idx_objective_events_objective
    ON objective_events(objective_id, id);
CREATE INDEX IF NOT EXISTS idx_objective_inbox_due
    ON objective_inbox(status, available_at, claim_expires);
CREATE INDEX IF NOT EXISTS idx_objective_cycles_objective
    ON objective_cycles(objective_id, started_at);

CREATE TRIGGER IF NOT EXISTS plans_immutable_update
BEFORE UPDATE ON plans BEGIN SELECT RAISE(ABORT, 'plans are immutable'); END;
CREATE TRIGGER IF NOT EXISTS plans_immutable_delete
BEFORE DELETE ON plans BEGIN SELECT RAISE(ABORT, 'plans are immutable'); END;

CREATE TRIGGER IF NOT EXISTS candidate_actions_contract_immutable
BEFORE UPDATE ON candidate_actions
WHEN OLD.id IS NOT NEW.id OR OLD.objective_id IS NOT NEW.objective_id
 OR OLD.plan_id IS NOT NEW.plan_id OR OLD.action_type IS NOT NEW.action_type
 OR OLD.payload_json IS NOT NEW.payload_json
 OR OLD.payload_sha256 IS NOT NEW.payload_sha256
 OR OLD.rationale IS NOT NEW.rationale
 OR OLD.expected_outcome IS NOT NEW.expected_outcome
 OR OLD.required_capability IS NOT NEW.required_capability
 OR OLD.verification_method IS NOT NEW.verification_method
 OR OLD.risk_class IS NOT NEW.risk_class OR OLD.reversible IS NOT NEW.reversible
 OR OLD.compensation_action_type IS NOT NEW.compensation_action_type
 OR OLD.compensation_payload_json IS NOT NEW.compensation_payload_json
 OR OLD.compensation_capability IS NOT NEW.compensation_capability
 OR OLD.compensation_verification_method IS NOT NEW.compensation_verification_method
 OR OLD.estimated_cost_minor IS NOT NEW.estimated_cost_minor
 OR OLD.proposed_by IS NOT NEW.proposed_by OR OLD.created_at IS NOT NEW.created_at
BEGIN SELECT RAISE(ABORT, 'candidate action contract is immutable'); END;
CREATE TRIGGER IF NOT EXISTS candidate_actions_immutable_delete
BEFORE DELETE ON candidate_actions
BEGIN SELECT RAISE(ABORT, 'candidate actions cannot be deleted'); END;

CREATE TRIGGER IF NOT EXISTS permits_contract_immutable
BEFORE UPDATE ON permits
WHEN OLD.id IS NOT NEW.id OR OLD.action_id IS NOT NEW.action_id
 OR OLD.capability IS NOT NEW.capability
 OR OLD.payload_sha256 IS NOT NEW.payload_sha256
 OR OLD.target_resource IS NOT NEW.target_resource
 OR OLD.policy_version IS NOT NEW.policy_version
 OR OLD.approval_artifact_id IS NOT NEW.approval_artifact_id
 OR OLD.constraints_json IS NOT NEW.constraints_json
 OR OLD.issued_to IS NOT NEW.issued_to OR OLD.issued_at IS NOT NEW.issued_at
 OR OLD.expires_at IS NOT NEW.expires_at
BEGIN SELECT RAISE(ABORT, 'permit contract is immutable'); END;
CREATE TRIGGER IF NOT EXISTS permits_immutable_delete
BEFORE DELETE ON permits
BEGIN SELECT RAISE(ABORT, 'permits cannot be deleted'); END;

CREATE TRIGGER IF NOT EXISTS execution_results_immutable_update
BEFORE UPDATE ON execution_results
BEGIN SELECT RAISE(ABORT, 'execution results are immutable'); END;
CREATE TRIGGER IF NOT EXISTS execution_results_immutable_delete
BEFORE DELETE ON execution_results
BEGIN SELECT RAISE(ABORT, 'execution results are immutable'); END;
CREATE TRIGGER IF NOT EXISTS verification_records_immutable_update
BEFORE UPDATE ON verification_records
BEGIN SELECT RAISE(ABORT, 'verification records are immutable'); END;
CREATE TRIGGER IF NOT EXISTS verification_records_immutable_delete
BEFORE DELETE ON verification_records
BEGIN SELECT RAISE(ABORT, 'verification records are immutable'); END;
CREATE TRIGGER IF NOT EXISTS objective_events_immutable_update
BEFORE UPDATE ON objective_events
BEGIN SELECT RAISE(ABORT, 'objective events are immutable'); END;
CREATE TRIGGER IF NOT EXISTS objective_events_immutable_delete
BEFORE DELETE ON objective_events
BEGIN SELECT RAISE(ABORT, 'objective events are immutable'); END;
"""


class ObjectiveStateError(ValueError):
    """Raised when a requested authority-state transition is inadmissible."""


class PermitError(PermissionError):
    """Raised when an action lacks an exact, live execution permit."""


@dataclass(frozen=True)
class Objective:
    id: str
    organization_id: str
    desired_outcome: str
    status: str
    originator: str
    owner: Optional[str]
    expires_at: Optional[int]
    reaffirmed_at: int
    created_at: int
    updated_at: int
    version: int


def objective_to_dict(conn: sqlite3.Connection, objective_id: str) -> dict[str, Any]:
    """Return the full authoritative objective record with decoded JSON fields."""
    row = conn.execute("SELECT * FROM objectives WHERE id = ?", (objective_id,)).fetchone()
    if row is None:
        raise KeyError(f"objective not found: {objective_id}")
    result = dict(row)
    for key in (
        "constraints_json",
        "authority_scope_json",
        "success_criteria_json",
        "termination_json",
        "permitted_systems_json",
        "prohibited_actions_json",
    ):
        result[key.removesuffix("_json")] = json.loads(result.pop(key))
    return result


def list_objectives(
    conn: sqlite3.Connection,
    *,
    status: Optional[str] = None,
    include_terminal: bool = False,
    organization_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    if status is not None and status not in OBJECTIVE_STATUSES:
        raise ValueError(f"unknown objective status: {status}")
    query = "SELECT id FROM objectives"
    params: list[Any] = []
    clauses: list[str] = []
    if organization_id is not None:
        clauses.append("organization_id = ?")
        params.append(organization_id)
    if status is not None:
        clauses.append("status = ?")
        params.append(status)
    elif not include_terminal:
        clauses.append(
            "status NOT IN ('closed','cancelled','expired','abandoned','superseded')"
        )
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY updated_at DESC, created_at DESC"
    return [
        objective_to_dict(conn, row["id"])
        for row in conn.execute(query, params).fetchall()
    ]


def objective_snapshot(conn: sqlite3.Connection, objective_id: str) -> dict[str, Any]:
    """Return an objective and all immutable governance records beneath it."""
    objective = objective_to_dict(conn, objective_id)

    def decoded_rows(query: str, params: tuple[Any, ...], json_fields: tuple[str, ...]):
        rows = []
        for row in conn.execute(query, params).fetchall():
            item = dict(row)
            for field in json_fields:
                item[field.removesuffix("_json")] = json.loads(item.pop(field))
            rows.append(item)
        return rows

    objective["plans"] = decoded_rows(
        "SELECT * FROM plans WHERE objective_id = ? ORDER BY version",
        (objective_id,),
        ("assumptions_json", "tasks_json", "dependencies_json", "risks_json"),
    )
    objective["actions"] = decoded_rows(
        "SELECT * FROM candidate_actions WHERE objective_id = ? ORDER BY created_at, id",
        (objective_id,),
        ("payload_json",),
    )
    objective["verifications"] = decoded_rows(
        """
        SELECT * FROM verification_records
         WHERE objective_id = ? ORDER BY created_at, id
        """,
        (objective_id,),
        ("evidence_json",),
    )
    objective["execution_results"] = decoded_rows(
        """
        SELECT r.*
          FROM execution_results r
          JOIN candidate_actions a ON a.id = r.action_id
         WHERE a.objective_id = ?
         ORDER BY r.finished_at, r.id
        """,
        (objective_id,),
        ("result_json",),
    )
    objective["events"] = decoded_rows(
        "SELECT * FROM objective_events WHERE objective_id = ? ORDER BY id",
        (objective_id,),
        ("payload_json",),
    )
    return objective


def in_doubt_executions(
    conn: sqlite3.Connection, organization_id: str
) -> list[dict[str, Any]]:
    """Return credential-free recovery posture for interrupted action pipelines."""
    rows = conn.execute(
        """SELECT objective.id AS objective_id,
                  objective.status AS objective_status,
                  action.id AS action_id,action.action_type,action.status AS action_status,
                  action.payload_json,action.updated_at,
                  permit.id AS permit_id,permit.consumed_at,
                  result.id AS execution_result_id,result.status AS result_status,
                  verification.id AS verification_id,
                  verification.verdict AS verification_verdict
             FROM objectives objective
             JOIN candidate_actions action ON action.objective_id=objective.id
             LEFT JOIN permits permit ON permit.id=(
                 SELECT p.id FROM permits p WHERE p.action_id=action.id
                  ORDER BY p.issued_at DESC,p.id DESC LIMIT 1
             )
             LEFT JOIN execution_results result ON result.id=(
                 SELECT r.id FROM execution_results r WHERE r.action_id=action.id
                  ORDER BY r.finished_at DESC,r.id DESC LIMIT 1
             )
             LEFT JOIN verification_records verification ON verification.id=(
                 SELECT v.id FROM verification_records v
                  WHERE v.action_id=action.id
                    AND (result.id IS NULL OR v.execution_result_id=result.id)
                  ORDER BY v.created_at DESC,v.id DESC LIMIT 1
             )
            WHERE objective.organization_id=?
              AND objective.status IN ('authorized','executing')
              AND action.status IN ('permitted','executing','succeeded','failed')
            ORDER BY action.updated_at,action.id""",
        (organization_id,),
    ).fetchall()
    now = _now()
    posture = []
    for row in rows:
        payload = json.loads(row["payload_json"])
        stage = (
            "permit_issued_pre_call"
            if row["action_status"] == "permitted"
            else "effect_outcome_unknown"
            if row["execution_result_id"] is None
            else "result_requires_verification"
            if row["verification_id"] is None
            else "verification_requires_finalization"
        )
        posture.append(
            {
                "objective_id": str(row["objective_id"]),
                "objective_status": str(row["objective_status"]),
                "action_id": str(row["action_id"]),
                "action_type": str(row["action_type"]),
                "action_status": str(row["action_status"]),
                "permit_id": (
                    str(row["permit_id"]) if row["permit_id"] else None
                ),
                "execution_result_id": (
                    str(row["execution_result_id"])
                    if row["execution_result_id"]
                    else None
                ),
                "verification_id": (
                    str(row["verification_id"])
                    if row["verification_id"]
                    else None
                ),
                "verification_verdict": row["verification_verdict"],
                "recovery_stage": stage,
                "has_replay_idempotency_key": len(
                    str(payload.get("idempotency_key") or "").strip()
                )
                >= 16,
                "age_seconds": max(0, now - int(row["updated_at"])),
            }
        )
    return posture


def objectives_db_path() -> Path:
    return get_hermes_home().resolve() / "objectives.db"


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def payload_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json(dict(payload)).encode("utf-8")).hexdigest()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def connect(path: Optional[Path] = None) -> sqlite3.Connection:
    resolved = Path(path or objectives_db_path()).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(resolved, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.executescript(SCHEMA_SQL)
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(objectives)")}
    if "organization_id" not in columns:
        conn.execute(
            "ALTER TABLE objectives ADD COLUMN "
            "organization_id TEXT NOT NULL DEFAULT '__unscoped__'"
        )
        conn.commit()
    action_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(candidate_actions)")
    }
    for name, declaration in (
        ("compensation_action_type", "TEXT"),
        ("compensation_payload_json", "TEXT"),
        ("compensation_capability", "TEXT"),
        ("compensation_verification_method", "TEXT"),
    ):
        if name not in action_columns:
            conn.execute(f"ALTER TABLE candidate_actions ADD COLUMN {name} {declaration}")
    plan_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(plans)")
    }
    if "inference_id" not in plan_columns:
        conn.execute("ALTER TABLE plans ADD COLUMN inference_id TEXT")
    result_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(execution_results)")
    }
    if "actual_cost_minor" not in result_columns:
        conn.execute(
            "ALTER TABLE execution_results ADD COLUMN actual_cost_minor INTEGER"
        )
    if "preserve_reservation" not in result_columns:
        conn.execute(
            """ALTER TABLE execution_results ADD COLUMN
               preserve_reservation INTEGER NOT NULL DEFAULT 0"""
        )
    inbox_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(objective_inbox)")
    }
    migrated_inbox_admission = False
    for name, declaration in (
        ("priority_class", "TEXT NOT NULL DEFAULT 'normal'"),
        ("priority", "INTEGER NOT NULL DEFAULT 50"),
        ("deadline_at", "INTEGER"),
    ):
        if name not in inbox_columns:
            conn.execute(
                f"ALTER TABLE objective_inbox ADD COLUMN {name} {declaration}"
            )
            migrated_inbox_admission = True
    if migrated_inbox_admission:
        from hermes_cli.objective_event_policy import classify

        for row in conn.execute(
            "SELECT id,event_type,payload_json FROM objective_inbox"
        ).fetchall():
            admission = classify(
                str(row["event_type"]), json.loads(row["payload_json"])
            )
            conn.execute(
                """UPDATE objective_inbox
                      SET priority_class=?,priority=?,deadline_at=? WHERE id=?""",
                (
                    admission.priority_class,
                    admission.priority,
                    admission.deadline_at,
                    row["id"],
                ),
            )
    conn.execute(
        """CREATE INDEX IF NOT EXISTS idx_objective_inbox_admission
           ON objective_inbox(status,priority DESC,deadline_at,available_at)"""
    )
    conn.execute(
        """CREATE TRIGGER IF NOT EXISTS objective_inbox_admission_immutable
           BEFORE UPDATE ON objective_inbox
           WHEN OLD.objective_id IS NOT NEW.objective_id
             OR OLD.event_type IS NOT NEW.event_type
             OR OLD.payload_json IS NOT NEW.payload_json
             OR OLD.dedupe_key IS NOT NEW.dedupe_key
             OR OLD.priority_class IS NOT NEW.priority_class
             OR OLD.priority IS NOT NEW.priority
             OR OLD.deadline_at IS NOT NEW.deadline_at
             OR OLD.created_at IS NOT NEW.created_at
           BEGIN
             SELECT RAISE(ABORT, 'objective event admission is immutable');
           END"""
    )
    conn.commit()
    return conn


@contextlib.contextmanager
def connect_closing(path: Optional[Path] = None) -> Iterator[sqlite3.Connection]:
    conn = connect(path)
    try:
        yield conn
    finally:
        conn.close()


def _now() -> int:
    return int(time.time())


def _append_event(
    conn: sqlite3.Connection,
    objective_id: str,
    kind: str,
    actor: str,
    payload: Any,
    *,
    previous_status: Optional[str] = None,
    next_status: Optional[str] = None,
) -> None:
    conn.execute(
        """
        INSERT INTO objective_events (
            objective_id, kind, actor, payload_json,
            previous_status, next_status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            objective_id,
            kind,
            actor,
            _json(payload),
            previous_status,
            next_status,
            _now(),
        ),
    )


def create_objective(
    conn: sqlite3.Connection,
    *,
    desired_outcome: str,
    originator: str,
    owner: Optional[str] = None,
    constraints: Any = None,
    authority_scope: Any = None,
    success_criteria: Any = None,
    termination_conditions: Any = None,
    permitted_systems: Any = None,
    prohibited_actions: Any = None,
    max_spend_minor: Optional[int] = None,
    currency: Optional[str] = None,
    expires_at: Optional[int] = None,
    organization_id: Optional[str] = None,
) -> Objective:
    outcome = desired_outcome.strip()
    if not outcome:
        raise ValueError("desired_outcome must not be empty")
    if max_spend_minor is not None and max_spend_minor < 0:
        raise ValueError("max_spend_minor must be non-negative")
    ts = _now()
    objective_id = _new_id("obj")
    if organization_id is None:
        try:
            from hermes_cli import organization_db

            ceo = organization_db.active_ceo(conn)
            organization_id = (
                ceo["organization_id"] if ceo is not None else "__unscoped__"
            )
        except sqlite3.OperationalError:
            organization_id = "__unscoped__"
    elif organization_id != "__unscoped__":
        # Production organization stores carry a tenant authority table. Do
        # not allow an objective to be attached to a guessed or foreign tenant
        # ID; lightweight unscoped stores used by isolated subsystems retain
        # their compatibility behavior when that table is absent.
        try:
            organization_row = conn.execute(
                "SELECT 1 FROM organizations WHERE id=?", (organization_id,)
            ).fetchone()
        except sqlite3.OperationalError:
            organization_row = True
        if organization_row is None:
            raise ValueError("objective organization is not configured")
    with conn:
        conn.execute(
            """
            INSERT INTO objectives (
                id, organization_id, desired_outcome, status, originator, owner,
                constraints_json, authority_scope_json, success_criteria_json,
                termination_json, permitted_systems_json,
                prohibited_actions_json, max_spend_minor, currency,
                expires_at, reaffirmed_at, created_at, updated_at
            ) VALUES (?, ?, ?, 'proposed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                objective_id,
                organization_id,
                outcome,
                originator,
                owner,
                _json(constraints or []),
                _json(authority_scope or {}),
                _json(success_criteria or []),
                _json(termination_conditions or []),
                _json(permitted_systems or []),
                _json(prohibited_actions or []),
                max_spend_minor,
                currency,
                expires_at,
                ts,
                ts,
                ts,
            ),
        )
        _append_event(conn, objective_id, "created", originator, {"outcome": outcome})
    return get_objective(conn, objective_id)


def get_objective(conn: sqlite3.Connection, objective_id: str) -> Objective:
    row = conn.execute("SELECT * FROM objectives WHERE id = ?", (objective_id,)).fetchone()
    if row is None:
        raise KeyError(f"objective not found: {objective_id}")
    return Objective(
        id=row["id"],
        organization_id=row["organization_id"],
        desired_outcome=row["desired_outcome"],
        status=row["status"],
        originator=row["originator"],
        owner=row["owner"],
        expires_at=row["expires_at"],
        reaffirmed_at=int(row["reaffirmed_at"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        version=row["version"],
    )


def reaffirm_objective(
    conn: sqlite3.Connection,
    objective_id: str,
    *,
    actor: str,
    reason: str,
) -> Objective:
    """Refresh standing intent without changing plan or execution authority."""
    now = _now()
    with conn:
        _assert_employee_actor_scope(conn, objective_id, actor)
        row = conn.execute(
            "SELECT status,expires_at FROM objectives WHERE id=?",
            (objective_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"objective not found: {objective_id}")
        if row["status"] in TERMINAL_OBJECTIVE_STATUSES:
            raise ObjectiveStateError(
                f"cannot reaffirm terminal objective {row['status']}"
            )
        if row["expires_at"] is not None and int(row["expires_at"]) <= now:
            raise ObjectiveStateError("cannot reaffirm an expired objective")
        conn.execute(
            "UPDATE objectives SET reaffirmed_at=?,updated_at=? WHERE id=?",
            (now, now, objective_id),
        )
        _append_event(
            conn,
            objective_id,
            "reaffirmed",
            actor,
            {"reason": reason},
        )
    return get_objective(conn, objective_id)


def transition_objective(
    conn: sqlite3.Connection,
    objective_id: str,
    next_status: str,
    *,
    actor: str,
    reason: str = "",
) -> Objective:
    if next_status not in OBJECTIVE_STATUSES:
        raise ObjectiveStateError(f"unknown objective status: {next_status}")
    with conn:
        _assert_employee_actor_scope(conn, objective_id, actor)
        row = conn.execute(
            "SELECT status, expires_at, version, organization_id FROM objectives WHERE id = ?",
            (objective_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"objective not found: {objective_id}")
        current = row["status"]
        if next_status not in _TRANSITIONS[current]:
            raise ObjectiveStateError(f"cannot transition objective {current} -> {next_status}")
        if row["expires_at"] is not None and row["expires_at"] <= _now() and next_status != "expired":
            raise ObjectiveStateError("objective has expired")
        if next_status == "verified" and not _has_passing_current_plan_verification(
            conn, objective_id
        ):
            raise ObjectiveStateError(
                "objective cannot be verified without a passing verification "
                "record for its current plan"
            )
        ts = _now()
        new_version = row["version"] + 1
        updated = conn.execute(
            """
            UPDATE objectives
               SET status = ?, updated_at = ?, version = version + 1
             WHERE id = ? AND status = ? AND version = ?
            """,
            (next_status, ts, objective_id, current, row["version"]),
        )
        if updated.rowcount != 1:
            raise ObjectiveStateError("objective changed concurrently; reload and retry")
        _append_event(
            conn,
            objective_id,
            "transitioned",
            actor,
            {"reason": reason},
            previous_status=current,
            next_status=next_status,
        )
    _mirror_objective_status_to_postgres(
        objective_id=objective_id,
        organization_id=row["organization_id"],
        status=next_status,
        version=new_version,
    )
    return get_objective(conn, objective_id)


def _mirror_objective_status_to_postgres(
    *, objective_id: str, organization_id: str, status: str, version: int
) -> None:
    """Push the just-committed objective status into the Postgres mirror
    (pg_objective_status) whenever Postgres coordination is configured, so
    postgres_authority.consume_permit's objective_id gate has real data to
    check against.

    This is a synchronous push after the SQLite commit above, not a
    cross-database transaction — it cannot be made atomic with it. On
    failure, log and re-raise distinctly WITHOUT rolling back the
    already-committed SQLite transition: the objective's SQLite state is
    durably true regardless of whether the Postgres mirror succeeded. A
    missing/stale mirror row makes Postgres-side permit consumption fail
    closed (see postgres_authority.consume_permit's objective_id gating) —
    it does not un-happen a real state transition SQLite already committed.

    Mirrors process-wide (matching authority_bridge._postgres_configured()'s
    own activation model on this codebase — there is no per-organization
    backend selection here) rather than per-organization.
    """
    import os

    if not (os.environ.get("AUTHORITY_POSTGRES_URL") or os.environ.get("DATABASE_URL")):
        return

    import logging

    logger = logging.getLogger(__name__)
    try:
        from hermes_cli.postgres_authority import connect as pg_connect
        from hermes_cli.postgres_authority import init_schema, mirror_objective_status

        pg_conn = pg_connect()
        try:
            init_schema(pg_conn)
            mirror_objective_status(
                pg_conn,
                objective_id=objective_id,
                organization_id=organization_id,
                status=status,
                version=version,
            )
        finally:
            pg_conn.close()
    except Exception:
        logger.exception(
            "Postgres objective-status mirror push failed for objective=%s "
            "org=%s status=%s version=%s; SQLite transition already "
            "committed and will NOT be rolled back. Postgres-side permit "
            "consumption gated on this objective_id will fail closed until "
            "the mirror catches up.",
            objective_id,
            organization_id,
            status,
            version,
        )
        raise


def create_plan(
    conn: sqlite3.Connection,
    objective_id: str,
    *,
    assumptions: Any,
    tasks: Any,
    dependencies: Any,
    risks: Any,
    created_by: str,
    inference_id: Optional[str] = None,
) -> str:
    with conn:
        objective = get_objective(conn, objective_id)
        _assert_employee_actor_scope(conn, objective_id, created_by)
        if objective.status not in {"accepted", "planned", "executing", "blocked"}:
            raise ObjectiveStateError(
                f"cannot plan objective while status is {objective.status}"
            )
        previous = conn.execute(
            "SELECT id, version FROM plans WHERE objective_id = ? ORDER BY version DESC LIMIT 1",
            (objective_id,),
        ).fetchone()
        version = (previous["version"] + 1) if previous else 1
        if inference_id is not None:
            inference = conn.execute(
                """SELECT objective_id,parse_status FROM planner_inferences
                    WHERE id=?""",
                (inference_id,),
            ).fetchone()
            if (
                inference is None
                or str(inference["objective_id"]) != objective_id
                or str(inference["parse_status"]) != "parsed"
            ):
                raise ObjectiveStateError(
                    "plan inference is missing, invalid, or belongs elsewhere"
                )
        plan_id = _new_id("plan")
        conn.execute(
            """
            INSERT INTO plans (
                id, objective_id, version, assumptions_json, tasks_json,
                dependencies_json, risks_json, created_by, created_at,
                supersedes_id, inference_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                plan_id,
                objective_id,
                version,
                _json(assumptions),
                _json(tasks),
                _json(dependencies),
                _json(risks),
                created_by,
                _now(),
                previous["id"] if previous else None,
                inference_id,
            ),
        )
        _append_event(
            conn,
            objective_id,
            "plan_created",
            created_by,
            {"plan_id": plan_id, "version": version},
        )
    return plan_id


def propose_action(
    conn: sqlite3.Connection,
    *,
    objective_id: str,
    plan_id: str,
    action_type: str,
    payload: Mapping[str, Any],
    expected_outcome: str,
    required_capability: str,
    verification_method: str,
    risk_class: str,
    reversible: bool,
    proposed_by: str,
    rationale: str = "",
    estimated_cost_minor: Optional[int] = None,
    compensation: Optional[Mapping[str, Any]] = None,
) -> str:
    objective = get_objective(conn, objective_id)
    _assert_employee_actor_scope(conn, objective_id, proposed_by)
    if objective.status not in {"planned", "authorized", "executing", "blocked"}:
        raise ObjectiveStateError(
            f"cannot propose an action while objective status is {objective.status}"
        )
    plan = conn.execute(
        "SELECT objective_id, version FROM plans WHERE id = ?", (plan_id,)
    ).fetchone()
    if plan is None or plan["objective_id"] != objective_id:
        raise ObjectiveStateError("action plan does not belong to objective")
    latest = conn.execute(
        "SELECT MAX(version) AS version FROM plans WHERE objective_id = ?",
        (objective_id,),
    ).fetchone()
    if latest is None or plan["version"] != latest["version"]:
        raise ObjectiveStateError("actions may only be proposed against the current plan")
    action_id = _new_id("act")
    compensation = dict(compensation or {})
    ts = _now()
    with conn:
        conn.execute(
            """
            INSERT INTO candidate_actions (
                id, objective_id, plan_id, action_type, payload_json,
                payload_sha256, rationale, expected_outcome,
                required_capability, verification_method, risk_class,
                reversible, estimated_cost_minor, status, proposed_by,
                compensation_action_type, compensation_payload_json,
                compensation_capability, compensation_verification_method,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'proposed', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                action_id,
                objective_id,
                plan_id,
                action_type,
                _json(dict(payload)),
                payload_sha256(payload),
                rationale,
                expected_outcome,
                required_capability,
                verification_method,
                risk_class,
                1 if reversible else 0,
                estimated_cost_minor,
                proposed_by,
                compensation.get("action_type"),
                _json(compensation.get("payload") or {}) if compensation else None,
                compensation.get("required_capability"),
                compensation.get("verification_method"),
                ts,
                ts,
            ),
        )
        _append_event(
            conn,
            objective_id,
            "action_proposed",
            proposed_by,
            {"action_id": action_id, "plan_id": plan_id},
        )
    return action_id


def issue_permit(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    capability: str,
    issued_to: str,
    policy_version: str,
    expires_at: int,
    target_resource: Optional[str] = None,
    approval_artifact_id: Optional[str] = None,
    constraints: Any = None,
) -> str:
    with conn:
        action = conn.execute(
            "SELECT * FROM candidate_actions WHERE id = ?", (action_id,)
        ).fetchone()
        if action is None:
            raise KeyError(f"action not found: {action_id}")
        if action["status"] != "proposed":
            raise PermitError(f"action is not awaiting a permit: {action['status']}")
        objective = get_objective(conn, action["objective_id"])
        _assert_employee_actor_scope(
            conn, action["objective_id"], issued_to, require_employee=True
        )
        if objective.status not in {"planned", "authorized", "executing"}:
            raise PermitError(
                f"objective status {objective.status} does not admit new permits"
            )
        plan = conn.execute(
            "SELECT version FROM plans WHERE id = ?", (action["plan_id"],)
        ).fetchone()
        latest = conn.execute(
            "SELECT MAX(version) AS version FROM plans WHERE objective_id = ?",
            (action["objective_id"],),
        ).fetchone()
        if (
            plan is None
            or latest is None
            or int(plan["version"]) != int(latest["version"])
        ):
            raise PermitError("action belongs to a superseded plan")
        if capability != action["required_capability"]:
            raise PermitError("permit capability does not match proposed action")
        action_payload = json.loads(action["payload_json"])
        action_target = str(action_payload.get("target_resource") or "").strip()
        requested_target = (
            str(target_resource).strip() if target_resource is not None else ""
        )
        if action_target and requested_target and requested_target != action_target:
            raise PermitError("permit target resource does not match action scope")
        if action_target:
            # Persist the exact immutable action scope even when callers omit the
            # redundant argument; execution remains bound to the payload hash.
            target_resource = action_target
        if expires_at <= _now():
            raise PermitError("permit expiry must be in the future")
        permit_id = _new_id("permit")
        conn.execute(
            """
            INSERT INTO permits (
                id, action_id, capability, payload_sha256, target_resource,
                policy_version, approval_artifact_id, constraints_json,
                issued_to, issued_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                permit_id,
                action_id,
                capability,
                action["payload_sha256"],
                target_resource,
                policy_version,
                approval_artifact_id,
                _json(constraints or {}),
                issued_to,
                _now(),
                expires_at,
            ),
        )
        conn.execute(
            "UPDATE candidate_actions SET status = 'permitted', updated_at = ? WHERE id = ?",
            (_now(), action_id),
        )
        _append_event(
            conn,
            action["objective_id"],
            "permit_issued",
            issued_to,
            {"action_id": action_id, "permit_id": permit_id},
        )
    return permit_id


def consume_permit(
    conn: sqlite3.Connection,
    permit_id: str,
    *,
    action_id: str,
    payload: Mapping[str, Any],
    executor: str,
    organization_id: str,
    current_policy_version: Optional[str] = None,
) -> None:
    with conn:
        permit = conn.execute("SELECT * FROM permits WHERE id = ?", (permit_id,)).fetchone()
        if permit is None:
            raise PermitError("permit not found")
        if permit["action_id"] != action_id:
            raise PermitError("permit is bound to a different action")
        objective = conn.execute(
            """SELECT o.status,o.organization_id FROM objectives o
                JOIN candidate_actions a ON a.objective_id=o.id
                WHERE a.id=?""",
            (action_id,),
        ).fetchone()
        if objective is None:
            raise PermitError("permit action objective was removed")
        if str(objective["organization_id"]) != organization_id:
            raise PermitError("permit organization does not match objective")
        if objective["status"] not in {"planned", "authorized", "executing"}:
            raise PermitError(
                f"objective status {objective['status']} no longer admits execution"
            )
        if permit["revoked_at"] is not None:
            raise PermitError("permit was revoked")
        if permit["consumed_at"] is not None:
            raise PermitError("permit was already consumed")
        if permit["expires_at"] <= _now():
            raise PermitError("permit has expired")
        if permit["issued_to"] != executor:
            raise PermitError("permit was issued to a different executor")
        if (
            current_policy_version is not None
            and str(permit["policy_version"]) != str(current_policy_version)
        ):
            raise PermitError("permit policy version is stale")
        if permit["payload_sha256"] != payload_sha256(payload):
            raise PermitError("execution payload does not match permitted payload")
        updated = conn.execute(
            "UPDATE permits SET consumed_at = ? WHERE id = ? AND consumed_at IS NULL",
            (_now(), permit_id),
        )
        if updated.rowcount != 1:
            raise PermitError("permit was consumed concurrently")
        if permit["approval_artifact_id"] is not None:
            from hermes_cli import approval_artifacts

            approval_artifacts.mark_consumed(
                conn,
                artifact_id=str(permit["approval_artifact_id"]),
                permit_id=permit_id,
            )
        conn.execute(
            "UPDATE candidate_actions SET status = 'executing', updated_at = ? WHERE id = ?",
            (_now(), action_id),
        )


def revoke_unconsumed_permits(
    conn: sqlite3.Connection,
    *,
    organization_id: str,
    actor: str,
    reason: str,
) -> list[str]:
    """Revoke outstanding execution authority after a charter revision."""
    if not actor.strip() or not reason.strip():
        raise ValueError("permit revocation requires actor and reason")
    now = _now()
    rows = conn.execute(
        """SELECT permit.id,permit.action_id,action.objective_id
             FROM permits permit
             JOIN candidate_actions action ON action.id=permit.action_id
             JOIN objectives objective ON objective.id=action.objective_id
            WHERE objective.organization_id=?
              AND permit.consumed_at IS NULL AND permit.revoked_at IS NULL
            ORDER BY permit.issued_at,permit.id""",
        (organization_id,),
    ).fetchall()
    revoked: list[str] = []
    with conn:
        for row in rows:
            conn.execute(
                "UPDATE permits SET revoked_at=? WHERE id=? AND revoked_at IS NULL",
                (now, row["id"]),
            )
            conn.execute(
                """UPDATE candidate_actions SET status='expired',updated_at=?
                    WHERE id=? AND status IN ('proposed','permitted')""",
                (now, row["action_id"]),
            )
            _append_event(
                conn,
                str(row["objective_id"]),
                "permit_revoked",
                actor,
                {
                    "permit_id": str(row["id"]),
                    "action_id": str(row["action_id"]),
                    "reason": reason.strip(),
                },
            )
            revoked.append(str(row["id"]))
    if revoked:
        from hermes_cli import finance_db

        has_reservations = conn.execute(
            """SELECT 1 FROM sqlite_master
                WHERE type='table' AND name='budget_reservations'"""
        ).fetchone()
        for row in rows:
            reservation = (
                conn.execute(
                    """SELECT 1 FROM budget_reservations
                        WHERE action_id=? AND status='reserved'""",
                    (row["action_id"],),
                ).fetchone()
                if has_reservations is not None
                else None
            )
            if reservation is not None:
                finance_db.release_reservation(
                    conn,
                    str(row["action_id"]),
                    reason="standing_charter_revised",
                )
    return revoked


def record_execution_result(
    conn: sqlite3.Connection,
    *,
    action_id: str,
    permit_id: str,
    executor: str,
    organization_id: str,
    status: str,
    result: Any,
    started_at: int,
    external_reference: Optional[str] = None,
    actual_cost_minor: Optional[int] = None,
    preserve_reservation: bool = False,
) -> str:
    if status not in {"succeeded", "failed"}:
        raise ValueError("execution status must be succeeded or failed")
    with conn:
        action = conn.execute(
            "SELECT objective_id, status FROM candidate_actions WHERE id = ?",
            (action_id,),
        ).fetchone()
        permit = conn.execute(
            "SELECT action_id, issued_to, consumed_at FROM permits WHERE id = ?",
            (permit_id,),
        ).fetchone()
        if action is None or permit is None:
            raise KeyError("action or permit not found")
        objective = conn.execute(
            "SELECT organization_id FROM objectives WHERE id=?",
            (action["objective_id"],),
        ).fetchone()
        if objective is None or str(objective["organization_id"]) != organization_id:
            raise PermitError("execution result organization does not match objective")
        if permit["action_id"] != action_id or permit["issued_to"] != executor:
            raise PermitError("execution result does not match permit")
        if permit["consumed_at"] is None or action["status"] != "executing":
            raise PermitError("permit must be consumed before recording a result")
        result_id = _new_id("result")
        finished_at = _now()
        conn.execute(
            """
            INSERT INTO execution_results (
                id, action_id, permit_id, executor, status, external_reference,
                result_json, actual_cost_minor, preserve_reservation,
                started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result_id,
                action_id,
                permit_id,
                executor,
                status,
                external_reference,
                _json(result),
                actual_cost_minor,
                int(preserve_reservation),
                started_at,
                finished_at,
            ),
        )
        conn.execute(
            "UPDATE candidate_actions SET status = ?, updated_at = ? WHERE id = ?",
            (status, finished_at, action_id),
        )
        _append_event(
            conn,
            action["objective_id"],
            "execution_recorded",
            executor,
            {"action_id": action_id, "result_id": result_id, "status": status},
        )
    return result_id


def deny_action(
    conn: sqlite3.Connection,
    action_id: str,
    *,
    actor: str,
    reason: str,
) -> None:
    if not reason.strip():
        raise ValueError("denial reason must not be empty")
    with conn:
        action = conn.execute(
            "SELECT objective_id, status FROM candidate_actions WHERE id = ?",
            (action_id,),
        ).fetchone()
        if action is None:
            raise KeyError(f"action not found: {action_id}")
        if action["status"] != "proposed":
            raise ObjectiveStateError(
                f"only proposed actions may be denied, got {action['status']}"
            )
        conn.execute(
            "UPDATE candidate_actions SET status = 'denied', updated_at = ? WHERE id = ?",
            (_now(), action_id),
        )
        _append_event(
            conn,
            action["objective_id"],
            "action_denied",
            actor,
            {"action_id": action_id, "reason": reason},
        )


def record_verification(
    conn: sqlite3.Connection,
    *,
    objective_id: str,
    organization_id: str,
    plan_id: str,
    verifier: str,
    method: str,
    verdict: str,
    evidence: Any,
    action_id: Optional[str] = None,
    execution_result_id: Optional[str] = None,
) -> str:
    if verdict not in {"pass", "fail", "inconclusive"}:
        raise ValueError("verification verdict must be pass, fail, or inconclusive")
    from hermes_cli import verification_evidence

    verification_evidence.validate(evidence, expected_observer=verifier)
    plan = conn.execute(
        "SELECT objective_id FROM plans WHERE id = ?", (plan_id,)
    ).fetchone()
    if plan is None or plan["objective_id"] != objective_id:
        raise ObjectiveStateError("verification plan does not belong to objective")
    objective = conn.execute(
        "SELECT organization_id FROM objectives WHERE id=?", (objective_id,)
    ).fetchone()
    if objective is None or str(objective["organization_id"]) != organization_id:
        raise ObjectiveStateError("verification organization does not match objective")
    evidence_json = _json(evidence)
    verification_id = _new_id("verify")
    with conn:
        conn.execute(
            """
            INSERT INTO verification_records (
                id, objective_id, plan_id, action_id, execution_result_id,
                verifier, method, verdict, evidence_json, evidence_sha256,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verification_id,
                objective_id,
                plan_id,
                action_id,
                execution_result_id,
                verifier,
                method,
                verdict,
                evidence_json,
                hashlib.sha256(evidence_json.encode("utf-8")).hexdigest(),
                _now(),
            ),
        )
        _append_event(
            conn,
            objective_id,
            "verification_recorded",
            verifier,
            {"verification_id": verification_id, "verdict": verdict},
        )
    return verification_id


def _has_passing_current_plan_verification(
    conn: sqlite3.Connection, objective_id: str
) -> bool:
    row = conn.execute(
        """
        SELECT v.id
          FROM verification_records v
          JOIN plans p ON p.id = v.plan_id
         WHERE v.objective_id = ?
           AND v.verdict = 'pass'
           AND p.version = (
               SELECT MAX(version) FROM plans WHERE objective_id = ?
           )
         LIMIT 1
        """,
        (objective_id, objective_id),
    ).fetchone()
    return row is not None


def _assert_employee_actor_scope(
    conn: sqlite3.Connection,
    objective_id: str,
    actor: str,
    *,
    require_employee: bool = False,
) -> None:
    """Check an employee actor, optionally requiring it for execution permits."""
    objective = conn.execute(
        "SELECT organization_id FROM objectives WHERE id=?", (objective_id,)
    ).fetchone()
    employee_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='employees'"
    ).fetchone()
    if employee_table is None:
        # Legacy objective-only stores may not yet have the organization
        # schema. Preserve their existing non-crashing behavior; fully
        # bootstrapped stores always have the employee identity boundary.
        return
    if objective is None or str(objective["organization_id"]) == "__unscoped__":
        # Legacy objective stores use synthetic planner/worker identities and
        # have no organization-level employee authority to resolve.
        return
    if not actor.startswith("employee:"):
        if not require_employee:
            return
        raise ObjectiveStateError(
            "organization-bound execution actor must be an employee identity"
        )
    employee_id = actor.split(":", 1)[1].strip()
    if not employee_id:
        raise ObjectiveStateError("employee actor identity is empty")
    employee = conn.execute(
        "SELECT organization_id,status FROM employees WHERE id=?", (employee_id,)
    ).fetchone()
    # The historical ``employee:ceo`` fixture identity remains supported, but
    # only as a lookup to the one active CEO in this objective's organization;
    # arbitrary employee aliases are never accepted as authority.
    if employee is None and employee_id in {"ceo", "founder"} and objective is not None:
        active_ceos = conn.execute(
            """SELECT organization_id,status FROM employees
                WHERE organization_id=? AND level='ceo' AND status='active'
                ORDER BY created_at,id""",
            (objective["organization_id"],),
        ).fetchall()
        if len(active_ceos) == 1:
            employee = active_ceos[0]
    if employee is None or (
        objective is None
        or str(employee["organization_id"]) != str(objective["organization_id"])
        or str(employee["status"]) not in {"proposed", "active", "on_leave"}
    ):
        raise ObjectiveStateError(
            "employee actor is not authorized for objective organization"
        )


def enqueue_objective_event(
    conn: sqlite3.Connection,
    *,
    objective_id: str,
    organization_id: Optional[str] = None,
    event_type: str,
    payload: Any,
    available_at: Optional[int] = None,
    dedupe_key: Optional[str] = None,
) -> str:
    """Append a durable wakeup event, idempotently when ``dedupe_key`` is set."""
    from hermes_cli.objective_event_policy import classify

    objective = get_objective(conn, objective_id)
    if (
        organization_id is not None
        and objective.organization_id != organization_id
    ):
        raise ValueError("objective event organization does not match objective")
    if not event_type.strip():
        raise ValueError("event_type must not be empty")
    event_id = _new_id("evt")
    ts = _now()
    admission = classify(event_type, payload)
    payload_json = _json(payload)
    with conn:
        try:
            conn.execute(
                """
                INSERT INTO objective_inbox (
                    id, objective_id, event_type, payload_json, dedupe_key,
                    priority_class, priority, deadline_at, status, available_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    event_id,
                    objective_id,
                    event_type,
                    payload_json,
                    dedupe_key,
                    admission.priority_class,
                    admission.priority,
                    admission.deadline_at,
                    available_at if available_at is not None else ts,
                    ts,
                    ts,
                ),
            )
        except sqlite3.IntegrityError:
            if dedupe_key is None:
                raise
            row = conn.execute(
                """SELECT id,objective_id,event_type,payload_json,priority_class,
                          priority,deadline_at
                     FROM objective_inbox WHERE dedupe_key = ?""",
                (dedupe_key,),
            ).fetchone()
            if row is None:
                raise
            if (
                str(row["objective_id"]) != objective_id
                or str(row["event_type"]) != event_type
                or str(row["payload_json"]) != payload_json
                or str(row["priority_class"]) != admission.priority_class
                or int(row["priority"]) != admission.priority
                or row["deadline_at"] != admission.deadline_at
            ):
                raise ValueError(
                    "objective event dedupe key was reused with different semantics"
                )
            return str(row["id"])
        _append_event(
            conn,
            objective_id,
            "event_enqueued",
            "event-listener",
            {"event_id": event_id, "event_type": event_type},
        )
    return event_id


def claim_objective_event(
    conn: sqlite3.Connection,
    *,
    runtime_id: str,
    claim_ttl_seconds: int = 300,
    organization_id: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Atomically claim one due event within an optional organization boundary."""
    if claim_ttl_seconds <= 0:
        raise ValueError("claim_ttl_seconds must be positive")
    ts = _now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        organization_clause = (
            "AND objective.organization_id = ?"
            if organization_id is not None
            else ""
        )
        params: list[Any] = [ts]
        if organization_id is not None:
            params.append(organization_id)
        params.extend(TERMINAL_OBJECTIVE_STATUSES)
        params.extend((ts, ts, ts, ts))
        row = conn.execute(
            f"""
            SELECT inbox.* FROM objective_inbox AS inbox
            JOIN objectives AS objective ON objective.id = inbox.objective_id
             WHERE inbox.available_at <= ?
               {organization_clause}
               AND objective.status NOT IN ({','.join('?' for _ in TERMINAL_OBJECTIVE_STATUSES)})
               AND (
                    inbox.status = 'pending'
                    OR (
                        inbox.status = 'processing'
                        AND inbox.claim_expires <= ?
                    )
               )
               AND NOT EXISTS (
                    SELECT 1 FROM objective_inbox active
                     WHERE active.objective_id = inbox.objective_id
                       AND active.status = 'processing'
                       AND active.claim_expires > ?
               )
             ORDER BY
               CASE WHEN inbox.deadline_at IS NOT NULL
                          AND inbox.deadline_at <= ? THEN 1 ELSE 0 END DESC,
               MIN(
                 100,
                 inbox.priority
                   + MIN(80, MAX(0, (? - inbox.created_at) / 3600) * 5)
               ) DESC,
               CASE WHEN inbox.deadline_at IS NULL THEN 1 ELSE 0 END,
               inbox.deadline_at,
               inbox.available_at, inbox.created_at, inbox.id
             LIMIT 1
            """,
            params,
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        updated = conn.execute(
            """
            UPDATE objective_inbox
               SET status = 'processing', claimed_by = ?, claim_expires = ?,
                   attempts = attempts + 1, updated_at = ?
             WHERE id = ?
               AND (
                    status = 'pending'
                    OR (status = 'processing' AND claim_expires <= ?)
               )
            """,
            (runtime_id, ts + claim_ttl_seconds, ts, row["id"], ts),
        )
        if updated.rowcount != 1:
            conn.rollback()
            return None
        claimed = conn.execute(
            "SELECT * FROM objective_inbox WHERE id = ?", (row["id"],)
        ).fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    result = dict(claimed)
    result["payload"] = json.loads(result.pop("payload_json"))
    return result


def renew_objective_event_claim(
    conn: sqlite3.Connection,
    event_id: str,
    *,
    runtime_id: str,
    claim_ttl_seconds: int,
) -> bool:
    """Extend a live claim without allowing a former owner to reclaim it."""
    if claim_ttl_seconds <= 0:
        raise ValueError("claim_ttl_seconds must be positive")
    ts = _now()
    with conn:
        updated = conn.execute(
            """UPDATE objective_inbox
                  SET claim_expires=?,updated_at=?
                WHERE id=? AND status='processing' AND claimed_by=?""",
            (ts + claim_ttl_seconds, ts, event_id, runtime_id),
        )
    return updated.rowcount == 1


class ObjectiveClaimKeeper:
    """Renew a file-backed event claim while planner/provider calls block."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        event_id: str,
        runtime_id: str,
        claim_ttl_seconds: int,
    ):
        if claim_ttl_seconds < 3:
            raise ValueError("claim TTL must be at least three seconds")
        self.event_id = event_id
        self.runtime_id = runtime_id
        self.claim_ttl_seconds = claim_ttl_seconds
        self._lost = threading.Event()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        database = next(
            (
                str(row["file"])
                for row in conn.execute("PRAGMA database_list").fetchall()
                if str(row["name"]) == "main"
            ),
            "",
        )
        self._database_path = Path(database) if database else None

    def __enter__(self) -> "ObjectiveClaimKeeper":
        # Separate SQLite connections cannot address a private in-memory store.
        # Such stores are used only by bounded embedding/tests; file-backed
        # production authority stores always receive active renewal.
        if self._database_path is None:
            return self
        interval = min(30.0, max(1.0, self.claim_ttl_seconds / 3))

        def maintain() -> None:
            try:
                with connect_closing(self._database_path) as renewal_conn:
                    while not self._stop.wait(interval):
                        if not renew_objective_event_claim(
                            renewal_conn,
                            self.event_id,
                            runtime_id=self.runtime_id,
                            claim_ttl_seconds=self.claim_ttl_seconds,
                        ):
                            self._lost.set()
                            return
            except Exception:
                self._lost.set()

        self._thread = threading.Thread(
            target=maintain,
            name=f"objective-claim-{self.event_id}",
            daemon=True,
        )
        self._thread.start()
        return self

    def assert_owned(self) -> None:
        if self._lost.is_set():
            raise ObjectiveStateError(
                "objective event claim renewal failed or ownership was lost"
            )

    def __exit__(self, exc_type, exc, traceback) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=min(5.0, self.claim_ttl_seconds / 2))
        self.assert_owned()


def finish_objective_event(
    conn: sqlite3.Connection,
    event_id: str,
    *,
    runtime_id: str,
    status: str,
    error: Optional[str] = None,
    retry_at: Optional[int] = None,
) -> None:
    if status not in {"completed", "failed", "pending"}:
        raise ValueError("event completion status must be completed, failed, or pending")
    if status == "pending" and retry_at is None:
        raise ValueError("retry_at is required when returning an event to pending")
    with conn:
        updated = conn.execute(
            """
            UPDATE objective_inbox
               SET status = ?, available_at = COALESCE(?, available_at),
                   claimed_by = NULL, claim_expires = NULL, last_error = ?,
                   updated_at = ?
             WHERE id = ? AND status = 'processing' AND claimed_by = ?
            """,
            (status, retry_at, error, _now(), event_id, runtime_id),
        )
        if updated.rowcount != 1:
            raise ObjectiveStateError("event claim is no longer owned by this runtime")
