"""Crash recovery, autonomy revocation, intervention, and resource leases."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Mapping, Optional


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS autonomy_control (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    mode TEXT NOT NULL, generation INTEGER NOT NULL,
    reason TEXT, changed_by TEXT NOT NULL, changed_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS resource_leases (
    resource_key TEXT PRIMARY KEY, owner TEXT NOT NULL,
    action_id TEXT NOT NULL, acquired_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL, fence_token INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS intervention_queue (
    id TEXT PRIMARY KEY, organization_id TEXT NOT NULL DEFAULT '__unscoped__',
    objective_id TEXT, action_id TEXT,
    category TEXT NOT NULL, summary TEXT NOT NULL,
    context_json TEXT NOT NULL, options_json TEXT NOT NULL,
    status TEXT NOT NULL, created_at INTEGER NOT NULL,
    resolved_at INTEGER, resolved_by TEXT, resolution_json TEXT,
    dedupe_key TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_intervention_open_action_category
    ON intervention_queue(action_id, category) WHERE status='open' AND action_id IS NOT NULL;
"""


class AutonomyRevokedError(PermissionError):
    pass


class ResourceConflictError(RuntimeError):
    pass


def ensure_schema(conn: sqlite3.Connection) -> None:
    # ``executescript`` commits an active SQLite transaction.  Runtime
    # authority checks are invoked at the last boundary before side effects,
    # so schema reads must not release a caller's in-flight state transition.
    if conn.in_transaction and conn.execute(
        "SELECT 1 FROM sqlite_master "
        "WHERE type='table' AND name='autonomy_control'"
    ).fetchone():
        return
    conn.executescript(SCHEMA_SQL)
    columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(intervention_queue)")
    }
    if "organization_id" not in columns:
        conn.execute(
            "ALTER TABLE intervention_queue ADD COLUMN "
            "organization_id TEXT NOT NULL DEFAULT '__unscoped__'"
        )
    if "dedupe_key" not in columns:
        conn.execute("ALTER TABLE intervention_queue ADD COLUMN dedupe_key TEXT")
    conn.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_intervention_open_dedupe
           ON intervention_queue(dedupe_key)
           WHERE status='open' AND dedupe_key IS NOT NULL"""
    )
    lease_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(resource_leases)")
    }
    if "fence_token" not in lease_columns:
        conn.execute(
            "ALTER TABLE resource_leases ADD COLUMN "
            "fence_token INTEGER NOT NULL DEFAULT 1"
        )
    with conn:
        bootstrap_cursor = conn.execute(
            """INSERT OR IGNORE INTO autonomy_control
               (singleton, mode, generation, changed_by, changed_at)
               VALUES (1, 'autonomous', 1, 'system:bootstrap', ?)""",
            (int(time.time()),),
        )
    if bootstrap_cursor.rowcount == 1:
        # This is the first-ever call to reach this INSERT for this store
        # (every later call is a no-op via OR IGNORE) — establish the
        # Postgres-side baseline once here, so
        # postgres_authority.consume_permit's autonomy gate has a row to
        # check against from the start, rather than only after the first
        # explicit set_autonomy_mode() call. Gating on rowcount avoids a
        # Postgres round-trip on every one of ensure_schema()'s many
        # call sites for an already-bootstrapped store.
        _mirror_autonomy_mode_to_postgres(conn, mode="autonomous", generation=1)


def autonomy_state(conn: sqlite3.Connection) -> dict[str, Any]:
    ensure_schema(conn)
    return dict(conn.execute("SELECT * FROM autonomy_control WHERE singleton=1").fetchone())


def set_autonomy_mode(
    conn: sqlite3.Connection,
    *,
    mode: str,
    actor: str,
    reason: str,
) -> int:
    """Change runtime mode and revoke all live leases by generation change."""
    if mode not in {"autonomous", "paused", "manual"}:
        raise ValueError("autonomy mode must be autonomous, paused, or manual")
    ensure_schema(conn)
    if mode == "autonomous":
        integrity_table = conn.execute(
            """SELECT 1 FROM sqlite_master
               WHERE type='table' AND name='authority_integrity_runs'"""
        ).fetchone()
        if integrity_table is not None:
            latest = conn.execute(
                """SELECT status FROM authority_integrity_runs
                   ORDER BY checked_at DESC,id DESC LIMIT 1"""
            ).fetchone()
            if latest is not None and latest["status"] != "ready":
                raise AutonomyRevokedError(
                    "autonomy cannot resume until authority integrity passes"
                )
        intervention_table = conn.execute(
            """SELECT 1 FROM sqlite_master
               WHERE type='table' AND name='intervention_queue'"""
        ).fetchone()
        if intervention_table is not None:
            blocker = conn.execute(
                """SELECT category FROM intervention_queue
                   WHERE status='open' AND category IN (
                     'authority_integrity_failure',
                     'post_restore_reconciliation'
                   ) ORDER BY created_at,id LIMIT 1"""
            ).fetchone()
            if blocker is not None:
                raise AutonomyRevokedError(
                    "autonomy cannot resume while recovery control is unresolved: "
                    + str(blocker["category"])
                )
    now = int(time.time())
    with conn:
        conn.execute(
            """UPDATE autonomy_control SET mode=?, generation=generation+1,
               reason=?, changed_by=?, changed_at=? WHERE singleton=1""",
            (mode, reason, actor, now),
        )
        if mode != "autonomous":
            conn.execute("DELETE FROM resource_leases")
            conn.execute(
                """UPDATE permits SET revoked_at=?
                   WHERE consumed_at IS NULL AND revoked_at IS NULL""",
                (now,),
            )
    if mode != "autonomous":
        from hermes_cli import approval_artifacts

        approval_artifacts.revoke_all_unexecuted(
            conn,
            actor=actor,
            reason=f"autonomy changed to {mode}: {reason}",
        )
        grant_table = conn.execute(
            """SELECT 1 FROM sqlite_master
               WHERE type='table' AND name='employee_task_grants'"""
        ).fetchone()
        if grant_table is not None:
            from hermes_cli import workforce_delegation

            organizations = conn.execute(
                "SELECT DISTINCT organization_id FROM employee_task_grants"
            ).fetchall()
            for organization in organizations:
                workforce_delegation.revoke_active_grants(
                    conn,
                    organization_id=str(organization["organization_id"]),
                    actor=actor,
                    reason=f"autonomy changed to {mode}: {reason}",
                )
    new_generation = int(autonomy_state(conn)["generation"])
    _mirror_autonomy_mode_to_postgres(conn, mode=mode, generation=new_generation)
    return new_generation


def _mirror_autonomy_mode_to_postgres(
    conn: sqlite3.Connection, *, mode: str, generation: int
) -> None:
    """Push the just-committed master-autonomy-stop mode into the Postgres
    mirror (pg_autonomy_control) for every known organization, whenever
    Postgres coordination is configured.

    autonomy_control is a whole-store singleton in SQLite — master-
    autonomy-stop is not per-organization, so every organization sharing
    this store must observe the same mode/generation transition. This is a
    synchronous push after the SQLite commit above; on failure it logs and
    re-raises WITHOUT rolling back the already-committed SQLite transition,
    matching objectives_db._mirror_objective_status_to_postgres.
    """
    import os

    if not (os.environ.get("AUTHORITY_POSTGRES_URL") or os.environ.get("DATABASE_URL")):
        return

    try:
        org_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='organizations'"
        ).fetchone()
        org_ids = (
            [str(row["id"]) for row in conn.execute("SELECT id FROM organizations")]
            if org_table is not None
            else []
        )
    except sqlite3.OperationalError:
        org_ids = []
    # The organizations table can exist (created as a side effect of an
    # unrelated lookup, e.g. organization_db.active_ceo's ensure_schema
    # call) while genuinely having zero rows yet — that is NOT the same as
    # "no organizations table at all" and must still mirror for the
    # unscoped/default organization, or the very first bootstrap push (and
    # any store that never creates an explicit organization) would silently
    # never reach Postgres.
    if not org_ids:
        org_ids = ["__unscoped__"]

    import logging

    from hermes_cli.postgres_authority import connect as pg_connect
    from hermes_cli.postgres_authority import init_schema, mirror_autonomy_mode

    logger = logging.getLogger(__name__)
    pg_conn = pg_connect()
    try:
        init_schema(pg_conn)
        for org_id in org_ids:
            try:
                mirror_autonomy_mode(
                    pg_conn, organization_id=org_id, mode=mode, generation=generation
                )
            except Exception:
                logger.exception(
                    "Postgres autonomy-mode mirror push failed for org=%s "
                    "mode=%s generation=%s; SQLite autonomy transition "
                    "already committed and will NOT be rolled back.",
                    org_id,
                    mode,
                    generation,
                )
                raise
    finally:
        pg_conn.close()


def assert_autonomous(conn: sqlite3.Connection) -> None:
    state = autonomy_state(conn)
    if state["mode"] != "autonomous":
        raise AutonomyRevokedError(
            f"autonomous execution is {state['mode']}: {state['reason'] or 'no reason'}"
        )


def acquire_resource_lease(
    conn: sqlite3.Connection,
    *,
    resource_key: str,
    owner: str,
    action_id: str,
    ttl_seconds: int = 300,
) -> int:
    if not resource_key or ttl_seconds <= 0:
        raise ValueError("resource key and positive lease TTL are required")
    assert_autonomous(conn)
    now = int(time.time())
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM resource_leases WHERE resource_key=?", (resource_key,)
        ).fetchone()
        if row is not None and int(row["expires_at"]) > now:
            if row["owner"] == owner and row["action_id"] == action_id:
                conn.commit()
                return int(row["fence_token"])
            raise ResourceConflictError(
                f"resource {resource_key!r} is leased by {row['owner']}"
            )
        fence_token = int(row["fence_token"]) + 1 if row is not None else 1
        conn.execute(
            """INSERT INTO resource_leases
               (resource_key, owner, action_id, acquired_at, expires_at, fence_token)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(resource_key) DO UPDATE SET owner=excluded.owner,
                 action_id=excluded.action_id, acquired_at=excluded.acquired_at,
                 expires_at=excluded.expires_at, fence_token=excluded.fence_token""",
            (resource_key, owner, action_id, now, now + ttl_seconds, fence_token),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return fence_token


def assert_resource_lease(
    conn: sqlite3.Connection,
    *,
    resource_key: str,
    owner: str,
    action_id: str,
    fence_token: int,
) -> None:
    """Reject a stale worker after expiry, takeover, or generation change."""
    assert_autonomous(conn)
    row = conn.execute(
        "SELECT * FROM resource_leases WHERE resource_key=?", (resource_key,)
    ).fetchone()
    now = int(time.time())
    if (
        row is None
        or row["owner"] != owner
        or row["action_id"] != action_id
        or int(row["fence_token"]) != fence_token
        or int(row["expires_at"]) <= now
    ):
        raise ResourceConflictError(
            f"resource lease for {resource_key!r} is stale or no longer owned"
        )


def renew_resource_lease(
    conn: sqlite3.Connection,
    *,
    resource_key: str,
    owner: str,
    action_id: str,
    fence_token: int,
    ttl_seconds: int,
) -> bool:
    """Renew only the exact current fence; never revive a displaced owner."""
    if ttl_seconds <= 0:
        raise ValueError("resource lease TTL must be positive")
    state = autonomy_state(conn)
    if state["mode"] != "autonomous":
        return False
    now = int(time.time())
    with conn:
        updated = conn.execute(
            """UPDATE resource_leases SET expires_at=?
                WHERE resource_key=? AND owner=? AND action_id=?
                  AND fence_token=?""",
            (
                now + ttl_seconds,
                resource_key,
                owner,
                action_id,
                fence_token,
            ),
        )
    return updated.rowcount == 1


class ResourceLeaseKeeper:
    """Maintain an exact resource fence across blocking provider operations."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        resource_key: str,
        owner: str,
        action_id: str,
        fence_token: int,
        ttl_seconds: int,
    ):
        if ttl_seconds < 3:
            raise ValueError("resource lease TTL must be at least three seconds")
        self.resource_key = resource_key
        self.owner = owner
        self.action_id = action_id
        self.fence_token = fence_token
        self.ttl_seconds = ttl_seconds
        self._stop = threading.Event()
        self._lost = threading.Event()
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

    def __enter__(self) -> "ResourceLeaseKeeper":
        if self._database_path is None:
            return self
        interval = min(30.0, max(1.0, self.ttl_seconds / 3))

        def maintain() -> None:
            from hermes_cli import objectives_db

            try:
                with objectives_db.connect_closing(self._database_path) as lease_conn:
                    while not self._stop.wait(interval):
                        if not renew_resource_lease(
                            lease_conn,
                            resource_key=self.resource_key,
                            owner=self.owner,
                            action_id=self.action_id,
                            fence_token=self.fence_token,
                            ttl_seconds=self.ttl_seconds,
                        ):
                            self._lost.set()
                            return
            except Exception:
                self._lost.set()

        self._thread = threading.Thread(
            target=maintain,
            name=f"resource-lease-{self.action_id}",
            daemon=True,
        )
        self._thread.start()
        return self

    def assert_owned(self) -> None:
        if self._lost.is_set():
            raise ResourceConflictError(
                f"resource lease for {self.resource_key!r} was lost during execution"
            )

    def __exit__(self, exc_type, exc, traceback) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=min(5.0, self.ttl_seconds / 2))
        self.assert_owned()


def release_resource_lease(
    conn: sqlite3.Connection,
    *,
    resource_key: str,
    owner: str,
    action_id: str,
    fence_token: int,
) -> None:
    with conn:
        conn.execute(
            """DELETE FROM resource_leases
               WHERE resource_key=? AND owner=? AND action_id=? AND fence_token=?""",
            (resource_key, owner, action_id, fence_token),
        )


def raise_intervention(
    conn: sqlite3.Connection,
    *,
    category: str,
    summary: str,
    context: Mapping[str, Any],
    options: list[Mapping[str, Any]],
    objective_id: Optional[str] = None,
    action_id: Optional[str] = None,
    organization_id: Optional[str] = None,
    dedupe_key: Optional[str] = None,
) -> str:
    """Create a bounded advisor handoff containing evidence and explicit choices."""
    ensure_schema(conn)
    if organization_id is None and objective_id is not None:
        objective = conn.execute(
            "SELECT organization_id FROM objectives WHERE id=?", (objective_id,)
        ).fetchone()
        if objective is not None:
            organization_id = str(objective["organization_id"])
    organization_id = organization_id or "__unscoped__"
    if action_id is not None:
        existing = conn.execute(
            """SELECT id,organization_id FROM intervention_queue
               WHERE action_id=? AND category=? AND status='open'""",
            (action_id, category),
        ).fetchone()
        if existing is not None:
            if str(existing["organization_id"]) != organization_id:
                raise PermissionError(
                    "action intervention belongs to another organization"
                )
            return str(existing["id"])
    if dedupe_key is not None:
        existing = conn.execute(
            """SELECT id,organization_id,category FROM intervention_queue
               WHERE dedupe_key=? AND status='open'""",
            (dedupe_key,),
        ).fetchone()
        if existing is not None:
            if str(existing["organization_id"]) != organization_id:
                raise PermissionError(
                    "intervention dedupe key belongs to another organization"
                )
            if str(existing["category"]) != category:
                raise ValueError(
                    "intervention dedupe key was reused for another category"
                )
            return str(existing["id"])
    item_id = f"intervention_{uuid.uuid4().hex}"
    with conn:
        conn.execute(
            """INSERT INTO intervention_queue
               (id, organization_id, objective_id, action_id, category, summary,
                context_json, options_json, status, created_at, dedupe_key)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)""",
            (
                item_id, organization_id, objective_id, action_id, category, summary,
                json.dumps(context, sort_keys=True),
                json.dumps(options, sort_keys=True),
                int(time.time()), dedupe_key,
            ),
        )
    return item_id


def list_interventions(
    conn: sqlite3.Connection,
    *,
    status: str = "open",
    organization_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    ensure_schema(conn)
    if status not in {"open", "resolved", "dismissed"}:
        raise ValueError("invalid intervention status")
    result = []
    query = "SELECT * FROM intervention_queue WHERE status=?"
    params: list[Any] = [status]
    if organization_id is not None:
        query += " AND organization_id=?"
        params.append(organization_id)
    query += " ORDER BY created_at,id"
    for row in conn.execute(query, params).fetchall():
        item = dict(row)
        item["context"] = json.loads(item.pop("context_json"))
        item["options"] = json.loads(item.pop("options_json"))
        if item.get("resolution_json"):
            item["resolution"] = json.loads(item.pop("resolution_json"))
        else:
            item.pop("resolution_json", None)
        result.append(item)
    return result


def resolve_intervention(
    conn: sqlite3.Connection,
    intervention_id: str,
    *,
    option_id: str,
    actor: str,
    evidence: Mapping[str, Any],
    organization_id: Optional[str] = None,
) -> None:
    """Resolve a recorded choice and durably wake or terminate its objective."""
    ensure_schema(conn)
    query = "SELECT * FROM intervention_queue WHERE id=?"
    params: list[Any] = [intervention_id]
    if organization_id is not None:
        query += " AND organization_id=?"
        params.append(organization_id)
    row = conn.execute(query, params).fetchone()
    if row is None or row["status"] != "open":
        raise ValueError("intervention is not open")
    if organization_id is None and str(row["organization_id"]) != "__unscoped__":
        raise PermissionError(
            "organization_id is required to resolve an organization-scoped intervention"
        )
    actor_identity = str(actor or "").strip().lower()
    if not actor_identity:
        raise PermissionError("intervention resolution requires an actor")
    if not (
        actor_identity.startswith("human:")
        or actor_identity in {"owner", "advisor", "approver"}
    ):
        raise PermissionError(
            "only an explicitly identified human advisor may resolve an intervention"
        )
    allowed = {str(item.get("id")) for item in json.loads(row["options_json"])}
    if option_id not in allowed:
        raise ValueError("resolution is not one of the intervention's recorded options")
    if not evidence:
        raise ValueError("intervention resolution requires evidence")
    context = json.loads(row["context_json"])
    emitted_objective_event = False
    if row["category"] == "outbound_spend_hold_unreconciled":
        from hermes_cli import payment_controls

        action_id = str(context.get("action_id") or row["action_id"] or "")
        if not action_id:
            raise ValueError("spend-hold intervention has no action identity")
        if option_id == "provider_readback":
            payment_intent_id = str(evidence.get("payment_intent_id") or "")
            if not payment_intent_id:
                raise ValueError(
                    "provider read-back resolution requires payment_intent_id"
                )
            intent = conn.execute(
                """SELECT id,action_id,direction,status FROM payment_intents
                    WHERE id=?""",
                (payment_intent_id,),
            ).fetchone()
            if (
                intent is None
                or str(intent["action_id"] or "") != action_id
                or str(intent["direction"]) != "outgoing"
                or str(intent["status"]) != "succeeded"
            ):
                raise ValueError(
                    "provider read-back must reference a succeeded outgoing payment"
                )
            readback = conn.execute(
                """SELECT 1 FROM payment_provider_readbacks
                    WHERE payment_intent_id=? AND status='succeeded'
                    ORDER BY observed_at DESC,id DESC LIMIT 1""",
                (payment_intent_id,),
            ).fetchone()
            if readback is None:
                raise ValueError(
                    "provider read-back requires a durable succeeded read-back record"
                )
            if not payment_controls.settle_spend_hold(conn, action_id):
                raise ValueError("spend hold is no longer reserved")
        elif option_id == "release":
            provider_status = str(evidence.get("provider_status") or "")
            settlement_reference = str(
                evidence.get("settlement_reference") or ""
            ).strip()
            if provider_status not in {"failed", "cancelled"}:
                raise ValueError(
                    "spend-hold release requires failed or cancelled provider status"
                )
            if not settlement_reference:
                raise ValueError(
                    "spend-hold release requires settlement_reference evidence"
                )
            if not payment_controls.release_spend_hold(
                conn,
                action_id,
                reason=f"advisor:{provider_status}:{settlement_reference}",
            ):
                raise ValueError("spend hold is no longer reserved")
    if row["category"] == "external_content_quarantine":
        content_id = str(context.get("content_id") or "")
        if option_id == "release_as_data":
            from hermes_cli import external_content, objectives_db

            external_content.approve_quarantined_content(
                conn,
                content_id,
                organization_id=str(row["organization_id"]),
                reviewer=actor,
                evidence=evidence,
            )
            envelope = external_content.context_envelope(conn, content_id)
            objectives_db.enqueue_objective_event(
                conn,
                objective_id=str(row["objective_id"]),
                event_type="external.content.reviewed",
                payload={
                    "review_evidence": dict(evidence),
                    "external_content": envelope,
                },
                dedupe_key=f"external-content-release:{content_id}",
            )
            emitted_objective_event = True
    if (
        row["category"] == "objective_acceptance_required"
        and option_id == "accept"
    ):
        from hermes_cli import objectives_db

        objective_id = str(row["objective_id"] or "")
        if not objective_id:
            raise ValueError("objective acceptance intervention has no objective")
        objective = objectives_db.get_objective(conn, objective_id)
        if objective.status != "proposed":
            raise ValueError(
                "objective acceptance requires the objective to remain proposed"
            )
        objectives_db.transition_objective(
            conn,
            objective_id,
            "accepted",
            actor=actor,
            reason=f"advisor accepted objective via intervention {intervention_id}",
        )
        objectives_db.enqueue_objective_event(
            conn,
            objective_id=objective_id,
            event_type="objective.accepted",
            payload={
                "intervention_id": intervention_id,
                "advisor": actor,
                "evidence": dict(evidence),
            },
            dedupe_key=f"objective-accepted:{objective_id}:{intervention_id}",
        )
        emitted_objective_event = True
    if (
        row["category"] == "objective_reaffirmation_required"
        and option_id == "reaffirm"
    ):
        from hermes_cli import objectives_db

        objective_id = str(row["objective_id"] or "")
        decision_basis = str(
            evidence.get("reason") or evidence.get("decision_basis") or ""
        ).strip()
        if len(decision_basis) < 8:
            raise ValueError(
                "objective reaffirmation requires a substantive decision basis"
            )
        objective = objectives_db.reaffirm_objective(
            conn,
            objective_id,
            actor=actor,
            reason="; ".join(
                str(value) for value in evidence.values()
                if isinstance(value, (str, int, float))
            ) or "advisor reaffirmed standing intent",
        )
        if objective.status == "blocked":
            objectives_db.transition_objective(
                conn,
                objective_id,
                "planned",
                actor=actor,
                reason=f"advisor reaffirmed objective via intervention {intervention_id}",
            )
        objectives_db.enqueue_objective_event(
            conn,
            objective_id=objective_id,
            event_type="objective.reaffirmed",
            payload={
                "intervention_id": intervention_id,
                "advisor": actor,
                "evidence": dict(evidence),
            },
            dedupe_key=f"objective-reaffirmed:{objective_id}:{intervention_id}",
        )
        emitted_objective_event = True
    if (
        row["category"] == "authority_insufficient"
        and option_id == "approve_exact_action"
    ):
        from hermes_cli import approval_artifacts, objectives_db
        from hermes_cli.config import load_config

        approval_config = (
            (load_config().get("agentic") or {}).get("action_approvals") or {}
        )

        artifact_id = approval_artifacts.issue_for_intervention(
            conn,
            intervention_id=intervention_id,
            organization_id=str(row["organization_id"]),
            actor=actor,
            evidence=evidence,
            default_ttl_seconds=max(
                1, int(approval_config.get("default_ttl_seconds", 900))
            ),
            maximum_ttl_seconds=max(
                1, int(approval_config.get("maximum_ttl_seconds", 3600))
            ),
        )
        objectives_db.enqueue_objective_event(
            conn,
            objective_id=str(row["objective_id"]),
            event_type="approval.granted",
            payload={
                "approval_artifact_id": artifact_id,
                "action_id": str(row["action_id"]),
                "intervention_id": intervention_id,
            },
            dedupe_key=f"approval-granted:{artifact_id}",
        )
        emitted_objective_event = True
    objective_id = str(row["objective_id"] or "")
    if objective_id and option_id == "abandon":
        from hermes_cli import objectives_db

        objective = objectives_db.get_objective(conn, objective_id)
        if objective.status == "blocked":
            objectives_db.transition_objective(
                conn,
                objective_id,
                "abandoned",
                actor=actor,
                reason=f"advisor resolved intervention {intervention_id}",
            )
    elif objective_id and not emitted_objective_event:
        from hermes_cli import objectives_db

        objectives_db.enqueue_objective_event(
            conn,
            objective_id=objective_id,
            event_type="intervention.resolved",
            payload={
                "intervention_id": intervention_id,
                "category": str(row["category"]),
                "option_id": option_id,
                "advisor": actor,
                "evidence": dict(evidence),
                "prior_context": context,
            },
            dedupe_key=f"intervention-resolution:{intervention_id}",
        )
    with conn:
        conn.execute(
            """UPDATE intervention_queue SET status='resolved', resolved_at=?,
               resolved_by=?, resolution_json=? WHERE id=? AND status='open'""",
            (
                int(time.time()), actor,
                json.dumps(
                    {"option_id": option_id, "evidence": evidence},
                    sort_keys=True,
                ),
                intervention_id,
            ),
        )


def recover_incomplete_executions(conn: sqlite3.Connection) -> list[str]:
    """Escalate uncertain effects after a crash; never blindly replay them."""
    ensure_schema(conn)
    rows = conn.execute(
        """SELECT a.objective_id, a.id action_id, a.action_type, p.id permit_id
           FROM candidate_actions a JOIN permits p ON p.action_id=a.id
          WHERE a.status='executing' AND p.consumed_at IS NOT NULL
            AND NOT EXISTS (
              SELECT 1 FROM execution_results r WHERE r.action_id=a.id
            )"""
    ).fetchall()
    created = []
    for row in rows:
        created.append(
            raise_intervention(
                conn,
                objective_id=row["objective_id"],
                action_id=row["action_id"],
                category="uncertain_external_effect",
                summary="Execution crashed after permit consumption; reconcile externally",
                context=dict(row),
                options=[
                    {"id": "reconcile", "label": "Read provider state"},
                    {"id": "compensate", "label": "Run approved compensation"},
                    {"id": "abandon", "label": "Stop objective"},
                ],
            )
        )
    return created
