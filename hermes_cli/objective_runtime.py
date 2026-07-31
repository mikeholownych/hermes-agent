"""Governed event-driven runtime for persistent Charterforge objectives.

This is the narrow operational loop.  Planner, executor, and verifier are
injected adapters so model inference and external systems remain at the edges;
authoritative state transitions and policy enforcement stay deterministic.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from email.utils import parsedate_to_datetime
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol, Sequence

from hermes_cli import (
    approval_artifacts,
    business_audit,
    compliance_db,
    finance_db,
    objective_policy,
    operational_control,
    organization_db,
    resource_budget,
)
from hermes_cli import objectives_db as db


def _rate_limit_retry_after(exc: BaseException) -> int | None:
    """Extract a provider retry hint without coupling to one SDK.

    Providers expose this as an SDK attribute, a ``Retry-After`` header (either
    seconds or an HTTP date), or a rate-limit reset epoch.  Returning ``None``
    means no structured hint was available; callers still apply their normal
    bounded exponential retry policy.
    """
    candidates: list[Any] = []
    for source in (exc, getattr(exc, "response", None)):
        if source is None:
            continue
        value = getattr(source, "retry_after", None)
        if value is not None:
            candidates.append(value)
        headers = getattr(source, "headers", None)
        if headers:
            try:
                normalized = {
                    str(key).lower(): value for key, value in headers.items()
                }
                value = normalized.get("retry-after")
                if value is None:
                    reset = next(
                        (
                            normalized.get(key)
                            for key in (
                                "x-ratelimit-reset",
                                "x-ratelimit-reset-requests",
                                "x-ratelimit-reset-tokens",
                            )
                            if normalized.get(key) is not None
                        ),
                        None,
                    )
                    if reset is not None:
                        candidates.append(max(0, float(reset) - time.time()))
            except AttributeError:
                value = None
            if value is not None:
                candidates.append(value)
    for value in candidates:
        try:
            try:
                seconds = int(float(value))
            except (TypeError, ValueError):
                seconds = max(
                    0,
                    int(
                        parsedate_to_datetime(str(value)).timestamp() - time.time()
                    ),
                )
        except (TypeError, ValueError, OverflowError, OSError):
            continue
        if seconds >= 0:
            return seconds
    message = str(exc).lower()
    if "429" in message or "rate limit" in message or "rate_limit" in message:
        return 0
    return None


@dataclass(frozen=True)
class ActionProposal:
    action_type: str
    payload: Mapping[str, Any]
    expected_outcome: str
    required_capability: str
    verification_method: str
    risk_class: str
    reversible: bool
    rationale: str = ""
    estimated_cost_minor: Optional[int] = None
    compensation: Optional[Mapping[str, Any]] = None


@dataclass(frozen=True)
class PlanProposal:
    assumptions: Any
    tasks: Any
    dependencies: Any
    risks: Any
    actions: Sequence[ActionProposal] = field(default_factory=tuple)
    objective_complete_when_verified: bool = False
    input_tokens: int = 0
    output_tokens: int = 0
    estimated_compute_cost_minor: int = 0
    inference_id: Optional[str] = None


@dataclass(frozen=True)
class ExecutionOutcome:
    status: str
    result: Any
    external_reference: Optional[str] = None
    actual_cost_minor: Optional[int] = None
    preserve_reservation: bool = False


@dataclass(frozen=True)
class VerificationOutcome:
    verdict: str
    evidence: Any


class Planner(Protocol):
    identity: str

    def propose(
        self, snapshot: Mapping[str, Any], event: Mapping[str, Any]
    ) -> PlanProposal: ...


class Executor(Protocol):
    identity: str

    def execute(
        self, action_type: str, payload: Mapping[str, Any]
    ) -> ExecutionOutcome: ...


class Verifier(Protocol):
    identity: str

    def verify(
        self,
        action: ActionProposal,
        execution: ExecutionOutcome,
    ) -> VerificationOutcome: ...

    def verify_objective(
        self,
        snapshot: Mapping[str, Any],
        plan: PlanProposal,
        action_verifications: Sequence[VerificationOutcome],
    ) -> VerificationOutcome: ...


@dataclass(frozen=True)
class CycleOutcome:
    event_id: Optional[str]
    objective_id: Optional[str]
    status: str
    reason: str
    action_ids: tuple[str, ...] = ()


class ObjectiveRuntime:
    def __init__(
        self,
        conn,
        *,
        planner: Planner,
        executor: Executor,
        verifier: Verifier,
        charter: Mapping[str, Any],
        policy_version: str,
        runtime_id: Optional[str] = None,
    ):
        self.conn = conn
        self.planner = planner
        self.executor = executor
        self.verifier = verifier
        planner_identity = str(getattr(planner, "identity", "") or "").strip()
        executor_identity = str(getattr(executor, "identity", "") or "").strip()
        verifier_identity = str(getattr(verifier, "identity", "") or "").strip()
        if not verifier_identity:
            raise ValueError("independent verifier must declare an identity")
        if verifier_identity in {planner_identity, executor_identity}:
            raise ValueError(
                "independent verifier identity must differ from planner and executor"
            )
        self.charter = charter
        self.policy_version = policy_version
        self.runtime_id = runtime_id or f"runtime_{uuid.uuid4().hex}"
        self._claim_keeper: Optional[db.ObjectiveClaimKeeper] = None
        self._authority_bridge = None
        finance_db.ensure_schema(conn)

    def _assert_event_claim(self) -> None:
        if self._claim_keeper is not None:
            self._claim_keeper.assert_owned()

    def tick(self) -> CycleOutcome:
        try:
            operational_control.assert_autonomous(self.conn)
        except operational_control.AutonomyRevokedError as exc:
            return CycleOutcome(None, None, "paused", str(exc))
        ceo = organization_db.active_ceo(self.conn)
        organization_id = (
            str(ceo["organization_id"]) if ceo is not None else "__unscoped__"
        )
        claim_ttl_seconds = max(
            3, int(self.charter.get("event_claim_ttl_seconds", 300) or 300)
        )
        event = db.claim_objective_event(
            self.conn,
            runtime_id=self.runtime_id,
            claim_ttl_seconds=claim_ttl_seconds,
            organization_id=organization_id,
        )
        if event is None:
            return CycleOutcome(None, None, "idle", "no due objective events")

        objective_id = event["objective_id"]
        cycle_id = f"cycle_{uuid.uuid4().hex}"
        started = int(time.time())
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO objective_cycles (
                    id, objective_id, inbox_event_id, runtime_id,
                    status, summary_json, started_at
                ) VALUES (?, ?, ?, ?, 'running', '{}', ?)
                """,
                (cycle_id, objective_id, event["id"], self.runtime_id, started),
            )
        try:
            # Mirror the claim into the Postgres authority store when
            # configured. Deliberately NOT wrapped in a swallow-all except:
            # a bridge failure here must be treated exactly like any other
            # mid-cycle failure — retried with backoff via the except
            # Exception handler below, not silently ignored. A lost claim
            # race (another worker already holds this objective's Postgres
            # claim — the real cross-process/cross-host exclusivity signal)
            # raises explicitly so the SQLite claim is released and retried,
            # rather than proceeding to execute without exclusive authority.
            from hermes_cli.authority_bridge import AuthorityBridge

            if self._authority_bridge is None:
                self._authority_bridge = AuthorityBridge(
                    organization_id=organization_id,
                    worker_id=self.runtime_id,
                )
            if self._authority_bridge.active:
                claim_token = f"{self.runtime_id}:{event['id']}"
                generation = self._authority_bridge.claim(
                    task_id=str(event["objective_id"]),
                    claim_token=claim_token,
                    claim_scope_url=f"urn:objective:{event['objective_id']}",
                    ttl_seconds=claim_ttl_seconds,
                    objective_id=str(event["objective_id"]),
                )
                if generation is None:
                    raise RuntimeError(
                        "lost the Postgres claim race for this objective; "
                        "another worker already holds it"
                    )

            with db.ObjectiveClaimKeeper(
                self.conn,
                event_id=str(event["id"]),
                runtime_id=self.runtime_id,
                claim_ttl_seconds=claim_ttl_seconds,
            ) as keeper:
                self._claim_keeper = keeper
                try:
                    outcome = self._run_claimed_event(event)
                finally:
                    self._claim_keeper = None
        except Exception as exc:
            if self._authority_bridge is not None:
                self._authority_bridge.release()
            retry = self.charter.get("retry_policy") or {}
            max_attempts = int(retry.get("max_attempts", 3))
            base_seconds = int(retry.get("base_backoff_seconds", 15))
            max_seconds = int(retry.get("max_backoff_seconds", 900))
            attempts = int(event.get("attempts") or 1)
            if attempts < max_attempts:
                delay = min(max_seconds, base_seconds * (2 ** (attempts - 1)))
                retry_after = _rate_limit_retry_after(exc)
                if retry_after is not None:
                    delay = min(max_seconds, max(delay, retry_after))
                    error = f"rate_limited: {exc}"
                else:
                    error = str(exc)
                db.finish_objective_event(
                    self.conn,
                    event["id"],
                    runtime_id=self.runtime_id,
                    status="pending",
                    error=error,
                    retry_at=int(time.time()) + delay,
                )
                self._finish_cycle(
                    cycle_id, "retry_scheduled",
                    {
                        "error": error,
                        "delay_seconds": delay,
                        "attempt": attempts,
                        "rate_limited": retry_after is not None,
                    },
                )
                return CycleOutcome(
                    str(event["id"]), objective_id, "retry_scheduled",
                    (
                        f"rate-limited provider; retry in {delay}s"
                        if retry_after is not None
                        else f"transient failure; retry in {delay}s"
                    ),
                )
            db.finish_objective_event(
                self.conn, event["id"], runtime_id=self.runtime_id,
                status="failed", error=str(exc),
            )
            operational_control.raise_intervention(
                self.conn,
                objective_id=objective_id,
                category="retry_exhausted",
                summary="Objective event exhausted its bounded retry policy",
                context={"error": str(exc), "attempts": attempts},
                options=[
                    {"id": "retry", "label": "Authorize another bounded attempt"},
                    {"id": "diagnose", "label": "Investigate provider or integration"},
                    {"id": "abandon", "label": "Abandon objective"},
                ],
            )
            current = db.get_objective(self.conn, objective_id)
            if current.status in {"planned", "authorized", "executing"}:
                db.transition_objective(
                    self.conn, objective_id, "blocked",
                    actor=self.runtime_id, reason="retry policy exhausted",
                )
            self._finish_cycle(cycle_id, "failed", {"error": str(exc)})
            return CycleOutcome(
                str(event["id"]), objective_id, "blocked",
                "bounded retry policy exhausted",
            )
        db.finish_objective_event(
            self.conn,
            event["id"],
            runtime_id=self.runtime_id,
            status="completed",
        )
        # The Postgres claim mirrors the objective_inbox event's own
        # lifecycle: this cycle's claimed work is done, regardless of
        # whether the objective itself reached a terminal state — matching
        # SQLite's own unconditional status="completed" above.
        if self._authority_bridge is not None:
            self._authority_bridge.complete(outcome=outcome.status)
        self._finish_cycle(
            cycle_id,
            outcome.status,
            {
                "reason": outcome.reason,
                "action_ids": list(outcome.action_ids),
            },
        )
        return outcome

    def _finish_cycle(self, cycle_id: str, status: str, summary: Any) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE objective_cycles
                   SET status = ?, summary_json = ?, finished_at = ?
                 WHERE id = ?
                """,
                (status, db._json(summary), int(time.time()), cycle_id),
            )

    def _raise_advisor_handoff(
        self,
        *,
        objective_id: str,
        category: str,
        summary: str,
        context: Mapping[str, Any],
        options: list[Mapping[str, Any]],
        action_id: Optional[str] = None,
    ) -> str:
        """Persist one actionable handoff for a stopped governance branch."""
        return operational_control.raise_intervention(
            self.conn,
            objective_id=objective_id,
            action_id=action_id,
            category=category,
            summary=summary,
            context={
                "runtime_id": self.runtime_id,
                **dict(context),
            },
            options=options,
            dedupe_key=(
                None
                if action_id is not None
                else f"objective:{objective_id}:{category}"
            ),
        )

    def _preserve_business_continuity(
        self,
        *,
        event_id: str,
        objective_id: str,
        action_ids: tuple[str, ...] = (),
    ) -> Optional[CycleOutcome]:
        """Keep the final root alive until a governed peer successor exists."""
        cadence = self.charter.get("operating_cadence") or {}
        if not bool(cadence.get("enabled", True)):
            return None
        from hermes_cli import objective_portfolio

        if not objective_portfolio.final_root_requires_successor(
            self.conn, objective_id
        ):
            return None
        allowed_capabilities = set(
            self.charter.get("allowed_capabilities") or []
        )
        allowed_systems = set(self.charter.get("allowed_systems") or [])
        if (
            "objectives.create" not in allowed_capabilities
            or "objectives" not in allowed_systems
        ):
            self._raise_advisor_handoff(
                objective_id=objective_id,
                category="business_continuity_authority_insufficient",
                summary=(
                    "The final root objective is proven complete, but the charter "
                    "does not authorize a governed successor"
                ),
                context={
                    "required_capability": "objectives.create",
                    "required_system": "objectives",
                    "completed_action_ids": list(action_ids),
                },
                options=[
                    {
                        "id": "grant_bounded_authority",
                        "label": "Add objectives.create and objectives to charter",
                    },
                    {
                        "id": "disable_continuity",
                        "label": "Explicitly disable autonomous operating cadence",
                    },
                    {"id": "abandon", "label": "End autonomous business operation"},
                ],
            )
            if db.get_objective(self.conn, objective_id).status != "blocked":
                db.transition_objective(
                    self.conn,
                    objective_id,
                    "blocked",
                    actor=self.runtime_id,
                    reason="business continuity lacks successor authority",
                )
            return CycleOutcome(
                event_id,
                objective_id,
                "escalated",
                "success proved, but final-root succession lacks standing authority",
                action_ids,
            )
        current = db.get_objective(self.conn, objective_id)
        if current.status == "executing":
            db.transition_objective(
                self.conn,
                objective_id,
                "planned",
                actor=self.runtime_id,
                reason="success proved; governed successor required before closure",
            )
        db.enqueue_objective_event(
            self.conn,
            objective_id=objective_id,
            event_type="objective.successor.required",
            payload={
                "predecessor_objective_id": objective_id,
                "required_action_type": "objectives.create_successor",
                "authority_boundary": {
                    "inherit_system_scope": True,
                    "preserve_prohibitions": True,
                    "budget_may_not_expand": True,
                    "structured_success_verifiers_required": True,
                },
            },
            dedupe_key=f"objective-successor-required:{objective_id}",
        )
        return CycleOutcome(
            event_id,
            objective_id,
            "continuity_required",
            "success proved; final root remains active until a successor is verified",
            action_ids,
        )

    @staticmethod
    def _stored_action(row: Mapping[str, Any]) -> ActionProposal:
        compensation = None
        if row["compensation_action_type"]:
            compensation = {
                "action_type": str(row["compensation_action_type"]),
                "payload": json.loads(row["compensation_payload_json"]),
                "required_capability": str(row["compensation_capability"]),
                "verification_method": str(
                    row["compensation_verification_method"]
                ),
            }
        return ActionProposal(
            action_type=str(row["action_type"]),
            payload=json.loads(row["payload_json"]),
            expected_outcome=str(row["expected_outcome"]),
            required_capability=str(row["required_capability"]),
            verification_method=str(row["verification_method"]),
            risk_class=str(row["risk_class"]),
            reversible=bool(row["reversible"]),
            rationale=str(row["rationale"] or ""),
            estimated_cost_minor=row["estimated_cost_minor"],
            compensation=compensation,
        )

    def _block_in_doubt_execution(
        self,
        *,
        event_id: str,
        objective_id: str,
        action_id: str,
        reason: str,
    ) -> CycleOutcome:
        self._raise_advisor_handoff(
            objective_id=objective_id,
            action_id=action_id,
            category="execution_in_doubt",
            summary="An interrupted external effect cannot be reconciled safely",
            context={"reason": reason},
            options=[
                {
                    "id": "provider_readback",
                    "label": "Supply authoritative provider read-back",
                },
                {"id": "compensate", "label": "Compensate the possible effect"},
                {"id": "abandon", "label": "Abandon the objective"},
            ],
        )
        if db.get_objective(self.conn, objective_id).status in {
            "authorized",
            "executing",
        }:
            db.transition_objective(
                self.conn,
                objective_id,
                "blocked",
                actor=self.runtime_id,
                reason=reason,
            )
        return CycleOutcome(
            event_id,
            objective_id,
            "blocked",
            reason,
            (action_id,),
        )

    def _ensure_recovered_execution_audit(
        self,
        *,
        objective_id: str,
        action: ActionProposal,
        action_id: str,
        permit_id: str,
        result: Mapping[str, Any],
    ) -> None:
        ceo = organization_db.active_ceo(self.conn)
        if ceo is None:
            return
        organization_id = str(ceo["organization_id"])
        result_id = str(result["id"])
        from hermes_cli import __version__

        compliance_db.ensure_schema(self.conn)
        business_audit.ensure_schema(self.conn)
        existing_kya = self.conn.execute(
            """SELECT 1 FROM kya_events
                WHERE organization_id=? AND event_type='action.reconciled'
                  AND action_id=? AND permit_id=?""",
            (organization_id, action_id, permit_id),
        ).fetchone()
        if existing_kya is None:
            compliance_db.append_kya_event(
                self.conn,
                organization_id=organization_id,
                agent_id=self.executor.identity,
                agent_version=__version__,
                event_type="action.reconciled",
                action_id=action_id,
                permit_id=permit_id,
                payload=action.payload,
                evidence={
                    "execution_result_id": result_id,
                    "external_reference": result["external_reference"],
                    "recovered": True,
                },
            )
        existing_audit = self.conn.execute(
            """SELECT 1 FROM business_audit_events
                WHERE organization_id=? AND event_type='execution.reconciled'
                  AND execution_result_id=?""",
            (organization_id, result_id),
        ).fetchone()
        if existing_audit is None:
            business_audit.append(
                self.conn,
                organization_id=organization_id,
                event_type="execution.reconciled",
                objective_id=objective_id,
                plan_id=str(result["plan_id"]),
                action_id=action_id,
                permit_id=permit_id,
                execution_result_id=result_id,
                payload={
                    "action_type": action.action_type,
                    "external_reference": result["external_reference"],
                    "status": result["status"],
                    "recovered": True,
                },
            )

    def _recover_incomplete_execution(
        self, event: Mapping[str, Any]
    ) -> Optional[CycleOutcome]:
        """Advance an interrupted exact action without asking the planner."""
        objective_id = str(event["objective_id"])
        objective_status = db.get_objective(self.conn, objective_id).status
        if objective_status not in {"authorized", "executing"}:
            return None
        action_row = self.conn.execute(
            """SELECT * FROM candidate_actions
                WHERE objective_id=?
                  AND status IN ('permitted','executing','succeeded','failed')
                ORDER BY updated_at DESC,id DESC LIMIT 1""",
            (objective_id,),
        ).fetchone()
        if action_row is None:
            return self._block_in_doubt_execution(
                event_id=str(event["id"]),
                objective_id=objective_id,
                action_id="unknown",
                reason="objective is executing without an attributable action",
            )
        action_id = str(action_row["id"])
        action = self._stored_action(action_row)
        permit = self.conn.execute(
            """SELECT * FROM permits WHERE action_id=?
                ORDER BY issued_at DESC,id DESC LIMIT 1""",
            (action_id,),
        ).fetchone()
        if permit is None:
            return self._block_in_doubt_execution(
                event_id=str(event["id"]),
                objective_id=objective_id,
                action_id=action_id,
                reason="executing action has no consumed exact permit",
            )
        if action_row["status"] == "permitted":
            now = int(time.time())
            if permit["consumed_at"] is not None:
                return self._block_in_doubt_execution(
                    event_id=str(event["id"]),
                    objective_id=objective_id,
                    action_id=action_id,
                    reason="permitted action references an already consumed permit",
                )
            if permit["revoked_at"] is not None or int(permit["expires_at"]) <= now:
                return self._block_in_doubt_execution(
                    event_id=str(event["id"]),
                    objective_id=objective_id,
                    action_id=action_id,
                    reason="interrupted pre-call permit is revoked or expired",
                )
            if objective_status == "authorized":
                db.transition_objective(
                    self.conn,
                    objective_id,
                    "executing",
                    actor=self.runtime_id,
                    reason="resuming an interrupted authorized action",
                )
        elif permit["consumed_at"] is None:
            return self._block_in_doubt_execution(
                event_id=str(event["id"]),
                objective_id=objective_id,
                action_id=action_id,
                reason="executing action has no consumed exact permit",
            )
        result = self.conn.execute(
            """SELECT * FROM execution_results WHERE action_id=?
                ORDER BY finished_at DESC,id DESC LIMIT 1""",
            (action_id,),
        ).fetchone()
        if result is None:
            idempotency_key = str(
                action.payload.get("idempotency_key") or ""
            ).strip()
            if len(idempotency_key) < 16:
                return self._block_in_doubt_execution(
                    event_id=str(event["id"]),
                    objective_id=objective_id,
                    action_id=action_id,
                    reason=(
                        "interrupted action lacks a durable idempotency key; "
                        "automatic replay is prohibited"
                    ),
                )
            resource_key = str(
                action.payload.get("resource_key")
                or action.payload.get("target_resource")
                or f"{action.payload.get('system', '')}:{action.action_type}"
            )
            ttl = max(
                3,
                int(
                    self.charter.get("resource_lease_ttl_seconds", 300)
                    or 300
                ),
            )
            try:
                fence_token = operational_control.acquire_resource_lease(
                    self.conn,
                    resource_key=resource_key,
                    owner=self.runtime_id,
                    action_id=action_id,
                    ttl_seconds=ttl,
                )
            except operational_control.ResourceConflictError as exc:
                return self._block_in_doubt_execution(
                    event_id=str(event["id"]),
                    objective_id=objective_id,
                    action_id=action_id,
                    reason=f"reconciliation resource conflict: {exc}",
                )
            try:
                with operational_control.ResourceLeaseKeeper(
                    self.conn,
                    resource_key=resource_key,
                    owner=self.runtime_id,
                    action_id=action_id,
                    fence_token=fence_token,
                    ttl_seconds=ttl,
                ) as keeper:
                    self._assert_event_claim()
                    keeper.assert_owned()
                    bridge_permit_id = None
                    if permit["consumed_at"] is None:
                        db.consume_permit(
                            self.conn,
                            str(permit["id"]),
                            action_id=action_id,
                            organization_id=db.get_objective(
                                self.conn, objective_id
                            ).organization_id,
                            payload=action.payload,
                            executor=self.executor.identity,
                            current_policy_version=self.policy_version,
                        )
                        permit = self.conn.execute(
                            "SELECT * FROM permits WHERE id=?",
                            (permit["id"],),
                        ).fetchone()
                        # Mirror issuance+consumption for a freshly-consumed
                        # permit only — an already-consumed permit being
                        # resumed here has no bridge-side counterpart this
                        # (possibly fresh) runtime instance can reconstruct,
                        # matching the same "don't repeat consumption" guard
                        # applied to the SQLite side above.
                        bridge_permit_id = self._authority_bridge.issue_permit(
                            actor=self.runtime_id,
                            executor=self.executor.identity,
                            capability=action.required_capability,
                            action_type=action.action_type,
                            target_resource=str(
                                action.payload.get("target_resource") or ""
                            ),
                            action_payload=action.payload,
                            policy_version=self.policy_version,
                        )
                        if bridge_permit_id is not None:
                            self._authority_bridge.consume_permit(
                                permit_id=bridge_permit_id,
                                action_payload=action.payload,
                            )
                    execute_governed = getattr(
                        self.executor, "execute_governed", None
                    )
                    execution = (
                        execute_governed(
                            action_id,
                            objective_id,
                            action.action_type,
                            action.payload,
                        )
                        if callable(execute_governed)
                        else self.executor.execute(
                            action.action_type, action.payload
                        )
                    )
                    self._assert_event_claim()
                    keeper.assert_owned()
            finally:
                operational_control.release_resource_lease(
                    self.conn,
                    resource_key=resource_key,
                    owner=self.runtime_id,
                    action_id=action_id,
                    fence_token=fence_token,
                )
            result_id = db.record_execution_result(
                self.conn,
                action_id=action_id,
                permit_id=str(permit["id"]),
                executor=self.executor.identity,
                organization_id=db.get_objective(
                    self.conn, objective_id
                ).organization_id,
                status=execution.status,
                result=execution.result,
                started_at=int(permit["consumed_at"]),
                external_reference=execution.external_reference,
                actual_cost_minor=execution.actual_cost_minor,
                preserve_reservation=execution.preserve_reservation,
            )
            self._authority_bridge.record_effect(
                effect_key=f"action:{action_id}:{permit['id']}",
                effect_type=action.action_type,
                permit_id=bridge_permit_id or "",
                provider_ref=str(execution.external_reference or ""),
                idempotency_key=f"action:{action_id}",
                payload={"status": execution.status, "result": execution.result},
            )
            db.enqueue_objective_event(
                self.conn,
                objective_id=objective_id,
                event_type="execution.reconciliation.continue",
                payload={"action_id": action_id, "result_id": result_id},
                dedupe_key=f"execution.reconciliation.continue:{result_id}",
            )
            return CycleOutcome(
                str(event["id"]),
                objective_id,
                "reconciliation_pending",
                "exact idempotent action replay produced a durable result",
                (action_id,),
            )

        execution = ExecutionOutcome(
            status=str(result["status"]),
            result=json.loads(result["result_json"]),
            external_reference=(
                str(result["external_reference"])
                if result["external_reference"] is not None
                else None
            ),
            actual_cost_minor=result["actual_cost_minor"],
            preserve_reservation=bool(result["preserve_reservation"]),
        )
        result_with_plan = {
            **dict(result),
            "plan_id": str(action_row["plan_id"]),
        }
        self._ensure_recovered_execution_audit(
            objective_id=objective_id,
            action=action,
            action_id=action_id,
            permit_id=str(permit["id"]),
            result=result_with_plan,
        )
        if execution.status != "succeeded":
            reservation = self.conn.execute(
                """SELECT status FROM budget_reservations
                    WHERE action_id=?""",
                (action_id,),
            ).fetchone()
            if (
                reservation is not None
                and reservation["status"] == "reserved"
                and not execution.preserve_reservation
            ):
                finance_db.release_reservation(
                    self.conn, action_id, reason="recovered_execution_failed"
                )
            self._raise_advisor_handoff(
                objective_id=objective_id,
                action_id=action_id,
                category="execution_failed",
                summary=f"Recovered action {action.action_type} failed",
                context={
                    "execution_result_id": str(result["id"]),
                    "result": execution.result,
                    "recovered": True,
                },
                options=[
                    {"id": "diagnose", "label": "Diagnose the integration"},
                    {"id": "retry", "label": "Authorize a bounded retry"},
                    {"id": "abandon", "label": "Abandon the objective"},
                ],
            )
            db.transition_objective(
                self.conn,
                objective_id,
                "blocked",
                actor=self.runtime_id,
                reason=f"recovered action {action_id} failed",
            )
            return CycleOutcome(
                str(event["id"]),
                objective_id,
                "blocked",
                "recovered execution failed",
                (action_id,),
            )

        reservation = self.conn.execute(
            "SELECT status FROM budget_reservations WHERE action_id=?",
            (action_id,),
        ).fetchone()
        if reservation is not None and reservation["status"] == "reserved":
            actual_cost = int(execution.actual_cost_minor or 0)
            if actual_cost:
                if not execution.external_reference:
                    return self._block_in_doubt_execution(
                        event_id=str(event["id"]),
                        objective_id=objective_id,
                        action_id=action_id,
                        reason=(
                            "recovered paid execution lacks an external "
                            "settlement reference"
                        ),
                    )
                finance_db.settle_reservation(
                    self.conn,
                    action_id=action_id,
                    actual_amount_minor=actual_cost,
                    external_reference=execution.external_reference,
                    evidence={
                        "execution_result_id": str(result["id"]),
                        "recovered": True,
                    },
                )
            else:
                finance_db.release_reservation(
                    self.conn,
                    action_id,
                    reason="recovered_execution_no_actual_cash_spend",
                )

        verification = self.conn.execute(
            """SELECT * FROM verification_records
                WHERE action_id=? AND execution_result_id=?
                ORDER BY created_at DESC,id DESC LIMIT 1""",
            (action_id, result["id"]),
        ).fetchone()
        if verification is None:
            self._assert_event_claim()
            observed = self.verifier.verify(action, execution)
            self._assert_event_claim()
            verification_id = db.record_verification(
                self.conn,
                objective_id=objective_id,
                organization_id=db.get_objective(
                    self.conn, objective_id
                ).organization_id,
                plan_id=str(action_row["plan_id"]),
                action_id=action_id,
                execution_result_id=str(result["id"]),
                verifier=self.verifier.identity,
                method=action.verification_method,
                verdict=observed.verdict,
                evidence=observed.evidence,
            )
            verdict = observed.verdict
        else:
            verification_id = str(verification["id"])
            verdict = str(verification["verdict"])
        if verdict != "pass":
            from hermes_cli import compensation

            obligation_id = compensation.create_for_failed_verification(
                self.conn, action_id=action_id
            )
            self._raise_advisor_handoff(
                objective_id=objective_id,
                action_id=action_id,
                category="action_evidence_insufficient",
                summary=f"Recovered verification returned {verdict}",
                context={
                    "execution_result_id": str(result["id"]),
                    "verification_id": verification_id,
                    "verdict": verdict,
                    "compensation_obligation_id": obligation_id,
                    "recovered": True,
                },
                options=[
                    {"id": "collect_evidence", "label": "Collect stronger evidence"},
                    {"id": "compensate", "label": "Execute compensation"},
                    {"id": "abandon", "label": "Abandon the objective"},
                ],
            )
            db.transition_objective(
                self.conn,
                objective_id,
                "blocked",
                actor=self.runtime_id,
                reason=f"recovered verification {verdict}",
            )
            return CycleOutcome(
                str(event["id"]),
                objective_id,
                "blocked",
                f"recovered verification {verdict}",
                (action_id,),
            )
        from hermes_cli import compensation

        compensation.resolve_for_action(
            self.conn,
            action_id=action_id,
            verification_id=verification_id,
        )
        db.transition_objective(
            self.conn,
            objective_id,
            "planned",
            actor=self.runtime_id,
            reason="interrupted action reconciled and independently verified",
        )
        db.enqueue_objective_event(
            self.conn,
            objective_id=objective_id,
            event_type="cycle.actions_verified",
            payload={
                "plan_id": str(action_row["plan_id"]),
                "action_ids": [action_id],
                "recovered": True,
            },
            dedupe_key=f"recovered.actions_verified:{action_id}",
        )
        return CycleOutcome(
            str(event["id"]),
            objective_id,
            "recovered",
            "interrupted action reconciled and independently verified",
            (action_id,),
        )

    def _run_claimed_event(self, event: Mapping[str, Any]) -> CycleOutcome:
        objective_id = str(event["objective_id"])
        objective = db.get_objective(self.conn, objective_id)
        ceo = organization_db.active_ceo(self.conn)
        if ceo is not None and objective.organization_id != ceo["organization_id"]:
            return CycleOutcome(
                str(event["id"]),
                objective_id,
                "escalated",
                "objective is not bound to the active CEO organization",
            )
        if ceo is None and objective.organization_id != "__unscoped__":
            return CycleOutcome(
                str(event["id"]),
                objective_id,
                "escalated",
                "organization-bound objective has no active CEO",
            )
        now = int(time.time())
        if objective.expires_at is not None and objective.expires_at <= now:
            if objective.status != "expired":
                db.transition_objective(
                    self.conn, objective_id, "expired", actor=self.runtime_id
                )
            return CycleOutcome(
                str(event["id"]), objective_id, "expired", "objective expired"
            )
        reaffirmation_ttl = int(
            self.charter.get("reaffirmation_ttl_seconds", 0) or 0
        )
        if (
            reaffirmation_ttl > 0
            and objective.status in {"accepted", "planned", "authorized", "executing"}
            and int(objective.reaffirmed_at) + reaffirmation_ttl <= now
        ):
            reason = "objective intent reaffirmation is stale"
            db.transition_objective(
                self.conn,
                objective_id,
                "blocked",
                actor=self.runtime_id,
                reason=reason,
            )
            self._raise_advisor_handoff(
                objective_id=objective_id,
                category="objective_reaffirmation_required",
                summary="Objective intent is stale and requires explicit reaffirmation",
                context={
                    "reaffirmed_at": objective.reaffirmed_at,
                    "reaffirmation_ttl_seconds": reaffirmation_ttl,
                    "stale_since": int(objective.reaffirmed_at) + reaffirmation_ttl,
                },
                options=[
                    {"id": "reaffirm", "label": "Reaffirm the objective and resume planning"},
                    {"id": "abandon", "label": "Abandon the stale objective"},
                ],
            )
            return CycleOutcome(
                str(event["id"]), objective_id, "escalated", reason
            )
        if objective.status in {
            "closed",
            "cancelled",
            "expired",
            "abandoned",
            "superseded",
        }:
            return CycleOutcome(
                str(event["id"]),
                objective_id,
                "ignored",
                f"objective is terminal: {objective.status}",
            )
        if objective.status == "proposed":
            # The Founder/CEO owns objective creation. A CEO-originated
            # objective inside the active organization's authority scope does
            # not require the advisor to dispatch the next routine turn; the
            # lifecycle transition itself is durable acceptance evidence.
            try:
                ceo = organization_db.active_ceo(self.conn)
            except sqlite3.OperationalError:
                # Unscoped/unit-test stores may not carry organization
                # authority tables; they must retain the external acceptance
                # handoff rather than turning a missing table into a retry.
                ceo = None
            ceo_identity = (
                f"employee:{ceo['id']}" if ceo is not None else ""
            )
            if (
                objective.originator == ceo_identity
                and ceo is not None
                and objective.organization_id == str(ceo["organization_id"])
            ):
                db.transition_objective(
                    self.conn,
                    objective_id,
                    "accepted",
                    actor=self.runtime_id,
                    reason=(
                        "Founder/CEO-originated objective accepted under standing "
                        "organizational authority"
                    ),
                )
            else:
                self._raise_advisor_handoff(
                    objective_id=objective_id,
                    category="objective_acceptance_required",
                    summary="Objective is proposed but has not been accepted into the operating portfolio",
                    context={
                        "objective_status": objective.status,
                        "originator": objective.originator,
                        "desired_outcome": objective.desired_outcome,
                        "event_type": str(event["event_type"]),
                    },
                    options=[
                        {"id": "accept", "label": "Accept objective into the operating portfolio"},
                        {"id": "abandon", "label": "Abandon the proposed objective"},
                    ],
                )
                return CycleOutcome(
                    str(event["id"]),
                    objective_id,
                    "escalated",
                    "objective has not been accepted",
                )
        recovered = self._recover_incomplete_execution(event)
        if recovered is not None:
            return recovered

        snapshot = db.objective_snapshot(self.conn, objective_id)
        limits = {
            **resource_budget.DEFAULT_LIMITS,
            **(self.charter.get("resource_limits") or {}),
        }
        try:
            resource_budget.assert_admissible(
                self.conn, objective_id, limits
            )
        except resource_budget.ResourceBudgetError as exc:
            operational_control.raise_intervention(
                self.conn,
                objective_id=objective_id,
                category="resource_budget_exhausted",
                summary=str(exc),
                context={"usage": resource_budget.usage(self.conn, objective_id)},
                options=[
                    {"id": "increase_budget", "label": "Increase bounded ceiling"},
                    {"id": "abandon", "label": "Abandon objective"},
                ],
            )
            if objective.status != "blocked":
                db.transition_objective(
                    self.conn, objective_id, "blocked",
                    actor=self.runtime_id, reason=str(exc),
                )
            return CycleOutcome(
                str(event["id"]), objective_id, "escalated", str(exc)
            )
        max_actions = int(limits.get("max_actions_per_cycle", 0))
        approval_artifact_id: Optional[str] = None
        approved_action_id: Optional[str] = None
        existing_approved_permit_id: Optional[str] = None
        if str(event["event_type"]) == "approval.granted":
            approval_artifact_id = str(
                event["payload"].get("approval_artifact_id") or ""
            )
            approved_action_id = str(event["payload"].get("action_id") or "")
            row = self.conn.execute(
                """SELECT a.*,p.assumptions_json,p.tasks_json,p.dependencies_json,
                          p.risks_json
                     FROM candidate_actions a JOIN plans p ON p.id=a.plan_id
                    WHERE a.id=? AND a.objective_id=?""",
                (approved_action_id, objective_id),
            ).fetchone()
            if (
                row is None
                or row["status"] not in {"proposed", "permitted"}
                or not approval_artifact_id
            ):
                raise ValueError(
                    "approved action event does not reference a live proposal"
                )
            if row["status"] == "permitted":
                permit = self.conn.execute(
                    """SELECT id FROM permits
                       WHERE action_id=? AND approval_artifact_id=?
                         AND consumed_at IS NULL AND revoked_at IS NULL
                         AND expires_at>?
                       ORDER BY issued_at DESC,id DESC LIMIT 1""",
                    (approved_action_id, approval_artifact_id, now),
                ).fetchone()
                if permit is None:
                    raise ValueError(
                        "approved action permit is no longer executable"
                    )
                existing_approved_permit_id = str(permit["id"])
            compensation = None
            if row["compensation_action_type"]:
                compensation = {
                    "action_type": str(row["compensation_action_type"]),
                    "payload": json.loads(row["compensation_payload_json"]),
                    "required_capability": str(row["compensation_capability"]),
                    "verification_method": str(
                        row["compensation_verification_method"]
                    ),
                }
            action = ActionProposal(
                action_type=str(row["action_type"]),
                payload=json.loads(row["payload_json"]),
                expected_outcome=str(row["expected_outcome"]),
                required_capability=str(row["required_capability"]),
                verification_method=str(row["verification_method"]),
                risk_class=str(row["risk_class"]),
                reversible=bool(row["reversible"]),
                rationale=str(row["rationale"] or ""),
                estimated_cost_minor=row["estimated_cost_minor"],
                compensation=compensation,
            )
            proposal = PlanProposal(
                assumptions=json.loads(row["assumptions_json"]),
                tasks=json.loads(row["tasks_json"]),
                dependencies=json.loads(row["dependencies_json"]),
                risks=json.loads(row["risks_json"]),
                actions=(action,),
            )
            plan_id = str(row["plan_id"])
        else:
            proposal = self.planner.propose(snapshot, event)
            if bool(
                getattr(self.planner, "accounts_resources_pre_call", False)
            ):
                resource_budget.assert_projected_admissible(
                    self.conn,
                    objective_id,
                    limits,
                    actions=len(proposal.actions),
                )
                resource_budget.record_actions(
                    self.conn,
                    objective_id=objective_id,
                    actions=len(proposal.actions),
                )
            else:
                resource_budget.record_cycle(
                    self.conn,
                    objective_id=objective_id,
                    actions=len(proposal.actions),
                    input_tokens=proposal.input_tokens,
                    output_tokens=proposal.output_tokens,
                    estimated_compute_cost_minor=proposal.estimated_compute_cost_minor,
                )
            plan_id = db.create_plan(
                self.conn,
                objective_id,
                assumptions=proposal.assumptions,
                tasks=proposal.tasks,
                dependencies=proposal.dependencies,
                risks=proposal.risks,
                created_by=self.planner.identity,
                inference_id=proposal.inference_id,
            )
        ceo_for_audit = organization_db.active_ceo(self.conn)
        if ceo_for_audit is not None and approved_action_id is None:
            business_audit.append(
                self.conn,
                organization_id=ceo_for_audit["organization_id"],
                event_type="plan.proposed",
                objective_id=objective_id,
                plan_id=plan_id,
                payload={
                    "planner": self.planner.identity,
                    "event_id": event["id"],
                    "assumptions": proposal.assumptions,
                    "risks": proposal.risks,
                    "inference_id": proposal.inference_id,
                },
            )
        current = db.get_objective(self.conn, objective_id)
        if current.status in {"accepted", "executing", "blocked"}:
            db.transition_objective(
                self.conn,
                objective_id,
                "planned",
                actor=self.runtime_id,
                reason=f"plan {plan_id}",
            )
        if max_actions <= 0 or len(proposal.actions) > max_actions:
            reason = (
                f"planner proposed {len(proposal.actions)} actions; "
                f"cycle permits at most {max_actions}"
            )
            operational_control.raise_intervention(
                self.conn,
                objective_id=objective_id,
                category="planner_contract_violation",
                summary="Planner attempted multiple effects from one observation",
                context={
                    "plan_id": plan_id,
                    "proposed_actions": len(proposal.actions),
                    "max_actions_per_cycle": max_actions,
                    "tasks_preserved": proposal.tasks,
                },
                options=[
                    {"id": "replan", "label": "Replan one next effect"},
                    {"id": "abandon", "label": "Abandon objective"},
                ],
            )
            if db.get_objective(self.conn, objective_id).status == "planned":
                db.transition_objective(
                    self.conn, objective_id, "blocked",
                    actor=self.runtime_id, reason=reason,
                )
            return CycleOutcome(
                str(event["id"]), objective_id, "escalated", reason
            )

        validate_proposal = getattr(self.executor, "validate_proposal", None)
        if callable(validate_proposal):
            try:
                for action in proposal.actions:
                    validate_proposal(action)
            except (TypeError, ValueError) as exc:
                reason = f"executor authority contract rejected proposal: {exc}"
                operational_control.raise_intervention(
                    self.conn,
                    objective_id=objective_id,
                    category="planner_contract_violation",
                    summary=reason,
                    context={"plan_id": plan_id},
                    options=[
                        {"id": "replan", "label": "Replan within registered contract"},
                        {"id": "abandon", "label": "Abandon objective"},
                    ],
                )
                if db.get_objective(self.conn, objective_id).status == "planned":
                    db.transition_objective(
                        self.conn, objective_id, "blocked",
                        actor=self.runtime_id, reason=reason,
                    )
                return CycleOutcome(
                    str(event["id"]), objective_id, "escalated", reason
                )

        if not proposal.actions and proposal.objective_complete_when_verified:
            objective_verification = self.verifier.verify_objective(
                db.objective_snapshot(self.conn, objective_id),
                proposal,
                [],
            )
            verification_id = db.record_verification(
                self.conn,
                objective_id=objective_id,
                organization_id=db.get_objective(
                    self.conn, objective_id
                ).organization_id,
                plan_id=plan_id,
                verifier=self.verifier.identity,
                method="objective success criteria",
                verdict=objective_verification.verdict,
                evidence=objective_verification.evidence,
            )
            if objective_verification.verdict == "pass":
                continuity = self._preserve_business_continuity(
                    event_id=str(event["id"]),
                    objective_id=objective_id,
                )
                if continuity is not None:
                    return continuity
                db.transition_objective(
                    self.conn, objective_id, "completed", actor=self.runtime_id
                )
                db.transition_objective(
                    self.conn, objective_id, "verified", actor=self.runtime_id
                )
                return CycleOutcome(
                    str(event["id"]),
                    objective_id,
                    "verified",
                    "existing external state satisfies objective success criteria",
                )
            self._raise_advisor_handoff(
                objective_id=objective_id,
                category="objective_evidence_insufficient",
                summary="Available evidence does not prove the objective complete",
                context={
                    "plan_id": plan_id,
                    "verification_id": verification_id,
                    "verdict": objective_verification.verdict,
                },
                options=[
                    {"id": "collect_evidence", "label": "Collect stronger evidence"},
                    {"id": "replan", "label": "Replan toward the success criteria"},
                    {"id": "abandon", "label": "Abandon the objective"},
                ],
            )
            db.transition_objective(
                self.conn,
                objective_id,
                "blocked",
                actor=self.runtime_id,
                reason=f"objective verification {objective_verification.verdict}",
            )
            return CycleOutcome(
                str(event["id"]),
                objective_id,
                "blocked",
                f"objective verification {objective_verification.verdict}",
            )

        if not proposal.actions:
            self._raise_advisor_handoff(
                objective_id=objective_id,
                category="no_admissible_action",
                summary="Planner found no admissible next action",
                context={"plan_id": plan_id, "tasks": proposal.tasks},
                options=[
                    {"id": "advise", "label": "Provide bounded advisory guidance"},
                    {"id": "expand_authority", "label": "Change the standing charter"},
                    {"id": "abandon", "label": "Abandon the objective"},
                ],
            )
            db.transition_objective(
                self.conn,
                objective_id,
                "blocked",
                actor=self.runtime_id,
                reason="planner proposed no admissible next action",
            )
            return CycleOutcome(
                str(event["id"]),
                objective_id,
                "escalated",
                "planner proposed no next action",
            )

        action_ids: list[str] = []
        proposals_by_id: dict[str, ActionProposal] = {}
        permits: dict[str, str] = {}
        action_verifications: list[VerificationOutcome] = []
        for action in proposal.actions:
            action_id = (
                approved_action_id
                if approved_action_id is not None
                else db.propose_action(
                    self.conn,
                    objective_id=objective_id,
                    plan_id=plan_id,
                    action_type=action.action_type,
                    payload=action.payload,
                    expected_outcome=action.expected_outcome,
                    required_capability=action.required_capability,
                    verification_method=action.verification_method,
                    risk_class=action.risk_class,
                    reversible=action.reversible,
                    proposed_by=self.planner.identity,
                    rationale=action.rationale,
                    estimated_cost_minor=action.estimated_cost_minor,
                    compensation=action.compensation,
                )
            )
            action_ids.append(action_id)
            proposals_by_id[action_id] = action
            if existing_approved_permit_id is not None:
                decision = objective_policy.PolicyDecision(
                    "permit", "resuming existing exact approved permit"
                )
                permit_id = existing_approved_permit_id
            else:
                try:
                    decision, permit_id = objective_policy.evaluate_and_record(
                        self.conn,
                        action_id,
                        charter=self.charter,
                        executor=self.executor.identity,
                        policy_version=self.policy_version,
                        approval_artifact_id=approval_artifact_id,
                    )
                except approval_artifacts.ApprovalArtifactError as exc:
                    self._raise_advisor_handoff(
                        objective_id=objective_id,
                        action_id=action_id,
                        category="approval_artifact_invalid",
                        summary=str(exc),
                        context={
                            "approval_artifact_id": approval_artifact_id,
                            "policy_version": self.policy_version,
                        },
                        options=[
                            {"id": "replan", "label": "Replan from current state"},
                            {
                                "id": "change_charter",
                                "label": "Review standing authority",
                            },
                            {"id": "abandon", "label": "Abandon objective"},
                        ],
                    )
                    if db.get_objective(self.conn, objective_id).status != "blocked":
                        db.transition_objective(
                            self.conn,
                            objective_id,
                            "blocked",
                            actor=self.runtime_id,
                            reason=str(exc),
                        )
                    return CycleOutcome(
                        str(event["id"]),
                        objective_id,
                        "blocked",
                        str(exc),
                        tuple(action_ids),
                    )
            if decision.verdict != "permit" or permit_id is None:
                self._raise_advisor_handoff(
                    objective_id=objective_id,
                    action_id=action_id,
                    category=(
                        "authority_insufficient"
                        if decision.verdict == "escalate"
                        else "action_denied"
                    ),
                    summary=decision.reason,
                    context={
                        "plan_id": plan_id,
                        "action_type": action.action_type,
                        "required_capability": action.required_capability,
                        "policy_version": self.policy_version,
                        "policy_verdict": decision.verdict,
                        "approval_eligible": decision.approval_eligible,
                        "payload_sha256": db.payload_sha256(action.payload),
                        "target_system": action.payload.get("system"),
                        "target_resource": action.payload.get("target_resource"),
                        "estimated_cost_minor": action.estimated_cost_minor or 0,
                        "risk_class": action.risk_class,
                        "state_evidence": action.payload.get("state_evidence"),
                    },
                    options=(
                        (
                            [
                                {
                                    "id": "approve_exact_action",
                                    "label": "Approve this exact action once",
                                },
                                {
                                    "id": "change_charter",
                                    "label": "Change the standing charter",
                                },
                                {"id": "replan", "label": "Replan within authority"},
                            ]
                            if decision.approval_eligible
                            else [
                                {
                                    "id": "change_charter",
                                    "label": "Change the standing charter",
                                },
                                {"id": "replan", "label": "Replan within authority"},
                            ]
                        )
                        if decision.verdict == "escalate"
                        else [
                            {"id": "replan", "label": "Replan within policy"},
                            {"id": "revise_objective", "label": "Revise the objective"},
                            {"id": "abandon", "label": "Abandon the objective"},
                        ]
                    ),
                )
                if db.get_objective(self.conn, objective_id).status == "planned":
                    db.transition_objective(
                        self.conn,
                        objective_id,
                        "blocked",
                        actor=self.runtime_id,
                        reason=decision.reason,
                    )
                return CycleOutcome(
                    str(event["id"]),
                    objective_id,
                    "escalated" if decision.verdict == "escalate" else "denied",
                    decision.reason,
                    tuple(action_ids),
                )
            permits[action_id] = permit_id

        db.transition_objective(
            self.conn, objective_id, "authorized", actor=self.runtime_id
        )
        db.transition_objective(
            self.conn, objective_id, "executing", actor=self.runtime_id
        )

        for action_id in action_ids:
            self._assert_event_claim()
            action = proposals_by_id[action_id]
            permit_id = permits[action_id]
            started = int(time.time())
            resource_key = str(
                action.payload.get("resource_key")
                or action.payload.get("target_resource")
                or f"{action.payload.get('system', '')}:{action.action_type}"
            )
            resource_lease_ttl = max(
                3,
                int(self.charter.get("resource_lease_ttl_seconds", 300) or 300),
            )
            try:
                fence_token = operational_control.acquire_resource_lease(
                    self.conn,
                    resource_key=resource_key,
                    owner=self.runtime_id,
                    action_id=action_id,
                    ttl_seconds=resource_lease_ttl,
                )
            except operational_control.ResourceConflictError as exc:
                operational_control.raise_intervention(
                    self.conn,
                    objective_id=objective_id,
                    action_id=action_id,
                    category="resource_conflict",
                    summary=str(exc),
                    context={"resource_key": resource_key},
                    options=[{"id": "retry", "label": "Retry after lease expiry"}],
                )
                db.transition_objective(
                    self.conn, objective_id, "blocked",
                    actor=self.runtime_id, reason=str(exc),
                )
                return CycleOutcome(
                    str(event["id"]), objective_id, "blocked", str(exc),
                    tuple(action_ids),
                )
            try:
                with operational_control.ResourceLeaseKeeper(
                    self.conn,
                    resource_key=resource_key,
                    owner=self.runtime_id,
                    action_id=action_id,
                    fence_token=fence_token,
                    ttl_seconds=resource_lease_ttl,
                ) as resource_keeper:
                    operational_control.assert_resource_lease(
                        self.conn,
                        resource_key=resource_key,
                        owner=self.runtime_id,
                        action_id=action_id,
                        fence_token=fence_token,
                    )
                    db.consume_permit(
                        self.conn,
                        permit_id,
                        action_id=action_id,
                        organization_id=db.get_objective(
                            self.conn, objective_id
                        ).organization_id,
                        payload=action.payload,
                        executor=self.executor.identity,
                        current_policy_version=self.policy_version,
                    )
                    bridge_permit_id = self._authority_bridge.issue_permit(
                        actor=self.runtime_id,
                        executor=self.executor.identity,
                        capability=action.required_capability,
                        action_type=action.action_type,
                        target_resource=str(
                            action.payload.get("target_resource") or ""
                        ),
                        action_payload=action.payload,
                        policy_version=self.policy_version,
                    )
                    if bridge_permit_id is not None:
                        self._authority_bridge.consume_permit(
                            permit_id=bridge_permit_id,
                            action_payload=action.payload,
                        )
                    execute_governed = getattr(
                        self.executor, "execute_governed", None
                    )
                    self._assert_event_claim()
                    resource_keeper.assert_owned()
                    execution = (
                        execute_governed(
                            action_id,
                            objective_id,
                            action.action_type,
                            action.payload,
                        )
                        if callable(execute_governed)
                        else self.executor.execute(
                            action.action_type, action.payload
                        )
                    )
                    self._assert_event_claim()
                    resource_keeper.assert_owned()
                    operational_control.assert_resource_lease(
                        self.conn,
                        resource_key=resource_key,
                        owner=self.runtime_id,
                        action_id=action_id,
                        fence_token=fence_token,
                    )
            finally:
                operational_control.release_resource_lease(
                    self.conn,
                    resource_key=resource_key,
                    owner=self.runtime_id,
                    action_id=action_id,
                    fence_token=fence_token,
                )
            result_id = db.record_execution_result(
                self.conn,
                action_id=action_id,
                permit_id=permit_id,
                executor=self.executor.identity,
                organization_id=db.get_objective(
                    self.conn, objective_id
                ).organization_id,
                status=execution.status,
                result=execution.result,
                started_at=started,
                external_reference=execution.external_reference,
                actual_cost_minor=execution.actual_cost_minor,
                preserve_reservation=execution.preserve_reservation,
            )
            self._authority_bridge.record_effect(
                effect_key=f"action:{action_id}:{permit_id}",
                effect_type=action.action_type,
                permit_id=bridge_permit_id or "",
                provider_ref=str(execution.external_reference or ""),
                idempotency_key=f"action:{action_id}",
                payload={"status": execution.status, "result": execution.result},
            )
            ceo = organization_db.active_ceo(self.conn)
            if ceo is not None:
                from hermes_cli import __version__

                compliance_db.append_kya_event(
                    self.conn,
                    organization_id=ceo["organization_id"],
                    agent_id=self.executor.identity,
                    agent_version=__version__,
                    event_type=f"action.{execution.status}",
                    action_id=action_id,
                    permit_id=permit_id,
                    payload=action.payload,
                    evidence={
                        "execution_result_id": result_id,
                        "external_reference": execution.external_reference,
                    },
                )
                business_audit.append(
                    self.conn,
                    organization_id=ceo["organization_id"],
                    event_type=f"execution.{execution.status}",
                    objective_id=objective_id,
                    plan_id=plan_id,
                    action_id=action_id,
                    permit_id=permit_id,
                    execution_result_id=result_id,
                    payload={
                        "action_type": action.action_type,
                        "external_reference": execution.external_reference,
                        "result": execution.result,
                    },
                )
            if execution.status != "succeeded":
                self._raise_advisor_handoff(
                    objective_id=objective_id,
                    action_id=action_id,
                    category="execution_failed",
                    summary=f"Governed action {action.action_type} failed",
                    context={
                        "plan_id": plan_id,
                        "execution_result_id": result_id,
                        "result": execution.result,
                        "external_reference": execution.external_reference,
                    },
                    options=[
                        {"id": "diagnose", "label": "Diagnose the integration"},
                        {"id": "retry", "label": "Authorize a bounded retry"},
                        {"id": "abandon", "label": "Abandon the objective"},
                    ],
                )
                reservation = self.conn.execute(
                    "SELECT status FROM budget_reservations WHERE action_id = ?",
                    (action_id,),
                ).fetchone()
                if (
                    reservation is not None
                    and reservation["status"] == "reserved"
                    and not execution.preserve_reservation
                ):
                    finance_db.release_reservation(
                        self.conn, action_id, reason="execution_failed"
                    )
                db.transition_objective(
                    self.conn,
                    objective_id,
                    "blocked",
                    actor=self.runtime_id,
                    reason=f"action {action_id} failed",
                )
                db.enqueue_objective_event(
                    self.conn,
                    objective_id=objective_id,
                    event_type="execution.failed",
                    payload={"action_id": action_id, "result_id": result_id},
                    dedupe_key=f"execution.failed:{result_id}",
                )
                return CycleOutcome(
                    str(event["id"]),
                    objective_id,
                    "blocked",
                    f"action {action_id} failed",
                    tuple(action_ids),
                )
            reservation = self.conn.execute(
                "SELECT status FROM budget_reservations WHERE action_id = ?",
                (action_id,),
            ).fetchone()
            if reservation is not None and reservation["status"] == "reserved":
                actual_cost = int(execution.actual_cost_minor or 0)
                if actual_cost:
                    if not execution.external_reference:
                        raise finance_db.BudgetError(
                            "paid execution requires an external settlement reference"
                        )
                    finance_db.settle_reservation(
                        self.conn,
                        action_id=action_id,
                        actual_amount_minor=actual_cost,
                        external_reference=execution.external_reference,
                        evidence={"execution_result_id": result_id},
                    )
                else:
                    finance_db.release_reservation(
                        self.conn, action_id, reason="no_actual_cash_spend"
                    )
            self._assert_event_claim()
            verification = self.verifier.verify(action, execution)
            self._assert_event_claim()
            action_verifications.append(verification)
            verification_id = db.record_verification(
                self.conn,
                objective_id=objective_id,
                organization_id=db.get_objective(
                    self.conn, objective_id
                ).organization_id,
                plan_id=plan_id,
                action_id=action_id,
                execution_result_id=result_id,
                verifier=self.verifier.identity,
                method=action.verification_method,
                verdict=verification.verdict,
                evidence=verification.evidence,
            )
            if verification.verdict != "pass":
                from hermes_cli import compensation

                obligation_id = compensation.create_for_failed_verification(
                    self.conn, action_id=action_id
                )
                self._raise_advisor_handoff(
                    objective_id=objective_id,
                    action_id=action_id,
                    category="action_evidence_insufficient",
                    summary=(
                        f"Independent verification returned "
                        f"{verification.verdict}"
                    ),
                    context={
                        "plan_id": plan_id,
                        "execution_result_id": result_id,
                        "verification_id": verification_id,
                        "verdict": verification.verdict,
                        "compensation_obligation_id": obligation_id,
                    },
                    options=[
                        {"id": "collect_evidence", "label": "Collect stronger evidence"},
                        {
                            "id": "compensate",
                            "label": "Execute the recorded compensation",
                        },
                        {"id": "abandon", "label": "Abandon the objective"},
                    ],
                )
                db.transition_objective(
                    self.conn,
                    objective_id,
                    "blocked",
                    actor=self.runtime_id,
                    reason=f"verification {verification.verdict} for {action_id}",
                )
                if obligation_id is not None:
                    db.enqueue_objective_event(
                        self.conn,
                        objective_id=objective_id,
                        event_type="compensation.required",
                        payload={
                            "compensation_obligation_id": obligation_id,
                            "original_action_id": action_id,
                            "failed_verification_id": verification_id,
                        },
                        dedupe_key=f"compensation.required:{obligation_id}",
                    )
                return CycleOutcome(
                    str(event["id"]),
                    objective_id,
                    "blocked",
                    f"verification {verification.verdict}",
                    tuple(action_ids),
                )
            from hermes_cli import compensation

            compensation.resolve_for_action(
                self.conn,
                action_id=action_id,
                verification_id=verification_id,
            )

        if not proposal.objective_complete_when_verified:
            db.transition_objective(
                self.conn,
                objective_id,
                "planned",
                actor=self.runtime_id,
                reason="verified actions advanced the plan; objective remains active",
            )
            db.enqueue_objective_event(
                self.conn,
                objective_id=objective_id,
                event_type="cycle.actions_verified",
                payload={"plan_id": plan_id, "action_ids": action_ids},
                dedupe_key=f"cycle.actions_verified:{plan_id}",
            )
            return CycleOutcome(
                str(event["id"]),
                objective_id,
                "progressed",
                "actions verified; objective remains active and will replan",
                tuple(action_ids),
            )

        objective_verification = self.verifier.verify_objective(
            db.objective_snapshot(self.conn, objective_id),
            proposal,
            action_verifications,
        )
        objective_verification_id = db.record_verification(
            self.conn,
            objective_id=objective_id,
            organization_id=db.get_objective(
                self.conn, objective_id
            ).organization_id,
            plan_id=plan_id,
            verifier=self.verifier.identity,
            method="objective success criteria",
            verdict=objective_verification.verdict,
            evidence=objective_verification.evidence,
        )
        if objective_verification.verdict != "pass":
            self._raise_advisor_handoff(
                objective_id=objective_id,
                category="objective_evidence_insufficient",
                summary="Executed actions did not prove the objective complete",
                context={
                    "plan_id": plan_id,
                    "verification_id": objective_verification_id,
                    "verdict": objective_verification.verdict,
                    "action_ids": action_ids,
                },
                options=[
                    {"id": "collect_evidence", "label": "Collect stronger evidence"},
                    {"id": "replan", "label": "Continue toward success criteria"},
                    {"id": "abandon", "label": "Abandon the objective"},
                ],
            )
            db.transition_objective(
                self.conn,
                objective_id,
                "blocked",
                actor=self.runtime_id,
                reason=f"objective verification {objective_verification.verdict}",
            )
            return CycleOutcome(
                str(event["id"]),
                objective_id,
                "blocked",
                f"objective verification {objective_verification.verdict}",
                tuple(action_ids),
            )

        continuity = self._preserve_business_continuity(
            event_id=str(event["id"]),
            objective_id=objective_id,
            action_ids=tuple(action_ids),
        )
        if continuity is not None:
            return continuity
        db.transition_objective(self.conn, objective_id, "completed", actor=self.runtime_id)
        db.transition_objective(self.conn, objective_id, "verified", actor=self.runtime_id)
        return CycleOutcome(
            str(event["id"]),
            objective_id,
            "verified",
            "all permitted actions passed independent verification",
            tuple(action_ids),
        )
