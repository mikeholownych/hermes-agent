"""Operator CLI for Charterforge's governed objective authority service."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from hermes_cli import objectives_db as db


def _json_value(raw: str, *, label: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} must be valid JSON: {exc}") from exc


def _print(value: Any, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True))
        return
    if isinstance(value, list):
        for item in value:
            print(f"{item['id']}  {item['status']:10s}  {item['desired_outcome']}")
        if not value:
            print("No objectives.")
        return
    print(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True))


def build_parser(parent_subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = parent_subparsers.add_parser(
        "objectives",
        aliases=["objective"],
        help="Govern durable objectives, plans, actions, permits, and evidence",
        description=(
            "Model-free authority state for persistent agentic operation. "
            "Models propose; this state machine authorizes, records, and verifies."
        ),
    )
    parser.add_argument(
        "--db",
        type=Path,
        help="Override the profile-aware objectives.db path (testing/embedding)",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    sub = parser.add_subparsers(dest="objective_command", required=True)

    create = sub.add_parser("create", help="Create a proposed objective")
    create.add_argument("outcome")
    create.add_argument("--originator", required=True)
    create.add_argument("--owner")
    create.add_argument("--success-criteria", default="[]", metavar="JSON")
    create.add_argument("--constraints", default="[]", metavar="JSON")
    create.add_argument("--authority-scope", default="{}", metavar="JSON")
    create.add_argument("--permitted-systems", default="[]", metavar="JSON")
    create.add_argument("--prohibited-actions", default="[]", metavar="JSON")
    create.add_argument("--termination-conditions", default="[]", metavar="JSON")
    create.add_argument("--expires-at", type=int, help="Unix timestamp")
    create.add_argument("--max-spend-minor", type=int)
    create.add_argument("--currency")

    ls = sub.add_parser("list", aliases=["ls"], help="List objectives")
    ls.add_argument("--status", choices=sorted(db.OBJECTIVE_STATUSES))
    ls.add_argument("--all", action="store_true", help="Include terminal objectives")
    ls.add_argument(
        "--organization-id",
        help="Filter to a single organization (default: all/legacy-unscoped)",
    )

    show = sub.add_parser("show", help="Show objective and governance history")
    show.add_argument("objective_id")

    transition = sub.add_parser("transition", help="Commit an admissible lifecycle transition")
    transition.add_argument("objective_id")
    transition.add_argument("status", choices=sorted(db.OBJECTIVE_STATUSES))
    transition.add_argument("--actor", required=True)
    transition.add_argument("--reason", default="")

    plan = sub.add_parser("plan", help="Append an immutable plan version")
    plan.add_argument("objective_id")
    plan.add_argument("--created-by", required=True)
    plan.add_argument("--assumptions", default="[]", metavar="JSON")
    plan.add_argument("--tasks", required=True, metavar="JSON")
    plan.add_argument("--dependencies", default="[]", metavar="JSON")
    plan.add_argument("--risks", default="[]", metavar="JSON")

    propose = sub.add_parser("propose-action", help="Record a candidate action")
    propose.add_argument("objective_id")
    propose.add_argument("plan_id")
    propose.add_argument("action_type")
    propose.add_argument("--payload", required=True, metavar="JSON")
    propose.add_argument("--expected-outcome", required=True)
    propose.add_argument("--capability", required=True)
    propose.add_argument("--verification-method", required=True)
    propose.add_argument("--risk-class", required=True)
    propose.add_argument("--proposed-by", required=True)
    propose.add_argument("--rationale", default="")
    propose.add_argument("--reversible", action="store_true")
    propose.add_argument("--estimated-cost-minor", type=int)
    propose.add_argument("--compensation", default="{}", metavar="JSON")

    permit = sub.add_parser("permit", help="Issue an exact, expiring action permit")
    permit.add_argument("action_id")
    permit.add_argument("--capability", required=True)
    permit.add_argument("--issued-to", required=True)
    permit.add_argument("--policy-version", required=True)
    permit.add_argument("--ttl-seconds", type=int, required=True)
    permit.add_argument("--target-resource")
    permit.add_argument("--approval-artifact-id")
    permit.add_argument("--constraints", default="{}", metavar="JSON")

    deny = sub.add_parser("deny", help="Deny a proposed action")
    deny.add_argument("action_id")
    deny.add_argument("--actor", required=True)
    deny.add_argument("--reason", required=True)

    evaluate = sub.add_parser(
        "evaluate-action",
        help="Evaluate against the setup charter and auto-issue a permit when admissible",
    )
    evaluate.add_argument("action_id")
    evaluate.add_argument("--executor", required=True)

    consume = sub.add_parser("consume-permit", help="Atomically consume a permit")
    consume.add_argument("permit_id")
    consume.add_argument("action_id")
    consume.add_argument("--payload", required=True, metavar="JSON")
    consume.add_argument("--executor", required=True)

    result = sub.add_parser("record-result", help="Record an executor result")
    result.add_argument("action_id")
    result.add_argument("permit_id")
    result.add_argument("--executor", required=True)
    result.add_argument("--status", required=True, choices=["succeeded", "failed"])
    result.add_argument("--result", required=True, metavar="JSON")
    result.add_argument("--started-at", type=int, required=True)
    result.add_argument("--external-reference")

    verify = sub.add_parser("verify", help="Record independent verification evidence")
    verify.add_argument("objective_id")
    verify.add_argument("plan_id")
    verify.add_argument("--verifier", required=True)
    verify.add_argument("--method", required=True)
    verify.add_argument("--verdict", required=True, choices=["pass", "fail", "inconclusive"])
    verify.add_argument("--evidence", required=True, metavar="JSON", help="Observed facts")
    verify.add_argument(
        "--source-kind",
        required=True,
        choices=[
            "provider_readback",
            "authoritative_database_readback",
            "deterministic_check",
        ],
    )
    verify.add_argument("--source-reference", required=True)
    verify.add_argument("--observed-at", type=int)
    verify.add_argument("--action-id")
    verify.add_argument("--execution-result-id")

    worker = sub.add_parser(
        "worker", help="Run the persistent governed objective worker"
    )
    worker.add_argument("--once", action="store_true")
    worker.add_argument("--interval-seconds", type=float, default=15)
    sub.add_parser("worker-status", help="Show worker heartbeat and health evidence")

    subscribe = sub.add_parser(
        "subscribe", help="Route a typed external event source to an objective"
    )
    subscribe.add_argument("objective_id")
    subscribe.add_argument("--source-type", required=True)
    subscribe.add_argument("--event-type", required=True)

    ingest = sub.add_parser(
        "ingest-event", help="Ingest an authenticated adapter event"
    )
    ingest.add_argument("--source-type", required=True)
    ingest.add_argument("--event-type", required=True)
    ingest.add_argument("--source-reference", required=True)
    ingest.add_argument("--payload", required=True, metavar="JSON")
    ingest.add_argument("--authentication", required=True, metavar="JSON")
    ingest.add_argument("--content")

    schedule = sub.add_parser(
        "schedule", help="Create a durable repeating objective trigger"
    )
    schedule.add_argument("objective_id")
    schedule.add_argument("--event-type", required=True)
    schedule.add_argument("--interval-seconds", type=int, required=True)
    schedule.add_argument("--next-fire-at", type=int, required=True)
    schedule.add_argument("--payload", default="{}", metavar="JSON")

    sub.add_parser("triggers", help="List external subscriptions and schedules")
    trigger_status = sub.add_parser(
        "trigger-status", help="Enable or disable a subscription or schedule"
    )
    trigger_status.add_argument("trigger_id")
    trigger_status.add_argument("status", choices=["active", "disabled"])

    parser.set_defaults(func=objectives_command)
    return parser


def objectives_command(args: argparse.Namespace) -> int:
    with db.connect_closing(getattr(args, "db", None)) as conn:
        command = args.objective_command
        if command == "create":
            objective = db.create_objective(
                conn,
                desired_outcome=args.outcome,
                originator=args.originator,
                owner=args.owner,
                constraints=_json_value(args.constraints, label="constraints"),
                authority_scope=_json_value(args.authority_scope, label="authority scope"),
                success_criteria=_json_value(
                    args.success_criteria, label="success criteria"
                ),
                termination_conditions=_json_value(
                    args.termination_conditions, label="termination conditions"
                ),
                permitted_systems=_json_value(
                    args.permitted_systems, label="permitted systems"
                ),
                prohibited_actions=_json_value(
                    args.prohibited_actions, label="prohibited actions"
                ),
                max_spend_minor=args.max_spend_minor,
                currency=args.currency,
                expires_at=args.expires_at,
            )
            _print(db.objective_to_dict(conn, objective.id), as_json=args.json)
        elif command in {"list", "ls"}:
            _print(
                db.list_objectives(
                    conn,
                    status=args.status,
                    include_terminal=args.all,
                    organization_id=args.organization_id,
                ),
                as_json=args.json,
            )
        elif command == "show":
            _print(db.objective_snapshot(conn, args.objective_id), as_json=True)
        elif command == "transition":
            objective = db.transition_objective(
                conn,
                args.objective_id,
                args.status,
                actor=args.actor,
                reason=args.reason,
            )
            _print(db.objective_to_dict(conn, objective.id), as_json=args.json)
        elif command == "plan":
            plan_id = db.create_plan(
                conn,
                args.objective_id,
                assumptions=_json_value(args.assumptions, label="assumptions"),
                tasks=_json_value(args.tasks, label="tasks"),
                dependencies=_json_value(args.dependencies, label="dependencies"),
                risks=_json_value(args.risks, label="risks"),
                created_by=args.created_by,
            )
            _print({"plan_id": plan_id}, as_json=True)
        elif command == "propose-action":
            action_id = db.propose_action(
                conn,
                objective_id=args.objective_id,
                plan_id=args.plan_id,
                action_type=args.action_type,
                payload=_json_value(args.payload, label="payload"),
                expected_outcome=args.expected_outcome,
                required_capability=args.capability,
                verification_method=args.verification_method,
                risk_class=args.risk_class,
                reversible=args.reversible,
                proposed_by=args.proposed_by,
                rationale=args.rationale,
                estimated_cost_minor=args.estimated_cost_minor,
                compensation=_json_value(
                    args.compensation, label="compensation"
                ) or None,
            )
            _print({"action_id": action_id}, as_json=True)
        elif command == "permit":
            if args.ttl_seconds <= 0:
                raise ValueError("ttl-seconds must be positive")
            permit_id = db.issue_permit(
                conn,
                args.action_id,
                capability=args.capability,
                issued_to=args.issued_to,
                policy_version=args.policy_version,
                expires_at=int(time.time()) + args.ttl_seconds,
                target_resource=args.target_resource,
                approval_artifact_id=args.approval_artifact_id,
                constraints=_json_value(args.constraints, label="constraints"),
            )
            _print({"permit_id": permit_id}, as_json=True)
        elif command == "deny":
            db.deny_action(
                conn, args.action_id, actor=args.actor, reason=args.reason
            )
            _print({"action_id": args.action_id, "status": "denied"}, as_json=True)
        elif command == "evaluate-action":
            from hermes_cli.config import load_config
            from hermes_cli.objective_policy import evaluate_and_record

            config = load_config()
            charter = config.get("agentic") or {}
            decision, permit_id = evaluate_and_record(
                conn,
                args.action_id,
                charter=charter,
                executor=args.executor,
                policy_version=str(charter.get("policy_version") or "charter-v1"),
            )
            _print(
                {
                    "action_id": args.action_id,
                    "decision": decision.verdict,
                    "reason": decision.reason,
                    "permit_id": permit_id,
                    "constraints": decision.constraints,
                },
                as_json=True,
            )
        elif command == "consume-permit":
            organization = conn.execute(
                """SELECT o.organization_id FROM objectives o
                   JOIN candidate_actions a ON a.objective_id=o.id
                   WHERE a.id=?""",
                (args.action_id,),
            ).fetchone()
            if organization is None:
                raise KeyError("action objective not found")
            db.consume_permit(
                conn,
                args.permit_id,
                action_id=args.action_id,
                organization_id=str(organization["organization_id"]),
                payload=_json_value(args.payload, label="payload"),
                executor=args.executor,
            )
            _print({"permit_id": args.permit_id, "status": "consumed"}, as_json=True)
        elif command == "record-result":
            organization = conn.execute(
                """SELECT o.organization_id FROM objectives o
                   JOIN candidate_actions a ON a.objective_id=o.id
                   WHERE a.id=?""",
                (args.action_id,),
            ).fetchone()
            if organization is None:
                raise KeyError("action objective not found")
            result_id = db.record_execution_result(
                conn,
                action_id=args.action_id,
                permit_id=args.permit_id,
                executor=args.executor,
                organization_id=str(organization["organization_id"]),
                status=args.status,
                result=_json_value(args.result, label="result"),
                started_at=args.started_at,
                external_reference=args.external_reference,
            )
            _print({"result_id": result_id}, as_json=True)
        elif command == "verify":
            from hermes_cli import verification_evidence

            verification_id = db.record_verification(
                conn,
                objective_id=args.objective_id,
                organization_id=db.get_objective(
                    conn, args.objective_id
                ).organization_id,
                plan_id=args.plan_id,
                verifier=args.verifier,
                method=args.method,
                verdict=args.verdict,
                evidence=verification_evidence.build(
                    observer=args.verifier,
                    source_kind=args.source_kind,
                    source_reference=args.source_reference,
                    observed_at=args.observed_at,
                    facts=_json_value(args.evidence, label="evidence"),
                ),
                action_id=args.action_id,
                execution_result_id=args.execution_result_id,
            )
            _print({"verification_id": verification_id}, as_json=True)
        elif command == "worker-status":
            from hermes_cli.objective_worker import worker_health

            _print(worker_health(conn), as_json=True)
        elif command == "worker":
            # The inspection connection closes before the process loop starts.
            pass
        elif command in {
            "subscribe", "ingest-event", "schedule", "triggers", "trigger-status"
        }:
            from hermes_cli import objective_triggers, organization_db

            ceo = organization_db.active_ceo(conn)
            if ceo is None:
                raise RuntimeError("solo-founder organization has not been initialized")
            organization_id = ceo["organization_id"]
            if command == "subscribe":
                item_id = objective_triggers.subscribe(
                    conn,
                    organization_id=organization_id,
                    objective_id=args.objective_id,
                    source_type=args.source_type,
                    event_type=args.event_type,
                )
                _print({"subscription_id": item_id}, as_json=True)
            elif command == "ingest-event":
                event_ids = objective_triggers.route_external_event(
                    conn,
                    organization_id=organization_id,
                    source_type=args.source_type,
                    event_type=args.event_type,
                    source_reference=args.source_reference,
                    payload=_json_value(args.payload, label="payload"),
                    authentication_evidence=_json_value(
                        args.authentication, label="authentication"
                    ),
                    content=args.content,
                )
                _print({"event_ids": event_ids}, as_json=True)
            elif command == "schedule":
                schedule_id = objective_triggers.create_schedule(
                    conn,
                    organization_id=organization_id,
                    objective_id=args.objective_id,
                    event_type=args.event_type,
                    interval_seconds=args.interval_seconds,
                    next_fire_at=args.next_fire_at,
                    payload=_json_value(args.payload, label="payload"),
                )
                _print({"schedule_id": schedule_id}, as_json=True)
            elif command == "trigger-status":
                objective_triggers.set_trigger_status(
                    conn,
                    organization_id=organization_id,
                    trigger_id=args.trigger_id,
                    status=args.status,
                )
                _print(
                    {"trigger_id": args.trigger_id, "status": args.status},
                    as_json=True,
                )
            else:
                _print(
                    objective_triggers.list_triggers(conn, organization_id),
                    as_json=True,
                )
        else:  # pragma: no cover - argparse enforces the command
            raise ValueError(f"unknown objectives command: {command}")
    if args.objective_command == "worker":
        from hermes_cli.objective_worker import run_forever

        return run_forever(
            db_path=getattr(args, "db", None),
            interval_seconds=args.interval_seconds,
            max_cycles=1 if args.once else None,
        )
    return 0
