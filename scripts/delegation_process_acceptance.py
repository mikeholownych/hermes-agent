#!/usr/bin/env python3
"""Process-separated Founder/CEO delegation acceptance contract.

This harness deliberately uses only the installed runtime APIs.  It does not
need pytest or a model provider: a CEO creates one exact grant, a separate
Python process validates that grant and closes the task with evidence, and a
fresh CEO runtime consumes the completion wake and verifies the parent goal.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from hermes_cli import (
    kanban_db,
    objective_adapters,
    objective_service,
    objectives_db,
    organization_db,
    profiles,
    workforce_delegation,
)
from hermes_cli.objective_runtime import (
    ActionProposal,
    ObjectiveRuntime,
    PlanProposal,
    VerificationOutcome,
)
from hermes_cli import verification_evidence


def _worker_code() -> str:
    return r'''
import asyncio, json, os
from pathlib import Path
from hermes_cli import kanban_db, workforce_delegation
from tools.code_execution_tool import execute_code
from tools.file_tools import patch_tool, read_file_tool, search_tool, write_file_tool
from tools.image_source import resolve_image_source, ResolveContext

grant = workforce_delegation.validate_worker_launch(
    enabled_toolsets=["terminal", "files"],
    enabled_skills=["security.audit"],
    enabled_capabilities=["security.audit", "file.read"],
    enabled_systems=["security", "localhost"],
)
input_path = os.environ["HERMES_WORKER_INPUT"]
allowed_read = json.loads(read_file_tool(input_path, task_id=os.environ["HERMES_KANBAN_TASK"]))
if not str(allowed_read.get("content") or "").startswith("1|delegated evidence"):
    raise RuntimeError(f"governed file read failed: {allowed_read}")
blocked_read = read_file_tool(
    str(Path(input_path).with_name("not-authorized.txt")),
    task_id=os.environ["HERMES_KANBAN_TASK"],
)
if "resource exceeds" not in blocked_read:
    raise RuntimeError(f"retargeted file read was not rejected: {blocked_read}")
blocked_write = write_file_tool(
    input_path,
    "must not write",
    task_id=os.environ["HERMES_KANBAN_TASK"],
)
if "not granted" not in blocked_write:
    raise RuntimeError(f"file write was not rejected: {blocked_write}")
blocked_patch = patch_tool(
    mode="replace",
    path=input_path,
    old_string="delegated evidence",
    new_string="must not patch",
    task_id=os.environ["HERMES_KANBAN_TASK"],
)
if "not granted" not in blocked_patch:
    raise RuntimeError(f"file patch was not rejected: {blocked_patch}")
blocked_search = search_tool(
    pattern="delegated",
    target="content",
    path=input_path,
    task_id=os.environ["HERMES_KANBAN_TASK"],
)
if "not granted" not in blocked_search:
    raise RuntimeError(f"file search was not rejected: {blocked_search}")
blocked_code = execute_code(
    "from pathlib import Path; print(Path('/etc/passwd').read_text())",
    task_id=os.environ["HERMES_KANBAN_TASK"],
)
if "not granted" not in blocked_code:
    raise RuntimeError(f"ungranted code execution was not rejected: {blocked_code}")
try:
    asyncio.run(
        resolve_image_source(
            input_path,
            ResolveContext(task_id=os.environ["HERMES_KANBAN_TASK"]),
        )
    )
except Exception as exc:
    if "not granted" not in str(exc):
        raise RuntimeError(f"ungranted vision read was not rejected: {exc}")
else:
    raise RuntimeError("ungranted vision read was not rejected")
with kanban_db.connect_closing(board=os.environ["HERMES_DELEGATION_BOARD"]) as board:
    task_id = os.environ["HERMES_KANBAN_TASK"]
    if not kanban_db.complete_task(
        board,
        task_id,
        summary="Read-back confirms the assigned control is compliant.",
        metadata={"evidence": {"control": "security.audit", "verdict": "pass"}},
    ):
        raise RuntimeError("worker could not close its assigned task")
Path(os.environ["HERMES_WORKER_EVIDENCE"]).write_text(
    json.dumps({
        "grant_id": grant["id"],
        "task_id": task_id,
        "evidence_recorded": True,
        "file_tool_boundary": "pass",
    })
)
'''


def main() -> int:
    home = Path(os.environ.get("HERMES_HOME", str(Path.home() / ".charterforge")))
    home.mkdir(parents=True, exist_ok=True)
    os.environ["HERMES_HOME"] = str(home)
    authority_path = Path(
        os.environ.get("HERMES_DELEGATION_AUTHORITY_DB", str(home / "delegation.db"))
    )
    board = os.environ.get("HERMES_DELEGATION_BOARD", "process-separated")
    evidence_path = home / "delegation-worker-evidence.json"
    conn = objectives_db.connect(authority_path)
    charter = {
        "enabled": True,
        "operating_mode": "autonomous",
        "operator_role": "advisor",
        "policy_version": "charter-v1",
        "allowed_capabilities": ["work.delegate", "security.audit", "file.read"],
        "forbidden_capabilities": [],
        "allowed_systems": ["kanban", "security", "localhost"],
        "approval_required_capabilities": [],
        "max_autonomous_risk": "low",
        "allow_irreversible": False,
        "max_autonomous_spend_minor": 100,
        "max_action_spend_minor": 100,
        "permit_ttl_seconds": 300,
        "operating_cadence": {"enabled": False},
        "solo_founder": {"toolsets": ["terminal", "files"], "skills": ["security.audit"]},
    }
    organization_id, ceo_id = organization_db.bootstrap_solo_founder(
        conn,
        organization_name="Delegation Acceptance Company",
        purpose="Prove process-separated employee handoffs",
        profile_name="default",
        charter=charter,
    )
    employee_id = organization_db.propose_employee(
        conn,
        organization_id=organization_id,
        display_name="Evidence Analyst",
        title="Evidence Analyst",
        level="individual_contributor",
        manager_id=ceo_id,
        proposed_by=f"employee:{ceo_id}",
        employment_type="agent",
    )
    organization_db.transition_employee(conn, employee_id, "approved", actor=f"employee:{ceo_id}")
    organization_db.transition_employee(conn, employee_id, "provisioning", actor=f"employee:{ceo_id}")
    organization_db.create_mandate(
        conn,
        employee_id,
        purpose="Return bounded audit evidence",
        responsibilities=["inspect the assigned record"],
        decision_rights=["report findings"],
        prohibited_actions=["work.delegate"],
        capabilities=["security.audit", "file.read"],
        systems=["security", "localhost"],
        kpis=["evidence returned"],
        escalation={"to": ceo_id},
        toolsets=["terminal", "files"],
        skills=["security.audit"],
        created_by=f"employee:{ceo_id}",
        budget_minor=100,
        expires_at=int(time.time()) + 3600,
    )
    organization_db.transition_employee(
        conn,
        employee_id,
        "active",
        actor=f"employee:{ceo_id}",
        profile_name="security-auditor",
    )
    mandate = organization_db.get_current_mandate(conn, employee_id)
    profile_dir = profiles.get_profile_dir("security-auditor")
    profile_dir.mkdir(parents=True, exist_ok=True)
    profiles.write_profile_meta(
        profile_dir,
        organization_id=organization_id,
        employee_id=employee_id,
        manager_employee_id=ceo_id,
        corporate_level="individual_contributor",
        employment_class="agent",
        mandate_id=str(mandate["id"]),
        mandate_version=int(mandate["version"]),
        mandate_expires_at=mandate["expires_at"],
    )
    objective = objectives_db.create_objective(
        conn,
        organization_id=organization_id,
        desired_outcome="Complete the delegated evidence review",
        originator=f"employee:{ceo_id}",
        permitted_systems=["kanban", "security", "localhost"],
        success_criteria=[{"verifier": "kanban.task.done_with_evidence", "params": {}}],
    )
    objectives_db.transition_objective(conn, objective.id, "accepted", actor=f"employee:{ceo_id}")
    objectives_db.transition_objective(conn, objective.id, "planned", actor=f"employee:{ceo_id}")
    plan_id = objectives_db.create_plan(
        conn,
        objective.id,
        assumptions=["the analyst has the exact grant"],
        tasks=[{"step": "complete bounded evidence review"}],
        dependencies=[],
        risks=[],
        created_by=f"employee:{ceo_id}",
    )
    executor = objective_adapters.ActionExecutorRegistry(
        identity=f"employee:{ceo_id}", authority_conn=conn
    )
    verifier = objective_adapters.IndependentVerifierRegistry()
    objective_adapters.register_kanban_adapters(
        executor, verifier, board=board, authority_conn=conn, manager_employee_id=ceo_id
    )
    payload = {
        "system": "kanban",
        "target_resource": board,
        "idempotency_key": "delegation-process-acceptance-0001",
        "title": "Return evidence",
        "body": "Inspect the assigned control and return read-back evidence.",
        "assignee": "security-auditor",
        "skills": ["security.audit"],
        "task_capabilities": ["security.audit", "file.read"],
        "task_systems": ["security", "localhost"],
        "task_toolsets": ["terminal", "files"],
        "task_system": "localhost",
        "task_target_resource": str(home / "delegation-worker-input.txt"),
        "task_budget_minor": 100,
        "task_expires_at": int(time.time()) + 1800,
    }
    action_id = objectives_db.propose_action(
        conn,
        objective_id=objective.id,
        plan_id=plan_id,
        action_type="kanban.create_task",
        payload=payload,
        expected_outcome="bounded task assigned",
        required_capability="work.delegate",
        verification_method="kanban.task.created",
        risk_class="low",
        reversible=True,
        proposed_by=f"employee:{ceo_id}",
    )
    delegated = executor.execute_governed(action_id, objective.id, "kanban.create_task", payload)
    if delegated.status != "succeeded":
        raise RuntimeError(f"CEO task creation failed: {delegated.result}")
    task_id = str(delegated.external_reference)
    grant = conn.execute(
        "SELECT id FROM employee_task_grants WHERE action_id=?", (action_id,)
    ).fetchone()
    if grant is None:
        raise RuntimeError("CEO task has no durable grant")
    permit_id = objectives_db.issue_permit(
        conn,
        action_id,
        capability="work.delegate",
        issued_to=f"employee:{ceo_id}",
        policy_version="charter-v1",
        expires_at=int(time.time()) + 300,
        target_resource=board,
        constraints={"organization_id": organization_id, "task_id": task_id},
    )
    objectives_db.consume_permit(
        conn,
        permit_id,
        action_id=action_id,
        payload=payload,
        executor=f"employee:{ceo_id}",
        organization_id=organization_id,
        current_policy_version="charter-v1",
    )
    objectives_db.record_execution_result(
        conn,
        action_id=action_id,
        permit_id=permit_id,
        executor=f"employee:{ceo_id}",
        organization_id=organization_id,
        status=delegated.status,
        result=delegated.result,
        started_at=int(time.time()),
        external_reference=task_id,
    )
    conn.commit()
    (home / "delegation-worker-input.txt").write_text("delegated evidence\n", encoding="utf-8")
    print(json.dumps({"phase": "ceo", "grant": str(grant["id"]), "task": task_id}))

    child_env = {
        **os.environ,
        "HERMES_EXECUTION_CONTRACT_ID": str(grant["id"]),
        "HERMES_KANBAN_TASK": task_id,
        "HERMES_BUSINESS_AUTHORITY_DB": str(authority_path),
        "HERMES_PROFILE": "security-auditor",
        "HERMES_DELEGATION_BOARD": board,
        "HERMES_KANBAN_BOARD": board,
        "HERMES_KANBAN_DB": str(kanban_db.kanban_db_path(board=board)),
        "HERMES_WORKER_EVIDENCE": str(evidence_path),
        "HERMES_WORKER_INPUT": str(home / "delegation-worker-input.txt"),
        # The acceptance worker reads the mounted authority volume directly;
        # do not let an inherited host-terminal Docker backend reinterpret the
        # local path as a nested sandbox path.
        "TERMINAL_ENV": "local",
    }
    child = subprocess.run(
        [sys.executable, "-c", _worker_code()],
        env=child_env,
        check=False,
        capture_output=True,
        text=True,
    )
    if child.returncode != 0:
        raise RuntimeError(f"subordinate process failed: {child.stderr.strip()}")
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    print(json.dumps({"phase": "subordinate", **evidence}))
    if objective_service.sync_kanban_events(conn, board=board) != 1:
        raise RuntimeError("CEO was not woken by the subordinate completion event")

    class CompletionPlanner:
        identity = f"employee:{ceo_id}"

        def propose(self, snapshot, event):
            return PlanProposal(
                assumptions=["employee evidence is present in provider read-back"],
                tasks=[],
                dependencies=[],
                risks=[],
                actions=(),
                objective_complete_when_verified=True,
            )

    class CompletionVerifier:
        identity = "control:delegation-acceptance-verifier"

        def verify_objective(self, snapshot, plan, action_verifications):
            with kanban_db.connect_closing(board=board) as board_conn:
                task = kanban_db.get_task(board_conn, task_id)
                run = board_conn.execute(
                    "SELECT metadata FROM task_runs WHERE task_id=? ORDER BY id DESC LIMIT 1",
                    (task_id,),
                ).fetchone()
            metadata = json.loads(run["metadata"] or "{}") if run else {}
            passed = bool(
                task
                and task.status == "done"
                and metadata.get("evidence", {}).get("verdict") == "pass"
            )
            return VerificationOutcome(
                "pass" if passed else "fail",
                verification_evidence.build(
                    observer=self.identity,
                    source_kind="authoritative_database_readback",
                    source_reference=f"kanban:{task_id}",
                    facts={"task_done": bool(task and task.status == "done"), "evidence": passed},
                ),
            )

    class NoopExecutor:
        identity = f"employee:{ceo_id}"

    runtime = ObjectiveRuntime(
        conn,
        planner=CompletionPlanner(),
        executor=NoopExecutor(),
        verifier=CompletionVerifier(),
        charter=charter,
        policy_version="charter-v1",
        runtime_id="runtime:delegation-acceptance-ceo",
    )
    outcome = runtime.tick()
    if outcome.status != "verified" or objectives_db.get_objective(conn, objective.id).status != "verified":
        raise RuntimeError(f"CEO verification failed: {outcome.status} {outcome.reason}")
    print(json.dumps({"phase": "ceo", "event": "kanban.task.done", "objective": "verified"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
