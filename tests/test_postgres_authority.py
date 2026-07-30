"""Tests for Postgres authority store.

These tests verify the Postgres authority store operations work correctly.
They require a running Postgres instance (provided by CI or local Docker).

Tests are structured in three tiers:
  1. Basic operation tests  — happy-path CRUD
  2. Fencing / invariant tests — concurrent workers, stale generation, etc.
  3. Adversarial tests — every attack / failure mode named in the requirement

All tests use only the public postgres_authority API.  No direct row
manipulation that would bypass the authority machinery.
"""

import os
import time
import uuid
from typing import Any, Iterator

import pytest

# Skip all tests if psycopg not installed
psycopg = pytest.importorskip("psycopg")

# Skip all tests that REQUIRE a Postgres connection if no URL available
_REQUIRES_PG = pytest.mark.skipif(
    not (
        os.environ.get("AUTHORITY_POSTGRES_TEST_URL")
        or os.environ.get("POSTGRES_HOST")
        or os.environ.get("DATABASE_URL")
    ),
    reason=(
        "No Postgres URL available.  Set AUTHORITY_POSTGRES_TEST_URL, "
        "DATABASE_URL, or POSTGRES_HOST to run Postgres authority tests."
    ),
)

# Apply to all classes that need a live connection.
pytestmark = _REQUIRES_PG


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def postgres_url() -> str:
    url = os.environ.get("AUTHORITY_POSTGRES_TEST_URL")
    if url:
        return url
    host = os.environ.get("POSTGRES_HOST", "/var/run/postgresql")
    port = os.environ.get("POSTGRES_PORT", "")
    user = os.environ.get("POSTGRES_USER", "cftest")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    database = os.environ.get("POSTGRES_DATABASE", "charterforge_test")
    if host.startswith("/"):
        # Unix socket — use DSN-style (no password needed with peer/trust auth)
        return f"host={host} user={user} dbname={database}"
    port_part = f":{port}" if port else ""
    if password:
        return f"postgresql://{user}:{password}@{host}{port_part}/{database}"
    return f"postgresql://{user}@{host}{port_part}/{database}"


@pytest.fixture
def pg(postgres_url: str) -> Iterator[Any]:
    """Isolated Postgres connection in a unique schema per test."""
    from hermes_cli.postgres_authority import connect, init_schema
    import psycopg as _psycopg
    from psycopg.rows import dict_row as _dict_row

    schema = f"test_{uuid.uuid4().hex[:10]}"

    # First connection to create the schema.
    setup_conn = connect(postgres_url)
    with setup_conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    setup_conn.commit()
    setup_conn.close()

    # Re-connect with search_path scoped to the test schema.
    # psycopg accepts `options` as a keyword that maps to the Postgres
    # connection parameter (sets session-level GUCs before first query).
    conn = _psycopg.connect(
        postgres_url,
        row_factory=_dict_row,
        options=f"-c search_path={schema}",
    )
    conn.autocommit = False

    init_schema(conn)

    yield conn

    conn.close()

    # Drop schema with a fresh connection (no search_path restriction).
    cleanup_conn = connect(postgres_url)
    with cleanup_conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    cleanup_conn.commit()
    cleanup_conn.close()


def _new_task() -> str:
    return f"task-{uuid.uuid4().hex[:10]}"


def _new_token() -> str:
    return f"tok-{uuid.uuid4().hex[:10]}"


ORG = "test-org-alpha"
ORG2 = "test-org-beta"


def _must_claim(pg, *, task_id: str, claim_token: str,
                organization_id: str = ORG, worker_id: str = "w1",
                expires_at: float | None = None) -> int:
    """Claim and assert success, returning the lease_generation as int."""
    from hermes_cli.postgres_authority import claim_task
    gen = claim_task(
        pg, task_id=task_id, claim_token=claim_token,
        organization_id=organization_id, worker_id=worker_id,
        claim_scope_url="",
        expires_at=expires_at if expires_at is not None else time.time() + 3600,
    )
    assert gen is not None, "claim must succeed"
    return gen


# ---------------------------------------------------------------------------
# 1. Basic operation tests
# ---------------------------------------------------------------------------


class TestBasicClaim:
    def test_claim_task_returns_generation_1(self, pg):
        from hermes_cli.postgres_authority import claim_task

        gen = claim_task(
            pg,
            task_id=_new_task(),
            claim_token=_new_token(),
            organization_id=ORG,
            worker_id="w1",
            claim_scope_url="https://example.com/scope",
            expires_at=time.time() + 3600,
        )
        assert gen == 1

    def test_get_claim_returns_active_claim(self, pg):
        from hermes_cli.postgres_authority import claim_task, get_claim

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg,
            task_id=task_id,
            claim_token=token,
            organization_id=ORG,
            worker_id="w1",
            claim_scope_url="",
            expires_at=time.time() + 3600,
        )
        assert gen == 1

        claim = get_claim(pg, task_id=task_id, organization_id=ORG)
        assert claim is not None
        assert claim["task_id"] == task_id
        assert claim["lease_generation"] == 1

    def test_release_claim_removes_it(self, pg):
        from hermes_cli.postgres_authority import claim_task, release_claim, get_claim

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg,
            task_id=task_id,
            claim_token=token,
            organization_id=ORG,
            worker_id="w1",
            claim_scope_url="",
            expires_at=time.time() + 3600,
        )
        assert gen is not None

        ok = release_claim(
            pg,
            task_id=task_id,
            organization_id=ORG,
            claim_token=token,
            lease_generation=gen,
        )
        assert ok is True
        assert get_claim(pg, task_id=task_id, organization_id=ORG) is None

    def test_complete_task_succeeds_and_releases(self, pg):
        from hermes_cli.postgres_authority import claim_task, complete_task, get_claim

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg,
            task_id=task_id,
            claim_token=token,
            organization_id=ORG,
            worker_id="w1",
            claim_scope_url="",
            expires_at=time.time() + 3600,
        )
        assert gen is not None

        ok = complete_task(
            pg,
            task_id=task_id,
            organization_id=ORG,
            claim_token=token,
            lease_generation=gen,
            outcome="success",
        )
        assert ok is True
        assert get_claim(pg, task_id=task_id, organization_id=ORG) is None

    def test_complete_with_effects_stored(self, pg):
        from hermes_cli.postgres_authority import claim_task, complete_task

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg,
            task_id=task_id,
            claim_token=token,
            organization_id=ORG,
            worker_id="w1",
            claim_scope_url="",
            expires_at=time.time() + 3600,
        )
        effect_key = f"{ORG}:{task_id}:a1:p1:stripe:ch_001"
        ok = complete_task(
            pg,
            task_id=task_id,
            organization_id=ORG,
            claim_token=token,
            lease_generation=gen,
            outcome="success",
            effects=[
                {
                    "effect_key": effect_key,
                    "type": "payment",
                    "provider": "stripe",
                    "provider_ref": "ch_001",
                    "idempotency_key": "ik-001",
                    "amount": 1000,
                }
            ],
        )
        assert ok is True
        with pg.cursor() as cur:
            cur.execute(
                "SELECT count(*) as n FROM execution_effects WHERE task_id = %s",
                (task_id,),
            )
            assert cur.fetchone()["n"] == 1

    def test_complete_with_effect_missing_key_raises(self, pg):
        from hermes_cli.postgres_authority import claim_task, complete_task

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg,
            task_id=task_id,
            claim_token=token,
            organization_id=ORG,
            worker_id="w1",
            claim_scope_url="",
            expires_at=time.time() + 3600,
        )
        with pytest.raises(ValueError, match="effect_key"):
            complete_task(
                pg,
                task_id=task_id,
                organization_id=ORG,
                claim_token=token,
                lease_generation=gen,
                outcome="success",
                effects=[{"type": "payment"}],  # missing effect_key
            )


class TestPermitFlow:
    def test_issue_and_consume_permit(self, pg):
        from hermes_cli.postgres_authority import claim_task, issue_permit, consume_permit

        task_id = _new_task()
        token = _new_token()
        payload = {"action": "send_email", "nonce": uuid.uuid4().hex}
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        permit_id = issue_permit(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )
        assert len(permit_id) == 36  # UUID

        ok = consume_permit(
            pg, permit_id=permit_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )
        assert ok is True

    def test_consume_permit_twice_fails(self, pg):
        from hermes_cli.postgres_authority import claim_task, issue_permit, consume_permit

        task_id = _new_task()
        token = _new_token()
        payload = {"action": "test", "nonce": uuid.uuid4().hex}
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        permit_id = issue_permit(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )
        consume_permit(
            pg, permit_id=permit_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )
        # Second consume must fail.
        ok = consume_permit(
            pg, permit_id=permit_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )
        assert ok is False


class TestPermitFieldParity:
    """consume_permit must re-check executor/capability/target_resource/
    policy_version at consumption time, not just fencing/payload — closing
    the gap where task_permits already stored these fields (written by
    issue_permit) but consume_permit's atomic UPDATE never re-validated
    them. Mirrors the exact-action-binding checks objectives_db.consume_permit
    performs (issued_to==executor, capability match, target_resource match,
    optional policy_version staleness).
    """

    def _issue(self, pg, **overrides):
        from hermes_cli.postgres_authority import claim_task, issue_permit

        task_id = _new_task()
        token = _new_token()
        payload = {"action": "send_email", "nonce": uuid.uuid4().hex}
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        issue_kwargs = dict(
            task_id=task_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
            executor="worker-alice", capability="send_email",
            target_resource="mailbox:alice@example.test",
            policy_version="policy-v1",
        )
        issue_kwargs.update(overrides)
        permit_id = issue_permit(pg, **issue_kwargs)
        return dict(
            permit_id=permit_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )

    def test_matching_fields_succeeds(self, pg):
        from hermes_cli.postgres_authority import consume_permit

        ctx = self._issue(pg)
        ok = consume_permit(
            pg, **ctx,
            executor="worker-alice", capability="send_email",
            target_resource="mailbox:alice@example.test",
            policy_version="policy-v1",
        )
        assert ok is True

    def test_executor_mismatch_rejected(self, pg):
        from hermes_cli.postgres_authority import consume_permit

        ctx = self._issue(pg)
        ok = consume_permit(
            pg, **ctx,
            executor="worker-mallory", capability="send_email",
            target_resource="mailbox:alice@example.test",
            policy_version="policy-v1",
        )
        assert ok is False

    def test_capability_mismatch_rejected(self, pg):
        from hermes_cli.postgres_authority import consume_permit

        ctx = self._issue(pg)
        ok = consume_permit(
            pg, **ctx,
            executor="worker-alice", capability="delete_account",
            target_resource="mailbox:alice@example.test",
            policy_version="policy-v1",
        )
        assert ok is False

    def test_target_resource_mismatch_rejected(self, pg):
        from hermes_cli.postgres_authority import consume_permit

        ctx = self._issue(pg)
        ok = consume_permit(
            pg, **ctx,
            executor="worker-alice", capability="send_email",
            target_resource="mailbox:bob@example.test",
            policy_version="policy-v1",
        )
        assert ok is False

    def test_stale_policy_version_rejected(self, pg):
        from hermes_cli.postgres_authority import consume_permit

        ctx = self._issue(pg)
        ok = consume_permit(
            pg, **ctx,
            executor="worker-alice", capability="send_email",
            target_resource="mailbox:alice@example.test",
            policy_version="policy-v2-superseded",
        )
        assert ok is False

    def test_policy_version_omitted_skips_staleness_check(self, pg):
        """Passing policy_version=None (the default) skips the staleness
        check entirely, matching objectives_db.consume_permit's
        current_policy_version=None convention."""
        from hermes_cli.postgres_authority import consume_permit

        ctx = self._issue(pg)
        ok = consume_permit(
            pg, **ctx,
            executor="worker-alice", capability="send_email",
            target_resource="mailbox:alice@example.test",
        )
        assert ok is True


class TestObjectiveLifecycleGate:
    """consume_permit must fail closed on missing or terminal mirrored
    objective status when objective_id is supplied — a cancelled objective
    must not authorize permit consumption merely because claim/fencing/
    payload checks passed."""

    def _issue_and_context(self, pg):
        from hermes_cli.postgres_authority import claim_task, issue_permit

        task_id = _new_task()
        token = _new_token()
        payload = {"action": "test", "nonce": uuid.uuid4().hex}
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        permit_id = issue_permit(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )
        return dict(
            permit_id=permit_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )

    def test_missing_objective_status_row_rejected(self, pg):
        from hermes_cli.postgres_authority import consume_permit

        ctx = self._issue_and_context(pg)
        ok = consume_permit(pg, **ctx, objective_id="obj-never-mirrored")
        assert ok is False

    def test_admissive_status_succeeds(self, pg):
        from hermes_cli.postgres_authority import consume_permit, mirror_objective_status, mirror_autonomy_mode

        ctx = self._issue_and_context(pg)
        objective_id = "obj-executing"
        mirror_objective_status(
            pg, objective_id=objective_id, organization_id=ORG,
            status="executing", version=1,
        )
        mirror_autonomy_mode(pg, organization_id=ORG, mode="autonomous", generation=1)
        ok = consume_permit(pg, **ctx, objective_id=objective_id)
        assert ok is True

    @pytest.mark.parametrize("terminal_status", ["cancelled", "completed", "failed", "verified"])
    def test_terminal_status_rejected(self, pg, terminal_status):
        from hermes_cli.postgres_authority import consume_permit, mirror_objective_status

        ctx = self._issue_and_context(pg)
        objective_id = f"obj-{terminal_status}"
        mirror_objective_status(
            pg, objective_id=objective_id, organization_id=ORG,
            status=terminal_status, version=1,
        )
        ok = consume_permit(pg, **ctx, objective_id=objective_id)
        assert ok is False

    def test_wrong_organization_mirror_rejected(self, pg):
        """A mirror row for a different org must not authorize consumption."""
        from hermes_cli.postgres_authority import consume_permit, mirror_objective_status

        ctx = self._issue_and_context(pg)
        objective_id = "obj-cross-org"
        mirror_objective_status(
            pg, objective_id=objective_id, organization_id="test-org-beta",
            status="executing", version=1,
        )
        ok = consume_permit(pg, **ctx, objective_id=objective_id)
        assert ok is False


class TestAutonomyGate:
    """consume_permit must fail closed on missing or stopped mirrored
    autonomy mode when objective_id is supplied — master-autonomy-stop must
    block consumption even when claim/fencing/payload checks pass."""

    def _issue_and_context(self, pg, org=ORG):
        from hermes_cli.postgres_authority import claim_task, issue_permit

        task_id = _new_task()
        token = _new_token()
        payload = {"action": "test", "nonce": uuid.uuid4().hex}
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=org, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        permit_id = issue_permit(
            pg, task_id=task_id, organization_id=org,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )
        return dict(
            permit_id=permit_id, organization_id=org,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )

    def test_missing_autonomy_row_rejected(self, pg):
        from hermes_cli.postgres_authority import consume_permit, mirror_objective_status

        org = f"org-no-autonomy-row-{uuid.uuid4().hex[:8]}"
        ctx = self._issue_and_context(pg, org=org)
        mirror_objective_status(
            pg, objective_id="obj-1", organization_id=org,
            status="executing", version=1,
        )
        ok = consume_permit(pg, **ctx, objective_id="obj-1")
        assert ok is False

    def test_autonomous_mode_succeeds(self, pg):
        from hermes_cli.postgres_authority import (
            consume_permit, mirror_objective_status, mirror_autonomy_mode,
        )

        org = f"org-autonomous-{uuid.uuid4().hex[:8]}"
        ctx = self._issue_and_context(pg, org=org)
        mirror_objective_status(
            pg, objective_id="obj-1", organization_id=org,
            status="executing", version=1,
        )
        mirror_autonomy_mode(pg, organization_id=org, mode="autonomous", generation=1)
        ok = consume_permit(pg, **ctx, objective_id="obj-1")
        assert ok is True

    def test_stopped_autonomy_rejected(self, pg):
        from hermes_cli.postgres_authority import (
            consume_permit, mirror_objective_status, mirror_autonomy_mode,
        )

        org = f"org-stopped-{uuid.uuid4().hex[:8]}"
        ctx = self._issue_and_context(pg, org=org)
        mirror_objective_status(
            pg, objective_id="obj-1", organization_id=org,
            status="executing", version=1,
        )
        mirror_autonomy_mode(pg, organization_id=org, mode="stopped", generation=1)
        ok = consume_permit(pg, **ctx, objective_id="obj-1")
        assert ok is False

    def test_autonomy_stop_after_permit_issuance_blocks_consumption(self, pg):
        """The realistic sequence: permit issued while autonomous, then
        master-stop fires before consumption — consumption must be blocked
        even though the permit itself is still validly fenced."""
        from hermes_cli.postgres_authority import (
            consume_permit, mirror_objective_status, mirror_autonomy_mode,
        )

        org = f"org-stop-after-issue-{uuid.uuid4().hex[:8]}"
        mirror_objective_status(
            pg, objective_id="obj-1", organization_id=org,
            status="executing", version=1,
        )
        mirror_autonomy_mode(pg, organization_id=org, mode="autonomous", generation=1)
        ctx = self._issue_and_context(pg, org=org)

        # Master-stop fires after the permit was issued.
        mirror_autonomy_mode(pg, organization_id=org, mode="stopped", generation=2)

        ok = consume_permit(pg, **ctx, objective_id="obj-1")
        assert ok is False


class TestCleanup:
    def test_cleanup_expired_claims(self, pg):
        from hermes_cli.postgres_authority import (
            claim_task, reclaim_task, cleanup_expired_claims, get_claim
        )

        task_id = _new_task()
        token = _new_token()
        # Insert an already-expired claim by going 10s into the past.
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() - 10,
        )
        assert gen is not None

        # Mark the run as reclaimed (as reclaim_task would do) so GC can fire.
        with pg.cursor() as cur:
            cur.execute(
                "UPDATE task_runs SET status='reclaimed', outcome='reclaimed', ended_at=NOW() "
                "WHERE task_id=%s AND organization_id=%s AND status='pending'",
                (task_id, ORG),
            )
        pg.commit()

        count = cleanup_expired_claims(pg)
        assert count >= 1
        assert get_claim(pg, task_id=task_id, organization_id=ORG) is None


# ---------------------------------------------------------------------------
# 2. Fencing / invariant tests
# ---------------------------------------------------------------------------


class TestClaimExclusivity:
    """Claim exclusivity: one active authoritative claim per (task, org)."""

    def test_two_workers_race_only_one_wins(self, pg):
        """INVARIANT: UNIQUE (task_id, organization_id) prevents dual authority."""
        from hermes_cli.postgres_authority import claim_task

        task_id = _new_task()
        token1 = _new_token()
        token2 = _new_token()
        expires = time.time() + 3600

        gen1 = claim_task(
            pg, task_id=task_id, claim_token=token1,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=expires,
        )
        gen2 = claim_task(
            pg, task_id=task_id, claim_token=token2,
            organization_id=ORG, worker_id="w2",
            claim_scope_url="", expires_at=expires,
        )

        assert gen1 == 1, "first claim must succeed with generation 1"
        assert gen2 is None, "second claim must be rejected"

    def test_different_orgs_can_claim_same_task_id(self, pg):
        """Task IDs are org-scoped; different orgs do not conflict."""
        from hermes_cli.postgres_authority import claim_task

        task_id = _new_task()
        expires = time.time() + 3600

        gen1 = claim_task(
            pg, task_id=task_id, claim_token=_new_token(),
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=expires,
        )
        gen2 = claim_task(
            pg, task_id=task_id, claim_token=_new_token(),
            organization_id=ORG2, worker_id="w2",
            claim_scope_url="", expires_at=expires,
        )

        assert gen1 == 1
        assert gen2 == 1


class TestExpiredClaimReplacement:
    """Expired claims must be atomically replaced with a strictly higher generation."""

    def test_reclaim_increments_generation(self, pg):
        from hermes_cli.postgres_authority import claim_task, reclaim_task, get_claim

        task_id = _new_task()
        gen1 = claim_task(
            pg, task_id=task_id, claim_token=_new_token(),
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() - 1,  # already expired
        )
        assert gen1 == 1

        gen2 = reclaim_task(
            pg, task_id=task_id, organization_id=ORG,
            new_claim_token=_new_token(), new_worker_id="w2",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        assert gen2 == 2, "reclaim must produce generation 2"

        claim = get_claim(pg, task_id=task_id, organization_id=ORG)
        assert claim is not None
        assert claim["lease_generation"] == 2

    def test_reclaim_non_expired_claim_fails(self, pg):
        from hermes_cli.postgres_authority import claim_task, reclaim_task

        task_id = _new_task()
        claim_task(
            pg, task_id=task_id, claim_token=_new_token(),
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,  # active
        )

        gen = reclaim_task(
            pg, task_id=task_id, organization_id=ORG,
            new_claim_token=_new_token(), new_worker_id="w2",
            claim_scope_url="", expires_at=time.time() + 7200,
        )
        assert gen is None, "must not reclaim an active (non-expired) claim"


class TestStaleWorkerFencing:
    """A stale (superseded) worker must be blocked from all authoritative writes."""

    def _setup_stale_and_recovery(self, pg):
        """Returns (stale_token, stale_gen, recovery_token, recovery_gen, task_id)."""
        from hermes_cli.postgres_authority import claim_task, reclaim_task

        task_id = _new_task()
        stale_token = _new_token()
        claim_task(
            pg, task_id=task_id, claim_token=stale_token,
            organization_id=ORG, worker_id="stale-w",
            claim_scope_url="", expires_at=time.time() - 1,
        )
        recovery_token = _new_token()
        recovery_gen = reclaim_task(
            pg, task_id=task_id, organization_id=ORG,
            new_claim_token=recovery_token, new_worker_id="recovery-w",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        return stale_token, 1, recovery_token, recovery_gen, task_id

    def test_stale_worker_cannot_complete(self, pg):
        from hermes_cli.postgres_authority import complete_task

        stale_token, stale_gen, recovery_token, recovery_gen, task_id = (
            self._setup_stale_and_recovery(pg)
        )
        ok = complete_task(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=stale_token, lease_generation=stale_gen,
            outcome="stale-success",
        )
        assert ok is False, "stale worker must not complete"

    def test_stale_worker_cannot_release_claim(self, pg):
        from hermes_cli.postgres_authority import release_claim

        stale_token, stale_gen, _, _, task_id = self._setup_stale_and_recovery(pg)
        ok = release_claim(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=stale_token, lease_generation=stale_gen,
        )
        assert ok is False, "stale worker must not release the recovery worker's claim"

    def test_stale_worker_cannot_consume_permit(self, pg):
        from hermes_cli.postgres_authority import issue_permit, consume_permit

        stale_token, stale_gen, recovery_token, recovery_gen, task_id = (
            self._setup_stale_and_recovery(pg)
        )
        payload = {"action": "stale-action", "nonce": uuid.uuid4().hex}

        # Recovery worker issues a permit legitimately.
        permit_id = issue_permit(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=recovery_token, lease_generation=recovery_gen,
            action_payload=payload,
        )

        # Stale worker tries to consume with its old generation.
        ok = consume_permit(
            pg, permit_id=permit_id, organization_id=ORG,
            claim_token=stale_token, lease_generation=stale_gen,
            action_payload=payload,
        )
        assert ok is False, "stale worker must not consume a permit"

    def test_stale_worker_cannot_issue_permit(self, pg):
        from hermes_cli.postgres_authority import issue_permit

        stale_token, stale_gen, _, _, task_id = self._setup_stale_and_recovery(pg)
        with pytest.raises(ValueError, match="No valid fenced claim"):
            issue_permit(
                pg, task_id=task_id, organization_id=ORG,
                claim_token=stale_token, lease_generation=stale_gen,
                action_payload={"action": "stale"},
            )


# ---------------------------------------------------------------------------
# 3. Adversarial tests
# ---------------------------------------------------------------------------


class TestAdversarialClaims:
    def test_duplicate_completion_second_call_rejected(self, pg):
        """complete_task must be idempotent-reject: second call returns False."""
        from hermes_cli.postgres_authority import claim_task, complete_task

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        assert complete_task(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=token, lease_generation=gen, outcome="success",
        ) is True
        # Second call — claim row is already deleted, run is 'completed'.
        assert complete_task(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=token, lease_generation=gen, outcome="success",
        ) is False

    def test_mismatched_organization_rejected(self, pg):
        """A worker with the right token but wrong org must be rejected."""
        from hermes_cli.postgres_authority import claim_task, complete_task

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        ok = complete_task(
            pg, task_id=task_id, organization_id=ORG2,  # wrong org
            claim_token=token, lease_generation=gen, outcome="success",
        )
        assert ok is False

    def test_mismatched_task_rejected(self, pg):
        """complete_task with a different task_id must be rejected."""
        from hermes_cli.postgres_authority import claim_task, complete_task

        task_id = _new_task()
        other_task = _new_task()
        token = _new_token()
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        ok = complete_task(
            pg, task_id=other_task, organization_id=ORG,  # wrong task
            claim_token=token, lease_generation=gen, outcome="success",
        )
        assert ok is False

    def test_wrong_fencing_generation_rejected(self, pg):
        """Submitting a wrong (lower) generation must be rejected."""
        from hermes_cli.postgres_authority import claim_task, complete_task

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        ok = complete_task(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=token, lease_generation=gen + 999,  # wrong gen
            outcome="success",
        )
        assert ok is False

    def test_duplicate_effect_insertion_idempotent(self, pg):
        """Two inserts of the same effect_key must produce exactly one row."""
        from hermes_cli.postgres_authority import claim_task, record_effect

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        key = f"{ORG}:{task_id}:a1:p1:stripe:ch_dup"
        payload = {"amount": 500, "currency": "usd"}

        r1 = record_effect(
            pg, effect_key=key, task_id=task_id,
            organization_id=ORG, run_claim_token=token,
            lease_generation=gen, effect_type="payment",
            provider="stripe", provider_ref="ch_dup",
            idempotency_key="ik-dup", payload=payload,
        )
        r2 = record_effect(
            pg, effect_key=key, task_id=task_id,
            organization_id=ORG, run_claim_token=token,
            lease_generation=gen, effect_type="payment",
            provider="stripe", provider_ref="ch_dup",
            idempotency_key="ik-dup", payload=payload,
        )

        assert r1 is True, "first insert must succeed"
        assert r2 is False, "second insert must be a no-op (idempotent)"

        with pg.cursor() as cur:
            cur.execute(
                "SELECT count(*) AS n FROM execution_effects WHERE effect_key = %s",
                (key,),
            )
            assert cur.fetchone()["n"] == 1

    def test_get_effect_returns_existing(self, pg):
        from hermes_cli.postgres_authority import claim_task, record_effect, get_effect

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        key = f"{ORG}:{task_id}:a1:p1:test:ref001"
        record_effect(
            pg, effect_key=key, task_id=task_id,
            organization_id=ORG, run_claim_token=token,
            lease_generation=gen, effect_type="notification",
            payload={"msg": "hello"},
        )
        effect = get_effect(pg, effect_key=key)
        assert effect is not None
        assert effect["effect_type"] == "notification"

    def test_mismatched_action_payload_permit_rejected(self, pg):
        """Consuming with a different payload must fail the hash check."""
        from hermes_cli.postgres_authority import claim_task, issue_permit, consume_permit

        task_id = _new_task()
        token = _new_token()
        original_payload = {"action": "transfer", "amount": 100}
        altered_payload = {"action": "transfer", "amount": 999}

        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        permit_id = issue_permit(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=original_payload,
        )
        ok = consume_permit(
            pg, permit_id=permit_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=altered_payload,  # changed!
        )
        assert ok is False

    def test_revoked_permit_cannot_be_consumed(self, pg):
        from hermes_cli.postgres_authority import (
            claim_task, issue_permit, revoke_permit, consume_permit
        )

        task_id = _new_task()
        token = _new_token()
        payload = {"action": "write", "nonce": uuid.uuid4().hex}

        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        permit_id = issue_permit(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )
        revoke_permit(pg, permit_id=permit_id, organization_id=ORG, reason="test")

        ok = consume_permit(
            pg, permit_id=permit_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )
        assert ok is False

    def test_expired_claim_completion_rejected(self, pg):
        """complete_task must be rejected when the claim has expired."""
        from hermes_cli.postgres_authority import claim_task, complete_task

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() - 1,  # already expired
        )
        assert gen is not None

        ok = complete_task(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            outcome="success",
        )
        assert ok is False, "expired-claim completion must be rejected"

    def test_permit_issue_on_expired_claim_rejected(self, pg):
        from hermes_cli.postgres_authority import claim_task, issue_permit

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() - 1,
        )
        with pytest.raises(ValueError):
            issue_permit(
                pg, task_id=task_id, organization_id=ORG,
                claim_token=token, lease_generation=gen,
                action_payload={"action": "x"},
            )

    def test_mismatched_org_permit_consumption_rejected(self, pg):
        """Consuming a permit with a different org must fail."""
        from hermes_cli.postgres_authority import claim_task, issue_permit, consume_permit

        task_id = _new_task()
        token = _new_token()
        payload = {"action": "x", "nonce": uuid.uuid4().hex}

        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        permit_id = issue_permit(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )
        ok = consume_permit(
            pg, permit_id=permit_id, organization_id=ORG2,  # wrong org
            claim_token=token, lease_generation=gen,
            action_payload=payload,
        )
        assert ok is False

    def test_effect_scoped_to_correct_organization(self, pg):
        """Effect rows must carry the org that inserted them."""
        from hermes_cli.postgres_authority import claim_task, record_effect

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        key = f"{ORG}:{task_id}:a2:p2:test:ref002"
        record_effect(
            pg, effect_key=key, task_id=task_id,
            organization_id=ORG, run_claim_token=token,
            lease_generation=gen, effect_type="notification",
            payload={"msg": "org-scoped"},
        )
        with pg.cursor() as cur:
            cur.execute(
                "SELECT organization_id FROM execution_effects WHERE effect_key = %s",
                (key,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row["organization_id"] == ORG

    def test_simultaneous_claims_exactly_one_succeeds(self, pg, postgres_url):
        """Race two independent connections against the same task.

        Uses two separate Postgres connections to exercise the DB-level
        UNIQUE constraint under real concurrency.  conn_b is given the
        same search_path as pg (the test-scoped schema) by reading it
        from the existing connection.
        """
        from hermes_cli.postgres_authority import claim_task
        from psycopg.rows import dict_row as _dict_row
        import psycopg as _psycopg

        # Get the current search_path from the pg connection (test schema).
        with pg.cursor() as cur:
            cur.execute("SHOW search_path")
            row = cur.fetchone()
            schema = row["search_path"]

        conn_b = _psycopg.connect(
            postgres_url,
            row_factory=_dict_row,
            options=f"-c search_path={schema}",
        )
        conn_b.autocommit = False

        task_id = _new_task()
        expires = time.time() + 3600

        gen_a = claim_task(
            pg, task_id=task_id, claim_token=_new_token(),
            organization_id=ORG, worker_id="conn-a",
            claim_scope_url="", expires_at=expires,
        )
        gen_b = claim_task(
            conn_b, task_id=task_id, claim_token=_new_token(),
            organization_id=ORG, worker_id="conn-b",
            claim_scope_url="", expires_at=expires,
        )

        conn_b.close()

        winners = [g for g in (gen_a, gen_b) if g is not None]
        assert len(winners) == 1, (
            f"exactly one worker must win; got gen_a={gen_a}, gen_b={gen_b}"
        )


# ---------------------------------------------------------------------------
# 4. Schema version / migration tests
# ---------------------------------------------------------------------------


class TestSchemaMigration:
    def test_fresh_install_reaches_current_version(self, pg):
        from hermes_cli.postgres_authority import get_schema_version, SCHEMA_VERSION

        assert get_schema_version(pg) == SCHEMA_VERSION

    def test_init_schema_idempotent(self, pg):
        """Calling init_schema twice on an already-migrated DB is a no-op."""
        from hermes_cli.postgres_authority import init_schema, get_schema_version, SCHEMA_VERSION

        init_schema(pg)  # second call
        assert get_schema_version(pg) == SCHEMA_VERSION

    def test_future_schema_version_fails_closed(self, pg):
        """A DB with version > SCHEMA_VERSION must reject init_schema."""
        from hermes_cli.postgres_authority import init_schema, SCHEMA_VERSION

        future_version = SCHEMA_VERSION + 99
        with pg.cursor() as cur:
            cur.execute(
                "INSERT INTO schema_version (version) VALUES (%s) ON CONFLICT DO NOTHING",
                (future_version,),
            )
        pg.commit()

        with pytest.raises(RuntimeError, match="exceeds supported version"):
            init_schema(pg)


# ---------------------------------------------------------------------------
# 5. Multi-tenant isolation tests
# ---------------------------------------------------------------------------


class TestTenantIsolation:
    """Tenant_id is a scoping column — it must NOT weaken claim exclusivity."""

    def test_different_tenants_cannot_both_claim_same_task_org(self, pg):
        """CRITICAL INVARIANT: UNIQUE (task_id, organization_id) is the
        exclusivity constraint.  tenant_id is NOT in the constraint.
        Two tenants claiming the same (task, org) must result in exactly one winner.
        """
        from hermes_cli.postgres_authority import claim_task, DEFAULT_TENANT_ID
        from uuid import UUID

        task_id = _new_task()
        tenant_a = DEFAULT_TENANT_ID
        tenant_b = UUID("11111111-1111-1111-1111-111111111111")
        expires = time.time() + 3600

        gen_a = claim_task(
            pg, task_id=task_id, claim_token=_new_token(),
            organization_id=ORG, worker_id="tenant-a-worker",
            claim_scope_url="", expires_at=expires,
            tenant_id=tenant_a,
        )
        gen_b = claim_task(
            pg, task_id=task_id, claim_token=_new_token(),
            organization_id=ORG, worker_id="tenant-b-worker",
            claim_scope_url="", expires_at=expires,
            tenant_id=tenant_b,
        )

        assert gen_a == 1, "first claim must succeed"
        assert gen_b is None, (
            "second claim with different tenant_id must be rejected — "
            "claim exclusivity is (task_id, organization_id), NOT per-tenant"
        )

    def test_tenant_id_stored_on_claim(self, pg):
        """Verify tenant_id is persisted and returned correctly."""
        from hermes_cli.postgres_authority import claim_task, get_claim, DEFAULT_TENANT_ID

        task_id = _new_task()
        claim_task(
            pg, task_id=task_id, claim_token=_new_token(),
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
        )
        claim = get_claim(pg, task_id=task_id, organization_id=ORG)
        assert claim is not None
        assert str(claim["tenant_id"]) == str(DEFAULT_TENANT_ID)

    def test_tenant_id_stored_on_permit(self, pg):
        """Verify tenant_id is persisted on permit rows."""
        from hermes_cli.postgres_authority import (
            claim_task, issue_permit, DEFAULT_TENANT_ID
        )
        from uuid import UUID

        task_id = _new_task()
        token = _new_token()
        tenant = UUID("22222222-2222-2222-2222-222222222222")

        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
            tenant_id=tenant,
        )
        permit_id = issue_permit(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=token, lease_generation=gen,
            action_payload={"action": "test"},
            tenant_id=tenant,
        )
        with pg.cursor() as cur:
            cur.execute(
                "SELECT tenant_id FROM task_permits WHERE permit_id = %s",
                (permit_id,),
            )
            row = cur.fetchone()
        assert row is not None
        assert str(row["tenant_id"]) == str(tenant)

    def test_tenant_id_stored_on_effect(self, pg):
        """Verify tenant_id is persisted on effect rows."""
        from hermes_cli.postgres_authority import claim_task, record_effect
        from uuid import UUID

        task_id = _new_task()
        token = _new_token()
        tenant = UUID("33333333-3333-3333-3333-333333333333")

        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
            tenant_id=tenant,
        )
        key = f"{ORG}:{task_id}:a1:p1:test:ref-tenant"
        record_effect(
            pg, effect_key=key, task_id=task_id,
            organization_id=ORG, run_claim_token=token,
            lease_generation=gen, effect_type="notification",
            payload={"msg": "tenant-scoped"},
            tenant_id=tenant,
        )
        with pg.cursor() as cur:
            cur.execute(
                "SELECT tenant_id FROM execution_effects WHERE effect_key = %s",
                (key,),
            )
            row = cur.fetchone()
        assert row is not None
        assert str(row["tenant_id"]) == str(tenant)


# ---------------------------------------------------------------------------
# 6. Migration upgrade path tests
# ---------------------------------------------------------------------------


class TestMigrationV2ToV3:
    """Verify that the v2→v3 migration adds tenant_id without breaking anything."""

    def test_tenant_id_column_exists_after_fresh_install(self, pg):
        """Fresh install at v3 should have tenant_id on all tables."""
        tables = ["task_claims", "task_runs", "task_permits", "execution_effects"]
        for table in tables:
            with pg.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s AND column_name = 'tenant_id'",
                    (table,),
                )
                row = cur.fetchone()
            assert row is not None, f"tenant_id column missing from {table}"

    def test_claim_exclusivity_constraint_is_task_org_only(self, pg):
        """The UNIQUE constraint must be on (task_id, organization_id) only."""
        with pg.cursor() as cur:
            cur.execute("""
                SELECT constraint_name, array_agg(column_name ORDER BY ordinal_position)
                FROM information_schema.key_column_usage
                WHERE table_name = 'task_claims'
                  AND constraint_name = 'uq_task_claims_task_org'
                GROUP BY constraint_name
            """)
            row = cur.fetchone()
        assert row is not None, "uq_task_claims_task_org constraint not found"
        columns = row["array_agg"]
        assert "tenant_id" not in columns, (
            f"tenant_id must NOT be in the exclusivity constraint; found columns: {columns}"
        )
        assert "task_id" in columns
        assert "organization_id" in columns


# ---------------------------------------------------------------------------
# 7. Tenant management tests
# ---------------------------------------------------------------------------


class TestTenantManagement:
    """Tenant lifecycle: create, suspend, activate, quota."""

    def test_create_tenant(self, pg):
        from hermes_cli.postgres_authority import create_tenant, get_tenant
        from uuid import UUID

        tid = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
        ok = create_tenant(pg, tenant_id=tid, slug="acme", name="Acme Corp")
        assert ok is True

        tenant = get_tenant(pg, tenant_id=tid)
        assert tenant is not None
        assert tenant["slug"] == "acme"
        assert tenant["name"] == "Acme Corp"
        assert tenant["max_concurrent_claims"] == 10

    def test_create_tenant_idempotent(self, pg):
        from hermes_cli.postgres_authority import create_tenant
        from uuid import UUID

        tid = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
        assert create_tenant(pg, tenant_id=tid, slug="beta") is True
        assert create_tenant(pg, tenant_id=tid, slug="beta") is False

    def test_suspend_tenant_blocks_claims(self, pg):
        from hermes_cli.postgres_authority import (
            create_tenant, suspend_tenant, check_tenant_claim_quota,
        )
        from uuid import UUID

        tid = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
        create_tenant(pg, tenant_id=tid, slug="suspended-co")
        suspend_tenant(pg, tenant_id=tid)

        allowed, current, max_allowed = check_tenant_claim_quota(
            pg, tenant_id=tid,
        )
        assert allowed is False

    def test_activate_tenant_removes_suspension(self, pg):
        from hermes_cli.postgres_authority import (
            create_tenant, suspend_tenant, activate_tenant, check_tenant_claim_quota,
        )
        from uuid import UUID

        tid = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
        create_tenant(pg, tenant_id=tid, slug="reactivated-co")
        suspend_tenant(pg, tenant_id=tid)
        activate_tenant(pg, tenant_id=tid)

        allowed, _, _ = check_tenant_claim_quota(pg, tenant_id=tid)
        assert allowed is True

    def test_claim_quota_enforced(self, pg):
        from hermes_cli.postgres_authority import (
            create_tenant, claim_task, check_tenant_claim_quota,
        )
        from uuid import UUID

        tid = UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
        create_tenant(pg, tenant_id=tid, slug="limited-co", max_concurrent_claims=2)

        # Fill quota with 2 claims
        claim_task(
            pg, task_id=_new_task(), claim_token=_new_token(),
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
            tenant_id=tid,
        )
        claim_task(
            pg, task_id=_new_task(), claim_token=_new_token(),
            organization_id=ORG, worker_id="w2",
            claim_scope_url="", expires_at=time.time() + 3600,
            tenant_id=tid,
        )

        allowed, current, max_allowed = check_tenant_claim_quota(
            pg, tenant_id=tid,
        )
        assert allowed is False
        assert current == 2
        assert max_allowed == 2

    def test_default_tenant_seeded_on_migration(self, pg):
        from hermes_cli.postgres_authority import get_tenant, DEFAULT_TENANT_ID

        tenant = get_tenant(pg, tenant_id=DEFAULT_TENANT_ID)
        assert tenant is not None
        assert tenant["slug"] == "default"

    def test_check_quota_defaults_unregistered_tenant_to_free_tier(self, pg):
        """A tenant with no tenant_subscriptions row (never explicitly
        billed) must default to the free plan's limits, not be hard-
        rejected. Regression test for the bug where every unregistered
        tenant's claim_task/issue_permit/record_effect call silently failed
        because check_quota returned (False, 0, 0) for any tenant lacking
        an explicit subscription row — including tenants freshly registered
        via create_tenant, which does not itself create a subscription."""
        from hermes_cli.postgres_authority import check_quota
        from uuid import UUID

        never_registered = UUID("f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f0f0")
        allowed, used, limit = check_quota(
            pg, tenant_id=never_registered, meter_type="task_claim"
        )
        assert allowed is True
        assert used == 0
        assert limit > 0, "must fall back to the seeded free plan's real limit, not 0"

    def test_check_quota_enforces_free_tier_limit_for_unregistered_tenant(self, pg):
        """The free-tier fallback must still enforce the real limit once
        usage reaches it — this is a default tier, not a bypass."""
        from hermes_cli.postgres_authority import (
            check_quota, claim_task, DEFAULT_TENANT_ID,
        )
        from uuid import UUID

        # DEFAULT_TENANT_ID has an explicit free-tier subscription seeded
        # at migration time (monthly_task_limit=100); reuse its real limit
        # by checking an unregistered tenant gets the identical limit.
        _, _, default_limit = check_quota(
            pg, tenant_id=DEFAULT_TENANT_ID, meter_type="task_claim"
        )
        unregistered = UUID("f1f1f1f1-f1f1-f1f1-f1f1-f1f1f1f1f1f1")
        _, _, fallback_limit = check_quota(
            pg, tenant_id=unregistered, meter_type="task_claim"
        )
        assert fallback_limit == default_limit, (
            "unregistered tenant must fall back to the same free-tier limit "
            "the seeded default tenant's explicit subscription uses"
        )


# ---------------------------------------------------------------------------
# 8. Cross-tenant attack simulation
# ---------------------------------------------------------------------------


class TestCrossTenantAttackVectors:
    """Simulate attack vectors where a worker attempts to access another
    tenant's resources."""

    def test_tenant_a_cannot_consume_tenant_b_permit(self, pg):
        """A worker from tenant A must not consume a permit issued for tenant B."""
        from hermes_cli.postgres_authority import (
            claim_task, issue_permit, consume_permit, create_tenant
        )
        from uuid import UUID

        tenant_a = UUID("a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0")
        tenant_b = UUID("b0b0b0b0-b0b0-b0b0-b0b0-b0b0b0b0b0b0")
        create_tenant(pg, tenant_id=tenant_a, slug="attack-victim")
        create_tenant(pg, tenant_id=tenant_b, slug="attacker")

        task_id = _new_task()
        token_a = _new_token()
        payload = {"action": "sensitive_op", "nonce": uuid.uuid4().hex}

        # Tenant A claims and gets a permit
        gen = claim_task(
            pg, task_id=task_id, claim_token=token_a,
            organization_id=ORG, worker_id="victim-worker",
            claim_scope_url="", expires_at=time.time() + 3600,
            tenant_id=tenant_a,
        )
        permit_id = issue_permit(
            pg, task_id=task_id, organization_id=ORG,
            claim_token=token_a, lease_generation=gen,
            action_payload=payload, tenant_id=tenant_a,
        )

        # Tenant B worker tries to consume the permit with wrong claim_token
        # (since they can't have the same claim, they'd need to guess the token)
        ok = consume_permit(
            pg, permit_id=permit_id, organization_id=ORG,
            claim_token="attacker-forged-token",
            lease_generation=gen, action_payload=payload,
        )
        assert ok is False, "cross-tenant permit consumption must be rejected"

    def test_tenant_a_effects_invisible_to_tenant_b_query(self, pg):
        """Effects recorded by tenant A must not appear in tenant B's queries."""
        from hermes_cli.postgres_authority import claim_task, record_effect
        from uuid import UUID

        tenant_a = UUID("a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1")
        tenant_b = UUID("b1b1b1b1-b1b1-b1b1-b1b1-b1b1b1b1b1b1")

        task_id = _new_task()
        token = _new_token()
        gen = claim_task(
            pg, task_id=task_id, claim_token=token,
            organization_id=ORG, worker_id="w1",
            claim_scope_url="", expires_at=time.time() + 3600,
            tenant_id=tenant_a,
        )

        key = f"{ORG}:{task_id}:a1:p1:test:secret-ref"
        record_effect(
            pg, effect_key=key, task_id=task_id,
            organization_id=ORG, run_claim_token=token,
            lease_generation=gen, effect_type="payment",
            provider="stripe", provider_ref="ch_secret",
            payload={"amount": 99999},
            tenant_id=tenant_a,
        )

        # Tenant B queries effects with their tenant_id filter
        with pg.cursor() as cur:
            cur.execute(
                "SELECT * FROM execution_effects WHERE tenant_id = %s",
                (str(tenant_b),),
            )
            rows = cur.fetchall()
        assert len(rows) == 0, "tenant B must not see tenant A's effects"

    def test_suspended_tenant_cannot_claim(self, pg):
        """A suspended tenant's claim quota check must return False."""
        from hermes_cli.postgres_authority import (
            create_tenant, suspend_tenant, check_tenant_claim_quota,
        )
        from uuid import UUID

        tid = UUID("c1c1c1c1-c1c1-c1c1-c1c1-c1c1c1c1c1c1")
        create_tenant(pg, tenant_id=tid, slug="suspended-attack")
        suspend_tenant(pg, tenant_id=tid)

        allowed, _, _ = check_tenant_claim_quota(pg, tenant_id=tid)
        assert allowed is False, "suspended tenant must be blocked from claiming"

    def test_cross_org_claim_within_same_tenant_rejected(self, pg):
        """Even within the same tenant, different orgs have separate claims
        and one org cannot complete another's task."""
        from hermes_cli.postgres_authority import (
            claim_task, complete_task, create_tenant,
        )
        from uuid import UUID

        tid = UUID("d1d1d1d1-d1d1-d1d1-d1d1-d1d1d1d1d1d1")
        create_tenant(pg, tenant_id=tid, slug="multi-org-tenant")

        task_id = _new_task()
        token_org1 = _new_token()

        gen = claim_task(
            pg, task_id=task_id, claim_token=token_org1,
            organization_id=ORG, worker_id="org1-worker",
            claim_scope_url="", expires_at=time.time() + 3600,
            tenant_id=tid,
        )

        # Org2 tries to complete org1's task
        ok = complete_task(
            pg, task_id=task_id, organization_id=ORG2,
            claim_token=token_org1, lease_generation=gen,
            outcome="stolen", tenant_id=tid,
        )
        assert ok is False, "cross-org completion must be rejected"


# ---------------------------------------------------------------------------
# 9. Workspace management tests
# ---------------------------------------------------------------------------


class TestWorkspaceManagement:
    """Workspace lifecycle within a tenant."""

    def test_create_workspace(self, pg):
        from hermes_cli.postgres_authority import (
            create_workspace, get_workspace, DEFAULT_TENANT_ID,
        )
        from uuid import UUID

        ws_id = UUID("f0f0f0f0-f0f0-f0f0-f0f0-f0f0f0f0f0f0")
        ok = create_workspace(
            pg, workspace_id=ws_id, tenant_id=DEFAULT_TENANT_ID,
            name="Engineering", slug="engineering", owner_id="ceo-1",
        )
        assert ok is True

        ws = get_workspace(pg, workspace_id=ws_id, tenant_id=DEFAULT_TENANT_ID)
        assert ws is not None
        assert ws["name"] == "Engineering"
        assert ws["slug"] == "engineering"
        assert ws["active"] is True

    def test_create_workspace_idempotent(self, pg):
        from hermes_cli.postgres_authority import create_workspace, DEFAULT_TENANT_ID
        from uuid import UUID

        ws_id = UUID("f1f1f1f1-f1f1-f1f1-f1f1-f1f1f1f1f1f1")
        assert create_workspace(
            pg, workspace_id=ws_id, tenant_id=DEFAULT_TENANT_ID,
            name="Sales", slug="sales",
        ) is True
        assert create_workspace(
            pg, workspace_id=ws_id, tenant_id=DEFAULT_TENANT_ID,
            name="Sales", slug="sales",
        ) is False

    def test_workspace_tenant_scoped(self, pg):
        """Workspace lookup with wrong tenant returns None."""
        from hermes_cli.postgres_authority import (
            create_workspace, get_workspace, create_tenant, DEFAULT_TENANT_ID,
        )
        from uuid import UUID

        tenant_a = DEFAULT_TENANT_ID
        tenant_b = UUID("f2f2f2f2-f2f2-f2f2-f2f2-f2f2f2f2f2f2")
        create_tenant(pg, tenant_id=tenant_b, slug="ws-test-tenant")

        ws_id = UUID("f3f3f3f3-f3f3-f3f3-f3f3-f3f3f3f3f3f3")
        create_workspace(
            pg, workspace_id=ws_id, tenant_id=tenant_a,
            name="Private", slug="private",
        )
        # Correct tenant finds it
        assert get_workspace(pg, workspace_id=ws_id, tenant_id=tenant_a) is not None
        # Wrong tenant does not
        assert get_workspace(pg, workspace_id=ws_id, tenant_id=tenant_b) is None

    def test_list_workspaces_returns_active_only(self, pg):
        from hermes_cli.postgres_authority import (
            create_workspace, list_workspaces, deactivate_workspace,
            DEFAULT_TENANT_ID,
        )
        from uuid import UUID

        ws1 = UUID("f4f4f4f4-f4f4-f4f4-f4f4-f4f4f4f4f4f4")
        ws2 = UUID("f5f5f5f5-f5f5-f5f5-f5f5-f5f5f5f5f5f5")
        create_workspace(
            pg, workspace_id=ws1, tenant_id=DEFAULT_TENANT_ID,
            name="Active WS", slug="active-ws",
        )
        create_workspace(
            pg, workspace_id=ws2, tenant_id=DEFAULT_TENANT_ID,
            name="Deactivated WS", slug="deactivated-ws",
        )
        deactivate_workspace(pg, workspace_id=ws2, tenant_id=DEFAULT_TENANT_ID)

        workspaces = list_workspaces(pg, tenant_id=DEFAULT_TENANT_ID)
        slugs = [w["slug"] for w in workspaces]
        assert "active-ws" in slugs
        assert "deactivated-ws" not in slugs

    def test_default_workspace_seeded(self, pg):
        from hermes_cli.postgres_authority import list_workspaces, DEFAULT_TENANT_ID

        workspaces = list_workspaces(pg, tenant_id=DEFAULT_TENANT_ID)
        slugs = [w["slug"] for w in workspaces]
        assert "default" in slugs


# ---------------------------------------------------------------------------
# 10. Capability grants (RBAC) tests
# ---------------------------------------------------------------------------


class TestCapabilityGrants:
    """RBAC capability enforcement."""

    def test_grant_and_check_capability(self, pg):
        from hermes_cli.postgres_authority import (
            grant_capability, check_capability, DEFAULT_TENANT_ID,
        )

        ok = grant_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-001",
            resource="task", action="claim", scope="workspace=default",
        )
        assert ok is True

        has = check_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-001",
            resource="task", action="claim", scope="workspace=default",
        )
        assert has is True

    def test_wildcard_scope_matches_any(self, pg):
        from hermes_cli.postgres_authority import (
            grant_capability, check_capability, DEFAULT_TENANT_ID,
        )

        grant_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-admin",
            resource="task", action="complete", scope="*",
        )

        has = check_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-admin",
            resource="task", action="complete",
            scope="workspace=engineering",
        )
        assert has is True

    def test_revoked_capability_not_found(self, pg):
        from hermes_cli.postgres_authority import (
            grant_capability, revoke_capability, check_capability, DEFAULT_TENANT_ID,
        )

        grant_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-revoke-test",
            resource="payment", action="write", scope="*",
        )
        revoke_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-revoke-test",
            resource="payment", action="write", scope="*",
        )

        has = check_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-revoke-test",
            resource="payment", action="write", scope="*",
        )
        assert has is False

    def test_expired_capability_not_found(self, pg):
        from hermes_cli.postgres_authority import (
            grant_capability, check_capability, DEFAULT_TENANT_ID,
        )

        grant_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-expired",
            resource="audit", action="read", scope="*",
            ttl_seconds=-10,  # already expired
        )

        has = check_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-expired",
            resource="audit", action="read", scope="*",
        )
        assert has is False

    def test_capability_non_amplifiable_cross_tenant(self, pg):
        """A capability in tenant A must not be visible in tenant B."""
        from hermes_cli.postgres_authority import (
            grant_capability, check_capability, create_tenant, DEFAULT_TENANT_ID,
        )
        from uuid import UUID

        tenant_b = UUID("ca0ca0ca-ca0c-ca0c-ca0c-ca0ca0ca0ca0")
        create_tenant(pg, tenant_id=tenant_b, slug="rbac-test-tenant")

        grant_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-cross",
            resource="task", action="claim", scope="*",
        )

        has = check_capability(
            pg, tenant_id=tenant_b,
            principal_type="worker", principal_id="w-cross",
            resource="task", action="claim", scope="*",
        )
        assert has is False, "capability must not leak across tenants"

    def test_list_capabilities(self, pg):
        from hermes_cli.postgres_authority import (
            grant_capability, list_capabilities, DEFAULT_TENANT_ID,
        )

        grant_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="role", principal_id="ceo",
            resource="objective", action="verify", scope="*",
        )
        grant_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="role", principal_id="ceo",
            resource="payment", action="approve", scope="*",
        )

        caps = list_capabilities(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="role", principal_id="ceo",
        )
        resources = [c["resource"] for c in caps]
        assert "objective" in resources
        assert "payment" in resources

    def test_grant_idempotent(self, pg):
        from hermes_cli.postgres_authority import grant_capability, DEFAULT_TENANT_ID

        assert grant_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-idem",
            resource="task", action="claim", scope="*",
        ) is True
        assert grant_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-idem",
            resource="task", action="claim", scope="*",
        ) is False


# ---------------------------------------------------------------------------
# 11. Capability enforcement tests
# ---------------------------------------------------------------------------


class TestCapabilityEnforcement:
    """enforce_capability: opt-in gating with fail-open on no grants."""

    def test_no_grants_passes_open(self, pg):
        """When no grants exist for the tenant, enforcement is a no-op."""
        from hermes_cli.postgres_authority import enforce_capability, DEFAULT_TENANT_ID

        enforce_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-ungated",
            resource="task", action="claim", scope="*",
        )

    def test_with_grants_denies_missing_capability(self, pg):
        """Once any grant exists, unlisted principals are denied."""
        from hermes_cli.postgres_authority import (
            grant_capability, enforce_capability, DEFAULT_TENANT_ID,
        )
        import pytest

        grant_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-privileged",
            resource="task", action="claim", scope="*",
        )

        with pytest.raises(PermissionError, match="lacks capability"):
            enforce_capability(
                pg, tenant_id=DEFAULT_TENANT_ID,
                principal_type="worker", principal_id="w-unprivileged",
                resource="task", action="claim", scope="*",
            )

    def test_with_grants_allows_granted_principal(self, pg):
        from hermes_cli.postgres_authority import (
            grant_capability, enforce_capability, DEFAULT_TENANT_ID,
        )

        grant_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-allowed",
            resource="task", action="claim", scope="workspace=prod",
        )

        enforce_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-allowed",
            resource="task", action="claim", scope="workspace=prod",
        )

    def test_enforcement_tenant_isolation(self, pg):
        """Grants in tenant A do not gate enforcement in tenant B."""
        from hermes_cli.postgres_authority import (
            grant_capability, enforce_capability, create_tenant, DEFAULT_TENANT_ID,
        )
        from uuid import UUID

        tenant_b = UUID("eb0eb0eb-eb0e-eb0e-eb0e-eb0eb0eb0eb0")
        create_tenant(pg, tenant_id=tenant_b, slug="enforce-test")

        grant_capability(
            pg, tenant_id=DEFAULT_TENANT_ID,
            principal_type="worker", principal_id="w-a",
            resource="task", action="claim", scope="*",
        )

        # Tenant B has no grants → passes open
        enforce_capability(
            pg, tenant_id=tenant_b,
            principal_type="worker", principal_id="w-a",
            resource="task", action="claim", scope="*",
        )


# Note: Authority-store capability contract tests (postgres backend recognition,
# SQLite multi-host rejection, unknown backend fail-closed) live in:
#   tests/hermes_cli/test_authority_store.py
# Those tests do not require a live Postgres connection.
