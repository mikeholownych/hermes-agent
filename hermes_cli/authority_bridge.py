"""Bridge between the objective workflow layer and the Postgres authority store.

When the authority backend is Postgres, this module mirrors critical
authority transitions (claim, permit, effect, complete) from the SQLite
workflow database into the Postgres coordination store. This provides:

  - Multi-host claim exclusivity via Postgres UNIQUE constraints
  - Lease-generation fencing for stale worker rejection
  - Tenant-scoped isolation of all authoritative records
  - Idempotent effect recording for crash recovery

The bridge is opt-in: when the backend is SQLite, it is a no-op.
All Postgres operations are additive — the SQLite workflow layer remains
the source of truth for objective lifecycle and planning. The Postgres
store provides the distributed fencing and coordination guarantees.
"""

from __future__ import annotations

import os
import time
from typing import Any, Optional
from uuid import UUID


def _postgres_configured() -> bool:
    return bool(
        os.environ.get("AUTHORITY_POSTGRES_URL")
        or os.environ.get("DATABASE_URL")
    )


def _resolve_tenant_id() -> Optional[UUID]:
    """Resolve the current tenant from session context or environment."""
    try:
        from gateway.session_context import get_tenant_id
        tid = get_tenant_id()
        if tid:
            return UUID(tid)
    except (ImportError, ValueError):
        pass
    raw = os.environ.get("HERMES_TENANT_ID", "").strip()
    if raw:
        try:
            return UUID(raw)
        except ValueError:
            pass
    return None


def _get_connection():
    """Get a Postgres connection, or None if not configured.

    Only "not configured" (checked above) makes a missing bridge
    legitimate. Once configured, a connection or schema-init failure must
    NOT silently degrade to "bridge inactive" — that would let a worker
    proceed on the SQLite side alone while believing Postgres coordination
    is unavailable for reasons an operator never sees, exactly the kind of
    silent authority-weakening fallback this store is meant to prevent.
    """
    if not _postgres_configured():
        return None
    from hermes_cli.postgres_authority import connect, init_schema
    conn = connect()
    init_schema(conn)
    return conn


class AuthorityBridge:
    """Stateful bridge for a single worker's authority operations.

    Holds the Postgres connection and tracks the current claim state
    (task_id, organization_id, lease_generation, claim_token) for
    fencing downstream operations.
    """

    def __init__(self, *, organization_id: str, worker_id: str):
        self.organization_id = organization_id
        self.worker_id = worker_id
        self.tenant_id = _resolve_tenant_id()
        self._conn = _get_connection()
        self._task_id: Optional[str] = None
        self._claim_token: Optional[str] = None
        self._lease_generation: Optional[int] = None
        self._objective_id: Optional[str] = None
        # Remembered from issue_permit() so consume_permit() can re-supply
        # them for exact-action/exact-target re-validation, rather than
        # silently skipping the check this bridge exists to enforce.
        self._permit_executor: Optional[str] = None
        self._permit_capability: Optional[str] = None
        self._permit_target_resource: Optional[str] = None
        self._permit_policy_version: Optional[str] = None

    @property
    def active(self) -> bool:
        return self._conn is not None

    @property
    def has_claim(self) -> bool:
        return self._lease_generation is not None

    def claim(
        self,
        *,
        task_id: str,
        claim_token: str,
        claim_scope_url: str = "",
        ttl_seconds: int = 300,
        objective_id: Optional[str] = None,
    ) -> Optional[int]:
        """Register a claim in the Postgres store.

        Returns the lease_generation if successful, None if the claim
        is already held by another worker.

        objective_id, if supplied, is remembered and forwarded to
        consume_permit() so consumption can be gated on the mirrored
        objective-lifecycle status and organization autonomy mode (see
        postgres_authority.consume_permit's objective_id parameter).
        """
        if not self.active:
            return None
        from hermes_cli.postgres_authority import claim_task

        gen = claim_task(
            self._conn,
            task_id=task_id,
            claim_token=claim_token,
            organization_id=self.organization_id,
            worker_id=self.worker_id,
            claim_scope_url=claim_scope_url,
            expires_at=time.time() + ttl_seconds,
            tenant_id=self.tenant_id,
        )
        if gen is not None:
            self._task_id = task_id
            self._claim_token = claim_token
            self._lease_generation = gen
            self._objective_id = objective_id
        return gen

    def issue_permit(
        self,
        *,
        actor: str,
        executor: str,
        capability: str,
        action_type: str,
        target_resource: str,
        action_payload: dict[str, Any],
        ttl_seconds: int = 300,
        policy_version: str = "",
    ) -> Optional[str]:
        """Issue a permit in the Postgres store against the current claim.

        Returns the permit_id if successful, None if bridge is inactive.
        Raises ValueError if no claim is held.

        Remembers executor/capability/target_resource/policy_version so
        consume_permit() can re-supply them for exact-action/exact-target
        re-validation at consumption time.
        """
        if not self.active:
            return None
        if not self.has_claim:
            raise ValueError("cannot issue permit without an active claim")
        from hermes_cli.postgres_authority import issue_permit

        permit_id = issue_permit(
            self._conn,
            task_id=self._task_id,
            organization_id=self.organization_id,
            claim_token=self._claim_token,
            lease_generation=self._lease_generation,
            actor=actor,
            executor=executor,
            capability=capability,
            action_type=action_type,
            target_resource=target_resource,
            action_payload=action_payload,
            ttl_seconds=ttl_seconds,
            policy_version=policy_version,
            tenant_id=self.tenant_id,
        )
        self._permit_executor = executor
        self._permit_capability = capability
        self._permit_target_resource = target_resource
        self._permit_policy_version = policy_version
        return permit_id

    def record_effect(
        self,
        *,
        effect_key: str,
        effect_type: str,
        permit_id: str = "",
        provider: str = "",
        provider_ref: str = "",
        idempotency_key: str = "",
        payload: dict[str, Any],
    ) -> Optional[bool]:
        """Record an execution effect in the Postgres store.

        Returns True if new, False if idempotent duplicate, None if inactive.
        """
        if not self.active:
            return None
        if not self.has_claim:
            raise ValueError("cannot record effect without an active claim")
        from hermes_cli.postgres_authority import record_effect

        return record_effect(
            self._conn,
            effect_key=effect_key,
            task_id=self._task_id,
            organization_id=self.organization_id,
            run_claim_token=self._claim_token,
            lease_generation=self._lease_generation,
            permit_id=permit_id,
            effect_type=effect_type,
            provider=provider,
            provider_ref=provider_ref,
            idempotency_key=idempotency_key,
            payload=payload,
            tenant_id=self.tenant_id,
        )

    def consume_permit(
        self,
        *,
        permit_id: str,
        action_payload: dict[str, Any],
    ) -> Optional[bool]:
        """Consume a permit in the Postgres store.

        Returns True if consumed, False if check fails, None if inactive.

        Re-supplies the executor/capability/target_resource/policy_version
        remembered from issue_permit(), and the objective_id remembered
        from claim(), so consumption is gated on exact-action/exact-target
        matching and (when an objective_id was supplied) mirrored
        objective-lifecycle/autonomy state — previously this call forwarded
        none of that and consumption succeeded on fencing/payload alone.
        """
        if not self.active:
            return None
        if not self.has_claim:
            raise ValueError("cannot consume permit without an active claim")
        from hermes_cli.postgres_authority import consume_permit

        return consume_permit(
            self._conn,
            permit_id=permit_id,
            organization_id=self.organization_id,
            claim_token=self._claim_token,
            lease_generation=self._lease_generation,
            action_payload=action_payload,
            executor=self._permit_executor or "",
            capability=self._permit_capability or "",
            target_resource=self._permit_target_resource or "",
            policy_version=self._permit_policy_version or None,
            objective_id=self._objective_id,
        )

    def complete(self, *, outcome: str) -> Optional[bool]:
        """Complete the current task in the Postgres store.

        Returns True if completed, False if fencing fails, None if inactive.
        """
        if not self.active:
            return None
        if not self.has_claim:
            raise ValueError("cannot complete without an active claim")
        from hermes_cli.postgres_authority import complete_task

        result = complete_task(
            self._conn,
            task_id=self._task_id,
            organization_id=self.organization_id,
            claim_token=self._claim_token,
            lease_generation=self._lease_generation,
            outcome=outcome,
            tenant_id=self.tenant_id,
        )
        if result:
            self._task_id = None
            self._claim_token = None
            self._lease_generation = None
        return result

    def release(self) -> Optional[bool]:
        """Release the current claim without completing."""
        if not self.active:
            return None
        if not self.has_claim:
            return False
        from hermes_cli.postgres_authority import release_claim

        result = release_claim(
            self._conn,
            task_id=self._task_id,
            organization_id=self.organization_id,
            claim_token=self._claim_token,
            lease_generation=self._lease_generation,
        )
        self._task_id = None
        self._claim_token = None
        self._lease_generation = None
        return result

    def close(self) -> None:
        """Close the Postgres connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
