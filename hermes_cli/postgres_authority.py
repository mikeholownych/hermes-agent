"""PostgreSQL authority store for production Charterforge deployments.

This module provides a Postgres-backed authority store for governed workers,
enabling:

  - Multi-worker coordination across containers/processes
  - Durable claim storage with atomic exclusivity per (task_id, organization_id)
  - Monotonically increasing lease_generation for fencing stale workers
  - Transactional run tracking with generation-fenced CAS
  - Idempotent execution-effect recording with stable effect_key
  - Full permit model matching the SQLite authority invariants

Design invariants:
  - Exactly one active claim per (task_id, organization_id) — enforced by
    UNIQUE (task_id, organization_id) on task_claims.
  - Every claim carries a lease_generation column that increments on each
    reclaim.  All write operations (release, complete, permit consume, effect
    insert) require the caller to supply the correct generation; a mismatched
    generation is an unconditional rejection.
  - No timestamp-only authority ordering.  Fencing token (generation) is the
    primary ordering mechanism; expiry is a secondary reclaim trigger.
  - Timestamps are passed as timezone-aware Python datetime objects through
    bound parameters — never as interpolated SQL fragments.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None  # type: ignore

# ---------------------------------------------------------------------------
# Schema versioning
# ---------------------------------------------------------------------------

# Current schema version.  Startup fails closed if the DB is on an
# unsupported version (higher than this) or a lower version that cannot
# be migrated automatically.
SCHEMA_VERSION = 13

# Each entry describes one migration step: (from_version, sql).
# Migrations are applied in order when current_version < SCHEMA_VERSION.
# The SQL must be idempotent where PostgreSQL supports it.
_MIGRATIONS: list[tuple[int, str]] = [
    # v0 → v1: initial tables
    (
        0,
        """
        CREATE TABLE IF NOT EXISTS task_claims (
            id              BIGSERIAL PRIMARY KEY,
            task_id         TEXT        NOT NULL,
            organization_id TEXT        NOT NULL,
            tenant_id       UUID        NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
            worker_id       TEXT,
            claim_scope_url TEXT,
            lease_generation BIGINT     NOT NULL DEFAULT 1,
            expires_at      TIMESTAMPTZ NOT NULL,
            claimed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            -- Exactly one active claim per task+org (tenant_id is NOT in this
            -- constraint — it is a row-level scoping column, not an exclusivity key).
            CONSTRAINT uq_task_claims_task_org UNIQUE (task_id, organization_id)
        );

        CREATE INDEX IF NOT EXISTS idx_task_claims_task_id
            ON task_claims(task_id);
        CREATE INDEX IF NOT EXISTS idx_task_claims_expires_at
            ON task_claims(expires_at);
        CREATE INDEX IF NOT EXISTS idx_task_claims_organization
            ON task_claims(organization_id);
        CREATE INDEX IF NOT EXISTS idx_task_claims_tenant
            ON task_claims(tenant_id);

        CREATE TABLE IF NOT EXISTS task_runs (
            id              BIGSERIAL   PRIMARY KEY,
            task_id         TEXT        NOT NULL,
            organization_id TEXT        NOT NULL,
            tenant_id       UUID        NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
            -- claim_token ties this run to its claim snapshot; not a FK
            -- so we keep the run row after the claim is deleted.
            claim_token     TEXT        NOT NULL,
            lease_generation BIGINT     NOT NULL,
            status          TEXT        NOT NULL DEFAULT 'pending',
            outcome         TEXT,
            started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            ended_at        TIMESTAMPTZ,

            CONSTRAINT uq_task_runs_task_org_gen
                UNIQUE (task_id, organization_id, lease_generation)
        );

        CREATE INDEX IF NOT EXISTS idx_task_runs_task_id
            ON task_runs(task_id);
        CREATE INDEX IF NOT EXISTS idx_task_runs_status
            ON task_runs(status);
        CREATE INDEX IF NOT EXISTS idx_task_runs_tenant
            ON task_runs(tenant_id);

        CREATE TABLE IF NOT EXISTS task_permits (
            id              BIGSERIAL   PRIMARY KEY,
            permit_id       TEXT        NOT NULL UNIQUE,
            task_id         TEXT        NOT NULL,
            organization_id TEXT        NOT NULL,
            tenant_id       UUID        NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
            claim_token     TEXT        NOT NULL,
            lease_generation BIGINT     NOT NULL,
            -- Full authority model fields (parity with SQLite)
            actor           TEXT        NOT NULL DEFAULT '',
            executor        TEXT        NOT NULL DEFAULT '',
            capability      TEXT        NOT NULL DEFAULT '',
            action_type     TEXT        NOT NULL DEFAULT '',
            target_resource TEXT        NOT NULL DEFAULT '',
            payload_hash    TEXT        NOT NULL,
            policy_version  TEXT        NOT NULL DEFAULT '',
            action_payload  JSONB       NOT NULL,
            issued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            consumed_at     TIMESTAMPTZ,
            expires_at      TIMESTAMPTZ NOT NULL,
            revoked_at      TIMESTAMPTZ,
            revocation_reason TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_task_permits_task_id
            ON task_permits(task_id);
        CREATE INDEX IF NOT EXISTS idx_task_permits_permit_id
            ON task_permits(permit_id);
        CREATE INDEX IF NOT EXISTS idx_task_permits_expires_at
            ON task_permits(expires_at);
        CREATE INDEX IF NOT EXISTS idx_task_permits_tenant
            ON task_permits(tenant_id);

        CREATE TABLE IF NOT EXISTS execution_effects (
            id              BIGSERIAL   PRIMARY KEY,
            -- Stable identity for idempotency: if the same governed action
            -- is replayed by a recovery worker the effect_key is identical
            -- and the INSERT ... ON CONFLICT is a no-op.
            effect_key      TEXT        NOT NULL UNIQUE,
            task_id         TEXT        NOT NULL,
            organization_id TEXT        NOT NULL,
            tenant_id       UUID        NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
            run_claim_token TEXT        NOT NULL,
            lease_generation BIGINT     NOT NULL,
            permit_id       TEXT,
            effect_type     TEXT        NOT NULL,
            provider        TEXT        NOT NULL DEFAULT '',
            provider_ref    TEXT        NOT NULL DEFAULT '',
            idempotency_key TEXT        NOT NULL DEFAULT '',
            payload         JSONB       NOT NULL,
            recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_execution_effects_task_id
            ON execution_effects(task_id);
        CREATE INDEX IF NOT EXISTS idx_execution_effects_effect_key
            ON execution_effects(effect_key);
        CREATE INDEX IF NOT EXISTS idx_execution_effects_tenant
            ON execution_effects(tenant_id);
        """,
    ),
    # v1 → v2: schema_version bookkeeping table
    (
        1,
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version     INTEGER     PRIMARY KEY,
            applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            description TEXT
        );
        """,
    ),
    # v2 → v3: add tenant_id for multi-tenant isolation (RFC-0.23.0).
    # tenant_id is a partitioning/scoping column — NOT part of the claim
    # exclusivity constraint.  The invariant remains: one active claim per
    # (task_id, organization_id).  tenant_id defaults to a fixed UUID so
    # existing single-tenant deployments migrate without data fixup.
    (
        2,
        """
        -- Default tenant for single-tenant deployments.
        ALTER TABLE task_claims
            ADD COLUMN IF NOT EXISTS tenant_id UUID
            NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000';
        CREATE INDEX IF NOT EXISTS idx_task_claims_tenant
            ON task_claims(tenant_id);

        ALTER TABLE task_runs
            ADD COLUMN IF NOT EXISTS tenant_id UUID
            NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000';
        CREATE INDEX IF NOT EXISTS idx_task_runs_tenant
            ON task_runs(tenant_id);

        ALTER TABLE task_permits
            ADD COLUMN IF NOT EXISTS tenant_id UUID
            NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000';
        CREATE INDEX IF NOT EXISTS idx_task_permits_tenant
            ON task_permits(tenant_id);

        ALTER TABLE execution_effects
            ADD COLUMN IF NOT EXISTS tenant_id UUID
            NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000';
        CREATE INDEX IF NOT EXISTS idx_execution_effects_tenant
            ON execution_effects(tenant_id);
        """,
    ),
    # v3 → v4: tenants registry table + per-tenant concurrent claim limit.
    (
        3,
        """
        CREATE TABLE IF NOT EXISTS tenants (
            id              UUID        PRIMARY KEY,
            slug            TEXT        NOT NULL UNIQUE,
            name            TEXT        NOT NULL DEFAULT '',
            max_concurrent_claims INTEGER NOT NULL DEFAULT 10,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            active_at       TIMESTAMPTZ,
            suspended_at    TIMESTAMPTZ
        );

        -- Seed the default tenant so existing rows satisfy FK if added later.
        INSERT INTO tenants (id, slug, name)
        VALUES ('00000000-0000-0000-0000-000000000000', 'default', 'Default Tenant')
        ON CONFLICT (id) DO NOTHING;
        """,
    ),
    # v4 → v5: workspaces table for multi-workspace isolation within a tenant.
    (
        4,
        """
        CREATE TABLE IF NOT EXISTS workspaces (
            id              UUID        PRIMARY KEY,
            tenant_id       UUID        NOT NULL,
            name            TEXT        NOT NULL,
            slug            TEXT        NOT NULL,
            owner_id        TEXT        NOT NULL DEFAULT '',
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            active          BOOLEAN     NOT NULL DEFAULT true,

            CONSTRAINT uq_workspaces_tenant_slug UNIQUE (tenant_id, slug)
        );

        CREATE INDEX IF NOT EXISTS idx_workspaces_tenant
            ON workspaces(tenant_id);

        -- Seed the default workspace for the default tenant.
        INSERT INTO workspaces (id, tenant_id, name, slug, owner_id)
        VALUES (
            '00000000-0000-0000-0000-000000000001',
            '00000000-0000-0000-0000-000000000000',
            'Default Workspace',
            'default',
            ''
        )
        ON CONFLICT (id) DO NOTHING;
        """,
    ),
    # v5 → v6: capability grants for RBAC enforcement.
    # Capabilities follow the grammar: resource:action:scope=value
    # Grants are never amplifiable — a worker cannot gain more permission
    # than its credential provides.
    (
        5,
        """
        CREATE TABLE IF NOT EXISTS capability_grants (
            id              BIGSERIAL   PRIMARY KEY,
            tenant_id       UUID        NOT NULL,
            workspace_id    UUID,
            -- The principal receiving the grant (worker_id, actor_id, role slug)
            principal_type  TEXT        NOT NULL,
            principal_id    TEXT        NOT NULL,
            -- Capability triple: resource:action:scope
            resource        TEXT        NOT NULL,
            action          TEXT        NOT NULL,
            scope           TEXT        NOT NULL DEFAULT '*',
            -- Grant metadata
            granted_by      TEXT        NOT NULL DEFAULT '',
            granted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at      TIMESTAMPTZ,
            revoked_at      TIMESTAMPTZ,

            CONSTRAINT uq_capability_grant
                UNIQUE (tenant_id, principal_type, principal_id, resource, action, scope)
        );

        CREATE INDEX IF NOT EXISTS idx_capability_grants_principal
            ON capability_grants(tenant_id, principal_type, principal_id);
        CREATE INDEX IF NOT EXISTS idx_capability_grants_resource
            ON capability_grants(tenant_id, resource, action);
        """,
    ),
    # v6 → v7: billing engine tables (RFC-0.24.0).
    (
        6,
        """
        CREATE TABLE IF NOT EXISTS billing_plans (
            plan_id         TEXT        PRIMARY KEY,
            name            TEXT        NOT NULL,
            tier            TEXT        NOT NULL
                CHECK (tier IN ('free', 'starter', 'pro', 'enterprise')),
            monthly_task_limit    INTEGER NOT NULL DEFAULT 0,
            monthly_permit_limit  INTEGER NOT NULL DEFAULT 0,
            monthly_effect_limit  INTEGER NOT NULL DEFAULT 0,
            price_cents     INTEGER     NOT NULL DEFAULT 0,
            stripe_price_id TEXT,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        -- Seed a free plan so every tenant has a usable default.
        INSERT INTO billing_plans (plan_id, name, tier, monthly_task_limit,
                                   monthly_permit_limit, monthly_effect_limit, price_cents)
        VALUES ('free', 'Free', 'free', 100, 100, 500, 0)
        ON CONFLICT (plan_id) DO NOTHING;

        CREATE TABLE IF NOT EXISTS tenant_subscriptions (
            tenant_id       UUID        NOT NULL PRIMARY KEY,
            plan_id         TEXT        NOT NULL REFERENCES billing_plans(plan_id),
            stripe_customer_id    TEXT,
            stripe_subscription_id TEXT,
            billing_cycle_start   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            billing_cycle_end     TIMESTAMPTZ NOT NULL
                DEFAULT (NOW() + INTERVAL '30 days'),
            status          TEXT        NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'past_due', 'canceled', 'trialing')),
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        -- Give the default tenant a free subscription.
        INSERT INTO tenant_subscriptions (tenant_id, plan_id)
        VALUES ('00000000-0000-0000-0000-000000000000', 'free')
        ON CONFLICT (tenant_id) DO NOTHING;

        CREATE TABLE IF NOT EXISTS usage_meters (
            meter_id        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id       UUID        NOT NULL,
            organization_id TEXT        NOT NULL,
            meter_type      TEXT        NOT NULL
                CHECK (meter_type IN ('task_claim', 'permit_consume', 'effect_record')),
            reference_id    TEXT        NOT NULL,
            recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            billing_period  TEXT        NOT NULL,
            UNIQUE (meter_type, reference_id)
        );

        CREATE INDEX IF NOT EXISTS idx_usage_meters_tenant_period
            ON usage_meters(tenant_id, billing_period);

        CREATE TABLE IF NOT EXISTS invoices (
            invoice_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id       UUID        NOT NULL,
            billing_period  TEXT        NOT NULL,
            line_items      JSONB       NOT NULL DEFAULT '[]',
            total_cents     INTEGER     NOT NULL DEFAULT 0,
            status          TEXT        NOT NULL DEFAULT 'draft'
                CHECK (status IN ('draft', 'issued', 'paid', 'void')),
            stripe_invoice_id TEXT,
            issued_at       TIMESTAMPTZ,
            paid_at         TIMESTAMPTZ,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (tenant_id, billing_period)
        );
        """,
    ),
    # v7 → v8: agent marketplace tables (RFC-0.25.0).
    (
        7,
        """
        CREATE TABLE IF NOT EXISTS marketplace_listings (
            listing_id      TEXT        PRIMARY KEY,
            name            TEXT        NOT NULL,
            description     TEXT        NOT NULL DEFAULT '',
            category        TEXT        NOT NULL DEFAULT 'general',
            author          TEXT        NOT NULL DEFAULT '',
            version         TEXT        NOT NULL,
            package_type    TEXT        NOT NULL
                CHECK (package_type IN ('builtin', 'pip', 'git', 'bundle')),
            package_ref     TEXT        NOT NULL DEFAULT '',
            entry_point     TEXT        NOT NULL DEFAULT '',
            capabilities_required TEXT[] NOT NULL DEFAULT '{}',
            dependencies    JSONB       NOT NULL DEFAULT '[]',
            metadata        JSONB       NOT NULL DEFAULT '{}',
            published_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            active          BOOLEAN     NOT NULL DEFAULT true
        );

        CREATE TABLE IF NOT EXISTS marketplace_installations (
            id              BIGSERIAL   PRIMARY KEY,
            tenant_id       UUID        NOT NULL,
            listing_id      TEXT        NOT NULL
                REFERENCES marketplace_listings(listing_id),
            version         TEXT        NOT NULL,
            installed_by    TEXT        NOT NULL DEFAULT '',
            installed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            enabled         BOOLEAN     NOT NULL DEFAULT true,
            config          JSONB       NOT NULL DEFAULT '{}',

            CONSTRAINT uq_installation_tenant_listing
                UNIQUE (tenant_id, listing_id)
        );

        CREATE INDEX IF NOT EXISTS idx_installations_tenant
            ON marketplace_installations(tenant_id);
        CREATE INDEX IF NOT EXISTS idx_installations_listing
            ON marketplace_installations(listing_id);

        CREATE TABLE IF NOT EXISTS marketplace_reviews (
            id              BIGSERIAL   PRIMARY KEY,
            listing_id      TEXT        NOT NULL
                REFERENCES marketplace_listings(listing_id),
            tenant_id       UUID        NOT NULL,
            rating          INTEGER     NOT NULL CHECK (rating BETWEEN 1 AND 5),
            review_text     TEXT        NOT NULL DEFAULT '',
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            CONSTRAINT uq_review_tenant_listing
                UNIQUE (tenant_id, listing_id)
        );

        -- Seed built-in tool listings
        INSERT INTO marketplace_listings
            (listing_id, name, description, category, author, version, package_type, entry_point)
        VALUES
            ('hermes.web-search', 'Web Search', 'Search the web via configured provider', 'search', 'hermes', '1.0.0', 'builtin', 'hermes_cli.tools.web_search'),
            ('hermes.code-exec', 'Code Execution', 'Sandboxed code execution environment', 'development', 'hermes', '1.0.0', 'builtin', 'hermes_cli.tools.code_exec'),
            ('hermes.file-ops', 'File Operations', 'File system read/write/search operations', 'filesystem', 'hermes', '1.0.0', 'builtin', 'hermes_cli.tools.file_ops'),
            ('hermes.calendar', 'Calendar', 'Calendar integration (Google, Outlook)', 'productivity', 'hermes', '1.0.0', 'builtin', 'hermes_cli.tools.calendar'),
            ('hermes.email', 'Email', 'Email send/receive integration', 'communication', 'hermes', '1.0.0', 'builtin', 'hermes_cli.tools.email')
        ON CONFLICT (listing_id) DO NOTHING;
        """,
    ),
    # v8 → v9: external integrations framework (RFC-0.26.0).
    (
        8,
        """
        CREATE TABLE IF NOT EXISTS integration_credentials (
            id              BIGSERIAL   PRIMARY KEY,
            tenant_id       UUID        NOT NULL,
            integration_id  TEXT        NOT NULL,
            provider        TEXT        NOT NULL,
            credential_type TEXT        NOT NULL
                CHECK (credential_type IN ('oauth2', 'api_key', 'smtp', 'webhook')),
            encrypted_data  TEXT        NOT NULL,
            scopes          TEXT[]      NOT NULL DEFAULT '{}',
            expires_at      TIMESTAMPTZ,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            CONSTRAINT uq_credential_tenant_integration
                UNIQUE (tenant_id, integration_id, provider)
        );

        CREATE INDEX IF NOT EXISTS idx_credentials_tenant
            ON integration_credentials(tenant_id);

        CREATE TABLE IF NOT EXISTS integration_webhooks (
            id              BIGSERIAL   PRIMARY KEY,
            tenant_id       UUID        NOT NULL,
            integration_id  TEXT        NOT NULL,
            provider        TEXT        NOT NULL,
            event_type      TEXT        NOT NULL,
            webhook_url     TEXT        NOT NULL,
            secret          TEXT        NOT NULL DEFAULT '',
            active          BOOLEAN     NOT NULL DEFAULT true,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            CONSTRAINT uq_webhook_tenant_event
                UNIQUE (tenant_id, integration_id, event_type)
        );

        CREATE INDEX IF NOT EXISTS idx_webhooks_tenant
            ON integration_webhooks(tenant_id);

        CREATE TABLE IF NOT EXISTS integration_events (
            id              BIGSERIAL   PRIMARY KEY,
            tenant_id       UUID        NOT NULL,
            integration_id  TEXT        NOT NULL,
            provider        TEXT        NOT NULL,
            event_type      TEXT        NOT NULL,
            event_id        TEXT        NOT NULL,
            payload         JSONB       NOT NULL DEFAULT '{}',
            processed       BOOLEAN     NOT NULL DEFAULT false,
            received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            CONSTRAINT uq_event_idempotency
                UNIQUE (tenant_id, integration_id, event_id)
        );

        CREATE INDEX IF NOT EXISTS idx_events_tenant_unprocessed
            ON integration_events(tenant_id, processed)
            WHERE processed = false;
        """,
    ),
    # v9 → v10: monitoring & observability (RFC-0.27.0).
    (
        9,
        """
        CREATE TABLE IF NOT EXISTS metrics (
            id              BIGSERIAL   PRIMARY KEY,
            tenant_id       UUID        NOT NULL,
            metric_name     TEXT        NOT NULL,
            metric_type     TEXT        NOT NULL
                CHECK (metric_type IN ('counter', 'gauge', 'histogram')),
            value           DOUBLE PRECISION NOT NULL,
            labels          JSONB       NOT NULL DEFAULT '{}',
            recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_metrics_tenant_name_time
            ON metrics(tenant_id, metric_name, recorded_at DESC);

        CREATE TABLE IF NOT EXISTS health_checks (
            id              BIGSERIAL   PRIMARY KEY,
            service_name    TEXT        NOT NULL,
            status          TEXT        NOT NULL
                CHECK (status IN ('healthy', 'degraded', 'unhealthy')),
            details         JSONB       NOT NULL DEFAULT '{}',
            checked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_health_checks_service
            ON health_checks(service_name, checked_at DESC);

        CREATE TABLE IF NOT EXISTS alert_rules (
            id              BIGSERIAL   PRIMARY KEY,
            tenant_id       UUID        NOT NULL,
            name            TEXT        NOT NULL,
            metric_name     TEXT        NOT NULL,
            condition       TEXT        NOT NULL,
            threshold       DOUBLE PRECISION NOT NULL,
            window_seconds  INTEGER     NOT NULL DEFAULT 300,
            notify_channel  TEXT        NOT NULL DEFAULT '',
            enabled         BOOLEAN     NOT NULL DEFAULT true,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            CONSTRAINT uq_alert_rule_name UNIQUE (tenant_id, name)
        );

        CREATE INDEX IF NOT EXISTS idx_alert_rules_tenant
            ON alert_rules(tenant_id);
        """,
    ),
    # v10 → v11: disaster recovery (RFC-0.28.0).
    (
        10,
        """
        CREATE TABLE IF NOT EXISTS backup_records (
            id              BIGSERIAL   PRIMARY KEY,
            tenant_id       UUID        NOT NULL,
            backup_type     TEXT        NOT NULL
                CHECK (backup_type IN ('full', 'incremental', 'snapshot')),
            status          TEXT        NOT NULL
                CHECK (status IN ('pending', 'running', 'completed', 'failed')),
            storage_path    TEXT        NOT NULL DEFAULT '',
            size_bytes      BIGINT      NOT NULL DEFAULT 0,
            schema_version  INTEGER     NOT NULL,
            started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at    TIMESTAMPTZ,
            expires_at      TIMESTAMPTZ,
            metadata        JSONB       NOT NULL DEFAULT '{}'
        );

        CREATE INDEX IF NOT EXISTS idx_backups_tenant
            ON backup_records(tenant_id);

        CREATE TABLE IF NOT EXISTS restore_points (
            id              BIGSERIAL   PRIMARY KEY,
            tenant_id       UUID        NOT NULL,
            name            TEXT        NOT NULL,
            backup_id       BIGINT      REFERENCES backup_records(id),
            schema_version  INTEGER     NOT NULL,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            CONSTRAINT uq_restore_point_name UNIQUE (tenant_id, name)
        );

        CREATE INDEX IF NOT EXISTS idx_restore_points_tenant
            ON restore_points(tenant_id);

        CREATE TABLE IF NOT EXISTS failover_drills (
            id              BIGSERIAL   PRIMARY KEY,
            tenant_id       UUID        NOT NULL,
            drill_type      TEXT        NOT NULL,
            status          TEXT        NOT NULL
                CHECK (status IN ('scheduled', 'running', 'passed', 'failed')),
            started_at      TIMESTAMPTZ,
            completed_at    TIMESTAMPTZ,
            results         JSONB       NOT NULL DEFAULT '{}',
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_drills_tenant
            ON failover_drills(tenant_id);
        """,
    ),
    # v11 → v12: security hardening (RFC-0.29.0).
    (
        11,
        """
        CREATE TABLE IF NOT EXISTS audit_log (
            id              BIGSERIAL   PRIMARY KEY,
            tenant_id       UUID        NOT NULL,
            actor_type      TEXT        NOT NULL,
            actor_id        TEXT        NOT NULL,
            action          TEXT        NOT NULL,
            resource_type   TEXT        NOT NULL DEFAULT '',
            resource_id     TEXT        NOT NULL DEFAULT '',
            outcome         TEXT        NOT NULL
                CHECK (outcome IN ('success', 'denied', 'error')),
            details         JSONB       NOT NULL DEFAULT '{}',
            ip_address      TEXT        NOT NULL DEFAULT '',
            user_agent      TEXT        NOT NULL DEFAULT '',
            recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_audit_log_tenant_time
            ON audit_log(tenant_id, recorded_at DESC);
        CREATE INDEX IF NOT EXISTS idx_audit_log_actor
            ON audit_log(tenant_id, actor_type, actor_id);
        CREATE INDEX IF NOT EXISTS idx_audit_log_resource
            ON audit_log(resource_type, resource_id);

        CREATE TABLE IF NOT EXISTS secrets (
            id              BIGSERIAL   PRIMARY KEY,
            tenant_id       UUID        NOT NULL,
            secret_name     TEXT        NOT NULL,
            encrypted_value TEXT        NOT NULL,
            version         INTEGER     NOT NULL DEFAULT 1,
            created_by      TEXT        NOT NULL DEFAULT '',
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            rotated_at      TIMESTAMPTZ,
            expires_at      TIMESTAMPTZ,

            CONSTRAINT uq_secret_name UNIQUE (tenant_id, secret_name)
        );

        CREATE INDEX IF NOT EXISTS idx_secrets_tenant
            ON secrets(tenant_id);
        """,
    ),
    # v12 → v13: objective-lifecycle and master-autonomy-stop mirrors.
    #
    # Objectives, plans, and mandates remain SQLite-owned (see
    # objectives_db.py / organization_db.py / operational_control.py); these
    # two tables are narrow, versioned, fail-closed *status mirrors* pushed
    # synchronously from the SQLite side whenever an org's authority_backend
    # is Postgres. consume_permit() below joins against them so that a
    # cancelled objective or a stopped-autonomy org cannot authorize permit
    # consumption purely because the claim/generation/payload fencing checks
    # passed. Absence of a row is always treated as "reject" (fail closed),
    # never as an implicit "admissible"/"autonomous" default. Scoped by
    # organization_id (not tenant_id) to match the SQLite objectives table's
    # own keying — organization_id remains the join key between the SQLite
    # workflow layer and this Postgres coordination store.
    (
        12,
        """
        CREATE TABLE IF NOT EXISTS pg_objective_status (
            objective_id     TEXT        NOT NULL,
            organization_id  TEXT        NOT NULL,
            status           TEXT        NOT NULL,
            version          BIGINT      NOT NULL DEFAULT 1,
            updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (objective_id, organization_id)
        );

        CREATE INDEX IF NOT EXISTS idx_pg_objective_status_org
            ON pg_objective_status(organization_id);

        CREATE TABLE IF NOT EXISTS pg_autonomy_control (
            organization_id  TEXT        PRIMARY KEY,
            mode             TEXT        NOT NULL DEFAULT 'autonomous',
            generation       BIGINT      NOT NULL DEFAULT 1,
            changed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
    ),
]

# Objective statuses that permit consumption is allowed to proceed under.
# Mirrors the admissive set checked by objectives_db.consume_permit's own
# objective-lifecycle gate (objectives_db.py:1138-1141).
_ADMISSIVE_OBJECTIVE_STATUSES = ("planned", "authorized", "executing")


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------


def get_postgres_url() -> str:
    """Get Postgres connection URL from environment.

    Prefers AUTHORITY_POSTGRES_URL, falls back to DATABASE_URL.

    Raises:
        RuntimeError: If no Postgres URL configured
    """
    url = os.environ.get("AUTHORITY_POSTGRES_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "Postgres authority store requires AUTHORITY_POSTGRES_URL or DATABASE_URL"
        )
    return url


DEFAULT_TENANT_ID = UUID("00000000-0000-0000-0000-000000000000")


def connect(url: Optional[str] = None) -> "psycopg.Connection":
    """Connect to Postgres authority store.

    Args:
        url: Optional Postgres URL (defaults to environment)

    Returns:
        Postgres connection with dict_row factory

    Raises:
        ImportError: If psycopg not installed
        RuntimeError: If no URL configured
    """
    if psycopg is None:
        raise ImportError(
            "psycopg is required for Postgres authority store: "
            "pip install psycopg[binary]"
        )

    resolved_url = url or get_postgres_url()
    conn = psycopg.connect(resolved_url, row_factory=dict_row)
    conn.autocommit = False
    return conn


def _ts(unix: float) -> datetime:
    """Convert a Unix timestamp to a timezone-aware UTC datetime.

    Used for all timestamp bound parameters — never interpolated into SQL.
    """
    return datetime.fromtimestamp(unix, tz=timezone.utc)


# ---------------------------------------------------------------------------
# Schema + migrations
# ---------------------------------------------------------------------------


def _detect_legacy_v1_schema(conn: "psycopg.Connection") -> bool:
    """Detect the pre-fencing legacy v1 schema (claim_lock based).

    The original v1 schema (commit 00197ac59) used a `claim_lock` column
    and had no lease_generation fencing. If this is detected, the database
    cannot be migrated by the current migration path and must be manually
    migrated or recreated.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'task_claims'
                  AND column_name = 'claim_lock'
                  AND table_schema = current_schema()
                """
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def init_schema(conn: "psycopg.Connection") -> None:
    """Apply all pending migrations and verify schema version.

    Behavior:
    - Fresh install: applies all migrations in order.
    - Already at current version: no-op.
    - Ahead of SCHEMA_VERSION (future schema): raises RuntimeError (fail closed).
    - Legacy v1 schema detected: raises RuntimeError (incompatible).
    - Mid-migration failure: rolls back the failed migration and re-raises.

    Args:
        conn: Postgres connection

    Raises:
        RuntimeError: If schema is ahead of known version or legacy v1 detected.
    """
    if _detect_legacy_v1_schema(conn):
        raise RuntimeError(
            "Legacy v1 authority schema detected (claim_lock column present). "
            "This schema predates lease-generation fencing and is incompatible "
            "with the current migration path. Run "
            "'hermes authority migrate-legacy' to upgrade, or recreate the "
            "authority database. See docs/multi-tenant-migration-guide.md."
        )

    with conn.cursor() as cur:
        # Create schema_version if it doesn't exist yet (bootstrapping case).
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
                version     INTEGER     PRIMARY KEY,
                applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                description TEXT
            )
            """
        )
        cur.execute("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        row = cur.fetchone()
        current = row["coalesce"] if row else 0

    conn.commit()

    if current > SCHEMA_VERSION:
        raise RuntimeError(
            f"Authority store schema version {current} exceeds supported "
            f"version {SCHEMA_VERSION}. Upgrade the Charterforge package or "
            "run a downgrade migration before starting."
        )

    # Apply each pending migration in a separate transaction so partial
    # failures roll back only the failing step.
    for from_version, migration_sql in _MIGRATIONS:
        if current > from_version:
            continue
        to_version = from_version + 1
        try:
            with conn.cursor() as cur:
                cur.execute(migration_sql)
                cur.execute(
                    "INSERT INTO schema_version (version, description) "
                    "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (to_version, f"migration {from_version} → {to_version}"),
                )
            conn.commit()
            current = to_version
        except Exception:
            conn.rollback()
            raise


def get_schema_version(conn: "psycopg.Connection") -> int:
    """Return the current schema version recorded in the database."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(version), 0) FROM schema_version"
            )
            row = cur.fetchone()
            return row["coalesce"] if row else 0
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Objective-lifecycle / autonomy-stop mirrors
#
# Objectives and mandates are SQLite-owned (objectives_db.py /
# organization_db.py / operational_control.py). These functions are called
# from the SQLite side (objectives_db.transition_objective,
# operational_control.set_autonomy_mode) immediately after their own
# successful SQLite commit, only for organizations whose authority_backend
# is Postgres. This is a synchronous push, not a cross-database transaction
# — it cannot be made atomic with the SQLite commit that triggers it. The
# CAS-guarded ON CONFLICT below only prevents a redelivered/out-of-order
# mirror write from regressing a newer status; it does not eliminate the
# staleness window between the two commits. consume_permit() is written to
# fail closed on a missing or non-admissive row rather than assume the two
# stores agree.
# ---------------------------------------------------------------------------


def mirror_objective_status(
    conn: "psycopg.Connection",
    *,
    objective_id: str,
    organization_id: str,
    status: str,
    version: int,
) -> None:
    """Push the current objective status into the Postgres mirror.

    Idempotent and monotonic: a write carrying a version less than or equal
    to what's already stored is a no-op, so redelivery or out-of-order
    arrival can never regress a newer status.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pg_objective_status
                (objective_id, organization_id, status, version, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (objective_id, organization_id) DO UPDATE
                SET status = EXCLUDED.status,
                    version = EXCLUDED.version,
                    updated_at = NOW()
                WHERE pg_objective_status.version < EXCLUDED.version
            """,
            (objective_id, organization_id, status, version),
        )
        conn.commit()


def get_objective_status(
    conn: "psycopg.Connection", *, objective_id: str, organization_id: str
) -> Optional[dict[str, Any]]:
    """Read the mirrored objective status, or None if never mirrored."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, version FROM pg_objective_status
            WHERE objective_id = %s AND organization_id = %s
            """,
            (objective_id, organization_id),
        )
        return cur.fetchone()


def mirror_autonomy_mode(
    conn: "psycopg.Connection",
    *,
    organization_id: str,
    mode: str,
    generation: int,
) -> None:
    """Push the current master-autonomy-stop mode into the Postgres mirror.

    Generation-guarded the same way as mirror_objective_status: a write
    carrying a generation less than or equal to what's stored is a no-op.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pg_autonomy_control
                (organization_id, mode, generation, changed_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (organization_id) DO UPDATE
                SET mode = EXCLUDED.mode,
                    generation = EXCLUDED.generation,
                    changed_at = NOW()
                WHERE pg_autonomy_control.generation < EXCLUDED.generation
            """,
            (organization_id, mode, generation),
        )
        conn.commit()


def get_autonomy_mode(
    conn: "psycopg.Connection", *, organization_id: str
) -> Optional[dict[str, Any]]:
    """Read the mirrored autonomy mode, or None if never mirrored."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT mode, generation FROM pg_autonomy_control
            WHERE organization_id = %s
            """,
            (organization_id,),
        )
        return cur.fetchone()


# ---------------------------------------------------------------------------
# Task claiming
# ---------------------------------------------------------------------------


def claim_task(
    conn: "psycopg.Connection",
    *,
    task_id: str,
    claim_token: str,
    organization_id: str,
    worker_id: str,
    claim_scope_url: str,
    expires_at: float,
    tenant_id: Optional[UUID] = None,
) -> Optional[int]:
    """Claim a task for execution (atomic CAS).

    Uses INSERT ... ON CONFLICT (task_id, organization_id) DO NOTHING so
    exactly one worker wins the race regardless of how many try simultaneously.

    Args:
        conn: Postgres connection
        task_id: Task to claim
        claim_token: Unique token for this claim attempt (run ID / UUID)
        organization_id: Organization scope
        worker_id: Worker identifier
        claim_scope_url: Scope URL for the claim
        expires_at: Unix timestamp when claim expires
        tenant_id: Tenant scope (defaults to DEFAULT_TENANT_ID)

    Returns:
        lease_generation (int >= 1) if claim succeeded, None if already claimed
    """
    expires_dt = _ts(expires_at)
    resolved_tenant = str(tenant_id or DEFAULT_TENANT_ID)

    # Quota enforcement: reject if free-tier tenant is over their hard limit.
    # Only tolerate "billing tables don't exist yet" (an older, pre-billing
    # schema) — any OTHER error (a real bug, a constraint violation, a
    # connection problem) must propagate rather than silently authorizing
    # the claim. A Postgres error also leaves the transaction aborted, so an
    # explicit rollback is required before the claim INSERT below can run.
    try:
        allowed, _, _ = check_quota(conn, tenant_id=resolved_tenant, meter_type="task_claim")
        if not allowed:
            return None
    except psycopg.errors.UndefinedTable:
        conn.rollback()

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO task_claims
                (task_id, organization_id, tenant_id, worker_id,
                 claim_scope_url, lease_generation, expires_at)
            VALUES
                (%s, %s, %s, %s, %s, 1, %s)
            ON CONFLICT (task_id, organization_id) DO NOTHING
            RETURNING lease_generation
            """,
            (task_id, organization_id, resolved_tenant, worker_id,
             claim_scope_url, expires_dt),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None

        generation = row["lease_generation"]

        # Record the immutable run row for this generation.
        cur.execute(
            """
            INSERT INTO task_runs
                (task_id, organization_id, tenant_id, claim_token,
                 lease_generation, status)
            VALUES
                (%s, %s, %s, %s, %s, 'pending')
            ON CONFLICT (task_id, organization_id, lease_generation) DO NOTHING
            """,
            (task_id, organization_id, resolved_tenant, claim_token, generation),
        )

        conn.commit()

        # Record billable usage (best-effort, non-blocking). The claim
        # itself already committed above; only tolerate "billing tables
        # don't exist yet," not arbitrary errors.
        try:
            record_usage(
                conn, tenant_id=resolved_tenant, organization_id=organization_id,
                meter_type="task_claim",
                reference_id=f"claim:{task_id}:{organization_id}:{generation}",
            )
        except psycopg.errors.UndefinedTable:
            conn.rollback()

        return generation


def reclaim_task(
    conn: "psycopg.Connection",
    *,
    task_id: str,
    organization_id: str,
    new_claim_token: str,
    new_worker_id: str,
    claim_scope_url: str,
    expires_at: float,
    tenant_id: Optional[UUID] = None,
) -> Optional[int]:
    """Atomically replace an expired claim with a new one.

    The old claim must be expired (expires_at <= NOW()).  The new lease
    generation is max(old_generation + 1, 1) so it is always strictly greater
    than any previous generation the stale worker could have observed.

    Args:
        conn: Postgres connection
        task_id: Task to reclaim
        organization_id: Organization scope
        new_claim_token: Token for the new claim
        new_worker_id: Worker identifier for the new claimer
        claim_scope_url: Scope URL
        expires_at: Unix timestamp when new claim expires
        tenant_id: Tenant scope (defaults to DEFAULT_TENANT_ID)

    Returns:
        New lease_generation (always > previous) if reclaim succeeded, else None.
    """
    expires_dt = _ts(expires_at)
    resolved_tenant = str(tenant_id or DEFAULT_TENANT_ID)

    with conn.cursor() as cur:
        # Lock the expired row and compute the next generation atomically.
        cur.execute(
            """
            UPDATE task_claims
            SET
                worker_id       = %s,
                claim_scope_url = %s,
                lease_generation = lease_generation + 1,
                expires_at      = %s,
                claimed_at      = NOW()
            WHERE task_id        = %s
              AND organization_id = %s
              AND tenant_id      = %s
              AND expires_at      <= NOW()
            RETURNING lease_generation
            """,
            (new_worker_id, claim_scope_url, expires_dt,
             task_id, organization_id, resolved_tenant),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None

        generation = row["lease_generation"]

        # Insert the immutable run record for this generation.
        cur.execute(
            """
            INSERT INTO task_runs
                (task_id, organization_id, tenant_id, claim_token,
                 lease_generation, status)
            VALUES
                (%s, %s, %s, %s, %s, 'pending')
            ON CONFLICT (task_id, organization_id, lease_generation) DO NOTHING
            """,
            (task_id, organization_id, resolved_tenant, new_claim_token, generation),
        )

        conn.commit()
        return generation


# ---------------------------------------------------------------------------
# Claim inspection
# ---------------------------------------------------------------------------


def get_claim(
    conn: "psycopg.Connection",
    *,
    task_id: str,
    organization_id: str,
) -> Optional[dict[str, Any]]:
    """Get the active (non-expired) claim for a task.

    Returns:
        Claim dict (includes lease_generation) or None if not claimed/expired.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM task_claims
            WHERE task_id        = %s
              AND organization_id = %s
              AND expires_at      > NOW()
            """,
            (task_id, organization_id),
        )
        return cur.fetchone()


# ---------------------------------------------------------------------------
# Fenced claim release
# ---------------------------------------------------------------------------


def release_claim(
    conn: "psycopg.Connection",
    *,
    task_id: str,
    organization_id: str,
    claim_token: str,
    lease_generation: int,
) -> bool:
    """Release a claim.

    Only succeeds when claim_token matches an active run for the given
    task+org with exactly the supplied lease_generation.  A stale worker
    cannot release the reclaimer's claim.

    Args:
        conn: Postgres connection
        task_id: Task to release
        organization_id: Organization scope
        claim_token: Must match the run's claim_token
        lease_generation: Must match current lease_generation

    Returns:
        True if released, False if fencing check failed.
    """
    with conn.cursor() as cur:
        # Verify the caller owns the active generation for this task+org.
        cur.execute(
            """
            SELECT 1 FROM task_runs
            WHERE task_id         = %s
              AND organization_id  = %s
              AND claim_token      = %s
              AND lease_generation = %s
              AND status          = 'pending'
            """,
            (task_id, organization_id, claim_token, lease_generation),
        )
        if not cur.fetchone():
            conn.rollback()
            return False

        # Remove the claim row — must also match generation on claims table.
        cur.execute(
            """
            DELETE FROM task_claims
            WHERE task_id         = %s
              AND organization_id  = %s
              AND lease_generation = %s
            RETURNING id
            """,
            (task_id, organization_id, lease_generation),
        )
        deleted = cur.fetchone()
        if not deleted:
            conn.rollback()
            return False

        conn.commit()
        return True


# ---------------------------------------------------------------------------
# Fenced task completion
# ---------------------------------------------------------------------------


def complete_task(
    conn: "psycopg.Connection",
    *,
    task_id: str,
    organization_id: str,
    claim_token: str,
    lease_generation: int,
    outcome: str,
    effects: Optional[list[dict[str, Any]]] = None,
    tenant_id: Optional[UUID] = None,
) -> bool:
    """Complete a task run.

    Three fencing checks must pass atomically:
    1. The run record for (task_id, org, generation) exists and is 'pending'.
    2. The claim_token matches that run's claim_token.
    3. The claim row still carries that generation AND has not expired.

    Args:
        conn: Postgres connection
        task_id: Task to complete
        organization_id: Organization scope
        claim_token: Must match the winning run's claim_token
        lease_generation: Must match current lease_generation on both tables
        outcome: Completion outcome string
        effects: Optional list of effects — each must include 'effect_key'
        tenant_id: Tenant scope (defaults to DEFAULT_TENANT_ID)

    Returns:
        True if completed, False if any fencing check fails.
    """
    resolved_tenant = str(tenant_id or DEFAULT_TENANT_ID)

    with conn.cursor() as cur:
        # --- Fence: verify run is pending for this exact generation ---
        cur.execute(
            """
            UPDATE task_runs
            SET status   = 'completed',
                outcome  = %s,
                ended_at = NOW()
            WHERE task_id         = %s
              AND organization_id  = %s
              AND claim_token      = %s
              AND lease_generation = %s
              AND status          = 'pending'
            RETURNING id
            """,
            (outcome, task_id, organization_id, claim_token, lease_generation),
        )
        updated = cur.fetchone()
        if not updated:
            conn.rollback()
            return False

        # --- Fence: verify claim row matches generation and is not expired ---
        cur.execute(
            """
            SELECT 1 FROM task_claims
            WHERE task_id         = %s
              AND organization_id  = %s
              AND lease_generation = %s
              AND expires_at       > NOW()
            """,
            (task_id, organization_id, lease_generation),
        )
        if not cur.fetchone():
            conn.rollback()
            return False

        # --- Record effects idempotently ---
        if effects:
            for effect in effects:
                key = effect.get("effect_key")
                if not key:
                    raise ValueError(
                        "Every effect must carry a stable 'effect_key' for "
                        "idempotent recovery.  Provide: "
                        "org_id:objective_id:action_id:permit_id:provider:provider_ref"
                    )
                cur.execute(
                    """
                    INSERT INTO execution_effects
                        (effect_key, task_id, organization_id, tenant_id,
                         run_claim_token, lease_generation,
                         permit_id, effect_type, provider,
                         provider_ref, idempotency_key, payload)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (effect_key) DO NOTHING
                    """,
                    (
                        key,
                        task_id,
                        organization_id,
                        resolved_tenant,
                        claim_token,
                        lease_generation,
                        effect.get("permit_id", ""),
                        effect.get("type", "unknown"),
                        effect.get("provider", ""),
                        effect.get("provider_ref", ""),
                        effect.get("idempotency_key", ""),
                        json.dumps(effect),
                    ),
                )

        # --- Release claim ---
        cur.execute(
            """
            DELETE FROM task_claims
            WHERE task_id         = %s
              AND organization_id  = %s
              AND lease_generation = %s
            """,
            (task_id, organization_id, lease_generation),
        )

        conn.commit()
        return True


# ---------------------------------------------------------------------------
# Permit issuance and consumption
# ---------------------------------------------------------------------------


def issue_permit(
    conn: "psycopg.Connection",
    *,
    task_id: str,
    organization_id: str,
    claim_token: str,
    lease_generation: int,
    action_payload: dict[str, Any],
    ttl_seconds: int = 300,
    # Full authority model fields (parity with SQLite)
    actor: str = "",
    executor: str = "",
    capability: str = "",
    action_type: str = "",
    target_resource: str = "",
    policy_version: str = "",
    tenant_id: Optional[UUID] = None,
) -> str:
    """Issue an execution permit for a governed action.

    Verifies the active claim has the correct generation before issuing.

    Args:
        conn: Postgres connection
        task_id: Task ID
        organization_id: Organization scope
        claim_token: Must match active claim
        lease_generation: Must match current lease_generation
        action_payload: Action payload (hashed for binding)
        ttl_seconds: Permit TTL in seconds
        actor / executor / capability / action_type / target_resource / policy_version:
            Full authority model fields matching SQLite semantics.
        tenant_id: Tenant scope (defaults to DEFAULT_TENANT_ID)

    Returns:
        Permit ID (UUID string)

    Raises:
        ValueError: If no valid fenced claim exists
    """
    import uuid

    permit_id = str(uuid.uuid4())
    expires_dt = _ts(time.time() + ttl_seconds)
    payload_hash = hashlib.sha256(
        json.dumps(action_payload, sort_keys=True).encode()
    ).hexdigest()
    resolved_tenant = str(tenant_id or DEFAULT_TENANT_ID)

    with conn.cursor() as cur:
        # Verify claim is alive and carries the correct generation.
        cur.execute(
            """
            SELECT 1 FROM task_claims
            WHERE task_id         = %s
              AND organization_id  = %s
              AND lease_generation = %s
              AND expires_at       > NOW()
            """,
            (task_id, organization_id, lease_generation),
        )
        if not cur.fetchone():
            raise ValueError(
                "No valid fenced claim for permit issuance "
                f"(task={task_id}, org={organization_id}, gen={lease_generation})"
            )

        # Verify the run record is still pending for this generation.
        cur.execute(
            """
            SELECT 1 FROM task_runs
            WHERE task_id         = %s
              AND organization_id  = %s
              AND claim_token      = %s
              AND lease_generation = %s
              AND status          = 'pending'
            """,
            (task_id, organization_id, claim_token, lease_generation),
        )
        if not cur.fetchone():
            raise ValueError(
                "No pending run for permit issuance "
                f"(task={task_id}, org={organization_id}, gen={lease_generation})"
            )

        cur.execute(
            """
            INSERT INTO task_permits
                (permit_id, task_id, organization_id, tenant_id, claim_token,
                 lease_generation, actor, executor, capability,
                 action_type, target_resource, payload_hash,
                 policy_version, action_payload, expires_at)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING permit_id
            """,
            (
                permit_id,
                task_id,
                organization_id,
                resolved_tenant,
                claim_token,
                lease_generation,
                actor,
                executor,
                capability,
                action_type,
                target_resource,
                payload_hash,
                policy_version,
                json.dumps(action_payload),
                expires_dt,
            ),
        )

        row = cur.fetchone()
        conn.commit()
        return row["permit_id"]


def consume_permit(
    conn: "psycopg.Connection",
    *,
    permit_id: str,
    organization_id: str,
    claim_token: str,
    lease_generation: int,
    action_payload: dict[str, Any],
    executor: str = "",
    capability: str = "",
    target_resource: str = "",
    policy_version: Optional[str] = None,
    objective_id: Optional[str] = None,
) -> bool:
    """Consume an execution permit (atomic, once-only).

    Fencing checks:
    1. permit_id must exist, not yet consumed, not revoked, not expired.
    2. organization_id must match.
    3. claim_token must match the permit's recorded claim_token.
    4. lease_generation must match the permit's recorded generation.
    5. SHA256 hash of action_payload must match the stored payload_hash.
    6. executor, capability, and target_resource must match the permit's
       recorded values exactly — a permit issued for one executor,
       capability, or target resource cannot be consumed against another
       (exact-action, exact-target binding; mirrors objectives_db.py's
       issued_to==executor / capability / target_resource checks). These
       fields were already stored at issuance by issue_permit() but never
       re-validated here until now.
    7. If policy_version is given, it must match the permit's recorded
       policy_version (stale-policy fencing). Omit (None) to skip this
       check, matching objectives_db.consume_permit's
       current_policy_version=None convention.
    8. If objective_id is given, the mirrored objective status
       (pg_objective_status) must be present and in an admissive state, and
       the mirrored master-autonomy-stop state (pg_autonomy_control) for
       organization_id must be present and 'autonomous'. Objectives and
       autonomy mode are SQLite-owned; these are narrow, versioned mirrors
       pushed synchronously from the SQLite side (see
       mirror_objective_status / mirror_autonomy_mode). A missing mirror
       row is always treated as a rejection, never as an implicit
       "admissible" / "autonomous" default — this is a deliberate fail-closed
       choice, not an oversight, since the mirror push cannot be made
       atomic with the SQLite commit that triggers it.
    9. A single atomic UPDATE with all conditions prevents races.

    Args:
        conn: Postgres connection
        permit_id: Permit to consume
        organization_id: Must match permit's organization
        claim_token: Must match permit's claim_token
        lease_generation: Must match permit's lease_generation
        action_payload: Must hash to the stored payload_hash
        executor: Must match the permit's recorded executor
        capability: Must match the permit's recorded capability
        target_resource: Must match the permit's recorded target_resource
        policy_version: If given, must match the permit's recorded
            policy_version
        objective_id: If given, gates consumption on the mirrored objective
            lifecycle state and organization autonomy mode

    Returns:
        True if consumed, False if any check fails.
    """
    payload_hash = hashlib.sha256(
        json.dumps(action_payload, sort_keys=True).encode()
    ).hexdigest()

    with conn.cursor() as cur:
        if objective_id is not None:
            cur.execute(
                """
                SELECT status FROM pg_objective_status
                WHERE objective_id = %s AND organization_id = %s
                """,
                (objective_id, organization_id),
            )
            status_row = cur.fetchone()
            if (
                status_row is None
                or status_row["status"] not in _ADMISSIVE_OBJECTIVE_STATUSES
            ):
                conn.rollback()
                return False

            cur.execute(
                """
                SELECT mode FROM pg_autonomy_control
                WHERE organization_id = %s
                """,
                (organization_id,),
            )
            autonomy_row = cur.fetchone()
            if autonomy_row is None or autonomy_row["mode"] != "autonomous":
                conn.rollback()
                return False

        policy_clause = ""
        params: list[Any] = [
            permit_id,
            organization_id,
            claim_token,
            lease_generation,
            payload_hash,
            executor,
            capability,
            target_resource,
        ]
        if policy_version is not None:
            policy_clause = "AND policy_version = %s"
            params.append(policy_version)

        cur.execute(
            f"""
            UPDATE task_permits
            SET consumed_at = NOW()
            WHERE permit_id       = %s
              AND organization_id  = %s
              AND claim_token      = %s
              AND lease_generation = %s
              AND payload_hash     = %s
              AND executor         = %s
              AND capability       = %s
              AND target_resource  = %s
              {policy_clause}
              AND consumed_at     IS NULL
              AND revoked_at      IS NULL
              AND expires_at       > NOW()
            RETURNING permit_id
            """,
            params,
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return False

        conn.commit()

        # Record billable usage (best-effort — billing tables may not exist
        # in older schemas, which is not itself an authorization failure).
        try:
            cur2 = conn.cursor()
            cur2.execute(
                "SELECT tenant_id FROM task_permits WHERE permit_id = %s",
                (permit_id,),
            )
            prow = cur2.fetchone()
            cur2.close()
            if prow:
                record_usage(
                    conn, tenant_id=str(prow["tenant_id"]),
                    organization_id=organization_id,
                    meter_type="permit_consume",
                    reference_id=f"permit:{permit_id}",
                )
        except psycopg.errors.UndefinedTable:
            conn.rollback()

        return True


def revoke_permit(
    conn: "psycopg.Connection",
    *,
    permit_id: str,
    organization_id: str,
    reason: str = "",
) -> bool:
    """Revoke an unconsumed permit.

    Args:
        conn: Postgres connection
        permit_id: Permit to revoke
        organization_id: Must match permit's organization
        reason: Human-readable revocation reason

    Returns:
        True if revoked, False if not found / already consumed.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE task_permits
            SET revoked_at        = NOW(),
                revocation_reason = %s
            WHERE permit_id       = %s
              AND organization_id  = %s
              AND consumed_at     IS NULL
              AND revoked_at      IS NULL
            RETURNING permit_id
            """,
            (reason, permit_id, organization_id),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return False

        conn.commit()
        return True


# ---------------------------------------------------------------------------
# Effect recording (standalone, for recovery workers)
# ---------------------------------------------------------------------------


def record_effect(
    conn: "psycopg.Connection",
    *,
    effect_key: str,
    task_id: str,
    organization_id: str,
    run_claim_token: str,
    lease_generation: int,
    effect_type: str,
    provider: str = "",
    provider_ref: str = "",
    idempotency_key: str = "",
    permit_id: str = "",
    payload: dict[str, Any],
    tenant_id: Optional[UUID] = None,
) -> bool:
    """Persist one execution-effect record, idempotently.

    The effect_key uniquely identifies the governed action so recovery can
    call this multiple times without creating duplicates.

    Returns:
        True if inserted (new effect), False if effect_key already exists
        (idempotent — not an error).
    """
    resolved_tenant = str(tenant_id or DEFAULT_TENANT_ID)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO execution_effects
                (effect_key, task_id, organization_id, tenant_id,
                 run_claim_token, lease_generation,
                 permit_id, effect_type, provider,
                 provider_ref, idempotency_key, payload)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (effect_key) DO NOTHING
            RETURNING id
            """,
            (
                effect_key,
                task_id,
                organization_id,
                resolved_tenant,
                run_claim_token,
                lease_generation,
                permit_id,
                effect_type,
                provider,
                provider_ref,
                idempotency_key,
                json.dumps(payload),
            ),
        )
        row = cur.fetchone()
        conn.commit()
        is_new = row is not None

    # Record billable usage for new effects only (best-effort). The effect
    # itself already committed above; only tolerate "billing tables don't
    # exist yet," not arbitrary errors.
    if is_new:
        try:
            record_usage(
                conn, tenant_id=resolved_tenant,
                organization_id=organization_id,
                meter_type="effect_record",
                reference_id=f"effect:{effect_key}",
            )
        except psycopg.errors.UndefinedTable:
            conn.rollback()

    return is_new


def get_effect(
    conn: "psycopg.Connection",
    *,
    effect_key: str,
) -> Optional[dict[str, Any]]:
    """Look up an effect record by its stable key.

    Returns:
        Effect dict or None.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM execution_effects WHERE effect_key = %s",
            (effect_key,),
        )
        return cur.fetchone()


# ---------------------------------------------------------------------------
# Monitoring helpers
# ---------------------------------------------------------------------------


def get_active_runs(
    conn: "psycopg.Connection",
    *,
    organization_id: str,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Get active runs for an organization."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tr.*, tc.worker_id, tc.claim_scope_url, tc.lease_generation AS claim_gen
            FROM task_runs tr
            JOIN task_claims tc
              ON  tr.task_id         = tc.task_id
              AND tr.organization_id  = tc.organization_id
              AND tr.lease_generation = tc.lease_generation
            WHERE tr.organization_id = %s
              AND tr.status           = 'pending'
              AND tc.expires_at       > NOW()
            ORDER BY tr.started_at DESC
            LIMIT %s
            """,
            (organization_id, limit),
        )
        return cur.fetchall()


def cleanup_expired_claims(conn: "psycopg.Connection") -> int:
    """Delete expired claims that have no pending run (safe GC).

    Claims with a pending run should be reclaimed via reclaim_task, not
    deleted here, so the run row remains for audit.

    Returns:
        Number of claim rows deleted.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM task_claims tc
            WHERE tc.expires_at < NOW()
              AND NOT EXISTS (
                  SELECT 1 FROM task_runs tr
                  WHERE tr.task_id         = tc.task_id
                    AND tr.organization_id  = tc.organization_id
                    AND tr.lease_generation = tc.lease_generation
                    AND tr.status          = 'pending'
              )
            RETURNING id
            """
        )
        rows = cur.fetchall()
        conn.commit()
        return len(rows)


# ---------------------------------------------------------------------------
# Tenant management
# ---------------------------------------------------------------------------


def create_tenant(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID,
    slug: str,
    name: str = "",
    max_concurrent_claims: int = 10,
) -> bool:
    """Register a new tenant. Idempotent — returns False if slug/id already exists."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tenants (id, slug, name, max_concurrent_claims)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            RETURNING id
            """,
            (str(tenant_id), slug, name, max_concurrent_claims),
        )
        row = cur.fetchone()
        conn.commit()
        return row is not None


def get_tenant(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID,
) -> Optional[dict[str, Any]]:
    """Look up a tenant by ID."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM tenants WHERE id = %s", (str(tenant_id),))
        return cur.fetchone()


def suspend_tenant(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID,
) -> bool:
    """Suspend a tenant — claims will be rejected while suspended."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tenants SET suspended_at = NOW()
            WHERE id = %s AND suspended_at IS NULL
            RETURNING id
            """,
            (str(tenant_id),),
        )
        row = cur.fetchone()
        conn.commit()
        return row is not None


def activate_tenant(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID,
) -> bool:
    """Activate a tenant — removes suspension."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tenants SET suspended_at = NULL, active_at = NOW()
            WHERE id = %s
            RETURNING id
            """,
            (str(tenant_id),),
        )
        row = cur.fetchone()
        conn.commit()
        return row is not None


def check_tenant_claim_quota(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID,
) -> tuple[bool, int, int]:
    """Check if a tenant can acquire another claim.

    Returns:
        (allowed, current_count, max_allowed)
        allowed is False if tenant is suspended or at quota.
    """
    resolved = str(tenant_id or DEFAULT_TENANT_ID)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max_concurrent_claims, suspended_at FROM tenants WHERE id = %s",
            (resolved,),
        )
        tenant = cur.fetchone()
        if not tenant:
            return (True, 0, 10)

        if tenant["suspended_at"] is not None:
            return (False, 0, 0)

        max_claims = tenant["max_concurrent_claims"]
        cur.execute(
            """
            SELECT COUNT(*) AS n FROM task_claims
            WHERE tenant_id = %s AND expires_at > NOW()
            """,
            (resolved,),
        )
        current = cur.fetchone()["n"]
        return (current < max_claims, current, max_claims)


# ---------------------------------------------------------------------------
# Workspace management
# ---------------------------------------------------------------------------


def create_workspace(
    conn: "psycopg.Connection",
    *,
    workspace_id: UUID,
    tenant_id: UUID,
    name: str,
    slug: str,
    owner_id: str = "",
) -> bool:
    """Create a workspace within a tenant. Idempotent."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO workspaces (id, tenant_id, name, slug, owner_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            RETURNING id
            """,
            (str(workspace_id), str(tenant_id), name, slug, owner_id),
        )
        row = cur.fetchone()
        conn.commit()
        return row is not None


def get_workspace(
    conn: "psycopg.Connection",
    *,
    workspace_id: UUID,
    tenant_id: UUID,
) -> Optional[dict[str, Any]]:
    """Look up a workspace by ID, scoped to the correct tenant."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM workspaces WHERE id = %s AND tenant_id = %s",
            (str(workspace_id), str(tenant_id)),
        )
        return cur.fetchone()


def list_workspaces(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID,
) -> list[dict[str, Any]]:
    """List all active workspaces for a tenant."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM workspaces WHERE tenant_id = %s AND active = true ORDER BY name",
            (str(tenant_id),),
        )
        return cur.fetchall()


def deactivate_workspace(
    conn: "psycopg.Connection",
    *,
    workspace_id: UUID,
    tenant_id: UUID,
) -> bool:
    """Deactivate a workspace (soft-delete)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE workspaces SET active = false, updated_at = NOW()
            WHERE id = %s AND tenant_id = %s AND active = true
            RETURNING id
            """,
            (str(workspace_id), str(tenant_id)),
        )
        row = cur.fetchone()
        conn.commit()
        return row is not None


# ---------------------------------------------------------------------------
# Capability grants (RBAC)
# ---------------------------------------------------------------------------


def grant_capability(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID,
    principal_type: str,
    principal_id: str,
    resource: str,
    action: str,
    scope: str = "*",
    workspace_id: Optional[UUID] = None,
    granted_by: str = "",
    ttl_seconds: Optional[int] = None,
) -> bool:
    """Grant a capability to a principal. Idempotent (ON CONFLICT DO NOTHING)."""
    expires_dt = _ts(time.time() + ttl_seconds) if ttl_seconds else None

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO capability_grants
                (tenant_id, workspace_id, principal_type, principal_id,
                 resource, action, scope, granted_by, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, principal_type, principal_id, resource, action, scope)
            DO NOTHING
            RETURNING id
            """,
            (
                str(tenant_id),
                str(workspace_id) if workspace_id else None,
                principal_type,
                principal_id,
                resource,
                action,
                scope,
                granted_by,
                expires_dt,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return row is not None


def revoke_capability(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID,
    principal_type: str,
    principal_id: str,
    resource: str,
    action: str,
    scope: str = "*",
) -> bool:
    """Revoke a capability grant."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE capability_grants
            SET revoked_at = NOW()
            WHERE tenant_id      = %s
              AND principal_type  = %s
              AND principal_id   = %s
              AND resource       = %s
              AND action         = %s
              AND scope          = %s
              AND revoked_at     IS NULL
            RETURNING id
            """,
            (str(tenant_id), principal_type, principal_id, resource, action, scope),
        )
        row = cur.fetchone()
        conn.commit()
        return row is not None


def check_capability(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID,
    principal_type: str,
    principal_id: str,
    resource: str,
    action: str,
    scope: str = "*",
) -> bool:
    """Check if a principal holds an active (non-revoked, non-expired) capability.

    Matches exact scope or wildcard ('*') grants.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM capability_grants
            WHERE tenant_id      = %s
              AND principal_type  = %s
              AND principal_id   = %s
              AND resource       = %s
              AND action         = %s
              AND (scope = %s OR scope = '*')
              AND revoked_at     IS NULL
              AND (expires_at IS NULL OR expires_at > NOW())
            LIMIT 1
            """,
            (str(tenant_id), principal_type, principal_id, resource, action, scope),
        )
        return cur.fetchone() is not None


def list_capabilities(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID,
    principal_type: str,
    principal_id: str,
) -> list[dict[str, Any]]:
    """List all active capabilities for a principal."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM capability_grants
            WHERE tenant_id      = %s
              AND principal_type  = %s
              AND principal_id   = %s
              AND revoked_at     IS NULL
              AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY resource, action
            """,
            (str(tenant_id), principal_type, principal_id),
        )
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Capability enforcement
# ---------------------------------------------------------------------------


def enforce_capability(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID,
    principal_type: str,
    principal_id: str,
    resource: str,
    action: str,
    scope: str = "*",
) -> None:
    """Enforce that a principal holds a required capability. Raises on denial.

    Fails open when no grants exist for the tenant (opt-in enforcement).
    Once any grant exists for the tenant, all principals must hold explicit
    grants for gated operations.

    Raises:
        PermissionError: Principal lacks the required capability.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS(SELECT 1 FROM capability_grants WHERE tenant_id = %s "
            "AND revoked_at IS NULL LIMIT 1)",
            (str(tenant_id),),
        )
        row = cur.fetchone()
        has_any_grants = row["exists"] if row else False

    if not has_any_grants:
        return

    if not check_capability(
        conn,
        tenant_id=tenant_id,
        principal_type=principal_type,
        principal_id=principal_id,
        resource=resource,
        action=action,
        scope=scope,
    ):
        raise PermissionError(
            f"{principal_type}:{principal_id} lacks capability "
            f"{resource}:{action}:{scope} in tenant {tenant_id}"
        )


# ---------------------------------------------------------------------------
# Environment / backend detection
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Billing: Plans
# ---------------------------------------------------------------------------


def create_plan(
    conn: "psycopg.Connection",
    *,
    plan_id: str,
    name: str,
    tier: str,
    monthly_task_limit: int = 0,
    monthly_permit_limit: int = 0,
    monthly_effect_limit: int = 0,
    price_cents: int = 0,
    stripe_price_id: str = "",
) -> dict:
    """Create a billing plan. Returns the plan dict."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO billing_plans
                (plan_id, name, tier, monthly_task_limit, monthly_permit_limit,
                 monthly_effect_limit, price_cents, stripe_price_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (plan_id) DO NOTHING
            RETURNING *
            """,
            (plan_id, name, tier, monthly_task_limit, monthly_permit_limit,
             monthly_effect_limit, price_cents, stripe_price_id or None),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else get_plan(conn, plan_id=plan_id)


def get_plan(conn: "psycopg.Connection", *, plan_id: str) -> Optional[dict]:
    """Get a plan by ID."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM billing_plans WHERE plan_id = %s", (plan_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def list_plans(conn: "psycopg.Connection") -> list[dict]:
    """List all billing plans."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM billing_plans ORDER BY price_cents ASC")
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Billing: Subscriptions
# ---------------------------------------------------------------------------


def subscribe_tenant(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    plan_id: str,
    stripe_customer_id: str = "",
    stripe_subscription_id: str = "",
) -> dict:
    """Subscribe a tenant to a plan. Upserts if subscription already exists."""
    tid = str(tenant_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tenant_subscriptions
                (tenant_id, plan_id, stripe_customer_id, stripe_subscription_id,
                 billing_cycle_start, billing_cycle_end)
            VALUES (%s, %s, %s, %s, NOW(), NOW() + INTERVAL '30 days')
            ON CONFLICT (tenant_id) DO UPDATE SET
                plan_id = EXCLUDED.plan_id,
                stripe_customer_id = EXCLUDED.stripe_customer_id,
                stripe_subscription_id = EXCLUDED.stripe_subscription_id,
                billing_cycle_start = NOW(),
                billing_cycle_end = NOW() + INTERVAL '30 days',
                status = 'active'
            RETURNING *
            """,
            (tid, plan_id, stripe_customer_id or None, stripe_subscription_id or None),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else {}


def get_subscription(conn: "psycopg.Connection", *, tenant_id: UUID | str) -> Optional[dict]:
    """Get the current subscription for a tenant."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM tenant_subscriptions WHERE tenant_id = %s",
            (str(tenant_id),),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def cancel_subscription(conn: "psycopg.Connection", *, tenant_id: UUID | str) -> bool:
    """Cancel a tenant's subscription."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tenant_subscriptions SET status = 'canceled'
            WHERE tenant_id = %s AND status != 'canceled'
            """,
            (str(tenant_id),),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


def update_subscription_status(
    conn: "psycopg.Connection", *, tenant_id: UUID | str, status: str
) -> bool:
    """Update subscription status (e.g. from Stripe webhook)."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tenant_subscriptions SET status = %s WHERE tenant_id = %s",
            (status, str(tenant_id)),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


# ---------------------------------------------------------------------------
# Billing: Usage metering
# ---------------------------------------------------------------------------


def _current_billing_period() -> str:
    """Return the current billing period as YYYY-MM."""
    now = datetime.now(timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"


def record_usage(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    organization_id: str,
    meter_type: str,
    reference_id: str,
    billing_period: str = "",
) -> bool:
    """Record a billable usage event. Idempotent via (meter_type, reference_id).

    Returns True if newly recorded, False if duplicate.
    """
    period = billing_period or _current_billing_period()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO usage_meters
                (tenant_id, organization_id, meter_type, reference_id, billing_period)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (meter_type, reference_id) DO NOTHING
            RETURNING meter_id
            """,
            (str(tenant_id), organization_id, meter_type, reference_id, period),
        )
        row = cur.fetchone()
    conn.commit()
    return row is not None


def get_usage_summary(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    billing_period: str = "",
) -> dict:
    """Get usage counts by meter_type for a tenant in a billing period."""
    period = billing_period or _current_billing_period()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT meter_type, COUNT(*) as count
            FROM usage_meters
            WHERE tenant_id = %s AND billing_period = %s
            GROUP BY meter_type
            """,
            (str(tenant_id), period),
        )
        rows = cur.fetchall()
    result = {"task_claim": 0, "permit_consume": 0, "effect_record": 0}
    for row in rows:
        result[row["meter_type"]] = row["count"]
    return result


def check_quota(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    meter_type: str,
) -> tuple[bool, int, int]:
    """Check if a tenant is within their quota for a meter type.

    Returns (allowed, used, limit). Enterprise tier always allowed.

    A tenant with no explicit subscription row defaults to the seeded
    'free' plan's limits rather than being hard-rejected. Tenant
    registration (create_tenant, or simply being referenced by an ad hoc
    tenant_id) and billing enrollment are two separate concerns; treating
    "no subscription yet" as "zero quota forever" would make every
    unregistered tenant permanently unable to claim, issue, or record
    anything — effectively a silent, blanket authority denial unrelated to
    any real billing decision. Defaulting to the free tier mirrors the
    explicit free-tier subscription already seeded for DEFAULT_TENANT_ID
    at migration time (see the v6->v7 migration) and keeps the actual quota
    enforcement (usage compared against a real limit) intact — this is not
    a bypass, it is the correct default tier.
    """
    tid = str(tenant_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bp.tier, bp.monthly_task_limit, bp.monthly_permit_limit,
                   bp.monthly_effect_limit
            FROM tenant_subscriptions ts
            JOIN billing_plans bp ON bp.plan_id = ts.plan_id
            WHERE ts.tenant_id = %s AND ts.status IN ('active', 'trialing')
            """,
            (tid,),
        )
        sub_row = cur.fetchone()

    if not sub_row:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tier, monthly_task_limit, monthly_permit_limit,
                       monthly_effect_limit
                FROM billing_plans WHERE plan_id = 'free'
                """
            )
            sub_row = cur.fetchone()
        if not sub_row:
            # The free plan itself is missing (should never happen post-
            # migration) — fail closed rather than assume unlimited access.
            return (False, 0, 0)

    limit_map = {
        "task_claim": sub_row["monthly_task_limit"],
        "permit_consume": sub_row["monthly_permit_limit"],
        "effect_record": sub_row["monthly_effect_limit"],
    }
    limit = limit_map.get(meter_type, 0)

    if sub_row["tier"] == "enterprise":
        usage = get_usage_summary(conn, tenant_id=tid)
        used = usage.get(meter_type, 0)
        return (True, used, 0)

    usage = get_usage_summary(conn, tenant_id=tid)
    used = usage.get(meter_type, 0)

    if sub_row["tier"] == "free":
        return (used < limit, used, limit)

    # Paid tiers: soft limit (allow overage, will be billed)
    return (True, used, limit)


# ---------------------------------------------------------------------------
# Billing: Invoices
# ---------------------------------------------------------------------------


def generate_invoice(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    billing_period: str = "",
) -> dict:
    """Generate an invoice for a tenant's billing period.

    Calculates line items from usage meters and the tenant's plan.
    Idempotent: returns existing invoice if already generated.
    """
    period = billing_period or _current_billing_period()
    tid = str(tenant_id)

    # Check for existing invoice
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM invoices WHERE tenant_id = %s AND billing_period = %s",
            (tid, period),
        )
        existing = cur.fetchone()
    if existing:
        return dict(existing)

    # Get subscription and plan
    sub = get_subscription(conn, tenant_id=tid)
    if not sub:
        return {}
    plan = get_plan(conn, plan_id=sub["plan_id"])
    if not plan:
        return {}

    usage = get_usage_summary(conn, tenant_id=tid, billing_period=period)

    line_items = []
    total_cents = plan["price_cents"]

    if total_cents > 0:
        line_items.append({
            "description": f"{plan['name']} plan - base fee",
            "amount_cents": plan["price_cents"],
        })

    # Overage for paid tiers
    if plan["tier"] in ("starter", "pro"):
        overage_rates = {"task_claim": 10, "permit_consume": 5, "effect_record": 2}
        limits = {
            "task_claim": plan["monthly_task_limit"],
            "permit_consume": plan["monthly_permit_limit"],
            "effect_record": plan["monthly_effect_limit"],
        }
        for meter_type, rate in overage_rates.items():
            used = usage.get(meter_type, 0)
            limit = limits[meter_type]
            if used > limit:
                overage = used - limit
                cost = overage * rate
                total_cents += cost
                line_items.append({
                    "description": f"{meter_type} overage: {overage} × ${rate/100:.2f}",
                    "amount_cents": cost,
                })

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO invoices (tenant_id, billing_period, line_items, total_cents, status)
            VALUES (%s, %s, %s, %s, 'draft')
            ON CONFLICT (tenant_id, billing_period) DO NOTHING
            RETURNING *
            """,
            (tid, period, json.dumps(line_items), total_cents),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else {}


def mark_invoice_paid(
    conn: "psycopg.Connection",
    *,
    invoice_id: str,
    stripe_invoice_id: str = "",
) -> bool:
    """Mark an invoice as paid."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE invoices
            SET status = 'paid', paid_at = NOW(), stripe_invoice_id = %s
            WHERE invoice_id = %s AND status != 'paid'
            """,
            (stripe_invoice_id or None, invoice_id),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


def get_invoice(conn: "psycopg.Connection", *, invoice_id: str) -> Optional[dict]:
    """Get an invoice by ID."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM invoices WHERE invoice_id = %s::uuid", (invoice_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def list_invoices(conn: "psycopg.Connection", *, tenant_id: UUID | str) -> list[dict]:
    """List all invoices for a tenant."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM invoices WHERE tenant_id = %s ORDER BY billing_period DESC",
            (str(tenant_id),),
        )
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Marketplace: Listings
# ---------------------------------------------------------------------------


def publish_listing(
    conn: "psycopg.Connection",
    *,
    listing_id: str,
    name: str,
    description: str = "",
    category: str = "general",
    author: str = "",
    version: str,
    package_type: str,
    package_ref: str = "",
    entry_point: str = "",
    capabilities_required: list[str] | None = None,
    dependencies: list[dict] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict:
    """Publish a tool/skill listing to the marketplace."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO marketplace_listings
                (listing_id, name, description, category, author, version,
                 package_type, package_ref, entry_point,
                 capabilities_required, dependencies, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (listing_id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                category = EXCLUDED.category,
                version = EXCLUDED.version,
                package_ref = EXCLUDED.package_ref,
                entry_point = EXCLUDED.entry_point,
                capabilities_required = EXCLUDED.capabilities_required,
                dependencies = EXCLUDED.dependencies,
                metadata = EXCLUDED.metadata
            RETURNING *
            """,
            (
                listing_id, name, description, category, author, version,
                package_type, package_ref, entry_point,
                capabilities_required or [],
                json.dumps(dependencies or []),
                json.dumps(metadata or {}),
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else {}


def get_listing(conn: "psycopg.Connection", *, listing_id: str) -> Optional[dict]:
    """Get a marketplace listing by ID."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM marketplace_listings WHERE listing_id = %s",
            (listing_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def search_listings(
    conn: "psycopg.Connection",
    *,
    query: str = "",
    category: str = "",
    limit: int = 50,
) -> list[dict]:
    """Search marketplace listings by name/description or category."""
    conditions = ["active = true"]
    params: list[Any] = []

    if query:
        conditions.append(
            "(name ILIKE %s OR description ILIKE %s)"
        )
        params.extend([f"%{query}%", f"%{query}%"])

    if category:
        conditions.append("category = %s")
        params.append(category)

    where = " AND ".join(conditions)
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM marketplace_listings WHERE {where} "
            f"ORDER BY published_at DESC LIMIT %s",
            params,
        )
        return [dict(r) for r in cur.fetchall()]


def update_listing_version(
    conn: "psycopg.Connection",
    *,
    listing_id: str,
    version: str,
    package_ref: str = "",
) -> bool:
    """Update a listing's version (and optionally package_ref)."""
    with conn.cursor() as cur:
        if package_ref:
            cur.execute(
                "UPDATE marketplace_listings SET version = %s, package_ref = %s "
                "WHERE listing_id = %s",
                (version, package_ref, listing_id),
            )
        else:
            cur.execute(
                "UPDATE marketplace_listings SET version = %s WHERE listing_id = %s",
                (version, listing_id),
            )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


# ---------------------------------------------------------------------------
# Marketplace: Installations
# ---------------------------------------------------------------------------


def install_tool(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    listing_id: str,
    version: str,
    installed_by: str = "",
    config: dict | None = None,
) -> dict:
    """Install a tool for a tenant. Upserts (re-install upgrades version)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO marketplace_installations
                (tenant_id, listing_id, version, installed_by, config)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, listing_id) DO UPDATE SET
                version = EXCLUDED.version,
                installed_by = EXCLUDED.installed_by,
                config = EXCLUDED.config,
                enabled = true,
                installed_at = NOW()
            RETURNING *
            """,
            (str(tenant_id), listing_id, version, installed_by,
             json.dumps(config or {})),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else {}


def uninstall_tool(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    listing_id: str,
) -> bool:
    """Remove a tool installation for a tenant."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM marketplace_installations "
            "WHERE tenant_id = %s AND listing_id = %s",
            (str(tenant_id), listing_id),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


def get_installed_tools(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
) -> list[dict]:
    """List all installed tools for a tenant (with listing metadata)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT i.*, l.name, l.description, l.category, l.package_type,
                   l.entry_point
            FROM marketplace_installations i
            JOIN marketplace_listings l ON l.listing_id = i.listing_id
            WHERE i.tenant_id = %s AND i.enabled = true
            ORDER BY i.installed_at DESC
            """,
            (str(tenant_id),),
        )
        return [dict(r) for r in cur.fetchall()]


def enable_tool(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    listing_id: str,
) -> bool:
    """Enable a disabled tool installation."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE marketplace_installations SET enabled = true "
            "WHERE tenant_id = %s AND listing_id = %s AND enabled = false",
            (str(tenant_id), listing_id),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


def disable_tool(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    listing_id: str,
) -> bool:
    """Disable a tool installation without uninstalling."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE marketplace_installations SET enabled = false "
            "WHERE tenant_id = %s AND listing_id = %s AND enabled = true",
            (str(tenant_id), listing_id),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


def get_tool_config(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    listing_id: str,
) -> Optional[dict]:
    """Get the configuration for an installed tool."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT config FROM marketplace_installations "
            "WHERE tenant_id = %s AND listing_id = %s",
            (str(tenant_id), listing_id),
        )
        row = cur.fetchone()
    return row["config"] if row else None


def update_tool_config(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    listing_id: str,
    config: dict,
) -> bool:
    """Update configuration for an installed tool."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE marketplace_installations SET config = %s "
            "WHERE tenant_id = %s AND listing_id = %s",
            (json.dumps(config), str(tenant_id), listing_id),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


# ---------------------------------------------------------------------------
# Marketplace: Reviews
# ---------------------------------------------------------------------------


def submit_review(
    conn: "psycopg.Connection",
    *,
    listing_id: str,
    tenant_id: UUID | str,
    rating: int,
    review_text: str = "",
) -> dict:
    """Submit or update a review for a listing (one review per tenant per listing)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO marketplace_reviews
                (listing_id, tenant_id, rating, review_text)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (tenant_id, listing_id) DO UPDATE SET
                rating = EXCLUDED.rating,
                review_text = EXCLUDED.review_text
            RETURNING *
            """,
            (listing_id, str(tenant_id), rating, review_text),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else {}


def get_reviews(
    conn: "psycopg.Connection",
    *,
    listing_id: str,
    limit: int = 20,
) -> list[dict]:
    """Get reviews for a listing, most recent first."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM marketplace_reviews WHERE listing_id = %s "
            "ORDER BY created_at DESC LIMIT %s",
            (listing_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Integrations: Credentials
# ---------------------------------------------------------------------------


def store_credential(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    integration_id: str,
    provider: str,
    credential_type: str,
    data: str,
    scopes: list[str] | None = None,
    expires_at: float | None = None,
) -> dict:
    """Store an integration credential (upserts on conflict)."""
    exp_dt = _ts(expires_at) if expires_at else None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO integration_credentials
                (tenant_id, integration_id, provider, credential_type,
                 encrypted_data, scopes, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, integration_id, provider) DO UPDATE SET
                credential_type = EXCLUDED.credential_type,
                encrypted_data = EXCLUDED.encrypted_data,
                scopes = EXCLUDED.scopes,
                expires_at = EXCLUDED.expires_at,
                updated_at = NOW()
            RETURNING *
            """,
            (str(tenant_id), integration_id, provider, credential_type,
             data, scopes or [], exp_dt),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else {}


def get_credential(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    integration_id: str,
    provider: str,
) -> Optional[dict]:
    """Get a stored credential."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM integration_credentials "
            "WHERE tenant_id = %s AND integration_id = %s AND provider = %s",
            (str(tenant_id), integration_id, provider),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def delete_credential(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    integration_id: str,
    provider: str,
) -> bool:
    """Delete a credential."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM integration_credentials "
            "WHERE tenant_id = %s AND integration_id = %s AND provider = %s",
            (str(tenant_id), integration_id, provider),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


def list_credentials(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
) -> list[dict]:
    """List all credentials for a tenant (without encrypted_data)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, tenant_id, integration_id, provider, credential_type, "
            "scopes, expires_at, created_at, updated_at "
            "FROM integration_credentials WHERE tenant_id = %s "
            "ORDER BY created_at DESC",
            (str(tenant_id),),
        )
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Integrations: Webhooks
# ---------------------------------------------------------------------------


def register_webhook(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    integration_id: str,
    provider: str,
    event_type: str,
    webhook_url: str,
    secret: str = "",
) -> dict:
    """Register a webhook subscription (upserts)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO integration_webhooks
                (tenant_id, integration_id, provider, event_type, webhook_url, secret)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, integration_id, event_type) DO UPDATE SET
                webhook_url = EXCLUDED.webhook_url,
                secret = EXCLUDED.secret,
                provider = EXCLUDED.provider,
                active = true
            RETURNING *
            """,
            (str(tenant_id), integration_id, provider, event_type, webhook_url, secret),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else {}


def deactivate_webhook(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    integration_id: str,
    event_type: str,
) -> bool:
    """Deactivate a webhook subscription."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE integration_webhooks SET active = false "
            "WHERE tenant_id = %s AND integration_id = %s AND event_type = %s "
            "AND active = true",
            (str(tenant_id), integration_id, event_type),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


def list_webhooks(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
) -> list[dict]:
    """List all active webhooks for a tenant."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM integration_webhooks "
            "WHERE tenant_id = %s AND active = true ORDER BY created_at DESC",
            (str(tenant_id),),
        )
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Integrations: Events
# ---------------------------------------------------------------------------


def record_integration_event(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    integration_id: str,
    provider: str,
    event_type: str,
    event_id: str,
    payload: dict[str, Any],
) -> bool:
    """Record an inbound integration event. Idempotent via (tenant, integration, event_id).

    Returns True if newly recorded, False if duplicate.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO integration_events
                (tenant_id, integration_id, provider, event_type, event_id, payload)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, integration_id, event_id) DO NOTHING
            RETURNING id
            """,
            (str(tenant_id), integration_id, provider, event_type, event_id,
             json.dumps(payload)),
        )
        row = cur.fetchone()
    conn.commit()
    return row is not None


def get_unprocessed_events(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    limit: int = 50,
) -> list[dict]:
    """Get unprocessed events for a tenant, oldest first."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM integration_events "
            "WHERE tenant_id = %s AND processed = false "
            "ORDER BY received_at ASC LIMIT %s",
            (str(tenant_id), limit),
        )
        return [dict(r) for r in cur.fetchall()]


def mark_event_processed(
    conn: "psycopg.Connection",
    *,
    event_id_internal: int,
) -> bool:
    """Mark an event as processed."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE integration_events SET processed = true WHERE id = %s AND processed = false",
            (event_id_internal,),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


# ---------------------------------------------------------------------------
# Monitoring: Metrics
# ---------------------------------------------------------------------------


def record_metric(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    metric_name: str,
    metric_type: str,
    value: float,
    labels: dict[str, str] | None = None,
) -> None:
    """Record a metric data point."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metrics (tenant_id, metric_name, metric_type, value, labels)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (str(tenant_id), metric_name, metric_type, value,
             json.dumps(labels or {})),
        )
    conn.commit()


def query_metrics(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    metric_name: str,
    since_seconds: int = 3600,
    limit: int = 1000,
) -> list[dict]:
    """Query metric data points for a tenant within a time window."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM metrics
            WHERE tenant_id = %s AND metric_name = %s
              AND recorded_at > NOW() - make_interval(secs => %s)
            ORDER BY recorded_at DESC LIMIT %s
            """,
            (str(tenant_id), metric_name, since_seconds, limit),
        )
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Monitoring: Health Checks
# ---------------------------------------------------------------------------


def record_health_check(
    conn: "psycopg.Connection",
    *,
    service_name: str,
    status: str,
    details: dict[str, Any] | None = None,
) -> None:
    """Record a health check result."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO health_checks (service_name, status, details) "
            "VALUES (%s, %s, %s)",
            (service_name, status, json.dumps(details or {})),
        )
    conn.commit()


def get_latest_health(
    conn: "psycopg.Connection",
    *,
    service_name: str = "",
) -> list[dict]:
    """Get the latest health check for each service (or one specific service)."""
    if service_name:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT ON (service_name) * FROM health_checks "
                "WHERE service_name = %s ORDER BY service_name, checked_at DESC",
                (service_name,),
            )
            return [dict(r) for r in cur.fetchall()]
    else:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT ON (service_name) * FROM health_checks "
                "ORDER BY service_name, checked_at DESC"
            )
            return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Monitoring: Alert Rules
# ---------------------------------------------------------------------------


def create_alert_rule(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    name: str,
    metric_name: str,
    condition: str,
    threshold: float,
    window_seconds: int = 300,
    notify_channel: str = "",
) -> dict:
    """Create an alert rule (upserts on name)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO alert_rules
                (tenant_id, name, metric_name, condition, threshold,
                 window_seconds, notify_channel)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, name) DO UPDATE SET
                metric_name = EXCLUDED.metric_name,
                condition = EXCLUDED.condition,
                threshold = EXCLUDED.threshold,
                window_seconds = EXCLUDED.window_seconds,
                notify_channel = EXCLUDED.notify_channel,
                enabled = true
            RETURNING *
            """,
            (str(tenant_id), name, metric_name, condition, threshold,
             window_seconds, notify_channel),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else {}


def list_alert_rules(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
) -> list[dict]:
    """List alert rules for a tenant."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM alert_rules WHERE tenant_id = %s AND enabled = true "
            "ORDER BY created_at DESC",
            (str(tenant_id),),
        )
        return [dict(r) for r in cur.fetchall()]


def disable_alert_rule(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    name: str,
) -> bool:
    """Disable an alert rule."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE alert_rules SET enabled = false "
            "WHERE tenant_id = %s AND name = %s AND enabled = true",
            (str(tenant_id), name),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


# ---------------------------------------------------------------------------
# Disaster Recovery: Backups
# ---------------------------------------------------------------------------


def create_backup(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    backup_type: str,
    storage_path: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict:
    """Create a backup record."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO backup_records
                (tenant_id, backup_type, status, storage_path,
                 schema_version, metadata)
            VALUES (%s, %s, 'pending', %s, %s, %s)
            RETURNING *
            """,
            (str(tenant_id), backup_type, storage_path,
             SCHEMA_VERSION, json.dumps(metadata or {})),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else {}


def complete_backup(
    conn: "psycopg.Connection",
    *,
    backup_id: int,
    size_bytes: int = 0,
    storage_path: str = "",
) -> bool:
    """Mark a backup as completed."""
    with conn.cursor() as cur:
        params = [backup_id]
        set_parts = ["status = 'completed'", "completed_at = NOW()"]
        if size_bytes:
            set_parts.append("size_bytes = %s")
            params.insert(-1, size_bytes)
            params = [size_bytes, backup_id]
        if storage_path:
            set_parts.append("storage_path = %s")
            params = [size_bytes, storage_path, backup_id] if size_bytes else [storage_path, backup_id]
        cur.execute(
            f"UPDATE backup_records SET {', '.join(set_parts)} WHERE id = %s",
            params,
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


def list_backups(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    limit: int = 20,
) -> list[dict]:
    """List backups for a tenant."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM backup_records WHERE tenant_id = %s "
            "ORDER BY started_at DESC LIMIT %s",
            (str(tenant_id), limit),
        )
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Disaster Recovery: Restore Points
# ---------------------------------------------------------------------------


def create_restore_point(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    name: str,
    backup_id: int | None = None,
) -> dict:
    """Create a named restore point (upserts on name)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO restore_points (tenant_id, name, backup_id, schema_version)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (tenant_id, name) DO UPDATE SET
                backup_id = EXCLUDED.backup_id,
                schema_version = EXCLUDED.schema_version,
                created_at = NOW()
            RETURNING *
            """,
            (str(tenant_id), name, backup_id, SCHEMA_VERSION),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else {}


def list_restore_points(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
) -> list[dict]:
    """List restore points for a tenant."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM restore_points WHERE tenant_id = %s "
            "ORDER BY created_at DESC",
            (str(tenant_id),),
        )
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Disaster Recovery: Failover Drills
# ---------------------------------------------------------------------------


def schedule_drill(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    drill_type: str,
) -> dict:
    """Schedule a failover drill."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO failover_drills (tenant_id, drill_type, status)
            VALUES (%s, %s, 'scheduled')
            RETURNING *
            """,
            (str(tenant_id), drill_type),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else {}


def complete_drill(
    conn: "psycopg.Connection",
    *,
    drill_id: int,
    status: str,
    results: dict[str, Any] | None = None,
) -> bool:
    """Complete a failover drill with results."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE failover_drills SET status = %s, completed_at = NOW(), "
            "results = %s WHERE id = %s",
            (status, json.dumps(results or {}), drill_id),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


def list_drills(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    limit: int = 20,
) -> list[dict]:
    """List failover drills for a tenant."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM failover_drills WHERE tenant_id = %s "
            "ORDER BY created_at DESC LIMIT %s",
            (str(tenant_id), limit),
        )
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Security: Audit Log
# ---------------------------------------------------------------------------


def record_audit_event(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    actor_type: str,
    actor_id: str,
    action: str,
    resource_type: str = "",
    resource_id: str = "",
    outcome: str,
    details: dict[str, Any] | None = None,
    ip_address: str = "",
    user_agent: str = "",
) -> None:
    """Record an immutable audit log entry."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit_log
                (tenant_id, actor_type, actor_id, action, resource_type,
                 resource_id, outcome, details, ip_address, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (str(tenant_id), actor_type, actor_id, action, resource_type,
             resource_id, outcome, json.dumps(details or {}),
             ip_address, user_agent),
        )
    conn.commit()


def query_audit_log(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    actor_id: str = "",
    action: str = "",
    resource_type: str = "",
    since_seconds: int = 86400,
    limit: int = 100,
) -> list[dict]:
    """Query audit log entries with optional filters."""
    conditions = ["tenant_id = %s", "recorded_at > NOW() - make_interval(secs => %s)"]
    params: list[Any] = [str(tenant_id), since_seconds]

    if actor_id:
        conditions.append("actor_id = %s")
        params.append(actor_id)
    if action:
        conditions.append("action = %s")
        params.append(action)
    if resource_type:
        conditions.append("resource_type = %s")
        params.append(resource_type)

    params.append(limit)
    where = " AND ".join(conditions)

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM audit_log WHERE {where} "
            f"ORDER BY recorded_at DESC LIMIT %s",
            params,
        )
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Security: Secret Management
# ---------------------------------------------------------------------------


def store_secret(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    secret_name: str,
    encrypted_value: str,
    created_by: str = "",
    expires_at: float | None = None,
) -> dict:
    """Store a secret (upserts, incrementing version on update)."""
    exp_dt = _ts(expires_at) if expires_at else None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO secrets
                (tenant_id, secret_name, encrypted_value, created_by, expires_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, secret_name) DO UPDATE SET
                encrypted_value = EXCLUDED.encrypted_value,
                version = secrets.version + 1,
                rotated_at = NOW(),
                expires_at = EXCLUDED.expires_at
            RETURNING *
            """,
            (str(tenant_id), secret_name, encrypted_value, created_by, exp_dt),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else {}


def get_secret(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    secret_name: str,
) -> Optional[dict]:
    """Get a secret by name."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM secrets WHERE tenant_id = %s AND secret_name = %s",
            (str(tenant_id), secret_name),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def delete_secret(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
    secret_name: str,
) -> bool:
    """Delete a secret."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM secrets WHERE tenant_id = %s AND secret_name = %s",
            (str(tenant_id), secret_name),
        )
        affected = cur.rowcount
    conn.commit()
    return affected > 0


def list_secrets(
    conn: "psycopg.Connection",
    *,
    tenant_id: UUID | str,
) -> list[dict]:
    """List secrets for a tenant (names and metadata only, no values)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, tenant_id, secret_name, version, created_by, "
            "created_at, rotated_at, expires_at "
            "FROM secrets WHERE tenant_id = %s ORDER BY secret_name",
            (str(tenant_id),),
        )
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Backend detection
# ---------------------------------------------------------------------------


def get_authority_backend() -> str:
    """Detect which authority backend to use.

    Returns 'postgres' if AUTHORITY_POSTGRES_URL or DATABASE_URL is set,
    otherwise 'sqlite'.
    """
    if os.environ.get("AUTHORITY_POSTGRES_URL") or os.environ.get("DATABASE_URL"):
        return "postgres"
    return "sqlite"


def get_authority_connection():
    """Get authority connection based on environment.

    Returns:
        Connection for the appropriate backend (Postgres or SQLite).
    """
    backend = get_authority_backend()
    if backend == "postgres":
        conn = connect()
        init_schema(conn)
        return conn
    else:
        from hermes_cli.kanban_db import connect as sqlite_connect
        return sqlite_connect()


__all__ = [
    "connect",
    "init_schema",
    "get_schema_version",
    "claim_task",
    "reclaim_task",
    "get_claim",
    "release_claim",
    "complete_task",
    "issue_permit",
    "consume_permit",
    "revoke_permit",
    "record_effect",
    "get_effect",
    "get_active_runs",
    "cleanup_expired_claims",
    "create_tenant",
    "get_tenant",
    "suspend_tenant",
    "activate_tenant",
    "check_tenant_claim_quota",
    "create_workspace",
    "get_workspace",
    "list_workspaces",
    "deactivate_workspace",
    "grant_capability",
    "revoke_capability",
    "check_capability",
    "list_capabilities",
    "enforce_capability",
    "create_plan",
    "get_plan",
    "list_plans",
    "subscribe_tenant",
    "get_subscription",
    "cancel_subscription",
    "update_subscription_status",
    "record_usage",
    "get_usage_summary",
    "check_quota",
    "generate_invoice",
    "mark_invoice_paid",
    "get_invoice",
    "list_invoices",
    "record_audit_event",
    "query_audit_log",
    "store_secret",
    "get_secret",
    "delete_secret",
    "list_secrets",
    "create_backup",
    "complete_backup",
    "list_backups",
    "create_restore_point",
    "list_restore_points",
    "schedule_drill",
    "complete_drill",
    "list_drills",
    "record_metric",
    "query_metrics",
    "record_health_check",
    "get_latest_health",
    "create_alert_rule",
    "list_alert_rules",
    "disable_alert_rule",
    "store_credential",
    "get_credential",
    "delete_credential",
    "list_credentials",
    "register_webhook",
    "deactivate_webhook",
    "list_webhooks",
    "record_integration_event",
    "get_unprocessed_events",
    "mark_event_processed",
    "publish_listing",
    "get_listing",
    "search_listings",
    "update_listing_version",
    "install_tool",
    "uninstall_tool",
    "get_installed_tools",
    "enable_tool",
    "disable_tool",
    "get_tool_config",
    "update_tool_config",
    "submit_review",
    "get_reviews",
    "get_authority_backend",
    "get_authority_connection",
    "DEFAULT_TENANT_ID",
    "SCHEMA_VERSION",
]
