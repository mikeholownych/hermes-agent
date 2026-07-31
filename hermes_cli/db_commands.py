"""Database commands for Charterforge authority store.

This module provides CLI commands for managing the authority database:

  - charterforge db init --postgres <url>
  - charterforge db upgrade
  - charterforge db downgrade
  - charterforge db status

These commands support both SQLite (dev) and Postgres (production) backends.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional


def cmd_init(args: argparse.Namespace) -> int:
    """Initialize the authority database.

    Creates schema tables and indexes. For Postgres, also runs migrations.

    Args:
        args: Parsed arguments

    Returns:
        Exit code (0 for success)
    """
    backend = args.backend or _detect_backend()

    if backend == "postgres":
        from hermes_cli.postgres_authority import connect, init_schema

        url = args.postgres_url or _get_postgres_url()
        if not url:
            print("ERROR: Postgres URL required. Set AUTHORITY_POSTGRES_URL or pass --postgres-url", file=sys.stderr)
            return 1

        print(f"Initializing Postgres authority store...")
        conn = connect(url)
        init_schema(conn)
        conn.close()
        print("✓ Postgres authority store initialized")
        return 0

    else:
        from hermes_cli.objectives_db import connect

        path = args.sqlite_path or _get_sqlite_path()
        print(f"Initializing SQLite authority store at {path}...")
        conn = connect(str(path))
        conn.close()
        print("✓ SQLite authority store initialized")
        return 0


def cmd_upgrade(args: argparse.Namespace) -> int:
    """Upgrade database schema to latest version.

    For Postgres, runs migrations.
    For SQLite, schema is auto-created on connect.

    Args:
        args: Parsed arguments

    Returns:
        Exit code (0 for success)
    """
    backend = args.backend or _detect_backend()

    if backend == "postgres":
        from hermes_cli.postgres_authority import connect, init_schema

        url = args.postgres_url or _get_postgres_url()
        if not url:
            print("ERROR: Postgres URL required", file=sys.stderr)
            return 1

        print("Upgrading Postgres schema...")
        conn = connect(url)
        init_schema(conn)
        conn.close()
        print("✓ Postgres schema upgraded")
        return 0

    else:
        print("SQLite schema is auto-created on connect. No upgrade needed.")
        return 0


def cmd_downgrade(args: argparse.Namespace) -> int:
    """Downgrade database schema.

    WARNING: This may drop tables. Use with caution.

    Args:
        args: Parsed arguments

    Returns:
        Exit code (0 for success)
    """
    backend = args.backend or _detect_backend()

    if backend == "postgres":
        from hermes_cli.postgres_authority import connect, get_schema_version, SCHEMA_VERSION, _MIGRATIONS

        url = args.postgres_url or _get_postgres_url()
        if not url:
            print("ERROR: Postgres URL required", file=sys.stderr)
            return 1

        conn = connect(url)
        current_version = get_schema_version(conn)

        if current_version <= 0:
            print("ERROR: No schema version found. Database may not be initialized.", file=sys.stderr)
            conn.close()
            return 1

        # Default to one step down unless --to-version specified
        target = getattr(args, "to_version", None)
        if target is None:
            target = current_version - 1
        else:
            target = int(target)

        if target < 0:
            print(f"ERROR: Cannot downgrade below version 0", file=sys.stderr)
            conn.close()
            return 1

        if target >= current_version:
            print(f"ERROR: Current version {current_version} is already >= target {target}", file=sys.stderr)
            conn.close()
            return 1

        print(f"Downgrading from version {current_version} to {target}...")

        # Walk backwards through migrations, applying reverse DDL where defined
        reversed_migrations = list(reversed(_MIGRATIONS))
        for from_version, migration_sql in reversed_migrations:
            to_version = from_version + 1
            if to_version > current_version:
                continue  # Skip migrations that haven't run yet

            if to_version <= target:
                break  # We've reached the target

            # Check if there's a reverse SQL defined (future: add to migrations tuple)
            # For now, only support v2→v1 downgrade with explicit drop statements
            print(f"  Downgrading from {to_version} to {from_version}...", end=" ")

            if to_version == 2:
                # Drop schema_version table (v1→v2 added it)
                with conn.cursor() as cur:
                    cur.execute("DROP TABLE IF EXISTS schema_version")
            elif to_version == 1:
                # Drop all authority tables (v0→v1 created them)
                with conn.cursor() as cur:
                    cur.execute("DROP TABLE IF EXISTS execution_effects")
                    cur.execute("DROP TABLE IF EXISTS task_permits")
                    cur.execute("DROP TABLE IF EXISTS task_runs")
                    cur.execute("DROP TABLE IF EXISTS task_claims")

            conn.commit()
            print("done")

            current_version = from_version

        conn.close()
        print(f"✓ Downgraded to schema version {target}")
        return 0

    else:
        print("SQLite does not support schema downgrade. Re-init to reset.", file=sys.stderr)
        return 1


def cmd_status(args: argparse.Namespace) -> int:
    """Show database connection and schema status.

    Args:
        args: Parsed arguments

    Returns:
        Exit code (0 for success)
    """
    backend = args.backend or _detect_backend()

    print(f"Authority Backend: {backend}")

    if backend == "postgres":
        from hermes_cli.postgres_authority import connect, SCHEMA_VERSION

        url = args.postgres_url or _get_postgres_url()
        if not url:
            print("ERROR: Postgres URL not configured", file=sys.stderr)
            return 1

        try:
            conn = connect(url)
            with conn.cursor() as cur:
                cur.execute("SELECT version, applied_at FROM schema_version ORDER BY version DESC LIMIT 1")
                row = cur.fetchone()

                if row:
                    print(f"Schema Version: {row['version']}")
                    print(f"Applied At: {row['applied_at']}")
                else:
                    print("Schema Version: (not initialized)")

                # Show table counts
                tables = ["task_claims", "task_runs", "task_permits", "execution_effects"]
                for table in tables:
                    cur.execute(f"SELECT COUNT(*) as count FROM {table}")
                    count = cur.fetchone()["count"]
                    print(f"  {table}: {count} rows")
                # Compare to expected version
                print(f"  Expected version: {SCHEMA_VERSION}")

            conn.close()
            print("✓ Postgres connection healthy")
            return 0

        except Exception as e:
            print(f"ERROR: Failed to connect: {e}", file=sys.stderr)
            return 1

    else:
        from hermes_cli.objectives_db import connect

        path = args.sqlite_path or _get_sqlite_path()
        print(f"Database Path: {path}")

        if not path.exists():
            print("Status: (not initialized)")
            return 0

        try:
            conn = connect(str(path))
            # SQLite schema is auto-created; just show it exists
            print("✓ SQLite connection healthy")
            return 0

        except Exception as e:
            print(f"ERROR: Failed to connect: {e}", file=sys.stderr)
            return 1


def _detect_backend() -> str:
    """Detect the authority backend from environment."""
    from hermes_cli.postgres_authority import get_authority_backend
    return get_authority_backend()


def _get_postgres_url() -> Optional[str]:
    """Get Postgres URL from environment."""
    import os
    return os.environ.get("AUTHORITY_POSTGRES_URL") or os.environ.get("DATABASE_URL")


def _get_sqlite_path() -> Path:
    """Get SQLite database path from environment."""
    from hermes_constants import get_hermes_home

    return get_hermes_home() / "authority.db"


def add_db_parser(subparsers: argparse._SubParsersAction) -> None:
    """Add database commands to CLI parser.

    Args:
        subparsers: Subparser group to add to
    """
    db = subparsers.add_parser(
        "db",
        help="Manage authority database (init, upgrade, status)"
    )
    db_sub = db.add_subparsers(dest="db_command", required=True)

    # charterforge db init
    init_parser = db_sub.add_parser(
        "init",
        help="Initialize authority database"
    )
    init_parser.add_argument(
        "--postgres-url",
        help="Postgres connection URL (or set AUTHORITY_POSTGRES_URL)"
    )
    init_parser.add_argument(
        "--sqlite-path",
        type=Path,
        help="SQLite database path (default: $HERMES_HOME/authority.db)"
    )
    init_parser.add_argument(
        "--backend",
        choices=["postgres", "sqlite"],
        help="Force backend (default: auto-detect)"
    )
    init_parser.set_defaults(func=cmd_init)

    # charterforge db upgrade
    upgrade_parser = db_sub.add_parser(
        "upgrade",
        help="Upgrade schema to latest version"
    )
    upgrade_parser.add_argument(
        "--postgres-url",
        help="Postgres connection URL"
    )
    upgrade_parser.add_argument(
        "--backend",
        choices=["postgres", "sqlite"],
        help="Force backend"
    )
    upgrade_parser.set_defaults(func=cmd_upgrade)

    # charterforge db downgrade
    downgrade_parser = db_sub.add_parser(
        "downgrade",
        help="Downgrade schema (DANGEROUS)"
    )
    downgrade_parser.add_argument(
        "--postgres-url",
        help="Postgres connection URL"
    )
    downgrade_parser.add_argument(
        "--backend",
        choices=["postgres", "sqlite"],
        help="Force backend"
    )
    downgrade_parser.add_argument(
        "--to-version",
        type=int,
        help="Target schema version (default: current - 1)"
    )
    downgrade_parser.set_defaults(func=cmd_downgrade)

    # charterforge db status
    status_parser = db_sub.add_parser(
        "status",
        help="Show database connection and schema status"
    )
    status_parser.add_argument(
        "--postgres-url",
        help="Postgres connection URL"
    )
    status_parser.add_argument(
        "--sqlite-path",
        type=Path,
        help="SQLite database path"
    )
    status_parser.add_argument(
        "--backend",
        choices=["postgres", "sqlite"],
        help="Force backend"
    )
    status_parser.set_defaults(func=cmd_status)


__all__ = [
    "cmd_init",
    "cmd_upgrade",
    "cmd_downgrade",
    "cmd_status",
    "add_db_parser",
]
