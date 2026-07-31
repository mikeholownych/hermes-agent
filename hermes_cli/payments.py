"""Pluggable receivables and payables with provider read-back verification."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from importlib import metadata
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from hermes_cli import accounting_db, compliance_db, finance_db, payment_controls

logger = logging.getLogger(__name__)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS payment_intents (
    id                 TEXT PRIMARY KEY,
    organization_id    TEXT NOT NULL,
    account_id         TEXT NOT NULL,
    objective_id       TEXT,
    action_id          TEXT,
    direction          TEXT NOT NULL,
    provider           TEXT NOT NULL,
    party_json         TEXT NOT NULL,
    amount_minor       INTEGER NOT NULL,
    currency           TEXT NOT NULL,
    purpose            TEXT NOT NULL,
    status             TEXT NOT NULL,
    provider_reference TEXT,
    payment_url        TEXT,
    idempotency_key    TEXT NOT NULL UNIQUE,
    metadata_json      TEXT NOT NULL,
    tax_minor          INTEGER NOT NULL DEFAULT 0,
    tax_rule_id        TEXT,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS payment_provider_readbacks (
    id TEXT PRIMARY KEY, payment_intent_id TEXT NOT NULL,
    provider TEXT NOT NULL, provider_reference TEXT NOT NULL,
    status TEXT NOT NULL, amount_minor INTEGER NOT NULL,
    currency TEXT NOT NULL, evidence_json TEXT NOT NULL,
    observed_at INTEGER NOT NULL,
    FOREIGN KEY(payment_intent_id) REFERENCES payment_intents(id)
);
CREATE INDEX IF NOT EXISTS idx_payment_readbacks_intent
    ON payment_provider_readbacks(payment_intent_id,observed_at,id);
CREATE TRIGGER IF NOT EXISTS payment_provider_readbacks_immutable_update
BEFORE UPDATE ON payment_provider_readbacks
BEGIN SELECT RAISE(ABORT, 'payment provider readbacks are immutable'); END;
CREATE TRIGGER IF NOT EXISTS payment_provider_readbacks_immutable_delete
BEFORE DELETE ON payment_provider_readbacks
BEGIN SELECT RAISE(ABORT, 'payment provider readbacks are immutable'); END;
"""


@dataclass(frozen=True)
class ProviderPayment:
    reference: str
    status: str
    amount_minor: int
    currency: str
    payment_url: Optional[str] = None
    evidence: Any = None


class UncertainProviderAction(RuntimeError):
    """A provider may have acted, but the caller did not receive a final reply.

    The reference is intentionally optional: providers that return a request
    or idempotency reference in a timeout can be reconciled immediately;
    providers that do not must remain stopped until an operator supplies
    authoritative evidence.
    """

    def __init__(self, message: str, *, provider_reference: str = "", evidence: Any = None):
        super().__init__(message)
        self.provider_reference = provider_reference
        self.evidence = evidence


class InboundPaymentRail(ABC):
    name: str

    @abstractmethod
    def create_receivable(
        self,
        *,
        amount_minor: int,
        currency: str,
        customer: Mapping[str, Any],
        purpose: str,
        idempotency_key: str,
    ) -> ProviderPayment: ...

    @abstractmethod
    def get_payment(self, reference: str) -> ProviderPayment: ...


class OutboundPaymentRail(ABC):
    name: str

    @abstractmethod
    def get_payment(self, reference: str) -> ProviderPayment: ...

    @abstractmethod
    def send_payment(
        self,
        *,
        amount_minor: int,
        currency: str,
        payee: Mapping[str, Any],
        instrument_reference: str,
        purpose: str,
        idempotency_key: str,
    ) -> ProviderPayment: ...


class PaymentRail(InboundPaymentRail, OutboundPaymentRail):
    """Compatibility contract for providers that implement both directions."""


def _load_rail_group(group: str, expected_type: type) -> dict[str, Any]:
    discovered: dict[str, Any] = {}
    entry_points = metadata.entry_points()
    selected = (
        entry_points.select(group=group)
        if hasattr(entry_points, "select")
        else entry_points.get(group, ())
    )
    for entry_point in selected:
        loaded = entry_point.load()
        try:
            rail = loaded() if isinstance(loaded, type) else loaded
        except Exception as exc:
            # An installed rail that isn't configured (missing API key/
            # credentials) must not take down discovery for every other
            # caller — callers elsewhere in the runtime (procurement,
            # objective adapters, business e2e) load *all* rails just to
            # find the ones they actually use. Skip the unusable rail
            # rather than propagating its constructor error.
            logger.warning(
                "payment rail entry point %r failed to load: %s",
                entry_point.name,
                exc,
            )
            continue
        if not isinstance(rail, expected_type):
            raise TypeError(
                f"payment rail entry point {entry_point.name!r} does not implement "
                f"{expected_type.__name__}"
            )
        if not rail.name:
            raise ValueError(f"payment rail entry point {entry_point.name!r} has no name")
        discovered[rail.name] = rail
    return discovered


def load_inbound_payment_rails() -> dict[str, InboundPaymentRail]:
    discovered = _load_rail_group(
        "charterforge.inbound_payment_rails", InboundPaymentRail
    )
    for group, expected_type in (
        ("charterforge.payment_rails", PaymentRail),
        ("hermes.inbound_payment_rails", InboundPaymentRail),
        ("hermes.payment_rails", PaymentRail),
    ):
        for name, rail in _load_rail_group(group, expected_type).items():
            discovered.setdefault(name, rail)
    return discovered


def load_outbound_payment_rails() -> dict[str, OutboundPaymentRail]:
    discovered = _load_rail_group(
        "charterforge.outbound_payment_rails", OutboundPaymentRail
    )
    for group, expected_type in (
        ("charterforge.payment_rails", PaymentRail),
        ("hermes.outbound_payment_rails", OutboundPaymentRail),
        ("hermes.payment_rails", PaymentRail),
    ):
        for name, rail in _load_rail_group(group, expected_type).items():
            discovered.setdefault(name, rail)
    return discovered


def load_payment_rails() -> dict[str, PaymentRail]:
    """Load canonical dual-direction providers plus legacy aliases."""
    discovered = _load_rail_group("charterforge.payment_rails", PaymentRail)
    for name, rail in _load_rail_group("hermes.payment_rails", PaymentRail).items():
        discovered.setdefault(name, rail)
    return discovered


def payment_rail_status() -> dict[str, list[dict[str, Any]]]:
    """Report discovered payment rails without requiring provider credentials.

    Entry-point construction is deliberately isolated here: optional providers
    commonly require a secret at construction time.  A missing secret must be
    visible as an unavailable rail, not make an operator's read-only discovery
    command crash or imply that payments are ready.
    """
    groups = {
        "inbound": (
            "charterforge.inbound_payment_rails",
            "charterforge.payment_rails",
            "hermes.inbound_payment_rails",
            "hermes.payment_rails",
        ),
        "outbound": (
            "charterforge.outbound_payment_rails",
            "charterforge.payment_rails",
            "hermes.outbound_payment_rails",
            "hermes.payment_rails",
        ),
    }
    result: dict[str, list[dict[str, Any]]] = {"inbound": [], "outbound": []}
    seen: set[tuple[str, str]] = set()
    entry_points = metadata.entry_points()
    for direction, group_names in groups.items():
        for group in group_names:
            selected = (
                entry_points.select(group=group)
                if hasattr(entry_points, "select")
                else entry_points.get(group, ())
            )
            for entry_point in selected:
                key = (direction, entry_point.name)
                if key in seen:
                    continue
                seen.add(key)
                record: dict[str, Any] = {
                    "name": entry_point.name,
                    "direction": direction,
                    "group": group,
                    "available": False,
                }
                try:
                    loaded = entry_point.load()
                    rail = loaded() if isinstance(loaded, type) else loaded
                    expected = InboundPaymentRail if direction == "inbound" else OutboundPaymentRail
                    if not isinstance(rail, expected):
                        raise TypeError(f"does not implement {expected.__name__}")
                    record["available"] = True
                    record["rail_name"] = rail.name
                except Exception as exc:  # discovery is read-only and fail-closed
                    record["reason"] = f"{type(exc).__name__}: {exc}"
                result[direction].append(record)
    for direction in result:
        result[direction].sort(key=lambda item: (item["name"], item["group"]))
    return result


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Initialize payment tables without releasing an active authority transaction."""
    if conn.in_transaction:
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(payment_intents)")
        }
        readback = conn.execute(
            """SELECT 1 FROM sqlite_master
                WHERE type='table' AND name='payment_provider_readbacks'"""
        ).fetchone()
        if {"tax_minor", "tax_rule_id"}.issubset(columns) and readback is not None:
            return
    conn.executescript(SCHEMA_SQL)
    columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(payment_intents)")
    }
    if "tax_minor" not in columns:
        conn.execute(
            "ALTER TABLE payment_intents ADD COLUMN tax_minor INTEGER NOT NULL DEFAULT 0"
        )
    if "tax_rule_id" not in columns:
        conn.execute("ALTER TABLE payment_intents ADD COLUMN tax_rule_id TEXT")


class PaymentService:
    def __init__(
        self,
        conn: sqlite3.Connection,
        rails: Optional[Mapping[str, PaymentRail]] = None,
        *,
        inbound_rails: Optional[Mapping[str, InboundPaymentRail]] = None,
        outbound_rails: Optional[Mapping[str, OutboundPaymentRail]] = None,
    ):
        self.conn = conn
        shared = dict(rails or {})
        self.inbound_rails = {**shared, **dict(inbound_rails or {})}
        self.outbound_rails = {**shared, **dict(outbound_rails or {})}
        finance_db.ensure_schema(conn)
        payment_controls.ensure_schema(conn)
        compliance_db.ensure_schema(conn)
        ensure_schema(conn)

    def create_receivable(
        self,
        *,
        organization_id: str,
        account_id: str,
        provider: str,
        amount_minor: int,
        currency: str,
        customer: Mapping[str, Any],
        customer_jurisdiction: str,
        purpose: str,
        idempotency_key: str,
        objective_id: Optional[str] = None,
        tax_minor: int = 0,
        tax_rule_id: Optional[str] = None,
    ) -> dict[str, Any]:
        if amount_minor <= 0:
            raise ValueError("receivable amount must be positive")
        existing = self._by_key(idempotency_key)
        if existing:
            self._assert_retry_matches(
                existing,
                organization_id=organization_id,
                account_id=account_id,
                objective_id=objective_id,
                direction="incoming",
                provider=provider,
                party=customer,
                amount_minor=amount_minor,
                currency=currency,
                purpose=purpose,
                tax_minor=tax_minor,
                tax_rule_id=tax_rule_id,
            )
            return existing
        assessment_id = compliance_db.authorize_payment_provider(
            self.conn,
            organization_id=organization_id,
            provider=provider,
            direction="inbound",
            jurisdiction=customer_jurisdiction,
        )
        if tax_minor < 0 or tax_minor >= amount_minor:
            raise ValueError("tax must be non-negative and less than the gross receivable")
        if tax_minor and not tax_rule_id:
            raise accounting_db.AccountingError(
                "tax-bearing receivable requires a verified tax rule"
            )
        if tax_minor:
            rule = self.conn.execute(
                """SELECT tr.id, tr.tax_code, rg.jurisdiction, rg.tax_type
                   FROM tax_rates tr
                   JOIN tax_registrations rg ON rg.id = tr.registration_id
                   WHERE tr.id = ? AND rg.organization_id = ?
                     AND rg.status = 'active'
                     AND rg.effective_from <= ? AND tr.effective_from <= ?
                     AND (rg.effective_to IS NULL OR rg.effective_to >= ?)
                     AND (tr.effective_to IS NULL OR tr.effective_to >= ?)""",
                (
                    tax_rule_id, organization_id, int(time.time()), int(time.time()),
                    int(time.time()), int(time.time()),
                ),
            ).fetchone()
            if rule is None:
                raise accounting_db.AccountingError(
                    "tax-bearing receivable requires a verified tax rule"
                )
            calculated, effective_rule_id = accounting_db.calculate_tax(
                self.conn,
                organization_id=organization_id,
                jurisdiction=rule["jurisdiction"],
                tax_type=rule["tax_type"],
                tax_code=rule["tax_code"],
                taxable_minor=amount_minor - tax_minor,
                occurred_at=int(time.time()),
            )
            if effective_rule_id != tax_rule_id or calculated != tax_minor:
                raise accounting_db.AccountingError(
                    "invoice tax does not match the effective verified tax rule"
                )
        rail = self._inbound_rail(provider)
        remote = rail.create_receivable(
            amount_minor=amount_minor,
            currency=currency,
            customer=customer,
            purpose=purpose,
            idempotency_key=idempotency_key,
        )
        self._validate_remote(remote, amount_minor, currency)
        intent_id = f"payment_{uuid.uuid4().hex}"
        now = int(time.time())
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO payment_intents (
                    id, organization_id, account_id, objective_id, direction,
                    provider, party_json, amount_minor, currency, purpose,
                    status, provider_reference, payment_url, idempotency_key,
                    metadata_json, tax_minor, tax_rule_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'incoming', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent_id,
                    organization_id,
                    account_id,
                    objective_id,
                    provider,
                    json.dumps(dict(customer), sort_keys=True),
                    amount_minor,
                    currency.upper(),
                    purpose,
                    remote.status,
                    remote.reference,
                    remote.payment_url,
                    idempotency_key,
                    json.dumps(
                        {
                            "provider_assessment_id": assessment_id,
                            "customer_jurisdiction": customer_jurisdiction,
                        },
                        sort_keys=True,
                    ),
                    tax_minor,
                    tax_rule_id,
                    now,
                    now,
                ),
            )
        accounting_db.post_journal(
            self.conn,
            organization_id=organization_id,
            description=f"Receivable: {purpose}",
            source_type="payment_intent",
            source_id=intent_id,
            currency=currency,
            lines=(
                {"account_code": "1100", "debit_minor": amount_minor, "party": customer},
                {"account_code": "4000", "credit_minor": amount_minor - tax_minor},
                *(
                    ({"account_code": "2100", "credit_minor": tax_minor,
                      "tax_code": tax_rule_id},)
                    if tax_minor else ()
                ),
            ),
            evidence={"provider_reference": remote.reference, "tax_rule_id": tax_rule_id},
        )
        return self._get(intent_id)

    def send_payable(
        self,
        *,
        organization_id: str,
        account_id: str,
        objective_id: str,
        action_id: str,
        provider: str,
        amount_minor: int,
        currency: str,
        payee: Mapping[str, Any],
        payee_jurisdiction: str,
        instrument_id: str,
        merchant_category: str,
        payee_id: str,
        purpose: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        existing = self._by_key(idempotency_key)
        if existing:
            self._assert_retry_matches(
                existing,
                organization_id=organization_id,
                account_id=account_id,
                objective_id=objective_id,
                action_id=action_id,
                direction="outgoing",
                provider=provider,
                party=payee,
                amount_minor=amount_minor,
                currency=currency,
                purpose=purpose,
            )
            return existing
        reservation = self.conn.execute(
            """
            SELECT * FROM budget_reservations
             WHERE action_id = ? AND status = 'reserved'
            """,
            (action_id,),
        ).fetchone()
        if reservation is None or int(reservation["amount_minor"]) != amount_minor:
            raise finance_db.BudgetError(
                "outgoing payment requires an exact open budget reservation"
            )
        spend_authority = payment_controls.authorize_spend(
            self.conn,
            instrument_id=instrument_id,
            provider=provider,
            amount_minor=amount_minor,
            currency=currency,
            merchant_category=merchant_category,
            payee_id=payee_id,
            action_id=action_id,
        )
        assessment_id = compliance_db.authorize_payment_provider(
            self.conn,
            organization_id=organization_id,
            provider=provider,
            direction="outbound",
            jurisdiction=payee_jurisdiction,
        )
        intent_id = f"payment_{uuid.uuid4().hex}"
        now = int(time.time())
        metadata = {
            "instrument_id": instrument_id,
            "merchant_category": merchant_category,
            "payee_id": payee_id,
            "spend_control_id": spend_authority["control_id"],
            "policy_version": spend_authority["policy_version"],
            "provider_assessment_id": assessment_id,
            "payee_jurisdiction": payee_jurisdiction,
        }
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO payment_intents (
                    id, organization_id, account_id, objective_id, action_id,
                    direction, provider, party_json, amount_minor, currency,
                    purpose, status, provider_reference, payment_url,
                    idempotency_key, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'outgoing', ?, ?, ?, ?, ?, 'processing',
                          NULL, NULL, ?, ?, ?, ?)
                """,
                (
                    intent_id,
                    organization_id,
                    account_id,
                    objective_id,
                    action_id,
                    provider,
                    json.dumps(dict(payee), sort_keys=True),
                    amount_minor,
                    currency.upper(),
                    purpose,
                    idempotency_key,
                    json.dumps(metadata, sort_keys=True),
                    now,
                    now,
                ),
            )
        try:
            remote = self._outbound_rail(provider).send_payment(
                amount_minor=amount_minor,
                currency=currency,
                payee=payee,
                instrument_reference=spend_authority["provider_instrument_id"],
                purpose=purpose,
                idempotency_key=idempotency_key,
            )
            self._validate_remote(remote, amount_minor, currency)
        except UncertainProviderAction as exc:
            metadata["uncertainty_evidence"] = exc.evidence
            with self.conn:
                self.conn.execute(
                    """UPDATE payment_intents
                       SET status='uncertain', provider_reference=?, metadata_json=?,
                           updated_at=? WHERE id=?""",
                    (
                        exc.provider_reference or None,
                        json.dumps(metadata, sort_keys=True),
                        int(time.time()),
                        intent_id,
                    ),
                )
            return self._get(intent_id)
        except Exception as exc:
            metadata["uncertainty_error"] = f"{type(exc).__name__}: {exc}"
            with self.conn:
                self.conn.execute(
                    """UPDATE payment_intents
                       SET status='uncertain', metadata_json=?, updated_at=?
                       WHERE id=?""",
                    (json.dumps(metadata, sort_keys=True), int(time.time()), intent_id),
                )
            raise
        with self.conn:
            self.conn.execute(
                """UPDATE payment_intents
                   SET status=?, provider_reference=?, payment_url=?, updated_at=?
                   WHERE id=?""",
                (remote.status, remote.reference, remote.payment_url, int(time.time()), intent_id),
            )
        return self.reconcile(intent_id)

    def reconcile(self, intent_id: str) -> dict[str, Any]:
        intent = self._get(intent_id)
        rail = (
            self._inbound_rail(intent["provider"])
            if intent["direction"] == "incoming"
            else self._outbound_rail(intent["provider"])
        )
        remote = rail.get_payment(intent["provider_reference"])
        self._validate_remote(remote, intent["amount_minor"], intent["currency"])
        evidence_json = json.dumps(remote.evidence, sort_keys=True)
        existing_readback = self.conn.execute(
            """SELECT * FROM payment_provider_readbacks
                WHERE payment_intent_id=? AND provider=?
                  AND provider_reference=? AND status=? AND amount_minor=?
                  AND currency=? AND evidence_json=?
                ORDER BY observed_at DESC,id DESC LIMIT 1""",
            (
                intent_id,
                intent["provider"],
                remote.reference,
                remote.status,
                remote.amount_minor,
                remote.currency.upper(),
                evidence_json,
            ),
        ).fetchone()
        readback_id = (
            str(existing_readback["id"])
            if existing_readback is not None
            else f"readback_{uuid.uuid4().hex}"
        )
        observed_at = int(time.time())
        if existing_readback is None:
            with self.conn:
                self.conn.execute(
                    """INSERT INTO payment_provider_readbacks
                       (id,payment_intent_id,provider,provider_reference,status,
                        amount_minor,currency,evidence_json,observed_at)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        readback_id, intent_id, intent["provider"], remote.reference,
                        remote.status, remote.amount_minor, remote.currency.upper(),
                        evidence_json, observed_at,
                    ),
                )
        with self.conn:
            self.conn.execute(
                "UPDATE payment_intents SET status = ?, updated_at = ? WHERE id = ?",
                (remote.status, observed_at, intent_id),
            )
        if remote.status == "succeeded":
            if intent["direction"] == "incoming":
                finance_db.record_entry(
                    self.conn,
                    account_id=intent["account_id"],
                    kind="customer_payment",
                    amount_minor=intent["amount_minor"],
                    currency=intent["currency"],
                    objective_id=intent["objective_id"],
                    external_reference=remote.reference,
                    idempotency_key=f"receipt:{intent_id}",
                    evidence={
                        "provider": remote.evidence,
                        "accounting": {"counter_account_code": "1100"},
                    },
                )
            else:
                reservation = self.conn.execute(
                    "SELECT status FROM budget_reservations WHERE action_id = ?",
                    (intent["action_id"],),
                ).fetchone()
                if reservation is not None and reservation["status"] == "reserved":
                    finance_db.settle_reservation(
                        self.conn,
                        action_id=intent["action_id"],
                        actual_amount_minor=intent["amount_minor"],
                        external_reference=remote.reference,
                        evidence=remote.evidence,
                    )
            if intent["direction"] == "outgoing":
                payment_controls.settle_spend_hold(
                    self.conn, str(intent["action_id"])
                )
        elif intent["direction"] == "outgoing" and remote.status in {
            "failed",
            "cancelled",
        }:
            payment_controls.release_spend_hold(
                self.conn,
                str(intent["action_id"]),
                reason=f"provider_status:{remote.status}",
            )
        result = self._get(intent_id)
        result["provider_readback_id"] = readback_id
        return result

    def _inbound_rail(self, name: str) -> InboundPaymentRail:
        rail = self.inbound_rails.get(name)
        if rail is None:
            raise KeyError(f"inbound payment rail is not configured: {name}")
        return rail

    def _outbound_rail(self, name: str) -> OutboundPaymentRail:
        rail = self.outbound_rails.get(name)
        if rail is None:
            raise KeyError(f"outbound payment rail is not configured: {name}")
        return rail

    def _get(self, intent_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM payment_intents WHERE id = ?", (intent_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"payment intent not found: {intent_id}")
        return dict(row)

    def _by_key(self, key: str) -> Optional[dict[str, Any]]:
        row = self.conn.execute(
            "SELECT id FROM payment_intents WHERE idempotency_key = ?", (key,)
        ).fetchone()
        return self._get(row["id"]) if row else None

    @staticmethod
    def _assert_retry_matches(
        existing: Mapping[str, Any], **expected: Any
    ) -> None:
        """Bind idempotency retries to the original financial intent."""
        for field, value in expected.items():
            actual = existing.get(field)
            if field == "party":
                actual = json.loads(str(existing.get("party_json") or "{}"))
            elif field == "currency":
                actual = str(actual or "").upper()
                value = str(value or "").upper()
            elif field in {"amount_minor", "tax_minor"}:
                actual = int(actual or 0)
                value = int(value or 0)
            if actual != value:
                raise ValueError(
                    "idempotency key was reused with different payment parameters"
                )

    @staticmethod
    def _validate_remote(
        payment: ProviderPayment, expected_amount: int, expected_currency: str
    ) -> None:
        if not str(payment.reference or "").strip():
            raise ValueError("payment provider omitted a reference")
        if not str(payment.status or "").strip():
            raise ValueError("payment provider omitted a status")
        if (
            isinstance(payment.amount_minor, bool)
            or not isinstance(payment.amount_minor, int)
            or payment.amount_minor <= 0
        ):
            raise ValueError("payment provider returned an invalid amount")
        provider_currency = str(payment.currency or "").strip().upper()
        if len(provider_currency) != 3 or not provider_currency.isalpha():
            raise ValueError("payment provider returned an invalid currency")
        if payment.amount_minor != expected_amount:
            raise ValueError("payment provider amount does not match intent")
        if provider_currency != expected_currency.upper():
            raise ValueError("payment provider currency does not match intent")
