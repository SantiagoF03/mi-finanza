"""Semi-automatic mode V1 — the single authority on whether a new
Recommendation may be persisted, plus the persistent analysis lease.

Design rules:

- Terminal/blocking states are declared ONCE here. No other module may keep
  its own implicit list.
- The gate is consulted immediately before persisting a Recommendation and
  from inside the lease, so two processes cannot both decide "allowed".
- Everything fails CLOSED: an unexpected error blocks creation.
- This module never approves, never executes, never touches a broker and
  never creates an OrderExecution.
"""

from __future__ import annotations

import logging
import secrets
from datetime import datetime, timedelta, timezone

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.models import AnalysisLease, OrderExecution, Recommendation

logger = logging.getLogger(__name__)

LEASE_NAME = "analysis_cycle"

# --- Stable skip codes (documented constants, never free text) -------------
SKIP_OPEN_RECOMMENDATION = "open_recommendation_requires_decision"
SKIP_EXECUTION_PENDING = "execution_requires_resolution"
SKIP_LEASE_UNAVAILABLE = "analysis_lease_unavailable"
SKIP_LEASE_ERROR = "analysis_lease_error"
SKIP_CREATION_CONFLICT = "recommendation_creation_conflict"
SKIP_COOLDOWN = "analysis_cooldown"

SKIP_CODES = frozenset({
    SKIP_OPEN_RECOMMENDATION,
    SKIP_EXECUTION_PENDING,
    SKIP_LEASE_UNAVAILABLE,
    SKIP_LEASE_ERROR,
    SKIP_CREATION_CONFLICT,
    SKIP_COOLDOWN,
})

# --- Recommendation states -------------------------------------------------
# TERMINAL = the human decision cycle for that recommendation is over and it
# can never come back. Anything NOT terminal blocks a new recommendation.
TERMINAL_RECOMMENDATION_STATUSES = frozenset({
    "rejected",
    "approved",
    "executed",
    "execution_failed",
    "superseded",
    "expired",
    "cancelled",
})

# Declared explicitly for readability/tests; the gate uses "not terminal".
NON_TERMINAL_RECOMMENDATION_STATUSES = frozenset({
    "pending",
    "blocked",
    "execution_pending",
    "submitting",
    "manual_reconciliation_required",
    "execution_partial",
})

# --- OrderExecution states -------------------------------------------------
# TERMINAL = provably no further submission and no pending human decision.
TERMINAL_EXECUTION_STATUSES = frozenset({
    "executed",
    "failed",
    "rejected_by_broker",
    "validation_failed",
    "not_sent_confirmed",
    "preflight_cancelled",
    "cancelled",
})

# Every non-terminal execution state blocks a new decision. execution_sent is
# included on purpose: until it is terminally confirmed (or reconciled) we
# cannot prove the outcome, so no new decision may be taken.
BLOCKING_EXECUTION_STATUSES = frozenset({
    "execution_requested",
    "execution_ready",
    "submitting",
    "execution_sent",
    "manual_reconciliation_required",
})


def _utcnow_naive() -> datetime:
    """Naive UTC — DB timestamps are naive UTC (datetime.utcnow defaults)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def is_recommendation_open(rec: Recommendation) -> bool:
    return (rec.status or "") not in TERMINAL_RECOMMENDATION_STATUSES


def is_execution_blocking(order_exec: OrderExecution) -> bool:
    return (order_exec.status or "") not in TERMINAL_EXECUTION_STATUSES


class GateResult:
    """Outcome of the creation gate. `allowed` is the only green light."""

    def __init__(
        self,
        allowed: bool,
        code: str | None = None,
        blocking_recommendation_id: int | None = None,
        blocking_execution_id: int | None = None,
        detail: str = "",
    ):
        self.allowed = allowed
        self.code = code
        self.blocking_recommendation_id = blocking_recommendation_id
        self.blocking_execution_id = blocking_execution_id
        self.detail = detail

    def as_skip(self, source: str, **extra) -> dict:
        payload = {
            "skipped": True,
            "status": "skipped",
            "code": self.code,
            "source": source,
            "blocking_recommendation_id": self.blocking_recommendation_id,
            "blocking_execution_id": self.blocking_execution_id,
            "message": self.detail,
        }
        payload.update(extra)
        return payload


def check_recommendation_creation_allowed(db: Session) -> GateResult:
    """THE gate. Blocks while any human decision or execution is outstanding.

    Fails closed: any unexpected error blocks creation.
    """
    try:
        # 1. Any non-terminal recommendation (includes an unresolved pilot,
        #    which is simply a non-terminal recommendation flagged as pilot).
        open_recs = (
            db.query(Recommendation)
            .filter(Recommendation.status.notin_(sorted(TERMINAL_RECOMMENDATION_STATUSES)))
            .order_by(Recommendation.id)
            .all()
        )
        for rec in open_recs:
            meta = rec.metadata_json or {}
            is_pilot = isinstance(meta, dict) and meta.get("execution_pilot") is True
            return GateResult(
                allowed=False,
                code=SKIP_OPEN_RECOMMENDATION,
                blocking_recommendation_id=rec.id,
                detail=(
                    f"La recomendación {rec.id} ({rec.status}"
                    f"{', piloto' if is_pilot else ''}) espera una decisión humana."
                ),
            )

        # 2. Any execution that is not provably finished.
        blocking_exec = (
            db.query(OrderExecution)
            .filter(OrderExecution.status.notin_(sorted(TERMINAL_EXECUTION_STATUSES)))
            .order_by(OrderExecution.id)
            .first()
        )
        if blocking_exec is not None:
            return GateResult(
                allowed=False,
                code=SKIP_EXECUTION_PENDING,
                blocking_recommendation_id=blocking_exec.recommendation_id,
                blocking_execution_id=blocking_exec.id,
                detail=(
                    f"La ejecución {blocking_exec.id} está en '{blocking_exec.status}' "
                    "y requiere resolución manual."
                ),
            )

        return GateResult(allowed=True)

    except Exception as exc:  # fail closed
        logger.error("Recommendation creation gate failed: %s", exc)
        return GateResult(
            allowed=False,
            code=SKIP_CREATION_CONFLICT,
            detail=f"No se pudo verificar el estado de bloqueo: {str(exc)[:200]}",
        )


# ---------------------------------------------------------------------------
# Persistent analysis lease
# ---------------------------------------------------------------------------


def new_owner_id() -> str:
    """Anonymous, non-sensitive owner token. Never a hostname or a secret."""
    return secrets.token_hex(8)


def _ensure_lease_row(db: Session) -> None:
    """Create the single lease row if missing (idempotent, race-safe).

    `name` is UNIQUE, so a concurrent insert loses with an IntegrityError and
    simply proceeds — the row exists either way.
    """
    from sqlalchemy.exc import IntegrityError

    exists = db.query(func.count(AnalysisLease.id)).filter(
        AnalysisLease.name == LEASE_NAME
    ).scalar()
    if exists:
        return
    try:
        db.add(AnalysisLease(name=LEASE_NAME, owner_id=None,
                             acquired_at=None, expires_at=None))
        db.commit()
    except IntegrityError:
        db.rollback()


def acquire_analysis_lease(db: Session, ttl_seconds: int | None = None) -> tuple[str | None, str | None]:
    """Atomically acquire the analysis lease.

    Returns (owner_id, error_code). owner_id is None when not acquired.

    ATOMICITY: acquisition is a SINGLE conditional UPDATE
        UPDATE analysis_leases
           SET owner_id=..., acquired_at=..., expires_at=...
         WHERE name=... AND (expires_at IS NULL OR expires_at <= now)
    Whichever transaction commits first flips expires_at into the future, so
    the loser's WHERE no longer matches and its rowcount is 0. There is no
    read-then-write window (the insecure "SELECT then INSERT" pattern), which
    is what makes this safe on SQLite, where the write lock serializes the
    UPDATEs.
    """
    settings = get_settings()
    ttl = ttl_seconds if ttl_seconds is not None else settings.analysis_lease_ttl_seconds
    if ttl <= 0:
        return None, SKIP_LEASE_ERROR

    try:
        _ensure_lease_row(db)
        now = _utcnow_naive()
        owner = new_owner_id()
        updated = (
            db.query(AnalysisLease)
            .filter(
                AnalysisLease.name == LEASE_NAME,
                (AnalysisLease.expires_at.is_(None)) | (AnalysisLease.expires_at <= now),
            )
            .update(
                {
                    "owner_id": owner,
                    "acquired_at": now,
                    "expires_at": now + timedelta(seconds=ttl),
                },
                synchronize_session=False,
            )
        )
        db.commit()
        if updated == 1:
            return owner, None
        return None, SKIP_LEASE_UNAVAILABLE
    except Exception as exc:  # fail closed
        logger.error("Analysis lease acquisition failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return None, SKIP_LEASE_ERROR


def renew_analysis_lease(db: Session, owner_id: str, ttl_seconds: int | None = None) -> bool:
    """Atomically extend the lease — ONLY for the current, still-valid owner.

    The conditional UPDATE requires name + owner_id + a lease that has not
    expired yet. rowcount != 1 means we lost it (expired, or taken over by
    another process), and the caller MUST NOT persist a Recommendation.

    A long analysis must renew rather than assume it finished within the TTL.
    """
    if not owner_id:
        return False
    settings = get_settings()
    ttl = ttl_seconds if ttl_seconds is not None else settings.analysis_lease_ttl_seconds
    if ttl <= 0:
        return False
    try:
        now = _utcnow_naive()
        renewed = (
            db.query(AnalysisLease)
            .filter(
                AnalysisLease.name == LEASE_NAME,
                AnalysisLease.owner_id == owner_id,
                AnalysisLease.expires_at.isnot(None),
                AnalysisLease.expires_at > now,   # still ours, not expired
            )
            .update({"expires_at": now + timedelta(seconds=ttl)}, synchronize_session=False)
        )
        db.commit()
        return renewed == 1
    except Exception as exc:  # fail closed
        logger.error("Analysis lease renewal failed: %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return False


def holds_analysis_lease(db: Session, owner_id: str) -> bool:
    """True only if `owner_id` currently owns a non-expired lease."""
    if not owner_id:
        return False
    try:
        now = _utcnow_naive()
        return (
            db.query(func.count(AnalysisLease.id))
            .filter(
                AnalysisLease.name == LEASE_NAME,
                AnalysisLease.owner_id == owner_id,
                AnalysisLease.expires_at.isnot(None),
                AnalysisLease.expires_at > now,
            )
            .scalar()
            or 0
        ) == 1
    except Exception as exc:  # fail closed
        logger.error("Analysis lease ownership check failed: %s", exc)
        return False


def verify_can_persist_recommendation(db: Session, owner_id: str) -> GateResult:
    """FINAL check, immediately before the INSERT.

    Closes the TOCTOU window between "gate said yes" and the actual write:
    1. renew the lease atomically (proves we still own it AND buys time);
    2. re-run the creation gate (a blocking recommendation/execution may have
       appeared during a long analysis).
    Anything failing here blocks the write — fail closed.
    """
    if not renew_analysis_lease(db, owner_id):
        return GateResult(
            allowed=False,
            code=SKIP_LEASE_UNAVAILABLE,
            detail=(
                "El lease de análisis se perdió o venció durante el ciclo; "
                "no se persiste la recomendación."
            ),
        )
    return check_recommendation_creation_allowed(db)


def release_analysis_lease(db: Session, owner_id: str) -> bool:
    """Best-effort release. Only the current owner can release."""
    if not owner_id:
        return False
    try:
        released = (
            db.query(AnalysisLease)
            .filter(AnalysisLease.name == LEASE_NAME, AnalysisLease.owner_id == owner_id)
            .update({"expires_at": _utcnow_naive()}, synchronize_session=False)
        )
        db.commit()
        return released == 1
    except Exception as exc:
        logger.warning("Analysis lease release failed (best-effort): %s", exc)
        try:
            db.rollback()
        except Exception:
            pass
        return False


def get_lease_state(db: Session) -> dict:
    """Non-sensitive lease state for observability. Owner is truncated."""
    try:
        lease = db.query(AnalysisLease).filter(AnalysisLease.name == LEASE_NAME).first()
    except Exception:
        return {"lease_held": None, "lease_owner": None, "lease_expires_at": None}

    if lease is None or lease.expires_at is None:
        return {"lease_held": False, "lease_owner": None, "lease_expires_at": None}

    held = lease.expires_at > _utcnow_naive()
    owner = lease.owner_id or ""
    return {
        "lease_held": held,
        # Anonymized: a short prefix is enough to correlate logs.
        "lease_owner": (owner[:8] + "…") if held and owner else None,
        "lease_expires_at": lease.expires_at.isoformat() if lease.expires_at else None,
    }
