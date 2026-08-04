"""Daily notional ledger and live-cash guard for the buy path.

Two guarantees the previous sell-only pilot never needed:

1. `max_daily_notional` per execution class — a per-order and per-batch limit
   says nothing about the twentieth batch of the day.
2. A live balance check immediately before submitting a buy. `snapshot.cash`
   is a stale, currency-blind number; using it to authorise a purchase is how
   an account ends up overdrawn or implicitly financed.

Daily budget is consumed ONLY at the point of no return (when an order is
committed as `submitting`), so a blocked or cancelled preflight never eats
into it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.broker.numeric import non_negative_decimal, positive_decimal, to_finite_decimal
from app.models.models import ExecutionDailyNotional

# Stable blocking codes.
DAILY_LIMIT_EXCEEDED = "daily_limit_exceeded"
LIVE_CASH_UNAVAILABLE = "live_cash_unavailable"
INSUFFICIENT_LIVE_CASH = "insufficient_live_cash"
CURRENCY_CASH_MISMATCH = "currency_cash_mismatch"
FEE_BUFFER_EXCEEDED = "fee_buffer_exceeded"


def trade_date_for(settings, *, now: datetime | None = None) -> str:
    """The local operating day, as YYYY-MM-DD.

    Uses the configured market timezone so a batch sent at 21:00 UTC counts
    against the Argentine day it actually belongs to, not the UTC one.
    """
    from app.market.calendar import resolve_market_schedule

    schedule = resolve_market_schedule(settings)
    tz = schedule.get("tzinfo")
    current = now or datetime.now(tz) if tz else (now or datetime.utcnow())
    if tz is not None and current.tzinfo is not None:
        current = current.astimezone(tz)
    return current.date().isoformat()


def get_daily_totals(
    db: Session, trade_date: str, execution_class: str, currency: str = ""
) -> Decimal:
    """Notional already submitted today for one class AND currency.

    ARS and USD are separate budgets: summing them would produce a number
    that means nothing and would let a dollar order eat a peso allowance.
    """
    row = (
        db.query(ExecutionDailyNotional)
        .filter(
            ExecutionDailyNotional.trade_date == trade_date,
            ExecutionDailyNotional.execution_class == execution_class,
            ExecutionDailyNotional.currency == (currency or ""),
        )
        .first()
    )
    if row is None:
        return Decimal("0")
    value = non_negative_decimal(row.submitted_notional)
    return value if value is not None else Decimal("0")


def check_daily_budget(
    db: Session,
    *,
    trade_date: str,
    execution_class: str,
    additional_notional: Decimal,
    max_daily_notional,
    currency: str = "",
) -> tuple[str | None, dict]:
    """ADVISORY pre-check: would `additional_notional` blow today's budget?

    This is a read, so it is inherently racy and is NOT what protects the
    limit — `reserve_daily_budget` does, atomically, at the point of no
    return. This exists so a preview can show the reason early instead of
    failing mysteriously later.

    A missing/unusable limit blocks: an unconfigured daily cap is not an
    absent one.
    """
    audit = {
        "trade_date": trade_date,
        "execution_class": execution_class,
        "currency": currency or "",
        "additional_notional": float(additional_notional),
        "check_kind": "advisory",
    }
    limit = positive_decimal(max_daily_notional)
    if limit is None:
        return DAILY_LIMIT_EXCEEDED, {**audit, "reason": "daily_limit_not_configured"}

    already = get_daily_totals(db, trade_date, execution_class, currency)
    projected = already + additional_notional
    audit.update({
        "already_submitted": float(already),
        "projected": float(projected),
        "max_daily_notional": float(limit),
    })
    if projected > limit:
        return DAILY_LIMIT_EXCEEDED, audit
    return None, {**audit, "passed": True}


def reserve_daily_budget(
    db: Session,
    *,
    trade_date: str,
    execution_class: str,
    currency: str,
    notional: Decimal,
    max_daily_notional,
) -> tuple[str | None, dict]:
    """ATOMICALLY check-and-reserve daily budget. THE authority on the limit.

    Deliberately NOT "SELECT, add in Python, UPDATE": two processes doing
    that both read the same total and both believe they fit, so the limit is
    silently exceeded. Here the arithmetic AND the comparison happen inside
    the database:

        UPDATE ... SET submitted = submitted + :amt
        WHERE key = ... AND submitted + :amt <= :limit

    A row that would exceed the limit simply does not match, so rowcount is 0
    and the caller is refused. The unique constraint on
    (trade_date, execution_class, currency) is what forces concurrent
    first-writers through this same UPDATE instead of each inserting their
    own row with the full allowance.

    Returns (error_code, audit). error_code None means the budget is
    reserved and the caller may proceed to submit.
    """
    key = {
        "trade_date": trade_date,
        "execution_class": execution_class,
        "currency": currency or "",
    }
    audit = {**key, "additional_notional": float(notional), "check_kind": "atomic_reserve"}

    limit = positive_decimal(max_daily_notional)
    if limit is None:
        return DAILY_LIMIT_EXCEEDED, {**audit, "reason": "daily_limit_not_configured"}
    audit["max_daily_notional"] = float(limit)

    amount = non_negative_decimal(notional)
    if amount is None:
        return DAILY_LIMIT_EXCEEDED, {**audit, "reason": "invalid_notional"}
    # Not a race: a single order bigger than the whole day cannot fit in any
    # interleaving, and the INSERT branch below has no limit clause of its own.
    if amount > limit:
        return DAILY_LIMIT_EXCEEDED, {**audit, "reason": "exceeds_daily_limit_alone"}

    amount_f = float(amount)
    limit_f = float(limit)
    now = datetime.utcnow()

    def _conditional_update() -> int:
        result = db.execute(
            text(
                "UPDATE execution_daily_notional "
                "SET submitted_notional = submitted_notional + :amt, "
                "    order_count = order_count + 1, "
                "    updated_at = :now "
                "WHERE trade_date = :trade_date "
                "  AND execution_class = :execution_class "
                "  AND currency = :currency "
                "  AND submitted_notional + :amt <= :limit"
            ),
            {**key, "amt": amount_f, "limit": limit_f, "now": now},
        )
        return result.rowcount or 0

    if _conditional_update() == 1:
        return None, {**audit, "passed": True, "path": "updated"}

    # Either the row does not exist yet, or it exists and the limit is blown.
    # Try to create it; the unique constraint decides who wins the race.
    try:
        with db.begin_nested():
            db.execute(
                text(
                    "INSERT INTO execution_daily_notional "
                    "(trade_date, execution_class, currency, submitted_notional, "
                    " order_count, updated_at) "
                    "VALUES (:trade_date, :execution_class, :currency, :amt, 1, :now)"
                ),
                {**key, "amt": amount_f, "now": now},
            )
        return None, {**audit, "passed": True, "path": "inserted"}
    except IntegrityError:
        # Someone else created the row between our UPDATE and our INSERT.
        # Retry the conditional UPDATE exactly once against their row.
        if _conditional_update() == 1:
            return None, {**audit, "passed": True, "path": "updated_after_conflict"}

    already = get_daily_totals(db, trade_date, execution_class, currency)
    return DAILY_LIMIT_EXCEEDED, {
        **audit,
        "already_submitted": float(already),
        "projected": float(already + amount),
        "passed": False,
    }


def release_daily_budget(
    db: Session,
    *,
    trade_date: str,
    execution_class: str,
    currency: str,
    notional: Decimal,
    reason: str,
) -> dict:
    """Give budget back. Deliberately NOT called automatically anywhere.

    A cancelled or rejected order does NOT free its daily allowance on its
    own: at the moment we learn about it we usually cannot prove the order
    never consumed anything at the broker, and quietly restoring budget would
    let a bad day's worth of orders be re-attempted. Releasing is an explicit
    administrative act with a stated reason, recorded in the audit.
    """
    amount = non_negative_decimal(notional)
    if amount is None:
        return {"released": False, "reason": "invalid_notional"}
    result = db.execute(
        text(
            "UPDATE execution_daily_notional "
            "SET submitted_notional = MAX(submitted_notional - :amt, 0), "
            "    updated_at = :now "
            "WHERE trade_date = :trade_date "
            "  AND execution_class = :execution_class "
            "  AND currency = :currency"
        ),
        {
            "trade_date": trade_date,
            "execution_class": execution_class,
            "currency": currency or "",
            "amt": float(amount),
            "now": datetime.utcnow(),
        },
    )
    return {
        "released": (result.rowcount or 0) == 1,
        "amount": float(amount),
        "reason": reason,
        "trade_date": trade_date,
        "execution_class": execution_class,
        "currency": currency or "",
    }


def evaluate_buy_cash(
    *,
    live_cash: dict | None,
    required_notional: Decimal,
    currency: str,
    fee_buffer_pct,
    min_cash_reserve,
    pending_notional: Decimal | None = None,
) -> tuple[str | None, dict]:
    """Can this buy be paid for, right now, in the right currency?

    Conservative by construction:
      needed = notional × (1 + fee_buffer_pct) + pending buys + min_cash_reserve
    and `needed` must be covered by the LIVE available balance in the SAME
    currency. Commissions, VAT and market fees are not itemised — the buffer
    is a deliberate over-estimate, because under-estimating them is what
    produces a rejected or partially financed order.
    """
    audit: dict = {
        "currency": currency,
        "required_notional": float(required_notional),
    }

    if not live_cash or not live_cash.get("available"):
        return LIVE_CASH_UNAVAILABLE, audit

    cash_currency = str(live_cash.get("currency") or "").strip().upper()
    if not cash_currency or cash_currency != str(currency or "").strip().upper():
        return CURRENCY_CASH_MISMATCH, {**audit, "cash_currency": cash_currency}

    available = non_negative_decimal(live_cash.get("cash"))
    if available is None:
        return LIVE_CASH_UNAVAILABLE, {**audit, "reason": "unusable_balance"}

    buffer_pct = to_finite_decimal(fee_buffer_pct)
    if buffer_pct is None or buffer_pct < 0:
        return FEE_BUFFER_EXCEEDED, {**audit, "reason": "fee_buffer_not_configured"}
    reserve = to_finite_decimal(min_cash_reserve)
    if reserve is None or reserve < 0:
        return INSUFFICIENT_LIVE_CASH, {**audit, "reason": "cash_reserve_not_configured"}

    with_fees = (required_notional * (Decimal("1") + buffer_pct)).quantize(Decimal("0.01"))
    pending = pending_notional or Decimal("0")
    needed = with_fees + pending + reserve

    audit.update({
        "live_cash_available": float(available),
        "fee_buffer_pct": float(buffer_pct),
        "notional_with_fees": float(with_fees),
        "pending_buy_notional": float(pending),
        "min_cash_reserve": float(reserve),
        "total_needed": float(needed),
        "committed": live_cash.get("committed"),
        "retrieved_at": live_cash.get("retrieved_at"),
    })

    if needed > available:
        return INSUFFICIENT_LIVE_CASH, audit
    return None, {**audit, "passed": True}


# Buy statuses that still hold money. An order in one of these either
# reached the broker or may have; until it is reconciled its cash is not
# freely available. Terminal statuses (executed, rejected_by_broker, failed,
# cancelled_by_user, not_sent_confirmed, preflight_cancelled) do NOT reserve:
# either the money already moved and the live balance reflects it, or nothing
# was ever sent.
PENDING_BUY_STATUSES = (
    "submitting",
    "execution_sent",
    "manual_reconciliation_required",
    "submission_unknown",
)


def pending_buy_notional(
    db: Session,
    *,
    currency: str,
    trade_date: str,
    settings=None,
    exclude_ids: set[int] | None = None,
) -> Decimal:
    """Unresolved buy notional for ONE currency on ONE operating day.

    Two bugs this replaces:

    1. It used to sum every currency together, so a pending USD purchase
       reduced the peso allowance (and vice versa) — an arbitrary, invisible
       haircut.
    2. It used to have no date bound at all, so a submission left unreconciled
       months ago kept reserving cash forever, silently shrinking every future
       order until someone noticed.

    An order still counts only if it belongs to `trade_date`. An older
    unresolved order is a reconciliation problem, not a permanent cash
    reservation — it is reported separately by
    `stale_pending_buys` so it is visible instead of silently draining
    capacity.
    """
    from app.models.models import OrderExecution

    target_currency = (currency or "").strip().upper()
    rows = (
        db.query(OrderExecution)
        .filter(
            OrderExecution.side == "buy",
            OrderExecution.status.in_(PENDING_BUY_STATUSES),
        )
        .all()
    )

    total = Decimal("0")
    for row in rows:
        if exclude_ids and row.id in exclude_ids:
            continue
        audit = (row.broker_response or {}).get("request_audit") or {}
        if _order_currency(row, audit) != target_currency:
            continue
        if _order_trade_date(row, audit, settings) != trade_date:
            continue
        value = positive_decimal(audit.get("actual_notional")) or positive_decimal(
            audit.get("estimated_notional")
        )
        if value is not None:
            total += value
    return total


def stale_pending_buys(db: Session, *, trade_date: str, settings=None) -> list[dict]:
    """Unresolved buys from a PREVIOUS operating day.

    They no longer reserve today's cash (that would drain capacity forever),
    but they are real unresolved submissions and must be visible. Surfaced in
    readiness and in the cash diagnostic so they get reconciled.
    """
    from app.models.models import OrderExecution

    rows = (
        db.query(OrderExecution)
        .filter(
            OrderExecution.side == "buy",
            OrderExecution.status.in_(PENDING_BUY_STATUSES),
        )
        .all()
    )
    stale = []
    for row in rows:
        audit = (row.broker_response or {}).get("request_audit") or {}
        row_date = _order_trade_date(row, audit, settings)
        if row_date == trade_date:
            continue
        stale.append({
            "execution_id": row.id,
            "symbol": row.symbol,
            "status": row.status,
            "currency": _order_currency(row, audit),
            "trade_date": row_date,
            "notional": float(
                positive_decimal(audit.get("actual_notional"))
                or positive_decimal(audit.get("estimated_notional"))
                or 0
            ),
        })
    return stale


def _order_currency(row, audit: dict) -> str:
    """Currency of an order, from its own signed audit. Never guessed."""
    identity = audit.get("instrument_identity") or {}
    currency = identity.get("currency") or audit.get("currency") or ""
    return str(currency).strip().upper()


def _order_trade_date(row, audit: dict, settings) -> str | None:
    """The operating day an order belongs to.

    Prefers the trade_date recorded at submission; falls back to converting
    sent_at/created_at into the market timezone, never into UTC's day.
    """
    recorded = audit.get("trade_date")
    if recorded:
        return str(recorded)
    stamp = row.sent_at or row.created_at
    if stamp is None:
        return None
    if settings is None:
        return stamp.date().isoformat()
    from app.market.calendar import resolve_market_schedule

    tz = resolve_market_schedule(settings).get("tzinfo")
    if tz is None:
        return stamp.date().isoformat()
    aware = stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)
    return aware.astimezone(tz).date().isoformat()
