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

from datetime import datetime
from decimal import Decimal

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


def get_daily_totals(db: Session, trade_date: str, execution_class: str) -> Decimal:
    """Notional already submitted today for one class."""
    row = (
        db.query(ExecutionDailyNotional)
        .filter(
            ExecutionDailyNotional.trade_date == trade_date,
            ExecutionDailyNotional.execution_class == execution_class,
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
) -> tuple[str | None, dict]:
    """Would `additional_notional` blow today's budget for this class?

    A missing/unusable limit blocks: an unconfigured daily cap is not an
    absent one.
    """
    audit = {
        "trade_date": trade_date,
        "execution_class": execution_class,
        "additional_notional": float(additional_notional),
    }
    limit = positive_decimal(max_daily_notional)
    if limit is None:
        return DAILY_LIMIT_EXCEEDED, {**audit, "reason": "daily_limit_not_configured"}

    already = get_daily_totals(db, trade_date, execution_class)
    projected = already + additional_notional
    audit.update({
        "already_submitted": float(already),
        "projected": float(projected),
        "max_daily_notional": float(limit),
    })
    if projected > limit:
        return DAILY_LIMIT_EXCEEDED, audit
    return None, {**audit, "passed": True}


def consume_daily_budget(
    db: Session,
    *,
    trade_date: str,
    execution_class: str,
    currency: str,
    notional: Decimal,
) -> None:
    """Record notional as submitted. Called at the point of no return only."""
    row = (
        db.query(ExecutionDailyNotional)
        .filter(
            ExecutionDailyNotional.trade_date == trade_date,
            ExecutionDailyNotional.execution_class == execution_class,
        )
        .first()
    )
    amount = float(notional)
    if row is None:
        db.add(
            ExecutionDailyNotional(
                trade_date=trade_date,
                execution_class=execution_class,
                currency=currency or "",
                submitted_notional=amount,
                order_count=1,
            )
        )
    else:
        row.submitted_notional = float(row.submitted_notional or 0.0) + amount
        row.order_count = int(row.order_count or 0) + 1


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


def pending_buy_notional(db: Session, *, exclude_ids: set[int] | None = None) -> Decimal:
    """Notional of buys already sent today that may still consume cash.

    An order that reached the broker and has not been resolved yet is money
    that is no longer freely available, even though the balance may not
    reflect it instantly.
    """
    from app.models.models import OrderExecution

    rows = (
        db.query(OrderExecution)
        .filter(
            OrderExecution.side == "buy",
            OrderExecution.status.in_(
                ["submitting", "execution_sent", "manual_reconciliation_required"]
            ),
        )
        .all()
    )
    total = Decimal("0")
    for row in rows:
        if exclude_ids and row.id in exclude_ids:
            continue
        audit = (row.broker_response or {}).get("request_audit") or {}
        value = positive_decimal(audit.get("actual_notional")) or positive_decimal(
            audit.get("estimated_notional")
        )
        if value is not None:
            total += value
    return total
