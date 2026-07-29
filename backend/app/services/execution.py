"""Execution service — approve triggers real IOL execution with safe order planning.

Flow:
1. User approves recommendation via API
2. For each RecommendationAction:
   a. Load latest portfolio snapshot to get real position data
   b. Calculate safe quantity from target_change_pct + position value
   c. Validate quantity > 0 and consistent
   d. Create OrderExecution row with full traceability
   e. If validation passes → send to broker
   f. If validation fails → status=validation_failed, no order sent
3. Notification dispatched on state changes

CRITICAL INVARIANTS:
- Scheduler NEVER calls this module
- LLM NEVER triggers execution
- Only user approve via API triggers execution
- Recommendation must be in pending/blocked state to approve
- FAIL CLOSED: if we can't compute a safe quantity, we don't send the order
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import secrets as _secrets
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

from sqlalchemy import desc
from sqlalchemy.orm import Session, joinedload

from app.broker.clients import IolBrokerClient, MockBrokerClient
from app.broker.numeric import decimal_str, non_negative_decimal, positive_decimal, to_finite_decimal
from app.core.config import get_settings
from app.models.models import (
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
    UserDecision,
)
from app.services.logs import app_log

logger = logging.getLogger(__name__)


def _get_fresh_quote(broker, symbol: str, side: str) -> dict:
    """Fresh quote for the LEGACY mock flow (never reaches IOL).

    Delegates to the broker's public get_execution_quote contract — the
    service never accesses private broker internals. Mock brokers keep the
    historical market-order semantics (price None).
    """
    # MockBrokerClient (and test doubles marked with _mock_orders) — always
    # provide a quote so the mock flow isn't blocked.
    if hasattr(broker, "_mock_orders"):
        return {"available": True, "price": None, "source": "market_order"}

    settings = get_settings()
    return broker.get_execution_quote(
        symbol, side, settings.iol_order_market, settings.iol_order_settlement
    )


def _get_execution_broker():
    """Unambiguous broker factory: mock | sandbox | real, fail closed."""
    from app.broker.environment import resolve_execution_environment

    settings = get_settings()
    env = resolve_execution_environment(settings)
    if env["environment"] == "mock":
        return MockBrokerClient()
    if env["errors"]:
        # Defensive: preview/approve block these earlier via blocking reasons.
        raise RuntimeError(f"Broker environment not usable: {','.join(env['errors'])}")
    return IolBrokerClient(
        api_base=env["api_base"],
        username=env["username"],
        password=env["password"],
    )


def _get_latest_snapshot(db: Session) -> PortfolioSnapshot | None:
    """Get the most recent portfolio snapshot with positions."""
    return (
        db.query(PortfolioSnapshot)
        .options(joinedload(PortfolioSnapshot.positions))
        .order_by(desc(PortfolioSnapshot.id))
        .first()
    )


def _find_position(snapshot: PortfolioSnapshot, symbol: str) -> PortfolioPosition | None:
    """Find a position by symbol in the snapshot."""
    for p in snapshot.positions:
        if p.symbol == symbol:
            return p
    return None


def _plan_order(
    action: RecommendationAction,
    snapshot: PortfolioSnapshot,
) -> dict:
    """Plan a safe order from a recommendation action using real portfolio data.

    Returns a plan dict with:
    - valid: bool
    - side: str
    - quantity_planned: float
    - portfolio_value_used: float
    - position_value_used: float
    - blocked_reason: str (empty if valid)
    - snapshot_price_ref: float | None  (for traceability ONLY, never sent to broker)
    """
    symbol = action.symbol
    target_pct = action.target_change_pct
    side = "sell" if target_pct < 0 else "buy"
    abs_pct = abs(target_pct)

    portfolio_value = snapshot.total_value if snapshot else 0
    position = _find_position(snapshot, symbol) if snapshot else None

    # --- Validation: sell (reduce position) ---
    if side == "sell":
        if not position:
            return {
                "valid": False,
                "side": side,
                "quantity_planned": 0,
                "portfolio_value_used": portfolio_value,
                "position_value_used": 0,
                "blocked_reason": f"No position found for {symbol} in latest snapshot. Cannot sell.",
                "snapshot_price_ref": None,
            }

        position_value = position.market_value or 0
        position_qty = position.quantity or 0

        if position_qty <= 0:
            return {
                "valid": False,
                "side": side,
                "quantity_planned": 0,
                "portfolio_value_used": portfolio_value,
                "position_value_used": position_value,
                "blocked_reason": f"Position quantity for {symbol} is {position_qty}. Cannot sell zero/negative.",
                "snapshot_price_ref": None,
            }

        # Calculate the amount to sell as % of portfolio value applied to position
        target_value = portfolio_value * abs_pct
        if position_value <= 0:
            return {
                "valid": False,
                "side": side,
                "quantity_planned": 0,
                "portfolio_value_used": portfolio_value,
                "position_value_used": position_value,
                "blocked_reason": f"Position market_value for {symbol} is {position_value}. Cannot calculate.",
                "snapshot_price_ref": None,
            }

        # Price per unit from position data
        price_per_unit = position_value / position_qty
        if price_per_unit <= 0:
            return {
                "valid": False,
                "side": side,
                "quantity_planned": 0,
                "portfolio_value_used": portfolio_value,
                "position_value_used": position_value,
                "blocked_reason": f"Derived price per unit for {symbol} is {price_per_unit}. Cannot calculate.",
                "snapshot_price_ref": None,
            }

        # Quantity to sell — cannot exceed held quantity
        raw_qty = target_value / price_per_unit
        quantity_planned = min(raw_qty, position_qty)
        # Round down to integer for most IOL instruments
        quantity_planned = math.floor(quantity_planned)

        if quantity_planned <= 0:
            return {
                "valid": False,
                "side": side,
                "quantity_planned": 0,
                "portfolio_value_used": portfolio_value,
                "position_value_used": position_value,
                "blocked_reason": f"Calculated sell quantity for {symbol} rounds to 0 (target_value={target_value:.2f}, price={price_per_unit:.2f}).",
                "snapshot_price_ref": price_per_unit,
            }

        return {
            "valid": True,
            "side": side,
            "quantity_planned": quantity_planned,
            "portfolio_value_used": portfolio_value,
            "position_value_used": position_value,
            "blocked_reason": "",
            "snapshot_price_ref": price_per_unit,
        }

    # --- Buy (increase position / new position) ---
    # For MVP: buy is supported but requires cash available
    cash = snapshot.cash if snapshot else 0
    target_value = portfolio_value * abs_pct

    if target_value <= 0:
        return {
            "valid": False,
            "side": side,
            "quantity_planned": 0,
            "portfolio_value_used": portfolio_value,
            "position_value_used": position.market_value if position else 0,
            "blocked_reason": f"Target buy value for {symbol} is 0. abs_pct={abs_pct}.",
            "snapshot_price_ref": None,
        }

    # Need a price — from position or fail
    price_per_unit = None
    if position and position.quantity and position.quantity > 0 and position.market_value:
        price_per_unit = position.market_value / position.quantity
    elif position and position.avg_price and position.avg_price > 0:
        price_per_unit = position.avg_price

    if not price_per_unit or price_per_unit <= 0:
        return {
            "valid": False,
            "side": side,
            "quantity_planned": 0,
            "portfolio_value_used": portfolio_value,
            "position_value_used": position.market_value if position else 0,
            "blocked_reason": f"No price reference for {symbol}. Cannot calculate buy quantity.",
            "snapshot_price_ref": None,
        }

    # Don't buy more than available cash
    buy_value = min(target_value, cash)
    quantity_planned = math.floor(buy_value / price_per_unit)

    if quantity_planned <= 0:
        return {
            "valid": False,
            "side": side,
            "quantity_planned": 0,
            "portfolio_value_used": portfolio_value,
            "position_value_used": position.market_value if position else 0,
            "blocked_reason": f"Buy quantity for {symbol} rounds to 0 (buy_value={buy_value:.2f}, price={price_per_unit:.2f}, cash={cash:.2f}).",
            "snapshot_price_ref": price_per_unit,
        }

    return {
        "valid": True,
        "side": side,
        "quantity_planned": quantity_planned,
        "portfolio_value_used": portfolio_value,
        "position_value_used": position.market_value if position else 0,
        "blocked_reason": "",
        "snapshot_price_ref": price_per_unit,
    }


# ---------------------------------------------------------------------------
# Execution Authorization V1 — canonical preview, HMAC signing, blocking codes
# ---------------------------------------------------------------------------

_APPROVABLE_STATUSES = {"pending", "blocked"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_naive() -> datetime:
    """Naive UTC now — DB timestamps are naive UTC (datetime.utcnow defaults)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def confirmation_phrase(recommendation_id: int) -> str:
    return f"EJECUTAR RECOMENDACION {recommendation_id}"


def _build_order_previews(actions: list, snapshot: PortfolioSnapshot) -> list[dict]:
    """Deterministic order plans with notional and portfolio percentage.

    This is THE single implementation of the execution plan calculation,
    used by both the preview endpoint and the approve validation.
    """
    orders = []
    for action in actions:
        plan = _plan_order(action, snapshot)
        valid = plan["valid"]
        blocked_reason = plan["blocked_reason"]
        price = plan["snapshot_price_ref"]
        qty = plan["quantity_planned"]
        estimated_notional = 0.0
        portfolio_pct = 0.0

        if valid:
            if not price or price <= 0:
                valid = False
                blocked_reason = f"No valid snapshot price reference for {action.symbol}. Cannot estimate notional."
            else:
                notional = (Decimal(str(qty)) * Decimal(str(price))).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
                estimated_notional = float(notional)
                pv = plan["portfolio_value_used"]
                if pv and pv > 0:
                    portfolio_pct = float(
                        (notional / Decimal(str(pv))).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
                    )

        orders.append({
            "recommendation_action_id": action.id,
            "symbol": action.symbol,
            "side": plan["side"],
            "target_change_pct": action.target_change_pct,
            "quantity_planned": qty,
            "snapshot_price_ref": price,
            "estimated_notional": estimated_notional,
            "portfolio_value_used": plan["portfolio_value_used"],
            "position_value_used": plan["position_value_used"],
            "portfolio_pct": portfolio_pct,
            "valid": valid,
            "blocked_reason": blocked_reason,
            "would_execute": False,
        })
    return orders


def _canonical_preview_payload(
    rec: Recommendation,
    snapshot: PortfolioSnapshot,
    orders: list[dict],
    generated_at_iso: str,
    expires_at_iso: str,
    settings,
) -> dict:
    """Canonical, deterministic payload for HMAC signing."""
    return {
        "recommendation_id": rec.id,
        "recommendation_created_at": rec.created_at.isoformat() if rec.created_at else "",
        "snapshot_id": snapshot.id,
        "generated_at": generated_at_iso,
        "expires_at": expires_at_iso,
        "broker_mode": settings.broker_mode,
        "configured_broker_mode": settings.broker_mode,
        # Authoritative environment (iol_use_sandbox can make it differ from
        # the configured mode). Changing it invalidates the signature.
        "effective_environment": _effective_environment(settings),
        "orders": [
            {
                "recommendation_action_id": o["recommendation_action_id"],
                "symbol": o["symbol"],
                "side": o["side"],
                "target_change_pct": o["target_change_pct"],
                "quantity_planned": o["quantity_planned"],
                "snapshot_price_ref": o["snapshot_price_ref"],
                "estimated_notional": o["estimated_notional"],
                "portfolio_value_used": o["portfolio_value_used"],
                "position_value_used": o["position_value_used"],
                "portfolio_pct": o["portfolio_pct"],
                # Instrument identity + per-symbol scope are signed too.
                "instrument_identity": o.get("instrument_identity") or {},
                "execution_scope": o.get("execution_scope") or {},
            }
            for o in orders
        ],
        "limits": {
            "max_order_value": settings.execution_max_order_value,
            "max_total_value": settings.execution_max_total_value,
            "max_portfolio_pct": settings.execution_max_portfolio_pct,
        },
        # Signed order policy: any change to market/settlement/order type/
        # validity/quote policy/deviation limit invalidates the hash.
        "execution_venue": _execution_venue(settings),
    }


def _sign_preview_payload(payload: dict, secret: str) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()


def _has_prior_execution(db: Session, recommendation_id: int) -> bool:
    """Server-side double-execution check: existing executions or approved decision."""
    if db.query(OrderExecution).filter(
        OrderExecution.recommendation_id == recommendation_id
    ).count() > 0:
        return True
    if db.query(UserDecision).filter(
        UserDecision.recommendation_id == recommendation_id,
        UserDecision.decision == "approved",
    ).count() > 0:
        return True
    return False


def _limits_configured(settings) -> bool:
    return (
        settings.execution_max_order_value > 0
        and settings.execution_max_total_value > 0
        and settings.execution_max_portfolio_pct > 0
    )


def _evaluate_limit_reasons(orders: list[dict], snapshot: PortfolioSnapshot, settings) -> list[str]:
    """Stable blocking codes for limit violations. Fail closed on currency mismatch."""
    reasons = []
    valid_orders = [o for o in orders if o["valid"]]

    if not _limits_configured(settings):
        reasons.append("execution_limits_not_configured")
        return reasons

    # No automatic currency conversion: every position involved must be in
    # the snapshot currency, otherwise notional comparisons are meaningless.
    snapshot_currency = getattr(snapshot, "currency", None)
    if snapshot_currency:
        for o in valid_orders:
            pos = _find_position(snapshot, o["symbol"])
            if pos is not None and pos.currency and pos.currency != snapshot_currency:
                reasons.append("currency_mismatch")
                break

    if any(o["estimated_notional"] > settings.execution_max_order_value for o in valid_orders):
        reasons.append("order_limit_exceeded")

    total = sum(o["estimated_notional"] for o in valid_orders)
    if total > settings.execution_max_total_value:
        reasons.append("total_limit_exceeded")

    if any(o["portfolio_pct"] > settings.execution_max_portfolio_pct for o in valid_orders):
        reasons.append("portfolio_pct_limit_exceeded")

    return reasons


_QUOTE_POLICY = "best_bid_for_sell_best_ask_for_buy"


def _effective_environment(settings) -> str:
    """THE authoritative environment for every execution decision."""
    from app.broker.environment import effective_execution_environment

    return effective_execution_environment(settings)


def _execution_locked(settings) -> bool:
    """Lock check against the EFFECTIVE environment.

    - sandbox effective → only SANDBOX_EXECUTION_ENABLED matters
    - real effective    → only ORDER_EXECUTION_ENABLED matters
    - mock effective    → no IOL lock applies
    """
    env = _effective_environment(settings)
    if env == "mock":
        return False
    if env == "sandbox":
        return not settings.sandbox_execution_enabled
    return not settings.order_execution_enabled


def _apply_execution_scope(orders: list[dict], snapshot, settings) -> list[str]:
    """Attach instrument identity + scope to each order and collect blocking
    codes. Mock effective environment is exempt (never reaches IOL)."""
    from app.broker.instrument_scope import evaluate_order_scope, load_instrument_policies

    if _effective_environment(settings) == "mock":
        return []

    policies, policy_errors = load_instrument_policies(settings)
    codes: list[str] = list(policy_errors)

    for order in orders:
        position = _find_position(snapshot, order["symbol"]) if snapshot else None
        scope_info, order_codes = evaluate_order_scope(
            order=order, position=position, policies=policies, settings=settings
        )
        order["instrument_identity"] = scope_info["instrument_identity"]
        order["execution_scope"] = scope_info["execution_scope"]
        if order_codes:
            # A scoped-out order can never be part of an approvable plan.
            order["valid"] = False
            existing = order.get("blocked_reason") or ""
            order["blocked_reason"] = (
                f"{existing} | scope: {', '.join(order_codes)}".strip(" |")
                if existing else f"scope: {', '.join(order_codes)}"
            )
        for code in order_codes:
            if code not in codes:
                codes.append(code)

    return codes


def _execution_venue(settings) -> dict:
    """Explicit, signed order policy — market/settlement are never hardcoded."""
    return {
        "market": settings.iol_order_market or "",
        "settlement": settings.iol_order_settlement or "",
        "order_type": settings.iol_order_type or "",
        "validity_minutes": settings.iol_order_validity_minutes,
        "quote_policy": _QUOTE_POLICY,
        "max_quote_age_seconds": settings.execution_max_quote_age_seconds,
        "max_price_deviation_pct": settings.execution_max_price_deviation_pct,
    }


def _preflight_policy_reasons(settings) -> list[str]:
    """V1 non-negotiable execution policy for sandbox/real.

    - the live position guard can NOT be disabled;
    - the phase is strictly sell-only (there is no cash/balance guard for
      buys yet, so buys stay out of scope entirely).
    Mock effective environment is exempt (never reaches IOL).
    """
    if _effective_environment(settings) == "mock":
        return []
    reasons = []
    if not settings.execution_require_live_position_check:
        reasons.append("live_position_verification_required")
    if not settings.execution_sell_only:
        reasons.append("sell_only_mode_required")
    return reasons


def _venue_blocking_reasons(settings) -> list[str]:
    """Order-policy and environment blocking codes for sandbox/real brokers.

    Mock mode is exempt: it never builds IOL requests, so the policy does not
    gate it (existing mock/staging flows stay usable).
    """
    from app.broker.environment import resolve_execution_environment
    from app.broker.order_request import (
        KNOWN_IOL_MARKETS,
        KNOWN_IOL_SETTLEMENTS,
        SUPPORTED_ORDER_TYPES,
    )

    env = resolve_execution_environment(settings)
    if env["environment"] == "mock":
        return []

    reasons: list[str] = list(env["errors"])

    market = settings.iol_order_market or ""
    settlement = settings.iol_order_settlement or ""
    if not market or not settlement or settings.iol_order_validity_minutes <= 0:
        reasons.append("iol_order_policy_not_configured")
    if market and market not in KNOWN_IOL_MARKETS:
        reasons.append("unsupported_iol_market")
    if settlement and settlement not in KNOWN_IOL_SETTLEMENTS:
        reasons.append("unsupported_iol_settlement")
    if (settings.iol_order_type or "") not in SUPPORTED_ORDER_TYPES:
        reasons.append("unsupported_iol_order_type")
    if settings.execution_max_quote_age_seconds <= 0:
        reasons.append("quote_policy_not_configured")
    if settings.execution_max_price_deviation_pct <= 0:
        reasons.append("price_deviation_limit_not_configured")
    return reasons


def get_execution_readiness() -> dict:
    """Read-only, non-sensitive execution readiness report.

    Never returns credentials, tokens, secrets or full URLs with auth info —
    only booleans, stable blocking codes and the API host name.
    """
    from app.broker.environment import api_host_of, resolve_execution_environment

    settings = get_settings()
    env = resolve_execution_environment(settings)
    venue_reasons = _venue_blocking_reasons(settings) + _preflight_policy_reasons(settings)

    from app.broker.instrument_scope import load_instrument_policies

    blocking: list[str] = []
    if _execution_locked(settings):
        blocking.append("execution_locked")
    if not settings.execution_admin_key:
        blocking.append("execution_admin_key_not_configured")
    if not settings.execution_preview_secret:
        blocking.append("preview_signing_not_configured")
    if not _limits_configured(settings):
        blocking.append("execution_limits_not_configured")
    blocking.extend(venue_reasons)

    policies, policy_errors = load_instrument_policies(settings)
    if env["environment"] != "mock":
        if not policies:
            blocking.append("execution_scope_not_configured")
        for code in policy_errors:
            if code not in blocking:
                blocking.append(code)

    policy_configured = bool(settings.iol_order_market and settings.iol_order_settlement) and not any(
        r in venue_reasons
        for r in (
            "iol_order_policy_not_configured",
            "unsupported_iol_market",
            "unsupported_iol_settlement",
            "unsupported_iol_order_type",
            "quote_policy_not_configured",
            "price_deviation_limit_not_configured",
        )
    )

    return {
        "broker_mode": settings.broker_mode,
        "configured_broker_mode": settings.broker_mode,
        "environment": env["environment"],
        "effective_environment": env["environment"],
        "api_host": api_host_of(env),
        "order_execution_enabled": settings.order_execution_enabled,
        "sandbox_execution_enabled": settings.sandbox_execution_enabled,
        "credentials_configured": bool(env["username"] and env["password"]),
        "execution_admin_key_configured": bool(settings.execution_admin_key),
        "preview_secret_configured": bool(settings.execution_preview_secret),
        "limits_configured": _limits_configured(settings),
        "order_policy_configured": policy_configured,
        "market": settings.iol_order_market or None,
        "settlement": settings.iol_order_settlement or None,
        "order_type": settings.iol_order_type,
        "execution_sell_only": bool(settings.execution_sell_only),
        "live_position_check_required": bool(settings.execution_require_live_position_check),
        # V1 invariants (not configurable away for sandbox/real)
        "sell_only_mode_required": True,
        "batch_preflight_enabled": True,
        "fresh_limit_revalidation_enabled": True,
        "execution_scope_configured": bool(policies) and not policy_errors,
        "allowed_symbols": sorted(policies.keys()),
        "instrument_policy_count": len(policies),
        "blocking_reasons": blocking,
        "ready_for_real_execution": env["environment"] == "real" and not blocking,
        "ready_for_sandbox_execution": env["environment"] == "sandbox" and not blocking,
    }


def build_execution_preview(
    db: Session,
    recommendation_id: int,
    *,
    generated_at: datetime | None = None,
) -> dict:
    """Single deterministic builder of the execution authorization preview.

    Used by GET /execution-preview AND by the server-side validation inside
    approve (rebuild + signature match).

    READ-ONLY by contract:
    - no broker client, no place_order, no IOL calls
    - no OrderExecution rows, no db.add, no db.commit
    - no Recommendation.status change, no UserDecision, no notifications

    Always dry_run=True / would_execute=False. can_submit_approval reflects
    whether the reinforced approval flow could be initiated.
    """
    settings = get_settings()

    rec = db.query(Recommendation).filter(Recommendation.id == recommendation_id).first()
    if not rec:
        return {"error": "Recommendation not found", "status_code": 404}

    actions = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == recommendation_id
    ).all()

    snapshot = _get_latest_snapshot(db)
    if not snapshot:
        return {"error": "No portfolio snapshot available for preview", "status_code": 400}

    orders_preview = _build_order_previews(actions, snapshot)

    gen_dt = generated_at or _utcnow()
    if gen_dt.tzinfo is None:
        gen_dt = gen_dt.replace(tzinfo=timezone.utc)
    expires_dt = gen_dt + timedelta(seconds=settings.execution_preview_ttl_seconds)
    generated_at_iso = gen_dt.isoformat()
    expires_at_iso = expires_dt.isoformat()

    admin_configured = bool(settings.execution_admin_key)
    signing_configured = bool(settings.execution_preview_secret)
    limits_configured = _limits_configured(settings)

    # Execution scope: attaches instrument identity + per-symbol scope to
    # every order and invalidates any order outside the allowlist.
    scope_reasons = _apply_execution_scope(orders_preview, snapshot, settings)

    # --- Blocking reasons: stable codes the frontend can rely on ---
    blocking_reasons: list[str] = []
    if _execution_locked(settings):
        blocking_reasons.append("execution_locked")
    if not admin_configured:
        blocking_reasons.append("execution_admin_key_not_configured")
    if not signing_configured:
        blocking_reasons.append("preview_signing_not_configured")
    blocking_reasons.extend(_evaluate_limit_reasons(orders_preview, snapshot, settings))
    blocking_reasons.extend(_venue_blocking_reasons(settings))
    blocking_reasons.extend(_preflight_policy_reasons(settings))
    for code in scope_reasons:
        if code not in blocking_reasons:
            blocking_reasons.append(code)
    if rec.status not in _APPROVABLE_STATUSES:
        blocking_reasons.append("recommendation_not_pending")
    if rec.superseded_at is not None:
        blocking_reasons.append("recommendation_superseded")
    if rec.created_at is not None:
        age = _utcnow_naive() - rec.created_at
        if age > timedelta(minutes=settings.execution_max_recommendation_age_minutes):
            blocking_reasons.append("recommendation_stale")
    if not orders_preview or any(not o["valid"] for o in orders_preview):
        blocking_reasons.append("invalid_order")
    if _has_prior_execution(db, recommendation_id):
        blocking_reasons.append("already_executed")

    preview_hash = ""
    if signing_configured:
        payload = _canonical_preview_payload(
            rec, snapshot, orders_preview, generated_at_iso, expires_at_iso, settings
        )
        preview_hash = _sign_preview_payload(payload, settings.execution_preview_secret)

    can_submit_approval = not blocking_reasons and bool(preview_hash)

    total_notional = round(sum(o["estimated_notional"] for o in orders_preview if o["valid"]), 2)

    return {
        "recommendation_id": recommendation_id,
        "recommendation_status": rec.status,
        "recommendation_action": rec.action,
        "recommendation_created_at": rec.created_at.isoformat() if rec.created_at else None,
        "snapshot_id": snapshot.id,
        "generated_at": generated_at_iso,
        "expires_at": expires_at_iso,
        "broker_mode": settings.broker_mode,
        "configured_broker_mode": settings.broker_mode,
        "effective_environment": _effective_environment(settings),
        "order_execution_enabled": settings.order_execution_enabled,
        "sandbox_execution_enabled": settings.sandbox_execution_enabled,
        "execution_sell_only": bool(settings.execution_sell_only),
        "live_position_check_required": bool(settings.execution_require_live_position_check),
        "execution_admin_auth_configured": admin_configured,
        "execution_limits_configured": limits_configured,
        "dry_run": True,
        "would_execute": False,
        "can_submit_approval": can_submit_approval,
        "blocking_reasons": blocking_reasons,
        "preview_hash": preview_hash,
        "message": "Execution preview only. No order was sent and no state was changed.",
        "actions_count": len(actions),
        "orders_preview": orders_preview,
        "limits": {
            "max_order_value": settings.execution_max_order_value,
            "max_total_value": settings.execution_max_total_value,
            "max_portfolio_pct": settings.execution_max_portfolio_pct,
            "total_estimated_notional": total_notional,
            "configured": limits_configured,
        },
        "execution_venue": _execution_venue(settings),
    }


def preview_execution_plan(db: Session, recommendation_id: int) -> dict:
    """Backward-compatible alias for the canonical preview builder."""
    return build_execution_preview(db, recommendation_id)


def _validate_reinforced_authorization(
    db: Session,
    rec: Recommendation,
    settings,
    execution_key: str | None,
    preview_hash: str | None,
    preview_generated_at: str | None,
    confirmation_text: str | None,
) -> tuple[dict | None, dict | None]:
    """Execution Authorization checks.

    Returns (error, validated_preview):
    - (error_dict, None) when any check fails
    - (None, rebuilt_preview) when everything passes — the caller MUST execute
      exactly these validated orders, never a recalculated plan (V2).

    Runs BEFORE any state change or broker interaction. The execution
    credential is never persisted, logged, or echoed back.
    """
    # 7. Secondary credential must be configured server-side
    if not settings.execution_admin_key:
        return {
            "error": "Ejecución bloqueada: credencial de ejecución no configurada en el servidor.",
            "code": "execution_admin_key_not_configured",
            "status_code": 423,
        }, None

    # 8. X-Execution-Key must match (constant-time)
    if not execution_key or not _secrets.compare_digest(str(execution_key), settings.execution_admin_key):
        return {
            "error": "Credencial de ejecución inválida o ausente.",
            "code": "invalid_execution_key",
            "status_code": 403,
        }, None

    # 9. Preview signing must be configured
    if not settings.execution_preview_secret:
        return {
            "error": "Ejecución bloqueada: firma de preview no configurada en el servidor.",
            "code": "preview_signing_not_configured",
            "status_code": 423,
        }, None

    # 10. Exact confirmation phrase
    expected = confirmation_phrase(rec.id)
    if not confirmation_text or confirmation_text.strip() != expected:
        return {
            "error": f"Confirmación incorrecta. Frase requerida exacta: '{expected}'.",
            "code": "confirmation_mismatch",
            "status_code": 422,
        }, None

    # 11. Preview must exist and not be expired
    if not preview_hash or not preview_generated_at:
        return {
            "error": "Se requiere preview_hash y preview_generated_at del preview revisado.",
            "code": "preview_required",
            "status_code": 409,
        }, None
    try:
        gen_dt = datetime.fromisoformat(preview_generated_at)
    except (ValueError, TypeError):
        return {
            "error": "preview_generated_at inválido.",
            "code": "preview_invalid",
            "status_code": 409,
        }, None
    if gen_dt.tzinfo is None:
        gen_dt = gen_dt.replace(tzinfo=timezone.utc)
    now = _utcnow()
    if now > gen_dt + timedelta(seconds=settings.execution_preview_ttl_seconds):
        return {
            "error": "El preview venció. Generá y revisá un preview nuevo.",
            "code": "preview_expired",
            "status_code": 409,
        }, None

    # 12-15. Rebuild the preview server-side and require an exact signature match.
    # Any drift (new snapshot, changed actions/quantities, tampered hash,
    # different limits) produces a different HMAC.
    rebuilt = build_execution_preview(db, rec.id, generated_at=gen_dt)
    if "error" in rebuilt:
        return {**rebuilt, "code": rebuilt.get("code", "preview_rebuild_failed")}, None
    server_hash = rebuilt.get("preview_hash") or ""
    if not server_hash or not _secrets.compare_digest(server_hash, str(preview_hash)):
        return {
            "error": (
                "El preview no coincide con el estado actual del servidor "
                "(snapshot, acciones u órdenes cambiaron, o el hash fue alterado)."
            ),
            "code": "preview_mismatch",
            "status_code": 409,
        }, None

    blocking = rebuilt.get("blocking_reasons", [])

    # Server-side CONFIGURATION problems are reported as such (423) before
    # order-level verdicts, so a missing scope/environment never masquerades
    # as an invalid order.
    for code in (
        "live_position_verification_required",
        "sell_only_mode_required",
        "execution_scope_not_configured",
        "instrument_policy_invalid",
        "broker_environment_requires_https",
        "broker_environment_url_invalid",
        "sandbox_environment_not_configured",
        "sandbox_environment_invalid",
        "sandbox_credentials_not_configured",
        "real_environment_not_configured",
        "real_environment_invalid",
        "real_credentials_not_configured",
        "unsupported_broker_mode",
    ):
        if code in blocking:
            return {
                "error": f"Ejecución bloqueada por configuración del servidor: {code}.",
                "code": code,
                "status_code": 423,
            }, None

    # Per-instrument scope violations reported with their specific code
    # (they also invalidate the order, but the root cause is more useful).
    for code in (
        "instrument_policy_missing",
        "symbol_not_allowed",
        "buy_execution_disabled",
        "instrument_identity_mismatch",
        "instrument_currency_mismatch",
        "instrument_market_mismatch",
        "instrument_settlement_mismatch",
        "quantity_step_mismatch",
        "symbol_quantity_limit_exceeded",
        "symbol_notional_limit_exceeded",
        "live_position_missing",
    ):
        if code in blocking:
            return {
                "error": f"Orden fuera del alcance autorizado: {code}.",
                "code": code,
                "status_code": 422,
            }, None

    # 16. Every order must be valid
    if "invalid_order" in blocking:
        return {
            "error": "Hay órdenes inválidas en el plan de ejecución.",
            "code": "invalid_order",
            "status_code": 422,
        }, None

    # 17-19. Limits must be configured and respected
    if "execution_limits_not_configured" in blocking:
        return {
            "error": "Ejecución bloqueada: límites de ejecución no configurados.",
            "code": "execution_limits_not_configured",
            "status_code": 423,
        }, None
    for code in ("currency_mismatch", "order_limit_exceeded", "total_limit_exceeded", "portfolio_pct_limit_exceeded"):
        if code in blocking:
            return {
                "error": f"Límite de ejecución violado: {code}.",
                "code": code,
                "status_code": 422,
            }, None

    # Order policy must be fully configured and valid for sandbox/real
    # execution (mock is exempt — it never builds IOL requests).
    for code in (
        "iol_order_policy_not_configured",
        "unsupported_iol_market",
        "unsupported_iol_settlement",
        "unsupported_iol_order_type",
        "quote_policy_not_configured",
        "price_deviation_limit_not_configured",
    ):
        if code in blocking:
            return {
                "error": f"Ejecución bloqueada por política de orden: {code}.",
                "code": code,
                "status_code": 423,
            }, None

    return None, rebuilt


def _crosscheck_validated_orders(actions: list, validated_orders: list[dict]) -> dict | None:
    """Belt-and-suspenders re-verification of the validated preview against
    the recommendation's actions. The HMAC already guarantees consistency;
    any mismatch here means something is deeply wrong → fail closed."""
    action_by_id = {a.id: a for a in actions}
    if len(validated_orders) != len(actions):
        return {
            "error": "El preview validado no cubre exactamente las acciones de la recomendación.",
            "code": "validated_orders_inconsistent",
            "status_code": 409,
        }
    for o in validated_orders:
        action = action_by_id.get(o.get("recommendation_action_id"))
        expected_side = None
        if action is not None:
            expected_side = "sell" if action.target_change_pct < 0 else "buy"
        if (
            action is None
            or action.symbol != o.get("symbol")
            or expected_side != o.get("side")
            or not o.get("valid")
            or not o.get("quantity_planned")
            or o.get("quantity_planned") <= 0
        ):
            return {
                "error": "Inconsistencia entre el preview validado y las acciones de la recomendación.",
                "code": "validated_orders_inconsistent",
                "status_code": 409,
            }
    return None


def approve_and_execute(
    db: Session,
    recommendation_id: int,
    note: str = "",
    *,
    execution_key: str | None = None,
    preview_hash: str | None = None,
    preview_generated_at: str | None = None,
    confirmation_text: str | None = None,
) -> dict:
    """Approve a recommendation and trigger order execution.

    Execution Authorization V2 (durability):
    - Real broker: ALWAYS requires the reinforced contract (execution key,
      signed non-expired preview, exact confirmation phrase, limits) on top
      of the ORDER_EXECUTION_ENABLED safety lock, and executes EXACTLY the
      validated preview orders — never a recalculated plan.
    - Mock broker: legacy direct approve still works (tests/staging); if the
      reinforced fields are provided, they are fully validated so staging can
      rehearse the real flow without touching IOL.

    Submission semantics: at-most-once automatic submission + manual
    reconciliation on uncertain outcome. An interrupted or dubious order is
    NEVER re-sent automatically.

    All validations run BEFORE any state change or broker interaction.
    Returns dict with execution results or error.
    """
    settings = get_settings()

    # 1. Recommendation exists
    rec = db.query(Recommendation).filter(Recommendation.id == recommendation_id).first()
    if not rec:
        return {"error": "Recommendation not found", "status_code": 404}

    # 2. Never executed before (server-side double-execution protection).
    # Checked BEFORE the status check so an interrupted execution (rec in
    # execution_pending/manual_reconciliation_required with persisted intents)
    # answers 409, never re-submits.
    if _has_prior_execution(db, recommendation_id):
        return {
            "error": "La recomendación ya tiene ejecuciones o una aprobación previa. No se reenvía automáticamente.",
            "code": "already_executed",
            "status_code": 409,
        }

    # 3. Allowed state
    if rec.status not in _APPROVABLE_STATUSES:
        if rec.status in {"execution_pending", "submitting", "manual_reconciliation_required"}:
            return {
                "error": (
                    f"La recomendación está en estado '{rec.status}': hay una ejecución en curso o "
                    "con resultado incierto. Requiere conciliación manual, no se reenvía."
                ),
                "code": "already_executed_or_in_progress",
                "status_code": 409,
            }
        return {"error": f"No se puede aprobar: estado actual es '{rec.status}'", "status_code": 400}

    # 4. Must be the current recommendation (not superseded)
    if rec.superseded_at is not None:
        return {
            "error": "La recomendación fue reemplazada por una más nueva.",
            "code": "recommendation_superseded",
            "status_code": 409,
        }

    # 5. Not older than the maximum allowed age
    if rec.created_at is not None:
        age = _utcnow_naive() - rec.created_at
        if age > timedelta(minutes=settings.execution_max_recommendation_age_minutes):
            return {
                "error": "La recomendación excede la antigüedad máxima permitida para ejecutar.",
                "code": "recommendation_stale",
                "status_code": 409,
            }

    # 6. SAFETY LOCKS — evaluated against the EFFECTIVE environment, fail
    # closed BEFORE any state change: no status change, no UserDecision, no
    # OrderExecution rows, no broker, no quotes, no notifications.
    # Real effective → ORDER_EXECUTION_ENABLED only.
    # Sandbox effective → SANDBOX_EXECUTION_ENABLED only.
    effective_env = _effective_environment(settings)
    if _execution_locked(settings):
        lock_var = "SANDBOX_EXECUTION_ENABLED" if effective_env == "sandbox" else "ORDER_EXECUTION_ENABLED"
        kind = "simulada" if effective_env == "sandbox" else "real"
        return {
            "error": (
                f"Safety lock activo: ejecución {kind} deshabilitada "
                f"({lock_var}=false). No se aprobó la recomendación "
                "ni se envió ninguna orden."
            ),
            "code": "execution_locked",
            "status_code": 423,
        }

    # 6b. V1 execution policy — non-negotiable for sandbox/real. Checked
    # BEFORE any DB write: no claim, no UserDecision, no OrderExecution,
    # no quote, no broker, no POST.
    policy_reasons = _preflight_policy_reasons(settings)
    if policy_reasons:
        code = policy_reasons[0]
        detail = (
            "El chequeo de posición real es obligatorio "
            "(EXECUTION_REQUIRE_LIVE_POSITION_CHECK=true)."
            if code == "live_position_verification_required"
            else "La fase V1 es estrictamente sell-only (EXECUTION_SELL_ONLY=true)."
        )
        return {
            "error": f"Ejecución bloqueada por política V1: {detail}",
            "code": code,
            "status_code": 423,
        }

    # 7-19. Reinforced authorization — mandatory for sandbox/real; opt-in for
    # mock (staging rehearsal) when any reinforced field is provided.
    reinforced = effective_env != "mock" or any(
        v is not None for v in (execution_key, preview_hash, preview_generated_at, confirmation_text)
    )
    validated_preview = None
    if reinforced:
        err, validated_preview = _validate_reinforced_authorization(
            db, rec, settings, execution_key, preview_hash, preview_generated_at, confirmation_text
        )
        if err:
            return err

    # Load actions (needed for the cross-check BEFORE claiming)
    actions = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == recommendation_id
    ).all()

    # V2: re-verify the validated orders against the recommendation's actions.
    # Fail closed pre-claim on any inconsistency.
    if validated_preview is not None and actions and rec.action != "mantener":
        err = _crosscheck_validated_orders(actions, validated_preview.get("orders_preview", []))
        if err:
            return err

    # 20. Atomic claim BEFORE any broker call — a concurrent approve for the
    # same recommendation loses the conditional update and gets a 409.
    claimed = (
        db.query(Recommendation)
        .filter(
            Recommendation.id == recommendation_id,
            Recommendation.status.in_(list(_APPROVABLE_STATUSES)),
        )
        .update({"status": "execution_pending"}, synchronize_session=False)
    )
    if claimed != 1:
        db.rollback()
        return {
            "error": "La recomendación ya fue reclamada o ejecutada por otra solicitud.",
            "code": "already_executed_or_in_progress",
            "status_code": 409,
        }
    db.flush()
    db.refresh(rec)

    # Claim succeeded (status=execution_pending) → record the decision.
    # The execution credential is deliberately NOT persisted or logged.
    decision = UserDecision(recommendation_id=recommendation_id, decision="approved", note=note)
    db.add(decision)
    db.flush()

    app_log(db, "Recomendación aprobada por usuario", context={
        "recommendation_id": recommendation_id,
        "action": rec.action,
    })

    if not actions or rec.action == "mantener":
        rec.status = "approved"
        db.commit()
        return {
            "recommendation_id": recommendation_id,
            "status": "approved",
            "executions": [],
            "message": "Aprobada sin órdenes (acción: mantener o sin activos afectados).",
        }

    if validated_preview is not None:
        # Durable reinforced path: persist intents, then submit exactly the
        # validated preview orders. Internal errors resolve conservatively.
        return _execute_validated_orders(db, rec, validated_preview, preview_generated_at)

    try:
        return _execute_claimed_orders(db, rec, actions, note)
    except Exception as exc:
        # Partial/unexpected failure: keep traceability, never auto-retry a
        # real order. The recommendation stays out of the approvable states.
        logger.error("Unexpected execution failure for recommendation %s: %s", recommendation_id, exc)
        try:
            rec.status = "execution_failed"
            db.commit()
        except Exception:
            db.rollback()
        return {
            "error": f"Fallo inesperado durante la ejecución: {str(exc)[:300]}",
            "code": "execution_failed",
            "status_code": 500,
        }


# States from which an order must NEVER be automatically re-submitted.
# execution_ready is included: a crash after preflight requires manual
# review, never an automatic retry.
_NO_RESUBMIT_STATUSES = {
    "submitting", "execution_sent", "manual_reconciliation_required", "execution_ready",
}


def _fail_order_definitive(db: Session, order_exec: OrderExecution, message: str) -> None:
    """Definitive pre-broker failure: nothing was sent, quantity_sent stays None."""
    order_exec.status = "failed"
    order_exec.error_message = message[:500]
    order_exec.completed_at = datetime.now(timezone.utc)
    db.commit()


def _check_live_position(order_exec: OrderExecution, preview_order: dict, live_positions: list) -> tuple[dict | None, str | None]:
    """Verify ONE order against the already-fetched live portfolio.

    Pure check (no DB writes): returns (live_position_check_audit, error_code).
    It can only BLOCK — the signed quantity is never resized.
    """
    from app.broker.instrument_scope import normalize_symbol

    identity = preview_order.get("instrument_identity") or {}
    symbol = normalize_symbol(order_exec.symbol)

    required_qty = positive_decimal(order_exec.quantity_planned)
    if required_qty is None:
        return None, "invalid_execution_quantity"

    match = None
    for p in live_positions:
        if normalize_symbol(p.get("symbol")) == symbol:
            match = p
            break
    if match is None:
        return None, "live_position_missing"

    if identity:
        if (match.get("asset_type") or "") != identity.get("asset_type") or \
           (match.get("instrument_type") or "") != identity.get("instrument_type"):
            return None, "instrument_identity_mismatch"
        if (match.get("currency") or "") != identity.get("currency"):
            return None, "instrument_currency_mismatch"

    available = non_negative_decimal(match.get("quantity"))
    if available is None:
        # NaN / Infinity / negative / non-numeric live quantity
        return None, "invalid_live_position_quantity"
    if available < required_qty:
        return None, "live_position_insufficient"

    return {
        "checked_at": _utcnow().isoformat(),
        "symbol": symbol,
        "quantity_available": float(available),
        "quantity_required": float(required_qty),
        "asset_type": match.get("asset_type") or "",
        "instrument_type": match.get("instrument_type") or "",
        "currency": match.get("currency") or "",
        "passed": True,
    }, None


def _preflight_one_order(
    order_exec: OrderExecution,
    preview_order: dict,
    broker,
    settings,
    live_positions: list,
    live_portfolio_value: Decimal,
    policies: dict,
) -> tuple[dict | None, str | None]:
    """Prepare ONE order completely, without sending anything.

    Returns (prepared, error_code). `prepared` carries the exact IOL request
    plus every audit fragment. NOTHING here touches the network except the
    executable quote query, and no order is ever submitted.
    """
    from app.broker.environment import api_host_of, resolve_execution_environment
    from app.broker.order_request import build_iol_order_request, compute_order_validity

    symbol = order_exec.symbol
    side = order_exec.side
    market = settings.iol_order_market
    settlement = settings.iol_order_settlement

    # 1-2. Live identity + quantity
    live_check, err = _check_live_position(order_exec, preview_order, live_positions)
    if err:
        return None, err

    quantity = positive_decimal(order_exec.quantity_planned)
    if quantity is None:
        return None, "invalid_execution_quantity"

    # 3. Executable quote (bid for sell / ask for buy — never last price)
    try:
        quote = broker.get_execution_quote(symbol, side, market, settlement)
    except Exception:
        return None, "quote_unavailable"
    if not quote or not quote.get("available") or quote.get("source") not in ("bid", "ask"):
        return None, "quote_unavailable"

    fresh_price = positive_decimal(quote.get("price"))
    if fresh_price is None:
        # Covers NaN / Infinity / zero / negative / non-numeric prices
        return None, "invalid_execution_price"

    # 4. Quote freshness
    try:
        retrieved_at = datetime.fromisoformat(quote["retrieved_at"])
        if retrieved_at.tzinfo is None:
            retrieved_at = retrieved_at.replace(tzinfo=timezone.utc)
        age_seconds = (_utcnow() - retrieved_at).total_seconds()
    except (KeyError, ValueError, TypeError):
        age_seconds = None
    if age_seconds is None or age_seconds > settings.execution_max_quote_age_seconds:
        return None, "quote_stale"

    # 5. Deviation vs the SIGNED snapshot reference
    snapshot_ref = positive_decimal(preview_order.get("snapshot_price_ref"))
    if snapshot_ref is None:
        return None, "invalid_execution_price"
    deviation = abs(fresh_price - snapshot_ref) / snapshot_ref
    deviation_pct = float(deviation.quantize(Decimal("0.000001")))
    if deviation_pct > settings.execution_max_price_deviation_pct:
        return None, "price_deviation_exceeded"

    # 6. Deterministic validity (ART, same operating day)
    validity, validity_err = compute_order_validity(settings.iol_order_validity_minutes)
    if validity_err:
        return None, validity_err

    # 7. Canonical form-urlencoded request (precioLimite only)
    order_request, build_err = build_iol_order_request(
        side=side, symbol=symbol, quantity=quantity, price=fresh_price,
        market=market, settlement=settlement,
        order_type=settings.iol_order_type, validity=validity,
    )
    if build_err:
        return None, build_err

    # 8. ACTUAL notional with the FRESH price (Decimal, never float)
    actual_notional = (quantity * fresh_price).quantize(Decimal("0.01"))
    if to_finite_decimal(actual_notional) is None or actual_notional <= 0:
        return None, "invalid_execution_notional"
    fresh_portfolio_pct = (actual_notional / live_portfolio_value).quantize(Decimal("0.000001"))

    # 9. Re-validate limits against the FRESH notional
    policy = policies.get(_normalized(symbol))
    if policy is None:
        return None, "instrument_policy_missing"
    if actual_notional > Decimal(str(policy["max_notional"])):
        return None, "fresh_symbol_notional_limit_exceeded"
    max_order_value = positive_decimal(settings.execution_max_order_value)
    if max_order_value is None or actual_notional > max_order_value:
        return None, "fresh_order_limit_exceeded"
    max_pct = positive_decimal(settings.execution_max_portfolio_pct)
    if max_pct is None or fresh_portfolio_pct > max_pct:
        return None, "fresh_portfolio_pct_limit_exceeded"

    # 10. Request audit (exactly what will be sent; never credentials)
    env = resolve_execution_environment(settings)
    return {
        "order_exec": order_exec,
        "order_request": order_request,
        "actual_notional": actual_notional,
        "audit": {
            "live_position_check": live_check,
            "execution_quote": {
                "price": float(fresh_price),
                "source": quote["source"],
                "retrieved_at": quote["retrieved_at"],
                "market": market,
                "settlement": settlement,
                "deviation_pct": deviation_pct,
            },
            "actual_notional": decimal_str(actual_notional),
            "fresh_portfolio_pct": decimal_str(fresh_portfolio_pct, "0.000001"),
            "live_portfolio_value_used": decimal_str(live_portfolio_value),
            "iol_request": {
                "environment": env["environment"],
                "api_host": api_host_of(env),
                "endpoint": order_request["endpoint"],
                "content_type": order_request["content_type"],
                **order_request["form_data"],
            },
        },
    }, None


def _normalized(symbol) -> str:
    from app.broker.instrument_scope import normalize_symbol
    return normalize_symbol(symbol)


def _live_portfolio_total(live: dict, live_positions: list) -> Decimal | None:
    """Finite positive live portfolio total, used ONLY for the percentage.

    Prefers an explicit total_value; otherwise derives it deterministically
    from the live positions plus cash (the broker snapshot contract does not
    always carry a precomputed total). Any non-finite component invalidates
    the whole value — fail closed.
    """
    explicit = positive_decimal(live.get("total_value"))
    if explicit is not None:
        return explicit
    if live.get("total_value") is not None:
        # Present but not a positive finite number → do NOT silently derive.
        return None

    total = Decimal("0")
    for p in live_positions:
        value = to_finite_decimal(p.get("market_value"))
        if value is None or value < 0:
            return None
        total += value
    cash = live.get("cash")
    if cash is not None:
        cash_dec = to_finite_decimal(cash)
        if cash_dec is None or cash_dec < 0:
            return None
        total += cash_dec
    return total if total > 0 else None


def prepare_validated_execution_batch(
    db: Session,
    intents: list,
    orders_by_action: dict,
    broker,
    settings,
) -> tuple[list[dict] | None, str | None]:
    """Full preflight for the WHOLE batch — no order is submitted here.

    Reads the live portfolio ONCE, validates every position, quote, notional
    and limit, builds every IOL request, checks the batch total, and only
    then commits every order as execution_ready.

    Returns (prepared_orders, error_code). On any failure NOTHING is
    submitted: the causing order is marked failed with its specific code and
    every other order becomes preflight_cancelled.
    """
    from app.broker.instrument_scope import load_instrument_policies

    policies, _ = load_instrument_policies(settings)

    # --- Single live portfolio read for the whole batch ---
    try:
        live = broker.get_portfolio_snapshot() or {}
        live_positions = live.get("positions") or []
    except Exception as exc:
        _cancel_batch(db, intents, None, "live_position_verification_failed",
                      f"could not read the live portfolio ({str(exc)[:150]})")
        return None, "live_position_verification_failed"

    live_portfolio_value = _live_portfolio_total(live, live_positions)
    if live_portfolio_value is None:
        # Missing / zero / negative / NaN / Infinity portfolio value
        _cancel_batch(db, intents, None, "invalid_portfolio_value",
                      "live portfolio total value is not a positive finite number")
        return None, "invalid_portfolio_value"

    prepared: list[dict] = []
    for order_exec in intents:
        preview_order = orders_by_action.get(order_exec.recommendation_action_id) or {}
        result, err = _preflight_one_order(
            order_exec, preview_order, broker, settings,
            live_positions, live_portfolio_value, policies,
        )
        if err:
            _cancel_batch(db, intents, order_exec, err, "preflight validation failed")
            return None, err
        prepared.append(result)

    # --- Batch total limit against FRESH notionals ---
    total_actual = sum((p["actual_notional"] for p in prepared), Decimal("0"))
    max_total = positive_decimal(settings.execution_max_total_value)
    if max_total is None or total_actual > max_total:
        # Batch-level limit: no order may be sent.
        _cancel_batch(db, intents, None, "fresh_total_limit_exceeded",
                      f"batch total {total_actual} exceeds {settings.execution_max_total_value}")
        return None, "fresh_total_limit_exceeded"

    batch_audit = {
        "total_actual_notional": decimal_str(total_actual),
        "max_total_value": decimal_str(max_total),
        "passed": True,
    }

    # --- Commit the FULL preflight: every order becomes execution_ready ---
    for p in prepared:
        order_exec = p["order_exec"]
        br = dict(order_exec.broker_response or {})
        request_audit = dict(br.get("request_audit") or {})
        request_audit.update(p["audit"])
        request_audit["batch_preflight"] = batch_audit
        br["request_audit"] = request_audit
        order_exec.broker_response = br
        order_exec.status = "execution_ready"
    db.commit()

    return prepared, None


def _cancel_batch(db: Session, intents: list, failed_order, code: str, detail: str) -> None:
    """Abort the whole batch before ANY submission.

    The causing order gets `failed` with its specific code; every other
    not-yet-submitted order becomes `preflight_cancelled`. quantity_sent
    stays None everywhere — nothing was ever sent, so this can never be a
    manual reconciliation case.
    """
    now = datetime.now(timezone.utc)
    for order_exec in intents:
        if order_exec.status in ("execution_sent", "manual_reconciliation_required", "submitting"):
            continue
        # failed_order None → batch-level failure (portfolio read, portfolio
        # value, batch total): every order failed for the same definitive
        # reason, so all of them carry the specific code.
        if failed_order is None or order_exec is failed_order:
            order_exec.status = "failed"
            order_exec.error_message = (
                f"{code}: {detail}. Order NOT sent — no order in this batch was submitted."
            )[:500]
        else:
            order_exec.status = "preflight_cancelled"
            order_exec.error_message = (
                f"preflight_cancelled: another order in the batch failed preflight ({code}). "
                "No order in this batch was submitted."
            )[:500]
        order_exec.quantity_sent = None
        order_exec.completed_at = now
    db.commit()


def _submit_prepared_order(db: Session, prepared: dict, broker) -> str:
    """Submit ONE already-prepared (execution_ready) order, at most once.

    Returns "sent" | "rejected" | "uncertain". The request body was fixed at
    preflight time and is sent verbatim — it is never rebuilt here.
    """
    order_exec = prepared["order_exec"]
    order_request = prepared["order_request"]

    db.refresh(order_exec)
    if order_exec.status != "execution_ready":
        # Never submit anything that is not execution_ready.
        if order_exec.status == "execution_sent":
            return "sent"
        if order_exec.status in ("submitting", "manual_reconciliation_required"):
            return "uncertain"
        return "skipped"

    # Point of no return: quantity_sent is set together with 'submitting'.
    order_exec.status = "submitting"
    order_exec.quantity_sent = order_exec.quantity_planned
    db.commit()

    try:
        result = broker.submit_order_request(order_request)
    except Exception as exc:
        result = {
            "outcome": "submission_uncertain",
            "order_id": "",
            "endpoint_used": order_request["endpoint"],
            "raw_response": {},
            "error": f"Unexpected submission failure: {str(exc)[:200]}",
        }

    order_exec.endpoint_used = result.get("endpoint_used", order_request["endpoint"])
    order_exec.broker_response = {
        **(order_exec.broker_response or {}),
        "broker_result": result.get("raw_response", {}),
    }

    outcome = result.get("outcome")
    if outcome == "sent":
        order_exec.broker_order_id = result.get("order_id", "")
        order_exec.sent_at = datetime.now(timezone.utc)
        order_exec.status = "execution_sent"
        db.commit()
        return "sent"
    if outcome == "rejected":
        order_exec.status = "rejected_by_broker"
        order_exec.error_message = result.get("error", "Broker rejected order")[:500]
        order_exec.completed_at = datetime.now(timezone.utc)
        db.commit()
        return "rejected"

    # submission_uncertain: IOL may have received the order. Manual
    # reconciliation, NEVER auto-retry.
    order_exec.status = "manual_reconciliation_required"
    order_exec.error_message = (
        f"Resultado incierto del broker: {result.get('error', 'unknown')[:300]}. "
        "Requiere conciliación manual. NO se reintenta automáticamente."
    )
    order_exec.completed_at = datetime.now(timezone.utc)
    db.commit()
    return "uncertain"


def _execute_validated_orders(
    db: Session,
    rec: Recommendation,
    validated_preview: dict,
    preview_generated_at: str | None,
) -> dict:
    """Durable execution of EXACTLY the validated preview orders (V2).

    Semantics: at-most-once automatic submission + manual reconciliation on
    uncertain outcome (IOL offers no idempotency keys, so exactly-once cannot
    be guaranteed — we guarantee we never auto-retry instead).

    Durability protocol:
    1. Persist ALL OrderExecution intents (with request_audit traceability,
       quantity_sent=None — nothing was sent yet) and COMMIT before
       instantiating the broker or fetching quotes.
    2. Per order: reload → verify still eligible → fetch fresh quote (a
       failure here is DEFINITIVE: place_order was never called → failed)
       → mark 'submitting' + quantity_sent=quantity_planned + COMMIT →
       single place_order call → persist outcome → COMMIT.
    3. A crash between 'submitting' and the outcome leaves the row in
       'submitting': visibly stuck, never auto-resent, approve returns 409.

    quantity_sent semantics: None until the moment we commit 'submitting'
    right before the broker call; it must never suggest a send happened
    while the order is only 'execution_requested'.

    No snapshot re-fetch, no _plan_order re-run: symbol/side/quantity come
    verbatim from the signed preview. The fresh quote decides ONLY the price
    (limit vs market), never symbol/side/quantity.
    """
    recommendation_id = rec.id
    orders = validated_preview.get("orders_preview", [])
    preview_hash = validated_preview.get("preview_hash", "")
    snapshot_id = validated_preview.get("snapshot_id")

    # --- Phase 1: persist intents, then COMMIT (durability point) ---
    intents: list[OrderExecution] = []
    for o in orders:
        request_audit = {
            "snapshot_id": snapshot_id,
            "preview_hash": preview_hash,
            "preview_generated_at": preview_generated_at,
            "estimated_notional": o.get("estimated_notional"),
            "portfolio_pct": o.get("portfolio_pct"),
            "execution_request_id": f"rec{recommendation_id}-act{o['recommendation_action_id']}-{preview_hash[:16]}",
        }
        order_exec = OrderExecution(
            recommendation_id=recommendation_id,
            recommendation_action_id=o["recommendation_action_id"],
            symbol=o["symbol"],
            side=o["side"],
            target_change_pct=o["target_change_pct"],
            status="execution_requested",
            validation_status="passed",
            portfolio_value_used=o["portfolio_value_used"],
            position_value_used=o["position_value_used"],
            quantity_planned=o["quantity_planned"],
            quantity=o["quantity_planned"],
            quantity_sent=None,
            broker_response={"request_audit": request_audit, "broker_result": None, "reconciliation_audit": []},
        )
        db.add(order_exec)
        intents.append(order_exec)

    # Claim + UserDecision + intents survive a process crash from here on.
    db.commit()

    # --- Phase 2: submit each intent at most once ---
    executions = []
    sent_count = 0
    uncertain_count = 0

    settings = get_settings()
    # Effective environment decides the contract: sandbox/real → canonical IOL.
    is_iol = _effective_environment(settings) != "mock"
    orders_by_action = {o["recommendation_action_id"]: o for o in orders}

    try:
        broker = _get_execution_broker()

        if is_iol:
            # ---- BATCH PREFLIGHT: prepare EVERYTHING before any POST ----
            # One live portfolio read, all positions/quotes/notionals/limits
            # validated, all requests built, all orders committed as
            # execution_ready. If anything fails, NO order is submitted.
            prepared_batch, preflight_error = prepare_validated_execution_batch(
                db, intents, orders_by_action, broker, settings
            )
            if preflight_error:
                for order_exec in intents:
                    db.refresh(order_exec)
                    executions.append(_exec_summary(order_exec))
                rec.status = "execution_failed"
                db.commit()
                return {
                    "recommendation_id": recommendation_id,
                    "status": rec.status,
                    "executions": executions,
                    "message": (
                        f"Preflight falló ({preflight_error}): ninguna orden del lote fue enviada."
                    ),
                }

            # ---- SUBMISSION: only execution_ready orders, one POST each ----
            for prepared in prepared_batch:
                outcome = _submit_prepared_order(db, prepared, broker)
                if outcome == "sent":
                    sent_count += 1
                elif outcome == "uncertain":
                    uncertain_count += 1
                executions.append(_exec_summary(prepared["order_exec"]))

            rec.status = (
                "manual_reconciliation_required" if uncertain_count
                else "approved" if sent_count == len(intents) and intents
                else "execution_failed" if sent_count == 0
                else "execution_partial"
            )
            db.commit()

            try:
                from app.notifications.dispatcher import dispatch_execution_notification
                for order_exec in intents:
                    dispatch_execution_notification(order_exec, db=db)
            except Exception as exc:
                logger.warning("Post-execution notification dispatch failed: %s", exc)

            return {
                "recommendation_id": recommendation_id,
                "status": rec.status,
                "executions": executions,
                "message": (
                    f"{len(executions)} órdenes procesadas: {sent_count} enviadas, "
                    f"{uncertain_count} con resultado incierto."
                ),
            }

        for order_exec in intents:
            # Reload the persisted row and verify single-attempt eligibility
            db.refresh(order_exec)
            if order_exec.status != "execution_requested":
                # submitting / sent / manual_reconciliation_required etc.
                # → never auto-resubmit
                executions.append(_exec_summary(order_exec))
                if order_exec.status == "execution_sent":
                    sent_count += 1
                elif order_exec.status in ("submitting", "manual_reconciliation_required"):
                    uncertain_count += 1
                continue

            # ---- Mock path (never IOL): historical market-order semantics ----
            # Fresh quote BEFORE 'submitting': any problem here is a
            # DEFINITIVE preparation failure — place_order was never called,
            # so this is 'failed', never 'manual_reconciliation_required'.
            quote_error = None
            quote = None
            try:
                quote = _get_fresh_quote(broker, order_exec.symbol, order_exec.side)
            except Exception as exc:
                quote_error = str(exc)[:300]

            if quote_error is not None or not quote.get("available"):
                order_exec.status = "failed"
                order_exec.error_message = (
                    f"Quote preparation failed for {order_exec.symbol} "
                    f"({quote_error or 'no fresh quote available'}). Order NOT sent — place_order was never called."
                )
                order_exec.completed_at = datetime.now(timezone.utc)
                db.commit()
                executions.append(_exec_summary(order_exec))
                continue

            # Point of no return: from here the request may reach the broker.
            # quantity_sent gets its value ONLY now, together with 'submitting'.
            order_exec.status = "submitting"
            order_exec.quantity_sent = order_exec.quantity_planned
            db.commit()

            try:
                result = broker.place_order(
                    symbol=order_exec.symbol,
                    side=order_exec.side,
                    quantity=order_exec.quantity_planned,
                    price=quote["price"],
                )
            except Exception as exc:
                # Uncertain outcome: IOL may or may not have received the
                # order. NEVER assume it is safe to retry.
                order_exec.status = "manual_reconciliation_required"
                order_exec.error_message = (
                    f"Resultado incierto del broker: {str(exc)[:300]}. "
                    "Requiere conciliación manual. NO se reintenta automáticamente."
                )
                order_exec.completed_at = datetime.now(timezone.utc)
                db.commit()
                uncertain_count += 1
                executions.append(_exec_summary(order_exec))
                continue

            # Persist the broker outcome immediately, preserving request_audit
            order_exec.broker_order_id = result.get("order_id", "")
            order_exec.endpoint_used = result.get("endpoint_used", "")
            order_exec.sent_at = datetime.now(timezone.utc)
            order_exec.broker_response = {
                **(order_exec.broker_response or {}),
                "broker_result": result.get("raw_response", result),
            }
            if result.get("status") == "sent":
                order_exec.status = "execution_sent"
                sent_count += 1
            elif result.get("status") == "rejected":
                order_exec.status = "rejected_by_broker"
                order_exec.error_message = result.get("error", "Broker rejected order")
                order_exec.completed_at = datetime.now(timezone.utc)
            else:
                order_exec.status = "failed"
                order_exec.error_message = result.get("error", "Unknown error")
                order_exec.completed_at = datetime.now(timezone.utc)
            db.commit()
            executions.append(_exec_summary(order_exec))

    except Exception as exc:
        # Infrastructure failure mid-loop: conservative resolution. Intents
        # already persisted; nothing gets re-sent automatically.
        logger.error("Infrastructure failure during validated execution of rec %s: %s", recommendation_id, exc)
        uncertain_count += 1

    # --- Phase 3: recommendation outcome ---
    total = len(intents)
    if uncertain_count > 0:
        rec.status = "manual_reconciliation_required"
    elif sent_count == total and total > 0:
        rec.status = "approved"
    elif sent_count == 0:
        rec.status = "execution_failed"
    else:
        rec.status = "execution_partial"
    db.commit()

    # Best-effort notification (notifications NEVER execute orders)
    try:
        from app.notifications.dispatcher import dispatch_execution_notification
        for order_exec in intents:
            dispatch_execution_notification(order_exec, db=db)
    except Exception as exc:
        logger.warning("Post-execution notification dispatch failed: %s", exc)

    return {
        "recommendation_id": recommendation_id,
        "status": rec.status,
        "executions": executions,
        "message": (
            f"{len(executions)} órdenes procesadas: {sent_count} enviadas, "
            f"{uncertain_count} con resultado incierto."
        ),
    }


def _execute_claimed_orders(db: Session, rec: Recommendation, actions: list, note: str) -> dict:
    """Legacy order loop (mock broker without reinforced fields — never IOL)."""
    recommendation_id = rec.id

    # Load latest snapshot for order planning
    snapshot = _get_latest_snapshot(db)

    # Create OrderExecution rows with planning and validation
    broker = _get_execution_broker()
    executions = []

    for action in actions:
        # --- ORDER PLANNING ---
        plan = _plan_order(action, snapshot)

        order_exec = OrderExecution(
            recommendation_id=recommendation_id,
            recommendation_action_id=action.id,
            symbol=action.symbol,
            side=plan["side"],
            target_change_pct=action.target_change_pct,
            status="execution_requested",
            portfolio_value_used=plan["portfolio_value_used"],
            position_value_used=plan["position_value_used"],
            quantity_planned=plan["quantity_planned"],
        )
        db.add(order_exec)
        db.flush()

        # --- VALIDATION ---
        if not plan["valid"]:
            order_exec.status = "validation_failed"
            order_exec.validation_status = "failed"
            order_exec.blocked_reason = plan["blocked_reason"]
            order_exec.error_message = plan["blocked_reason"]
            order_exec.completed_at = datetime.now(timezone.utc)

            app_log(db, f"Orden {plan['side']} para {action.symbol} bloqueada por validación", context={
                "order_execution_id": order_exec.id,
                "recommendation_id": recommendation_id,
                "symbol": action.symbol,
                "blocked_reason": plan["blocked_reason"],
            })

            executions.append(_exec_summary(order_exec))
            continue

        # Validation passed
        order_exec.validation_status = "passed"
        order_exec.quantity = plan["quantity_planned"]
        order_exec.quantity_sent = plan["quantity_planned"]

        app_log(db, f"Orden {plan['side']} solicitada para {action.symbol}", context={
            "order_execution_id": order_exec.id,
            "recommendation_id": recommendation_id,
            "symbol": action.symbol,
            "side": plan["side"],
            "target_change_pct": action.target_change_pct,
            "quantity_planned": plan["quantity_planned"],
            "portfolio_value_used": plan["portfolio_value_used"],
            "position_value_used": plan["position_value_used"],
        })

        # --- FRESH QUOTE (never use snapshot-derived price for broker) ---
        quote = _get_fresh_quote(broker, action.symbol, plan["side"])
        if not quote["available"]:
            order_exec.status = "validation_failed"
            order_exec.validation_status = "failed"
            order_exec.blocked_reason = (
                f"No fresh quote available for {action.symbol}. Cannot send order without live pricing."
            )
            order_exec.error_message = order_exec.blocked_reason
            order_exec.completed_at = datetime.now(timezone.utc)

            app_log(db, f"Orden {plan['side']} para {action.symbol} bloqueada: sin cotización fresca", context={
                "order_execution_id": order_exec.id,
                "symbol": action.symbol,
            })
            executions.append(_exec_summary(order_exec))
            continue

        # --- BROKER EXECUTION ---
        # price=quote["price"] → if None, broker sends precioMercado (market order)
        # if quote has a fresh price, broker sends precioLimite with live price
        try:
            result = broker.place_order(
                symbol=action.symbol,
                side=plan["side"],
                quantity=plan["quantity_planned"],
                price=quote["price"],
            )

            order_exec.broker_order_id = result.get("order_id", "")
            order_exec.broker_response = result.get("raw_response", {})
            order_exec.endpoint_used = result.get("endpoint_used", "")
            order_exec.sent_at = datetime.now(timezone.utc)

            if result.get("status") == "sent":
                order_exec.status = "execution_sent"
            elif result.get("status") == "rejected":
                order_exec.status = "rejected_by_broker"
                order_exec.error_message = result.get("error", "Broker rejected order")
                order_exec.completed_at = datetime.now(timezone.utc)
            else:
                order_exec.status = "failed"
                order_exec.error_message = result.get("error", "Unknown error")
                order_exec.completed_at = datetime.now(timezone.utc)

        except Exception as exc:
            order_exec.status = "failed"
            order_exec.error_message = str(exc)[:500]
            order_exec.completed_at = datetime.now(timezone.utc)

        executions.append(_exec_summary(order_exec))

    # Best-effort notification
    try:
        from app.notifications.dispatcher import dispatch_execution_notification
        for action in actions:
            exec_row = db.query(OrderExecution).filter(
                OrderExecution.recommendation_action_id == action.id
            ).first()
            if exec_row:
                dispatch_execution_notification(exec_row, db=db)
    except Exception as exc:
        logger.warning("Post-approval notification dispatch failed: %s", exc)

    rec.status = "approved"
    db.commit()

    return {
        "recommendation_id": recommendation_id,
        "status": "approved",
        "executions": executions,
        "message": f"{len(executions)} órdenes procesadas.",
    }


def _exec_summary(order_exec: OrderExecution) -> dict:
    return {
        "id": order_exec.id,
        "symbol": order_exec.symbol,
        "side": order_exec.side,
        "status": order_exec.status,
        "validation_status": order_exec.validation_status,
        "quantity_planned": order_exec.quantity_planned,
        "quantity_sent": order_exec.quantity_sent,
        "broker_order_id": order_exec.broker_order_id,
        "endpoint_used": order_exec.endpoint_used,
        "error": order_exec.error_message,
        "blocked_reason": order_exec.blocked_reason,
    }


def reject_recommendation(db: Session, recommendation_id: int, note: str = "") -> dict:
    """Reject a recommendation. No execution."""
    rec = db.query(Recommendation).filter(Recommendation.id == recommendation_id).first()
    if not rec:
        return {"error": "Recommendation not found", "status_code": 404}

    if rec.status not in {"pending", "blocked"}:
        return {"error": f"No se puede rechazar: estado actual es '{rec.status}'", "status_code": 400}

    rec.status = "rejected"
    decision = UserDecision(recommendation_id=recommendation_id, decision="rejected", note=note)
    db.add(decision)
    db.commit()

    app_log(db, "Recomendación rechazada por usuario", context={
        "recommendation_id": recommendation_id,
    })

    return {"recommendation_id": recommendation_id, "status": "rejected"}


def get_executions_for_recommendation(db: Session, recommendation_id: int) -> list[dict]:
    """Get all executions for a given recommendation."""
    execs = db.query(OrderExecution).filter(
        OrderExecution.recommendation_id == recommendation_id
    ).order_by(desc(OrderExecution.created_at)).all()
    return [_exec_to_dict(e) for e in execs]


def get_recent_executions(db: Session, limit: int = 20) -> list[dict]:
    """Get recent executions across all recommendations."""
    execs = db.query(OrderExecution).order_by(desc(OrderExecution.created_at)).limit(limit).all()
    return [_exec_to_dict(e) for e in execs]


def get_execution_by_id(db: Session, execution_id: int) -> dict | None:
    """Get a single execution by ID, with full audit detail and the
    associated recommendation. Never includes credentials or tokens."""
    e = db.query(OrderExecution).filter(OrderExecution.id == execution_id).first()
    if not e:
        return None
    result = _exec_to_dict(e)
    rec = db.query(Recommendation).filter(Recommendation.id == e.recommendation_id).first()
    result["recommendation"] = {
        "id": rec.id,
        "status": rec.status,
        "action": rec.action,
        "created_at": rec.created_at.isoformat() if rec.created_at else None,
        "superseded_at": rec.superseded_at.isoformat() if rec.superseded_at else None,
    } if rec else None
    return result


def _extract_broker_response_audits(broker_response) -> dict:
    """Safely split broker_response into its audit components.

    Legacy rows may hold a raw broker payload instead of the structured
    {request_audit, broker_result, reconciliation_audit} shape.
    """
    if not isinstance(broker_response, dict):
        return {"request_audit": None, "broker_result": broker_response, "reconciliation_audit": []}
    if "request_audit" in broker_response or "reconciliation_audit" in broker_response or "broker_result" in broker_response:
        return {
            "request_audit": broker_response.get("request_audit"),
            "broker_result": broker_response.get("broker_result"),
            "reconciliation_audit": broker_response.get("reconciliation_audit") or [],
            "broker_status_checks": broker_response.get("broker_status_checks") or [],
        }
    return {"request_audit": None, "broker_result": broker_response, "reconciliation_audit": [], "broker_status_checks": []}


def _exec_to_dict(e: OrderExecution) -> dict:
    audits = _extract_broker_response_audits(e.broker_response)
    return {
        "id": e.id,
        "recommendation_id": e.recommendation_id,
        "symbol": e.symbol,
        "side": e.side,
        "target_change_pct": e.target_change_pct,
        "status": e.status,
        "validation_status": e.validation_status,
        "quantity_planned": e.quantity_planned,
        "quantity_sent": e.quantity_sent,
        "portfolio_value_used": e.portfolio_value_used,
        "position_value_used": e.position_value_used,
        "blocked_reason": e.blocked_reason,
        "broker_order_id": e.broker_order_id,
        "endpoint_used": e.endpoint_used,
        "error_message": e.error_message,
        "executed_quantity": e.executed_quantity,
        "executed_price": e.executed_price,
        "created_at": e.created_at.isoformat() if e.created_at else None,
        "sent_at": e.sent_at.isoformat() if e.sent_at else None,
        "completed_at": e.completed_at.isoformat() if e.completed_at else None,
        "request_audit": audits.get("request_audit"),
        "broker_result": audits.get("broker_result"),
        "reconciliation_audit": audits.get("reconciliation_audit"),
        "broker_status_checks": audits.get("broker_status_checks", []),
    }


# ---------------------------------------------------------------------------
# Execution Reconciliation V1 — manual resolution of uncertain orders.
#
# INVARIANTS:
# - reconciliation NEVER creates orders, NEVER calls place_order, NEVER
#   re-executes a recommendation, NEVER returns an order to execution_requested
#   nor a recommendation to pending/blocked;
# - only a human with the execution credential + exact phrase can reconcile;
# - every action is appended to an append-only reconciliation_audit list;
# - it works regardless of ORDER_EXECUTION_ENABLED: the safety lock blocks
#   NEW orders, it must not prevent resolving a past uncertain one.
# ---------------------------------------------------------------------------

_RECONCILABLE_STATUSES = {"submitting", "manual_reconciliation_required"}

_RECONCILIATION_ACTIONS = {
    "confirm_not_sent": {
        "target_status": "not_sent_confirmed",
        "phrase": "CONCILIAR EJECUCION {execution_id} COMO NO ENVIADA",
    },
    "confirm_sent": {
        "target_status": "execution_sent",
        "phrase": "CONCILIAR EJECUCION {execution_id} COMO ENVIADA",
    },
    "confirm_rejected": {
        "target_status": "rejected_by_broker",
        "phrase": "CONCILIAR EJECUCION {execution_id} COMO RECHAZADA",
    },
    "confirm_executed": {
        "target_status": "executed",
        "phrase": "CONCILIAR EJECUCION {execution_id} COMO EJECUTADA",
    },
}

_UNCERTAIN_ORDER_STATUSES = {"submitting", "manual_reconciliation_required", "execution_requested"}
_SENT_ORDER_STATUSES = {"execution_sent", "executed"}
_DEFINITIVE_FAIL_STATUSES = {"failed", "rejected_by_broker", "not_sent_confirmed", "validation_failed"}


def reconciliation_phrase(execution_id: int, action: str) -> str:
    return _RECONCILIATION_ACTIONS[action]["phrase"].format(execution_id=execution_id)


def _recompute_recommendation_outcome(db: Session, recommendation_id: int) -> str | None:
    """Recompute the aggregate recommendation status from ALL its orders.

    Never returns the recommendation to pending/blocked (it can never become
    approvable again).
    """
    rec = db.query(Recommendation).filter(Recommendation.id == recommendation_id).first()
    if not rec:
        return None
    orders = db.query(OrderExecution).filter(
        OrderExecution.recommendation_id == recommendation_id
    ).all()
    if not orders:
        return rec.status

    statuses = [o.status for o in orders]
    if any(s in _UNCERTAIN_ORDER_STATUSES for s in statuses):
        new_status = "manual_reconciliation_required"
    elif all(s in _SENT_ORDER_STATUSES for s in statuses):
        new_status = "approved"
    elif any(s in _SENT_ORDER_STATUSES for s in statuses):
        new_status = "execution_partial"
    else:
        new_status = "execution_failed"

    rec.status = new_status
    return new_status


def get_reconciliation_queue(db: Session) -> list[dict]:
    """Orders pending manual inspection: any order in submitting or
    manual_reconciliation_required, plus every order of an execution_partial
    recommendation (full picture for the partial batch)."""
    direct = db.query(OrderExecution).filter(
        OrderExecution.status.in_(sorted(_RECONCILABLE_STATUSES))
    ).all()

    partial_rec_ids = [
        r.id for r in db.query(Recommendation).filter(Recommendation.status == "execution_partial").all()
    ]
    partial_orders = (
        db.query(OrderExecution).filter(OrderExecution.recommendation_id.in_(partial_rec_ids)).all()
        if partial_rec_ids else []
    )

    rec_status_cache: dict[int, str] = {}

    def _rec_status(rec_id: int) -> str:
        if rec_id not in rec_status_cache:
            rec = db.query(Recommendation).filter(Recommendation.id == rec_id).first()
            rec_status_cache[rec_id] = rec.status if rec else "unknown"
        return rec_status_cache[rec_id]

    seen: dict[int, OrderExecution] = {}
    for e in list(direct) + list(partial_orders):
        seen[e.id] = e

    items = []
    for e in sorted(seen.values(), key=lambda x: x.id, reverse=True):
        audits = _extract_broker_response_audits(e.broker_response)
        items.append({
            "id": e.id,
            "recommendation_id": e.recommendation_id,
            "recommendation_status": _rec_status(e.recommendation_id),
            "symbol": e.symbol,
            "side": e.side,
            "status": e.status,
            "quantity_planned": e.quantity_planned,
            "quantity_sent": e.quantity_sent,
            "broker_order_id": e.broker_order_id or "",
            "endpoint_used": e.endpoint_used or "",
            "error_message": e.error_message or "",
            "created_at": e.created_at.isoformat() if e.created_at else None,
            "sent_at": e.sent_at.isoformat() if e.sent_at else None,
            "request_audit": audits.get("request_audit"),
            "reconciliation_audit": audits.get("reconciliation_audit"),
        })
    return items


def reconcile_execution(
    db: Session,
    execution_id: int,
    *,
    execution_key: str | None,
    action: str,
    confirmation_text: str | None,
    note: str = "",
    broker_order_id: str | None = None,
    executed_quantity: float | None = None,
    executed_price: float | None = None,
) -> dict:
    """Manually resolve an uncertain order. NEVER sends, retries or creates
    orders — it only records a human decision with full audit trail.

    Credentials are validated first (no information leak about rows), the
    execution key is checked constant-time and never persisted or logged.
    """
    settings = get_settings()

    # Credential gates first
    if not settings.execution_admin_key:
        return {
            "error": "Conciliación bloqueada: credencial de ejecución no configurada en el servidor.",
            "code": "execution_admin_key_not_configured",
            "status_code": 423,
        }
    if not execution_key or not _secrets.compare_digest(str(execution_key), settings.execution_admin_key):
        return {
            "error": "Credencial de ejecución inválida o ausente.",
            "code": "invalid_execution_key",
            "status_code": 403,
        }

    if action not in _RECONCILIATION_ACTIONS:
        return {
            "error": f"Acción de conciliación inválida: '{action}'.",
            "code": "invalid_reconciliation_action",
            "status_code": 422,
        }

    order_exec = db.query(OrderExecution).filter(OrderExecution.id == execution_id).first()
    if not order_exec:
        return {"error": "Execution not found", "status_code": 404}

    expected_phrase = reconciliation_phrase(execution_id, action)
    if not confirmation_text or confirmation_text.strip() != expected_phrase:
        return {
            "error": f"Confirmación incorrecta. Frase requerida exacta: '{expected_phrase}'.",
            "code": "confirmation_mismatch",
            "status_code": 422,
        }

    previous_status = order_exec.status
    # Eligibility: only uncertain states — with ONE tested exception:
    # execution_sent may be corrected to executed with explicit evidence.
    if action == "confirm_executed":
        allowed_from = _RECONCILABLE_STATUSES | {"execution_sent"}
    else:
        allowed_from = _RECONCILABLE_STATUSES
    if previous_status not in allowed_from:
        return {
            "error": f"El estado '{previous_status}' no es conciliable con la acción '{action}'.",
            "code": "not_reconcilable",
            "status_code": 409,
        }

    # Per-action evidence requirements
    clean_broker_id = (broker_order_id or "").strip()
    clean_note = (note or "").strip()
    if action == "confirm_sent" and not clean_broker_id:
        return {
            "error": "confirm_sent requiere broker_order_id no vacío.",
            "code": "broker_order_id_required",
            "status_code": 422,
        }
    if action == "confirm_rejected" and not clean_note:
        return {
            "error": "confirm_rejected requiere una nota no vacía con el motivo.",
            "code": "note_required",
            "status_code": 422,
        }
    if action == "confirm_executed":
        if not clean_broker_id and not (order_exec.broker_order_id or "").strip():
            return {
                "error": "confirm_executed requiere broker_order_id.",
                "code": "broker_order_id_required",
                "status_code": 422,
            }
        if not executed_quantity or executed_quantity <= 0:
            return {
                "error": "confirm_executed requiere executed_quantity > 0.",
                "code": "executed_quantity_required",
                "status_code": 422,
            }
        if not executed_price or executed_price <= 0:
            return {
                "error": "confirm_executed requiere executed_price > 0.",
                "code": "executed_price_required",
                "status_code": 422,
            }

    new_status = _RECONCILIATION_ACTIONS[action]["target_status"]

    # Atomic conditional transition — a concurrent/repeated reconciliation
    # loses the update and gets a 409; the same change is never applied twice.
    claimed = (
        db.query(OrderExecution)
        .filter(OrderExecution.id == execution_id, OrderExecution.status == previous_status)
        .update({"status": new_status}, synchronize_session=False)
    )
    if claimed != 1:
        db.rollback()
        return {
            "error": "La ejecución fue conciliada por otra solicitud. Revisá el estado actual.",
            "code": "reconciliation_conflict",
            "status_code": 409,
        }
    db.refresh(order_exec)

    now = _utcnow()

    # Append-only audit entry — never includes credentials of any kind.
    audit_entry = {
        "timestamp": now.isoformat(),
        "action": action,
        "previous_status": previous_status,
        "new_status": new_status,
        "note": clean_note,
        "broker_order_id": clean_broker_id or None,
        "executed_quantity": executed_quantity,
        "executed_price": executed_price,
        "source": "manual_user",
    }
    br = dict(order_exec.broker_response or {})
    audit_list = list(br.get("reconciliation_audit") or [])
    audit_list.append(audit_entry)
    br["reconciliation_audit"] = audit_list
    order_exec.broker_response = br

    if action == "confirm_not_sent":
        # Confirmed externally that IOL never received it. quantity_sent goes
        # back to None: nothing was actually sent. NEVER re-sent, never back
        # to execution_requested.
        order_exec.quantity_sent = None
        order_exec.completed_at = now
    elif action == "confirm_sent":
        order_exec.broker_order_id = clean_broker_id
        if not order_exec.sent_at:
            order_exec.sent_at = now
    elif action == "confirm_rejected":
        order_exec.error_message = clean_note
        order_exec.completed_at = now
    elif action == "confirm_executed":
        if clean_broker_id:
            order_exec.broker_order_id = clean_broker_id
        order_exec.executed_quantity = executed_quantity
        order_exec.executed_price = executed_price
        order_exec.completed_at = now

    db.flush()
    recommendation_status = _recompute_recommendation_outcome(db, order_exec.recommendation_id)

    app_log(db, "Conciliación manual de ejecución aplicada", context={
        "execution_id": execution_id,
        "recommendation_id": order_exec.recommendation_id,
        "action": action,
        "previous_status": previous_status,
        "new_status": new_status,
        "source": "manual_user",
    })
    db.commit()

    return {
        "execution_id": execution_id,
        "recommendation_id": order_exec.recommendation_id,
        "action": action,
        "previous_status": previous_status,
        "new_status": new_status,
        "recommendation_status": recommendation_status,
        "reconciliation_audit": audit_list,
    }


def refresh_broker_status(db: Session, execution_id: int, *, execution_key: str | None) -> dict:
    """READ-ONLY broker status check by broker_order_id.

    Uses the existing get_order_status (GET /api/v2/operaciones/{id} on IOL).
    NEVER calls place_order, never creates or retries orders, and NEVER
    changes the execution status automatically: IOL state labels are not a
    contract we control, so mapping to 'executed' stays a manual decision
    (conservative choice per spec). The raw result is appended to
    broker_response.broker_status_checks for the human to inspect.
    """
    settings = get_settings()

    if not settings.execution_admin_key:
        return {
            "error": "Consulta bloqueada: credencial de ejecución no configurada en el servidor.",
            "code": "execution_admin_key_not_configured",
            "status_code": 423,
        }
    if not execution_key or not _secrets.compare_digest(str(execution_key), settings.execution_admin_key):
        return {
            "error": "Credencial de ejecución inválida o ausente.",
            "code": "invalid_execution_key",
            "status_code": 403,
        }

    order_exec = db.query(OrderExecution).filter(OrderExecution.id == execution_id).first()
    if not order_exec:
        return {"error": "Execution not found", "status_code": 404}

    if not (order_exec.broker_order_id or "").strip():
        return {
            "error": "La ejecución no tiene broker_order_id: no hay nada que consultar.",
            "code": "broker_order_id_missing",
            "status_code": 422,
        }

    broker = _get_execution_broker()
    try:
        result = broker.get_order_status(order_exec.broker_order_id)
    except Exception as exc:
        return {
            "error": f"Consulta de estado falló: {str(exc)[:300]}",
            "code": "broker_status_query_failed",
            "status_code": 502,
        }

    now = _utcnow()
    br = dict(order_exec.broker_response or {})
    checks = list(br.get("broker_status_checks") or [])
    checks.append({"timestamp": now.isoformat(), "raw": result})
    br["broker_status_checks"] = checks
    order_exec.broker_response = br

    app_log(db, "Consulta read-only de estado de orden al broker", context={
        "execution_id": execution_id,
        "broker_order_id": order_exec.broker_order_id,
        "broker_status": result.get("status"),
    })
    db.commit()

    return {
        "execution_id": execution_id,
        "broker_order_id": order_exec.broker_order_id,
        "broker_status": result.get("status"),
        "status_unchanged": True,
        "current_status": order_exec.status,
        "raw_stored": True,
        "message": (
            "Consulta read-only registrada. El estado de la ejecución NO se cambió "
            "automáticamente: la conciliación sigue siendo manual."
        ),
    }
