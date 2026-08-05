"""On-demand, READ-ONLY resolution of instruments we do not hold.

A buy must be preparable for something that is not in the portfolio yet, but
"the user typed it" and "an LLM suggested it" are not evidence that a symbol
exists, is tradeable, or is denominated in the currency we assume.

This module resolves a symbol using only official IOL read endpoints and
records WHERE each field came from, so the catalog can distinguish a verified
instrument from a plausible-looking guess. It can never place, cancel or
approve anything.

Endpoints used (read-only):
    GET /api/v2/{mercado}/Titulos/{simbolo}              → title detail
    GET /api/v2/{mercado}/Titulos/{simbolo}/Cotizacion   → quote (verified)
    GET /api/v2/Titulos/FCI/{simbolo}                    → fund detail

A symbol is admissible for resolution only if it arrives from a bounded,
non-free-text source: the current recommendation, the watchlist, the enabled
universe or an existing position.
"""

from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from app.broker.execution_class import (
    FAMILY_FUND,
    FAMILY_SECURITIES,
    execution_family_of,
    resolve_execution_class,
)
from app.broker.instrument_catalog import (
    PROV_ADMIN_OVERRIDE,
    PROV_CLASS_DEFAULT,
    PROV_IOL_FCI,
    PROV_IOL_QUOTE,
    PROV_IOL_TITLE_DETAIL,
    normalize_symbol,
    upsert_instrument,
)

logger = logging.getLogger(__name__)

# Where a symbol may legitimately come from. Free user/LLM text is NOT here:
# a symbol must already be inside a bounded set the system controls.
SOURCE_RECOMMENDATION = "recommendation"
SOURCE_WATCHLIST = "watchlist"
SOURCE_UNIVERSE = "universe"
SOURCE_POSITION = "position"

ADMISSIBLE_SOURCES = (
    SOURCE_RECOMMENDATION, SOURCE_WATCHLIST, SOURCE_UNIVERSE, SOURCE_POSITION,
)


def admissible_symbols(db: Session, settings) -> dict[str, str]:
    """Every symbol we are allowed to resolve, mapped to its source.

    This is the gate that stops "a string appeared somewhere" from becoming
    "a tradeable instrument". A symbol absent from all of these sets is never
    resolved, no matter who asked.
    """
    from app.models.models import (
        PortfolioSnapshot,
        Recommendation,
        RecommendationAction,
    )
    from sqlalchemy import desc
    from sqlalchemy.orm import joinedload

    result: dict[str, str] = {}

    for symbol in getattr(settings, "market_universe_assets", None) or []:
        key = normalize_symbol(symbol)
        if key:
            result.setdefault(key, SOURCE_UNIVERSE)
    for symbol in getattr(settings, "whitelist_assets", None) or []:
        key = normalize_symbol(symbol)
        if key:
            result.setdefault(key, SOURCE_UNIVERSE)
    for symbol in getattr(settings, "watchlist_assets", None) or []:
        key = normalize_symbol(symbol)
        if key:
            result.setdefault(key, SOURCE_WATCHLIST)

    snapshot = (
        db.query(PortfolioSnapshot)
        .options(joinedload(PortfolioSnapshot.positions))
        .order_by(desc(PortfolioSnapshot.id))
        .first()
    )
    if snapshot:
        for position in snapshot.positions:
            key = normalize_symbol(position.symbol)
            if key:
                result[key] = SOURCE_POSITION

    open_recs = (
        db.query(Recommendation)
        .filter(Recommendation.status.in_(["pending", "blocked"]))
        .all()
    )
    if open_recs:
        actions = (
            db.query(RecommendationAction)
            .filter(RecommendationAction.recommendation_id.in_([r.id for r in open_recs]))
            .all()
        )
        for action in actions:
            key = normalize_symbol(action.symbol)
            if key:
                result.setdefault(key, SOURCE_RECOMMENDATION)

    return result


def _clean_str(value) -> str:
    return str(value or "").strip()


def fetch_title_detail(broker, market: str, symbol: str) -> tuple[dict | None, str | None]:
    """GET /api/v2/{mercado}/Titulos/{simbolo} — official read-only detail.

    Returns (payload, error_code). Never raises: an unreachable or malformed
    detail is a resolution failure, not an exception in the middle of a
    preview.
    """
    try:
        resp = broker._authorized_get(f"/api/v2/{market}/Titulos/{symbol}")
        data = resp.json()
    except Exception as exc:
        logger.info("title detail unavailable for %s: %s", symbol, str(exc)[:120])
        return None, "title_detail_unavailable"
    if not isinstance(data, dict):
        return None, "title_detail_unavailable"
    return data, None


def resolve_instrument(
    db: Session,
    *,
    symbol: str,
    broker,
    settings,
    source: str,
    class_policies: dict,
    tick_overrides: dict | None = None,
) -> dict:
    """Resolve ONE symbol read-only and write a CANDIDATE catalog entry.

    Deliberately produces a `candidate`, not a `verified` instrument, unless
    every operative field ends up with a verifying provenance. In particular
    a tick or step that only a class default could supply keeps the entry a
    candidate — visible, inspectable, and not executable.

    Returns a report dict. Never submits, cancels or approves anything.
    """
    key = normalize_symbol(symbol)
    report: dict = {"symbol": key, "source": source, "resolved": False}
    if not key:
        return {**report, "error": "invalid_symbol"}
    if source not in ADMISSIBLE_SOURCES:
        return {**report, "error": "symbol_source_not_admissible"}

    overrides = (tick_overrides or {}).get(key, {})

    # --- Title detail: the authority on type, currency and market ---
    market_hint = ""
    for policy in class_policies.values():
        markets = policy.get("markets") or []
        if markets:
            market_hint = markets[0]
            break
    if not market_hint:
        return {**report, "error": "class_policy_not_configured"}

    detail, detail_error = fetch_title_detail(broker, market_hint, key)
    if detail_error:
        return {**report, "error": detail_error}

    from app.broker.clients import _map_currency, _normalize_asset_type

    asset_type = _normalize_asset_type(_clean_str(detail.get("tipo")))
    currency = _map_currency(detail.get("moneda"))
    detail_market = _clean_str(detail.get("mercado")) or market_hint
    description = _clean_str(detail.get("descripcion"))

    execution_class = resolve_execution_class(asset_type)
    if not execution_class:
        return {**report, "error": "instrument_class_unsupported",
                "asset_type": asset_type}
    policy = class_policies.get(execution_class)
    if not policy:
        return {**report, "error": "class_policy_not_configured",
                "execution_class": execution_class}

    if detail_market not in (policy.get("markets") or []):
        # The symbol is not on the market this class trades on. Refusing here
        # is the point: a same-named instrument on another venue is a
        # different instrument.
        return {**report, "error": "instrument_market_mismatch",
                "market": detail_market}
    if currency not in (policy.get("currencies") or []):
        return {**report, "error": "instrument_currency_mismatch",
                "currency": currency}

    family = execution_family_of(execution_class)
    settlement = (policy.get("settlements") or [""])[0]

    provenance = {
        "broker_symbol": PROV_IOL_TITLE_DETAIL,
        "asset_type": PROV_IOL_TITLE_DETAIL,
        "instrument_type": PROV_IOL_TITLE_DETAIL,
        "currency": PROV_IOL_TITLE_DETAIL,
        "market": PROV_IOL_TITLE_DETAIL,
        # Settlement is a POLICY choice (which plazo we trade), not something
        # the title detail states about the instrument.
        "settlement": PROV_CLASS_DEFAULT,
    }

    # --- Quotes: BOTH sides, independently ---
    # Selling needs a bid, buying needs an ask, and one says nothing about
    # the other: an instrument can have a bid and no ask (nobody is offering)
    # or an ask and no bid (nobody is buying). Deriving both capabilities from
    # a single sell quote claimed a buy was possible on the strength of a bid.
    sell_quote_ok = buy_quote_ok = False
    if family == FAMILY_SECURITIES:
        sell_quote_ok = _side_quotable(broker, key, "sell", detail_market, settlement)
        buy_quote_ok = _side_quotable(broker, key, "buy", detail_market, settlement)

    quote_supported = sell_quote_ok or buy_quote_ok
    provenance["quote_supported"] = PROV_IOL_QUOTE if quote_supported else PROV_CLASS_DEFAULT

    # --- Tick / step: official metadata, else admin override, else a guess ---
    tick = _first_present(detail, ("alteracionMinima", "minimaAlteracion", "tickSize"))
    step = _first_present(detail, ("lote", "loteMinimo", "cantidadMinima"))
    minimum = _first_present(detail, ("minimo", "cantidadMinima"))

    if "price_tick" in overrides:
        tick, provenance["price_tick"] = overrides["price_tick"], PROV_ADMIN_OVERRIDE
    elif tick is not None:
        provenance["price_tick"] = PROV_IOL_TITLE_DETAIL
    else:
        tick, provenance["price_tick"] = policy.get("default_price_tick"), PROV_CLASS_DEFAULT

    if "quantity_step" in overrides:
        step, provenance["quantity_step"] = overrides["quantity_step"], PROV_ADMIN_OVERRIDE
    elif step is not None:
        provenance["quantity_step"] = PROV_IOL_TITLE_DETAIL
    else:
        step, provenance["quantity_step"] = (
            policy.get("default_quantity_step"), PROV_CLASS_DEFAULT
        )

    if "minimum_quantity" in overrides:
        minimum = overrides["minimum_quantity"]
        provenance["minimum_quantity"] = PROV_ADMIN_OVERRIDE
    elif minimum is not None:
        provenance["minimum_quantity"] = PROV_IOL_TITLE_DETAIL
    else:
        minimum, provenance["minimum_quantity"] = step, PROV_CLASS_DEFAULT

    # --- Capabilities: strictly per side, from that side's own evidence ---
    buy_supported = buy_quote_ok
    sell_supported = sell_quote_ok
    provenance["buy_supported"] = PROV_IOL_QUOTE if buy_supported else PROV_CLASS_DEFAULT
    provenance["sell_supported"] = PROV_IOL_QUOTE if sell_supported else PROV_CLASS_DEFAULT

    entry, identity_changed = upsert_instrument(
        db,
        broker_symbol=key,
        asset_type=asset_type,
        instrument_type=asset_type,
        currency=currency,
        market=detail_market,
        settlement=settlement,
        source=f"resolver:{source}",
        description=description,
        quantity_step=step,
        price_tick=tick,
        minimum_quantity=minimum,
        buy_supported=buy_supported,
        sell_supported=sell_supported,
        quote_supported=quote_supported,
        cancellation_supported=False,
        max_age_seconds=policy.get("catalog_max_age_seconds"),
        provenance=provenance,
    )

    from app.broker.instrument_catalog import unverified_fields, verification_status_of

    return {
        **report,
        "resolved": True,
        "identity_changed": identity_changed,
        "execution_class": execution_class,
        "execution_family": family,
        "currency": currency,
        "market": detail_market,
        "verification_status": verification_status_of(entry),
        "unverified_fields": unverified_fields(entry),
        "field_provenance": dict(entry.field_provenance or {}),
    }


def _side_quotable(broker, symbol: str, side: str, market: str, settlement: str) -> bool:
    """Is THIS side of the book actually available?

    `sell` requires a bid (someone is buying), `buy` requires an ask (someone
    is selling). The quote's own `source` is checked: a provider that answered
    with the wrong side is not evidence for this one.
    """
    expected = "bid" if side == "sell" else "ask"
    try:
        quote = broker.get_execution_quote(symbol, side, market, settlement)
    except Exception:
        return False
    if not quote or not quote.get("available"):
        return False
    if quote.get("source") != expected:
        return False
    price = quote.get("price")
    if price is None or isinstance(price, bool):
        return False
    try:
        return float(price) > 0
    except (TypeError, ValueError):
        return False


def _first_present(payload: dict, keys: tuple[str, ...]):
    """First key present with a usable positive number.

    IOL's title detail does not always carry the minimum alteration under a
    single documented name. We read the names we know and, when none is
    present, fall back to a class default that is explicitly marked as
    UNVERIFIED rather than inventing a field name.
    """
    for name in keys:
        if name not in payload:
            continue
        value = payload.get(name)
        if value is None or isinstance(value, bool):
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if number > 0:
            return number
    return None


def resolve_missing_instruments(
    db: Session,
    *,
    symbols: list[str],
    broker,
    settings,
) -> dict:
    """Resolve several symbols read-only. Used by the admin refresh endpoint."""
    from app.broker.execution_class import load_class_policies
    from app.broker.instrument_catalog import _symbol_tick_overrides

    class_policies, policy_errors = load_class_policies(settings)
    if not class_policies:
        return {"resolved": [], "errors": ["class_policy_not_configured"]}

    allowed = admissible_symbols(db, settings)
    overrides = _symbol_tick_overrides(settings)

    reports = []
    for symbol in symbols:
        key = normalize_symbol(symbol)
        source = allowed.get(key)
        if not source:
            reports.append({
                "symbol": key,
                "resolved": False,
                "error": "symbol_source_not_admissible",
            })
            continue
        reports.append(resolve_instrument(
            db, symbol=key, broker=broker, settings=settings, source=source,
            class_policies=class_policies, tick_overrides=overrides,
        ))
    db.commit()
    return {
        "resolved": reports,
        "errors": policy_errors,
        "admissible_symbol_count": len(allowed),
    }
