"""Canonical IOL order request builder — pure, deterministic, fail closed.

The official personal-account endpoints
    POST /api/v2/operar/Comprar
    POST /api/v2/operar/Vender
document Content-Type application/x-www-form-urlencoded with fields
mercado / simbolo / cantidad / precio / plazo / validez / tipoOrden.

This module builds EXACTLY that form body. It never sends anything. Only
limit orders (precioLimite) are supported: a missing price can never be
silently converted into a market order.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

try:
    from zoneinfo import ZoneInfo
    _ART = ZoneInfo("America/Argentina/Buenos_Aires")
except Exception:  # pragma: no cover — tzdata missing in minimal images
    # Argentina has no DST currently; a fixed -3 offset is a safe fallback.
    _ART = timezone(timedelta(hours=-3), name="America/Argentina/Buenos_Aires")

IOL_FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"

IOL_SIDE_ENDPOINTS = {
    "sell": "/api/v2/operar/Vender",
    "buy": "/api/v2/operar/Comprar",
}

# Values known/used by this project. Nothing outside these sets is accepted —
# we do not invent markets or settlement terms IOL has not documented for us.
KNOWN_IOL_MARKETS = {"bCBA"}
KNOWN_IOL_SETTLEMENTS = {"t0", "t1", "t2"}
SUPPORTED_ORDER_TYPES = {"precioLimite"}  # precioMercado is banned in V1


def now_art() -> datetime:
    return datetime.now(_ART)


def compute_order_validity(validity_minutes: int, *, now: datetime | None = None) -> tuple[str | None, str | None]:
    """Deterministic order validity in America/Argentina/Buenos_Aires.

    Returns (validity_iso, error_code). The validity must be in the future
    and must not cross into the next operating day; otherwise fail closed
    with order_validity_expired. Serialized as naive local ISO seconds
    (IOL expects local Buenos Aires time).
    """
    if not validity_minutes or validity_minutes <= 0:
        return None, "order_validity_expired"
    current = now or now_art()
    if current.tzinfo is None:
        current = current.replace(tzinfo=_ART)
    validity = current + timedelta(minutes=validity_minutes)
    if validity <= current:
        return None, "order_validity_expired"
    if validity.date() != current.date():
        # Would leak into the next operating day → fail closed.
        return None, "order_validity_expired"
    return validity.replace(tzinfo=None).isoformat(timespec="seconds"), None


def _serialize_quantity(quantity) -> tuple[str | None, str | None]:
    """Exact integer quantity, no silent truncation."""
    try:
        qty_dec = Decimal(str(quantity))
    except (InvalidOperation, ValueError, TypeError):
        return None, "invalid_broker_quantity"
    if not qty_dec.is_finite() or qty_dec <= 0:
        return None, "invalid_broker_quantity"
    if qty_dec != qty_dec.to_integral_value():
        # 14.5 títulos no existe: NEVER truncate silently.
        return None, "invalid_broker_quantity"
    return str(int(qty_dec)), None


def _serialize_price(price) -> tuple[str | None, str | None]:
    """Stable Decimal serialization — no float repr instability, no
    arbitrary rounding."""
    try:
        price_dec = Decimal(str(price))
    except (InvalidOperation, ValueError, TypeError):
        return None, "invalid_broker_price"
    if not price_dec.is_finite() or price_dec <= 0:
        return None, "invalid_broker_price"
    normalized = price_dec.normalize()
    # normalize() may produce exponent notation for round numbers (2E+1) —
    # always serialize positionally.
    return format(normalized, "f"), None


def build_iol_order_request(
    *,
    side: str,
    symbol: str,
    quantity,
    price,
    market: str,
    settlement: str,
    order_type: str,
    validity: str,
) -> tuple[dict | None, str | None]:
    """Build the exact form-urlencoded request for an IOL personal order.

    Returns (request, error_code). request:
    {
        "endpoint": "/api/v2/operar/Vender",
        "content_type": "application/x-www-form-urlencoded",
        "form_data": {mercado, simbolo, cantidad, precio, plazo, validez, tipoOrden},
    }
    All values are strings, exactly as they will be sent. Fail closed on any
    unsupported or invalid field.
    """
    endpoint = IOL_SIDE_ENDPOINTS.get(side)
    if not endpoint:
        return None, "invalid_broker_side"
    if not symbol or not str(symbol).strip():
        return None, "invalid_broker_symbol"
    if market not in KNOWN_IOL_MARKETS:
        return None, "unsupported_iol_market"
    if settlement not in KNOWN_IOL_SETTLEMENTS:
        return None, "unsupported_iol_settlement"
    if order_type not in SUPPORTED_ORDER_TYPES:
        return None, "unsupported_iol_order_type"
    if not validity:
        return None, "order_validity_expired"

    qty_str, err = _serialize_quantity(quantity)
    if err:
        return None, err
    price_str, err = _serialize_price(price)
    if err:
        return None, err

    return {
        "endpoint": endpoint,
        "content_type": IOL_FORM_CONTENT_TYPE,
        "form_data": {
            "mercado": market,
            "simbolo": str(symbol).strip(),
            "cantidad": qty_str,
            "precio": price_str,
            "plazo": settlement,
            "validez": validity,
            "tipoOrden": order_type,
        },
    }, None
