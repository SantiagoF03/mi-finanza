"""Market price-tick rules — pure, deterministic, fail closed.

THE single home for the fact that a price tick can depend on the price.

For equities on bCBA (ACCIONES and CEDEARS), IOL's "alteración mínima" is not a
per-instrument constant: it is a function of the order's own price. Treating it
as a fixed number is safe only while the price stays inside one band. COME at
41.10 has a tick of 0.05; the same instrument at 50.01 has a tick of 0.10, and
a limit price built with the old tick is no longer a valid order.

So the tick is never stored as an answer. What gets stored is *which rule
applies*; the answer is recomputed from whatever price is actually being used,
every time — at preview, and again immediately before sending.

This module knows nothing about the network, the database, settings or the
clock. It is a table and a comparison, so it can be tested exhaustively and
called from anywhere without ordering concerns.

Deliberately NOT duplicated in execution.py, pilot_readiness.py or
order_request.py: three copies of a band table is three chances for them to
disagree, and the one that disagrees is the one that prices a real order.
"""

from __future__ import annotations

from decimal import Decimal

from app.broker.numeric import to_finite_decimal

# --- Rule identifiers -------------------------------------------------------
# Versioned on purpose. If BYMA changes the bands, the new table gets a new
# identifier and every instrument verified under the old one must be verified
# again — a silently updated table would re-price instruments nobody re-checked.
RULE_BYMA_EQUITY_V1 = "BYMA_EQUITY_PRICE_BANDS_V1"

KNOWN_PRICE_TICK_RULES = frozenset({RULE_BYMA_EQUITY_V1})

# --- The band table ---------------------------------------------------------
# (upper_bound_inclusive, tick). `None` as the bound means "no upper limit".
#
#   price <= 1.00        → 0.001
#   1.00 < price <= 10   → 0.01
#   10 < price <= 50     → 0.05
#   50 < price <= 100    → 0.10
#   100 < price <= 250   → 0.25
#   250 < price          → 0.50
#
# Bounds are INCLUSIVE upper edges: exactly 50.00 is still the 0.05 band, and
# 50.01 is the first price in the 0.10 band. Every boundary in this table has
# its own test, because an off-by-one-cent here misprices real orders.
BYMA_EQUITY_BANDS_V1: tuple[tuple[Decimal | None, Decimal], ...] = (
    (Decimal("1.00"), Decimal("0.001")),
    (Decimal("10.00"), Decimal("0.01")),
    (Decimal("50.00"), Decimal("0.05")),
    (Decimal("100.00"), Decimal("0.10")),
    (Decimal("250.00"), Decimal("0.25")),
    (None, Decimal("0.50")),
)

# Scope of the audited rule. Narrow on purpose: the band table was checked for
# argentine equities in pesos on bCBA, and nothing else. A bond, a letra or an
# instrument on another venue may well use different bands, and guessing which
# is exactly the failure this module exists to prevent.
BYMA_EQUITY_RULE_SCOPE = {
    "execution_family": "securities",
    "execution_classes": frozenset({"ACCIONES", "CEDEARS"}),
    "markets": frozenset({"bCBA"}),
    "currencies": frozenset({"ARS"}),
}


class PriceTickRuleError(ValueError):
    """A price the rule refuses to answer for."""


def resolve_byma_equity_price_tick(price) -> Decimal:
    """The minimum price alteration for ONE price, per BYMA_EQUITY_PRICE_BANDS_V1.

    Accepts anything `Decimal(str(value))` can represent exactly; rejects
    None, booleans, NaN, Infinity, zero and negatives by raising
    `PriceTickRuleError`. It never rounds an invalid price into a valid one —
    returning a tick for a price we could not read would be the system
    inventing the number that decides whether an order is well-formed.

    Comparisons are Decimal throughout. With floats, `50.01 <= 50.00` can hinge
    on binary representation, and the two sides of that comparison are
    different ticks.
    """
    dec = to_finite_decimal(price)
    if dec is None:
        raise PriceTickRuleError(f"unusable price for tick resolution: {price!r}")
    if dec <= 0:
        raise PriceTickRuleError(f"price must be strictly positive: {price!r}")

    for upper, tick in BYMA_EQUITY_BANDS_V1:
        if upper is None or dec <= upper:
            return tick
    # Unreachable: the last band has no upper bound. Kept so a future edit that
    # removes the open-ended band fails loudly instead of returning None.
    raise PriceTickRuleError(f"no band matched price {dec}")


def resolve_byma_equity_price_band(price) -> dict:
    """The band a price falls in, as an auditable description.

    Returned alongside the tick so a signed preview records not just *what*
    tick was used but *why* — and so the preflight can compare bands rather
    than re-deriving the reasoning.
    """
    tick = resolve_byma_equity_price_tick(price)
    dec = to_finite_decimal(price)

    lower: Decimal | None = None
    for upper, band_tick in BYMA_EQUITY_BANDS_V1:
        if upper is None or dec <= upper:
            return {
                "rule": RULE_BYMA_EQUITY_V1,
                # A stable, human-readable label. Comparing labels is how the
                # preflight detects a band change without floating-point games.
                "band": _band_label(lower, upper),
                "lower_exclusive": str(lower) if lower is not None else None,
                "upper_inclusive": str(upper) if upper is not None else None,
                "tick": tick,
            }
        lower = upper
    raise PriceTickRuleError(f"no band matched price {price!r}")


def _band_label(lower: Decimal | None, upper: Decimal | None) -> str:
    if lower is None:
        return f"<={upper}"
    if upper is None:
        return f">{lower}"
    return f">{lower}<={upper}"


# --- Applicability ----------------------------------------------------------


def price_tick_rule_applies(
    rule: str | None,
    *,
    execution_family: str | None,
    execution_class: str | None,
    market: str | None,
    currency: str | None,
) -> bool:
    """May THIS rule price THIS instrument?

    Everything must match: family, class, market and currency. A rule verified
    for argentine equities in pesos says nothing about a bond, an ON, a letra,
    an unclassified ETF, an FCI, another venue or another currency — for all of
    those the caller keeps its existing fixed-tick behaviour, and blocks if
    there is no verified fixed tick.

    Deliberately not a fallback: an unknown rule identifier returns False
    rather than defaulting to the only rule we have.
    """
    if rule != RULE_BYMA_EQUITY_V1:
        return False
    scope = BYMA_EQUITY_RULE_SCOPE
    return (
        str(execution_family or "") == scope["execution_family"]
        and str(execution_class or "").upper() in scope["execution_classes"]
        # Market is compared exactly: canonicalisation happens once, at the
        # resolver's boundary, so by the time a value reaches here it is
        # already the authorised spelling.
        and str(market or "") in scope["markets"]
        and str(currency or "").upper() in scope["currencies"]
    )


def resolve_price_tick_for_rule(rule: str | None, price) -> Decimal:
    """Dispatch to the named rule. Unknown rule ⇒ refuse, never guess."""
    if rule == RULE_BYMA_EQUITY_V1:
        return resolve_byma_equity_price_tick(price)
    raise PriceTickRuleError(f"unknown price tick rule: {rule!r}")


def resolve_price_band_for_rule(rule: str | None, price) -> dict:
    if rule == RULE_BYMA_EQUITY_V1:
        return resolve_byma_equity_price_band(price)
    raise PriceTickRuleError(f"unknown price tick rule: {rule!r}")


def describe_rule(rule: str | None) -> dict | None:
    """Non-sensitive description of a known rule, for readiness reports."""
    if rule != RULE_BYMA_EQUITY_V1:
        return None
    return {
        "rule": RULE_BYMA_EQUITY_V1,
        "execution_family": BYMA_EQUITY_RULE_SCOPE["execution_family"],
        "execution_classes": sorted(BYMA_EQUITY_RULE_SCOPE["execution_classes"]),
        "markets": sorted(BYMA_EQUITY_RULE_SCOPE["markets"]),
        "currencies": sorted(BYMA_EQUITY_RULE_SCOPE["currencies"]),
        "bands": [
            {
                "upper_inclusive": str(upper) if upper is not None else None,
                "tick": str(tick),
            }
            for upper, tick in BYMA_EQUITY_BANDS_V1
        ],
    }
