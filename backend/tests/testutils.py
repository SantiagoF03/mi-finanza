"""Shared helpers for tests written before semi-automatic mode V1.

In V1 at most ONE open recommendation may exist: a cycle refuses to create a
new one while a human decision is still outstanding, instead of superseding
it. Tests that legitimately need a SECOND cycle must therefore make the
human decision explicit — which is exactly the production flow.

`decide_open_recommendations` records that decision (rejected = terminal, no
execution) so the next cycle is allowed to run. It never approves anything
and never touches the execution layer.
"""

from __future__ import annotations

from app.models.models import Recommendation
from app.services.analysis_gate import TERMINAL_RECOMMENDATION_STATUSES


def decide_open_recommendations(db) -> list[int]:
    """Mark every open recommendation as rejected (a terminal human decision).

    Returns the ids that were decided. Nothing is executed or approved.
    """
    decided = []
    open_recs = (
        db.query(Recommendation)
        .filter(Recommendation.status.notin_(sorted(TERMINAL_RECOMMENDATION_STATUSES)))
        .all()
    )
    for rec in open_recs:
        rec.status = "rejected"
        decided.append(rec.id)
    if decided:
        db.commit()
    return decided


# ---------------------------------------------------------------------------
# Execution catalog helpers
# ---------------------------------------------------------------------------


def verified_provenance(family: str = "securities") -> dict:
    """Field provenance that makes a catalog entry `verified`.

    Mirrors what a real resolution + administrative verification produces:
    identity from IOL's title detail, quoting proven by an actual quote, and
    tick/step explicitly signed off by a human. Tests whose subject is NOT
    verification use this so they exercise their real assertion instead of
    tripping over an unverified instrument.

    Use plain `upsert_instrument(...)` without provenance to get a
    `candidate` — that is the fail-closed default and has its own tests.
    """
    from app.broker.instrument_catalog import (
        PROV_ADMIN_OVERRIDE,
        PROV_IOL_QUOTE,
        PROV_IOL_TITLE_DETAIL,
    )

    identity = {
        "broker_symbol": PROV_IOL_TITLE_DETAIL,
        "asset_type": PROV_IOL_TITLE_DETAIL,
        "instrument_type": PROV_IOL_TITLE_DETAIL,
        "currency": PROV_IOL_TITLE_DETAIL,
    }
    if family == "fund":
        return identity
    return {
        **identity,
        "market": PROV_IOL_TITLE_DETAIL,
        "settlement": PROV_ADMIN_OVERRIDE,
        "price_tick": PROV_ADMIN_OVERRIDE,
        "quantity_step": PROV_ADMIN_OVERRIDE,
        "minimum_quantity": PROV_ADMIN_OVERRIDE,
        "buy_supported": PROV_IOL_QUOTE,
        "sell_supported": PROV_IOL_QUOTE,
        "quote_supported": PROV_IOL_QUOTE,
    }
