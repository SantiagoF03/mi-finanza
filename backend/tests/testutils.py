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
