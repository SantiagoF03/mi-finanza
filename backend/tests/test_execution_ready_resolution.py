"""Execution Ready Resolution & Configuration Hardening.

Covers:
- execution_ready (durable PRE-POST state) surfacing in the reconciliation
  queue with full batch context, and resolvable ONLY as confirm_not_sent —
  never resumed, re-sent or returned to an approvable state;
- quote timestamps: future beyond the allowed clock skew, naive and
  malformed timestamps are rejected with quote_timestamp_invalid;
- Settings numeric hardening: NaN/Infinity/negative/out-of-range values make
  the application fail at startup instead of producing ambiguous comparisons
  during an order.

No test here touches the real IOL API.
"""

import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import Settings, get_settings
from app.db.session import Base
from app.models.models import (
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
)
from app.services.execution import (
    _quote_age_seconds,
    get_reconciliation_queue,
    reconcile_execution,
    reconciliation_phrase,
)

TEST_ADMIN_KEY = "test-execution-admin-key"


@pytest.fixture(autouse=True)
def no_iol_ever():
    with mock.patch(
        "app.services.execution.IolBrokerClient",
        side_effect=AssertionError("Real IOL client must never be instantiated in tests"),
    ):
        yield


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


@contextmanager
def exec_settings(**overrides):
    s = get_settings()
    saved = {k: getattr(s, k) for k in overrides}
    try:
        for k, v in overrides.items():
            setattr(s, k, v)
        yield s
    finally:
        for k, v in saved.items():
            setattr(s, k, v)


def _make_batch(db: Session, statuses=("execution_ready",), rec_status="execution_pending"):
    """Recommendation with one OrderExecution per given status."""
    snap = PortfolioSnapshot(total_value=100000, cash=12000, currency="USD")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="AAPL", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="USD", quantity=20,
        market_value=38000, avg_price=180, pnl_pct=0.11,
    ))
    rec = Recommendation(
        action="rebalancear", status=rec_status, suggested_pct=0.05, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    rows = []
    for idx, status in enumerate(statuses):
        action = RecommendationAction(
            recommendation_id=rec.id, symbol=f"SYM{idx}",
            target_change_pct=-0.05, reason="test",
        )
        db.add(action)
        db.flush()
        oe = OrderExecution(
            recommendation_id=rec.id, recommendation_action_id=action.id,
            symbol=f"SYM{idx}", side="sell", target_change_pct=-0.05,
            status=status, validation_status="passed",
            quantity_planned=2, quantity=2, quantity_sent=None,
            broker_response={
                "request_audit": {"snapshot_id": snap.id, "preview_hash": "h" * 64,
                                  "iol_request": {"simbolo": f"SYM{idx}"}},
                "broker_result": None,
                "reconciliation_audit": [],
            },
        )
        db.add(oe)
        rows.append(oe)
    db.commit()
    return rec, rows


def _reconcile(db, oe, action, **kwargs):
    return reconcile_execution(
        db, oe.id,
        execution_key=TEST_ADMIN_KEY,
        action=action,
        confirmation_text=kwargs.pop("phrase", None) or reconciliation_phrase(oe.id, action),
        **kwargs,
    )


# ───────────────────────────────────────────────────────────────────
# Parte 1 — execution_ready en la cola (tests 1-2)
# ───────────────────────────────────────────────────────────────────


def test_execution_ready_appears_in_queue(db):
    rec, rows = _make_batch(db, statuses=("execution_ready",))
    queue = get_reconciliation_queue(db)
    assert [i["id"] for i in queue] == [rows[0].id]
    assert queue[0]["status"] == "execution_ready"


def test_queue_includes_the_rest_of_the_batch(db):
    """One execution_ready order pulls its whole batch into the queue."""
    rec, rows = _make_batch(db, statuses=("execution_ready", "preflight_cancelled", "failed"))
    queue = get_reconciliation_queue(db)
    assert {i["id"] for i in queue} == {r.id for r in rows}
    statuses = {i["status"] for i in queue}
    assert statuses == {"execution_ready", "preflight_cancelled", "failed"}


def test_recommendation_with_execution_ready_is_manual_review(db):
    """_recompute_recommendation_outcome treats execution_ready as pending
    manual review — never approved/pending/blocked."""
    rec, rows = _make_batch(db, statuses=("execution_ready", "execution_sent"))
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        # Reconciling the SENT one recomputes the aggregate
        result = _reconcile(db, rows[1], "confirm_executed", broker_order_id="X-1",
                            executed_quantity=2, executed_price=1900.0, note="ok")
    assert result["recommendation_status"] == "manual_reconciliation_required"
    db.expire_all()
    assert db.get(Recommendation, rec.id).status == "manual_reconciliation_required"


# ───────────────────────────────────────────────────────────────────
# Parte 2/3 — resolución segura (tests 3-14)
# ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("action,kwargs", [
    ("confirm_sent", {"broker_order_id": "IOL-1"}),
    ("confirm_executed", {"broker_order_id": "IOL-1", "executed_quantity": 2, "executed_price": 1900.0}),
    ("confirm_rejected", {"note": "rechazada"}),
])
def test_execution_ready_only_allows_confirm_not_sent(db, action, kwargs):
    rec, rows = _make_batch(db, statuses=("execution_ready",))
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        result = _reconcile(db, rows[0], action, **kwargs)
    assert result["status_code"] == 409
    assert result["code"] == "not_reconcilable"
    db.expire_all()
    assert db.get(OrderExecution, rows[0].id).status == "execution_ready"


def test_confirm_not_sent_from_execution_ready(db):
    rec, rows = _make_batch(db, statuses=("execution_ready",))
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        result = _reconcile(db, rows[0], "confirm_not_sent",
                            note="preflight completo, proceso caído antes del envío")
    assert result["new_status"] == "not_sent_confirmed"
    assert result["previous_status"] == "execution_ready"
    db.expire_all()
    row = db.get(OrderExecution, rows[0].id)
    assert row.status == "not_sent_confirmed"
    assert row.quantity_sent is None


def test_audit_keeps_statuses_and_reason_code(db):
    rec, rows = _make_batch(db, statuses=("execution_ready",))
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        _reconcile(db, rows[0], "confirm_not_sent", note="nunca se envió")
    db.expire_all()
    audit = db.get(OrderExecution, rows[0].id).broker_response["reconciliation_audit"]
    assert len(audit) == 1
    entry = audit[0]
    assert entry["action"] == "confirm_not_sent"
    assert entry["previous_status"] == "execution_ready"
    assert entry["new_status"] == "not_sent_confirmed"
    assert entry["reason_code"] == "prepared_but_not_submitted"
    assert entry["source"] == "manual_user"
    # The preflight audit is preserved
    assert db.get(OrderExecution, rows[0].id).broker_response["request_audit"]["iol_request"]


@pytest.mark.parametrize("forbidden", ["pending", "blocked", "approved"])
def test_recommendation_never_returns_to_approvable(db, forbidden):
    rec, rows = _make_batch(db, statuses=("execution_ready",))
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        result = _reconcile(db, rows[0], "confirm_not_sent", note="x")
    assert result["recommendation_status"] != forbidden
    db.expire_all()
    assert db.get(Recommendation, rec.id).status == "execution_failed"
    assert db.get(Recommendation, rec.id).status not in ("pending", "blocked", "approved")


def test_reconciling_execution_ready_never_touches_broker(db):
    rec, rows = _make_batch(db, statuses=("execution_ready",))
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        with mock.patch("app.services.execution._get_execution_broker") as factory:
            _reconcile(db, rows[0], "confirm_not_sent", note="x")
    factory.assert_not_called()


def test_no_place_order_or_submit_from_reconciliation(db):
    """Reconciliation code path must never reference a submission call."""
    from pathlib import Path

    rec, rows = _make_batch(db, statuses=("execution_ready",))
    broker = mock.MagicMock()
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
            _reconcile(db, rows[0], "confirm_not_sent", note="x")
    broker.place_order.assert_not_called()
    broker.submit_order_request.assert_not_called()

    # And no retry/resubmit action exists at all
    from app.services.execution import _RECONCILIATION_ACTIONS
    assert set(_RECONCILIATION_ACTIONS) == {
        "confirm_not_sent", "confirm_sent", "confirm_rejected", "confirm_executed",
    }
    assert "retry" not in _RECONCILIATION_ACTIONS
    assert "resubmit" not in _RECONCILIATION_ACTIONS

    src = Path(__file__).resolve().parent.parent / "app" / "api" / "routes.py"
    routes = src.read_text()
    for forbidden in ("/resume", "/retry", "/resubmit"):
        assert forbidden not in routes, f"no {forbidden} endpoint may exist"


def test_repeated_reconciliation_returns_409_and_applies_once(db):
    rec, rows = _make_batch(db, statuses=("execution_ready",))
    with exec_settings(execution_admin_key=TEST_ADMIN_KEY):
        first = _reconcile(db, rows[0], "confirm_not_sent", note="primera")
        second = _reconcile(db, rows[0], "confirm_not_sent", note="repetida")
    assert first["new_status"] == "not_sent_confirmed"
    assert second["status_code"] == 409
    db.expire_all()
    audit = db.get(OrderExecution, rows[0].id).broker_response["reconciliation_audit"]
    assert len(audit) == 1


# ───────────────────────────────────────────────────────────────────
# Parte 4 — timestamps de cotización (tests 15-19)
# ───────────────────────────────────────────────────────────────────


def _ts(delta_seconds: float, tz=timezone.utc) -> str:
    return (datetime.now(tz) + timedelta(seconds=delta_seconds)).isoformat()


def test_slightly_future_quote_passes_within_skew():
    with exec_settings(execution_quote_clock_skew_seconds=2, execution_max_quote_age_seconds=15) as s:
        age, err = _quote_age_seconds(_ts(1), s)
    assert err is None
    assert age == 0.0  # clamped, treated as brand new


def test_too_future_quote_is_invalid():
    with exec_settings(execution_quote_clock_skew_seconds=2, execution_max_quote_age_seconds=15) as s:
        age, err = _quote_age_seconds(_ts(3), s)
    assert err == "quote_timestamp_invalid"


def test_naive_timestamp_is_invalid():
    with exec_settings(execution_quote_clock_skew_seconds=2) as s:
        naive = datetime.now().replace(tzinfo=None).isoformat()
        _, err = _quote_age_seconds(naive, s)
    assert err == "quote_timestamp_invalid"


@pytest.mark.parametrize("bad", ["", None, "not-a-timestamp", 12345, "2026-13-45T99:99:99+00:00"])
def test_malformed_timestamp_is_invalid(bad):
    with exec_settings(execution_quote_clock_skew_seconds=2) as s:
        _, err = _quote_age_seconds(bad, s)
    assert err == "quote_timestamp_invalid"


def test_old_quote_still_reports_age_for_staleness():
    with exec_settings(execution_quote_clock_skew_seconds=2, execution_max_quote_age_seconds=15) as s:
        age, err = _quote_age_seconds(_ts(-600), s)
    assert err is None
    assert age > 15  # caller turns this into quote_stale


def test_non_utc_aware_timestamp_is_accepted_and_normalized():
    tz = timezone(timedelta(hours=-3))
    with exec_settings(execution_quote_clock_skew_seconds=2, execution_max_quote_age_seconds=15) as s:
        age, err = _quote_age_seconds(_ts(-1, tz=tz), s)
    assert err is None
    assert 0 <= age <= 15


# ───────────────────────────────────────────────────────────────────
# Parte 5 — validación de settings numéricos (tests 20-28)
# ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("field,value", [
    ("execution_quote_clock_skew_seconds", -1),
    ("execution_quote_clock_skew_seconds", 11),
    ("execution_quote_clock_skew_seconds", float("nan")),
    ("execution_max_price_deviation_pct", float("nan")),
    ("execution_max_price_deviation_pct", -0.01),
    ("execution_max_order_value", float("inf")),
    ("execution_max_order_value", float("-inf")),
    ("execution_max_total_value", -1),
    ("execution_max_portfolio_pct", float("nan")),
    ("execution_preview_ttl_seconds", 0),
    ("execution_preview_ttl_seconds", -5),
    ("execution_max_recommendation_age_minutes", 0),
    ("execution_max_quote_age_seconds", 0),
    ("execution_max_quote_age_seconds", float("inf")),
    ("iol_order_validity_minutes", 0),
    ("iol_order_validity_minutes", float("nan")),
])
def test_invalid_numeric_settings_fail_startup(field, value):
    with pytest.raises(ValidationError):
        Settings(**{field: value})


@pytest.mark.parametrize("field", [
    "execution_max_price_deviation_pct",
    "execution_max_order_value",
    "execution_max_total_value",
    "execution_max_portfolio_pct",
])
def test_zero_still_allowed_on_fail_closed_limits(field):
    """0 means NOT CONFIGURED (blocks execution) — it must remain loadable."""
    settings = Settings(**{field: 0})
    assert getattr(settings, field) == 0


def test_valid_clock_skew_bounds_are_accepted():
    assert Settings(execution_quote_clock_skew_seconds=0).execution_quote_clock_skew_seconds == 0
    assert Settings(execution_quote_clock_skew_seconds=10).execution_quote_clock_skew_seconds == 10


def test_default_settings_are_valid_and_fail_closed():
    s = Settings()
    assert s.order_execution_enabled is False
    assert s.sandbox_execution_enabled is False
    assert s.execution_sell_only is True
    assert s.execution_instrument_policies == {}
    assert s.execution_require_live_position_check is True
    assert s.execution_quote_clock_skew_seconds == 2
    assert s.execution_max_order_value == 0.0
    assert s.execution_max_total_value == 0.0
    assert s.execution_max_portfolio_pct == 0.0


def test_bool_is_rejected_as_numeric_setting():
    """True must never stand in for 1 in a financial threshold."""
    with pytest.raises(ValidationError):
        Settings(execution_max_price_deviation_pct=True)
