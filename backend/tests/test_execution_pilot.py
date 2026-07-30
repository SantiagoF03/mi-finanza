"""Execution pilot — controlled BYMA single-unit sell recommendation.

The pilot endpoint may ONLY create a Recommendation + RecommendationAction.
It can never send an order: no broker, no quote, no place_order, no
submit_order_request. The sole execution path stays
POST /recommendations/{id}/approve, still gated by ORDER_EXECUTION_ENABLED.

No test here contacts IOL.
"""

import json
from contextlib import contextmanager
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.db.session import Base
from app.main import _patch_schema
from app.models.models import (
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
)
from app.services.execution import build_execution_preview
from app.services.execution_pilot import (
    PILOT_CONFIRMATION,
    create_execution_pilot_recommendation,
)

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"

BYMA_POLICY = {
    "asset_type": "ACCIONES", "instrument_type": "ACCIONES", "currency": "ARS",
    "market": "bCBA", "settlement": "t1",
    "quantity_step": 1, "price_tick": 0.25,
    "max_quantity": 1, "max_notional": 500,
}


@pytest.fixture(autouse=True)
def no_broker_ever():
    """Any broker construction inside a pilot creation is a hard failure."""
    with mock.patch(
        "app.services.execution.IolBrokerClient",
        side_effect=AssertionError("no broker may be built during pilot creation"),
    ):
        yield


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)
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


def _pilot_settings(**extra):
    """Production-like: pilot creation armed, real sending still blocked."""
    base = dict(
        broker_mode="real", iol_use_sandbox=False,
        iol_real_username="u", iol_real_password="p",
        order_execution_enabled=False,          # sending stays blocked
        execution_pilot_creation_enabled=True,  # only creation is armed
        sandbox_execution_enabled=False,
        execution_admin_key=TEST_ADMIN_KEY,
        execution_preview_secret=TEST_PREVIEW_SECRET,
        execution_preview_ttl_seconds=300,
        execution_max_recommendation_age_minutes=60,
        execution_max_order_value=500.0, execution_max_total_value=500.0,
        execution_max_portfolio_pct=0.50,
        iol_order_market="bCBA", iol_order_settlement="t1",
        iol_order_type="precioLimite", iol_order_validity_minutes=10,
        execution_max_quote_age_seconds=15, execution_max_price_deviation_pct=0.05,
        execution_sell_only=True, execution_require_live_position_check=True,
        execution_instrument_policies={"BYMA": dict(BYMA_POLICY)},
    )
    base.update(extra)
    return base


def _make_snapshot(db: Session, symbol="BYMA", quantity=307,
                   asset_type="ACCIONES", instrument_type="ACCIONES",
                   currency="ARS", market_value=92100.0):
    snap = PortfolioSnapshot(total_value=200000, cash=10000, currency="ARS")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol=symbol, asset_type=asset_type,
        instrument_type=instrument_type, currency=currency, quantity=quantity,
        market_value=market_value, avg_price=280, pnl_pct=0.05,
    ))
    db.commit()
    return snap


def _create(db, **overrides):
    payload = dict(
        execution_key=TEST_ADMIN_KEY,
        symbol="BYMA", side="sell", quantity=1,
        confirmation_text=PILOT_CONFIRMATION,
        note="Piloto controlado de ejecución real",
    )
    payload.update(overrides)
    return create_execution_pilot_recommendation(db, **payload)


def _counts(db):
    return db.query(Recommendation).count(), db.query(RecommendationAction).count()


# ───────────────────────────────────────────────────────────────────
# Locks y credenciales
# ───────────────────────────────────────────────────────────────────


def test_pilot_lock_false_blocks_with_zero_writes(db):
    _make_snapshot(db)
    before = _counts(db)
    with exec_settings(**_pilot_settings(execution_pilot_creation_enabled=False)):
        result = _create(db)
    assert result["status_code"] == 423
    assert result["code"] == "execution_pilot_creation_disabled"
    assert _counts(db) == before


def test_order_execution_enabled_blocks_pilot_creation(db):
    """The pilot may only be PREPARED while real sending is blocked."""
    _make_snapshot(db)
    before = _counts(db)
    with exec_settings(**_pilot_settings(order_execution_enabled=True)):
        result = _create(db)
    assert result["status_code"] == 423
    assert result["code"] == "order_execution_must_be_disabled"
    assert _counts(db) == before


@pytest.mark.parametrize("key", [None, "", "wrong-key"])
def test_missing_or_wrong_execution_key_is_403(db, key):
    _make_snapshot(db)
    before = _counts(db)
    with exec_settings(**_pilot_settings()):
        result = _create(db, execution_key=key)
    assert result["status_code"] == 403
    assert _counts(db) == before


@pytest.mark.parametrize("phrase", [
    "", "crear piloto byma 1", "CREAR PILOTO BYMA 2", "CREAR PILOTO BYMA", None,
])
def test_wrong_confirmation_phrase_is_422(db, phrase):
    _make_snapshot(db)
    before = _counts(db)
    with exec_settings(**_pilot_settings()):
        result = _create(db, confirmation_text=phrase)
    assert result["status_code"] == 422
    assert result["code"] == "confirmation_mismatch"
    assert _counts(db) == before


# ───────────────────────────────────────────────────────────────────
# Payload estrictamente literal
# ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("symbol", ["SPY", "AAPL", "BYMAX", "", None])
def test_other_symbols_are_blocked(db, symbol):
    _make_snapshot(db)
    before = _counts(db)
    with exec_settings(**_pilot_settings()):
        result = _create(db, symbol=symbol)
    assert result["status_code"] == 422
    assert result["code"] == "pilot_symbol_not_allowed"
    assert _counts(db) == before


def test_symbol_is_normalized_not_guessed(db):
    """Case/whitespace normalize to BYMA; nothing else is inferred."""
    _make_snapshot(db)
    with exec_settings(**_pilot_settings()):
        result = _create(db, symbol=" byma ")
    assert "error" not in result
    assert result["symbol"] == "BYMA"


@pytest.mark.parametrize("side", ["buy", "BUY ", "", None, "hold"])
def test_non_sell_sides_are_blocked(db, side):
    _make_snapshot(db)
    before = _counts(db)
    with exec_settings(**_pilot_settings()):
        result = _create(db, side=side)
    assert result["status_code"] == 422
    assert result["code"] == "pilot_side_not_allowed"
    assert _counts(db) == before


@pytest.mark.parametrize("quantity", [0, 2, 1.5, -1, None, "1abc", float("nan"), float("inf")])
def test_quantities_other_than_one_are_blocked(db, quantity):
    _make_snapshot(db)
    before = _counts(db)
    with exec_settings(**_pilot_settings()):
        result = _create(db, quantity=quantity)
    assert result["status_code"] == 422
    assert result["code"] == "pilot_quantity_not_allowed"
    assert _counts(db) == before


# ───────────────────────────────────────────────────────────────────
# Snapshot, identidad y política
# ───────────────────────────────────────────────────────────────────


def test_missing_snapshot_is_blocked(db):
    with exec_settings(**_pilot_settings()):
        result = _create(db)
    assert result["status_code"] == 409
    assert result["code"] == "snapshot_missing"


def test_byma_absent_from_snapshot_is_blocked(db):
    _make_snapshot(db, symbol="SPY")
    before = _counts(db)
    with exec_settings(**_pilot_settings()):
        result = _create(db)
    assert result["code"] == "pilot_position_missing"
    assert _counts(db) == before


@pytest.mark.parametrize("quantity", [0, 0.5])
def test_position_below_one_is_blocked(db, quantity):
    _make_snapshot(db, quantity=quantity)
    with exec_settings(**_pilot_settings()):
        result = _create(db)
    assert result["code"] in ("pilot_position_insufficient", "pilot_position_missing")


@pytest.mark.parametrize("kwargs,expected", [
    ({"asset_type": "CEDEAR"}, "pilot_identity_mismatch"),
    ({"instrument_type": "ETF"}, "pilot_identity_mismatch"),
    ({"currency": "USD"}, "pilot_currency_mismatch"),
])
def test_identity_mismatch_is_blocked(db, kwargs, expected):
    _make_snapshot(db, **kwargs)
    before = _counts(db)
    with exec_settings(**_pilot_settings()):
        result = _create(db)
    assert result["code"] == expected
    assert _counts(db) == before


def test_missing_policy_is_blocked(db):
    _make_snapshot(db)
    before = _counts(db)
    with exec_settings(**_pilot_settings(execution_instrument_policies={})):
        result = _create(db)
    assert result["code"] == "instrument_policy_missing"
    assert _counts(db) == before


def test_policy_max_quantity_below_one_is_blocked(db):
    _make_snapshot(db)
    before = _counts(db)
    with exec_settings(**_pilot_settings(
        execution_instrument_policies={"BYMA": {**BYMA_POLICY, "max_quantity": 0.5}},
    )):
        result = _create(db)
    assert result["code"] == "symbol_quantity_limit_exceeded"
    assert _counts(db) == before


def test_incompatible_quantity_step_is_blocked(db):
    _make_snapshot(db)
    with exec_settings(**_pilot_settings(
        execution_instrument_policies={"BYMA": {**BYMA_POLICY, "quantity_step": 3, "max_quantity": 10}},
    )):
        result = _create(db)
    assert result["code"] == "quantity_step_mismatch"


def test_second_pending_pilot_is_blocked(db):
    _make_snapshot(db)
    with exec_settings(**_pilot_settings()):
        first = _create(db)
        assert "error" not in first
        second = _create(db)
    assert second["status_code"] == 409
    assert second["code"] == "pilot_already_pending"
    assert db.query(Recommendation).filter(
        Recommendation.status == "pending").count() == 1


# ───────────────────────────────────────────────────────────────────
# Creación válida
# ───────────────────────────────────────────────────────────────────


def test_valid_creation_persists_recommendation_and_action(db):
    snap = _make_snapshot(db)
    with exec_settings(**_pilot_settings()):
        result = _create(db)

    assert "error" not in result
    rec = db.get(Recommendation, result["recommendation_id"])
    assert rec.status == "pending"
    assert rec.action == "rebalancear"
    meta = rec.metadata_json
    assert meta["execution_pilot"] is True
    assert meta["pilot_symbol"] == "BYMA"
    assert meta["pilot_side"] == "sell"
    assert meta["pilot_quantity"] == 1
    assert meta["created_manually"] is True
    assert meta["snapshot_id"] == snap.id

    action = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec.id).one()
    assert action.symbol == "BYMA"
    assert action.quantity_override == 1


def test_creation_never_touches_broker_or_sends(db):
    _make_snapshot(db)
    with exec_settings(**_pilot_settings()):
        with mock.patch("app.services.execution._get_execution_broker") as factory:
            result = _create(db)
    assert "error" not in result
    factory.assert_not_called()
    # Nothing executable was created
    assert db.query(OrderExecution).count() == 0


def test_pilot_supersedes_previous_recommendation_keeping_history(db):
    _make_snapshot(db)
    old = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.1, confidence=0.7,
        rationale="vender 14 SPY", risks="k", executive_summary="SPY",
        metadata_json={"origin": "automatic"},
    )
    db.add(old)
    db.flush()
    db.add(RecommendationAction(recommendation_id=old.id, symbol="SPY",
                                target_change_pct=-0.1, reason="auto"))
    db.commit()
    old_id = old.id

    with exec_settings(**_pilot_settings()):
        result = _create(db)

    assert old_id in result["superseded_recommendation_ids"]
    db.expire_all()
    old_row = db.get(Recommendation, old_id)
    assert old_row is not None, "history must never be deleted"
    assert old_row.superseded_at is not None
    assert old_row.metadata_json["origin"] == "automatic"  # original data kept
    assert old_row.metadata_json["superseded_by_execution_pilot"] == result["recommendation_id"]
    # The SPY action is untouched — it was not converted into BYMA
    spy_action = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == old_id).one()
    assert spy_action.symbol == "SPY"
    assert spy_action.quantity_override is None


# ───────────────────────────────────────────────────────────────────
# Integridad del preview
# ───────────────────────────────────────────────────────────────────


def test_preview_shows_byma_sell_quantity_one(db):
    _make_snapshot(db)
    with exec_settings(**_pilot_settings()):
        created = _create(db)
        preview = build_execution_preview(db, created["recommendation_id"])

    orders = preview["orders_preview"]
    assert len(orders) == 1
    order = orders[0]
    assert order["symbol"] == "BYMA"
    assert order["side"] == "sell"
    assert order["quantity_planned"] == 1
    assert order["quantity_override"] == 1
    assert order["valid"] is True
    # 1 × (92100/307 = 300.0)
    assert order["estimated_notional"] == 300.0
    assert order["instrument_identity"]["asset_type"] == "ACCIONES"
    assert order["execution_scope"]["price_tick"] == 0.25
    assert "SPY" not in json.dumps(preview)


def test_preview_still_locked_with_order_execution_disabled(db):
    _make_snapshot(db)
    with exec_settings(**_pilot_settings()):
        created = _create(db)
        preview = build_execution_preview(db, created["recommendation_id"])
    assert "execution_locked" in preview["blocking_reasons"]
    assert preview["can_submit_approval"] is False
    assert preview["order_execution_enabled"] is False
    assert preview["dry_run"] is True
    assert preview["would_execute"] is False


def test_changing_override_invalidates_preview_hash(db):
    from datetime import datetime as _dt

    _make_snapshot(db)
    with exec_settings(**_pilot_settings()):
        created = _create(db)
        rec_id = created["recommendation_id"]
        preview = build_execution_preview(db, rec_id)
        gen = _dt.fromisoformat(preview["generated_at"])

        action = db.query(RecommendationAction).filter(
            RecommendationAction.recommendation_id == rec_id).one()
        action.quantity_override = 2
        db.commit()
        changed = build_execution_preview(db, rec_id, generated_at=gen)

    assert changed["preview_hash"] != preview["preview_hash"]


# ───────────────────────────────────────────────────────────────────
# quantity_override fail-closed fuera del piloto
# ───────────────────────────────────────────────────────────────────


def test_override_on_non_pilot_recommendation_is_rejected(db):
    """A non-pilot recommendation carrying an override must fail closed."""
    _make_snapshot(db)
    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.05, confidence=0.7,
        rationale="auto", risks="k", executive_summary="auto", metadata_json={},
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(recommendation_id=rec.id, symbol="BYMA",
                                target_change_pct=-0.05, reason="auto",
                                quantity_override=1))
    db.commit()

    with exec_settings(**_pilot_settings()):
        preview = build_execution_preview(db, rec.id)

    order = preview["orders_preview"][0]
    assert order["valid"] is False
    assert "execution-pilot" in order["blocked_reason"]
    assert "invalid_order" in preview["blocking_reasons"]
    assert preview["can_submit_approval"] is False


@pytest.mark.parametrize("bad_override", [0, -1, 2.5])
def test_invalid_override_on_pilot_is_rejected(db, bad_override):
    _make_snapshot(db)
    with exec_settings(**_pilot_settings()):
        created = _create(db)
        rec_id = created["recommendation_id"]
        action = db.query(RecommendationAction).filter(
            RecommendationAction.recommendation_id == rec_id).one()
        action.quantity_override = bad_override
        db.commit()
        preview = build_execution_preview(db, rec_id)

    order = preview["orders_preview"][0]
    assert order["valid"] is False
    assert preview["can_submit_approval"] is False


def test_override_above_snapshot_position_is_rejected(db):
    _make_snapshot(db, quantity=1)
    with exec_settings(**_pilot_settings(
        execution_instrument_policies={"BYMA": {**BYMA_POLICY, "max_quantity": 10}},
    )):
        created = _create(db)
        rec_id = created["recommendation_id"]
        action = db.query(RecommendationAction).filter(
            RecommendationAction.recommendation_id == rec_id).one()
        action.quantity_override = 5  # more than the 1 held
        db.commit()
        preview = build_execution_preview(db, rec_id)

    order = preview["orders_preview"][0]
    assert order["valid"] is False
    assert "exceeds the snapshot position" in order["blocked_reason"]


def test_automatic_recommendations_keep_current_behavior(db):
    """No override → the quantity is still derived from target_change_pct."""
    snap = PortfolioSnapshot(total_value=100000, cash=12000, currency="USD")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="AAPL", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="USD", quantity=20,
        market_value=38000, avg_price=180, pnl_pct=0.11,
    ))
    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.05, confidence=0.7,
        rationale="auto", risks="k", executive_summary="auto", metadata_json={},
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(recommendation_id=rec.id, symbol="AAPL",
                                target_change_pct=-0.05, reason="auto"))
    db.commit()

    with exec_settings(**_pilot_settings(
        execution_instrument_policies={"AAPL": {
            "asset_type": "CEDEAR", "instrument_type": "CEDEAR", "currency": "USD",
            "market": "bCBA", "settlement": "t1",
            "quantity_step": 1, "price_tick": 0.01,
            "max_quantity": 100, "max_notional": 1_000_000,
        }},
    )):
        preview = build_execution_preview(db, rec.id)

    order = preview["orders_preview"][0]
    assert order["quantity_override"] is None
    assert order["quantity_planned"] == 2   # 5% of 100000 / 1900 → 2, unchanged
    assert order["valid"] is True


# ───────────────────────────────────────────────────────────────────
# Contrato del endpoint y caminos de ejecución
# ───────────────────────────────────────────────────────────────────


def test_endpoint_requires_api_key():
    from fastapi.testclient import TestClient
    from app.main import app

    settings = get_settings()
    original = settings.api_key
    try:
        settings.api_key = "test-secret-key-123"
        client = TestClient(app)
        resp = client.post("/api/execution-pilot/recommendations", json={
            "symbol": "BYMA", "side": "sell", "quantity": 1,
            "confirmation_text": PILOT_CONFIRMATION,
        })
        assert resp.status_code == 403
    finally:
        settings.api_key = original


def test_no_new_execution_endpoint_exists():
    """The pilot must not introduce any sending route."""
    from pathlib import Path

    routes = (Path(__file__).resolve().parent.parent / "app" / "api" / "routes.py").read_text()
    for forbidden in ("/place-order", "/execution-pilot/orders", "/execution-pilot/approve",
                      "/execution-pilot/submit", "/resume", "/retry", "/resubmit"):
        assert forbidden not in routes, f"{forbidden} must not exist"
    # approve remains the single execution path
    assert routes.count('@router.post("/recommendations/{recommendation_id}/approve")') == 1


def test_pilot_module_never_references_sending_calls():
    """Static guarantee over real code (docstrings/comments excluded, since
    they legitimately explain what the module must NOT do)."""
    import ast
    from pathlib import Path

    path = Path(__file__).resolve().parent.parent / "app" / "services" / "execution_pilot.py"
    tree = ast.parse(path.read_text())

    called_names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            called_names.add(node.id)
        elif isinstance(node, ast.Attribute):
            called_names.add(node.attr)
        elif isinstance(node, ast.ImportFrom):
            called_names.update(alias.name for alias in node.names)

    for forbidden in ("place_order", "submit_order_request", "get_execution_quote",
                      "_get_execution_broker", "approve_and_execute",
                      "IolBrokerClient", "MockBrokerClient"):
        assert forbidden not in called_names, f"execution_pilot must not reference {forbidden}"
