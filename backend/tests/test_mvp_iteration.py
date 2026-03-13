"""Tests for MVP iteration: catalog, safe execution, push, settings, universe.

Covers:
1. approve en mock sigue funcionando
2. approve en real no envía orden si no puede calcular quantity segura
3. approve en real envía orden si la validación pasa
4. validation failure deja estado claro y no rompe recommendation
5. scheduler nunca ejecuta órdenes
6. push sender se dispara solo con alertas/recomendaciones relevantes
7. cooldown anti-spam se respeta
8. subscriptions inválidas se manejan bien
9. settings persistidos sobreviven recarga
10. refresh del catálogo crea instrumentos nuevos
11. deduplicación de símbolos funciona
12. universe dinámico se alimenta desde instrument_catalog
13. external_opportunity puede salir de catálogo dinámico sin whitelist manual
14. whitelist queda como override opcional pero no dependencia principal
15. triage, unchanged, cooldown y external_opportunities siguen intactos
"""

import math
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.db.session import Base
from app.models.models import (
    InstrumentCatalog,
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    PushSubscription,
    Recommendation,
    RecommendationAction,
    UserSettings,
)


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _make_recommendation(db: Session, action="rebalancear", status="pending", symbol="AAPL", pct=-0.05) -> Recommendation:
    rec = Recommendation(
        action=action,
        status=status,
        suggested_pct=abs(pct),
        confidence=0.7,
        rationale="Test rationale",
        risks="Test risks",
        executive_summary="Test summary",
        metadata_json={},
    )
    db.add(rec)
    db.flush()
    act = RecommendationAction(
        recommendation_id=rec.id,
        symbol=symbol,
        target_change_pct=pct,
        reason="Test reason",
    )
    db.add(act)
    db.flush()
    return rec


def _make_snapshot(db: Session) -> PortfolioSnapshot:
    snap = PortfolioSnapshot(total_value=100000, cash=12000, currency="USD")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="AAPL", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="USD", quantity=20,
        market_value=38000, avg_price=180, pnl_pct=0.11,
    ))
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="MSFT", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="USD", quantity=12,
        market_value=28000, avg_price=340, pnl_pct=0.08,
    ))
    db.flush()
    return snap


# ---------------------------------------------------------------------------
# 1. Approve en mock sigue funcionando
# ---------------------------------------------------------------------------


def test_approve_mock_still_works(db):
    """Approve in mock mode creates execution rows with planned quantity."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()

    from app.services.execution import approve_and_execute

    with patch("app.services.execution._get_execution_broker") as mock_fn:
        mock_broker = mock_fn.return_value
        mock_broker.place_order.return_value = {
            "order_id": "MOCK-123", "status": "sent",
            "raw_response": {"mock": True},
        }
        result = approve_and_execute(db, rec.id, note="test")

    assert result["status"] == "approved"
    assert len(result["executions"]) == 1
    ex = result["executions"][0]
    assert ex["symbol"] == "AAPL"
    assert ex["status"] == "execution_sent"
    assert ex["quantity_planned"] is not None
    assert ex["quantity_planned"] > 0


# ---------------------------------------------------------------------------
# 2. Approve no envía si no puede calcular quantity segura
# ---------------------------------------------------------------------------


def test_approve_no_send_if_no_position(db):
    """If position doesn't exist, order gets validation_failed, NOT sent."""
    # Snapshot with no AAPL position
    snap = PortfolioSnapshot(total_value=100000, cash=12000, currency="USD")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="MSFT", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="USD", quantity=12,
        market_value=28000, avg_price=340, pnl_pct=0.08,
    ))
    db.flush()

    rec = _make_recommendation(db, symbol="AAPL", pct=-0.05)
    db.commit()

    from app.services.execution import approve_and_execute

    with patch("app.services.execution._get_execution_broker") as mock_fn:
        mock_broker = mock_fn.return_value
        result = approve_and_execute(db, rec.id)

    assert result["status"] == "approved"
    ex = result["executions"][0]
    assert ex["status"] == "validation_failed"
    assert "No position found" in ex["blocked_reason"]
    # Broker should NOT have been called
    mock_broker.place_order.assert_not_called()


# ---------------------------------------------------------------------------
# 3. Approve envía si validación pasa
# ---------------------------------------------------------------------------


def test_approve_sends_if_validation_passes(db):
    """With valid position data, order is sent to broker with correct quantity."""
    _make_snapshot(db)
    rec = _make_recommendation(db, symbol="AAPL", pct=-0.05)
    db.commit()

    from app.services.execution import approve_and_execute

    with patch("app.services.execution._get_execution_broker") as mock_fn:
        mock_broker = mock_fn.return_value
        mock_broker.place_order.return_value = {
            "order_id": "MOCK-456", "status": "sent",
            "raw_response": {},
        }
        result = approve_and_execute(db, rec.id)

    ex = result["executions"][0]
    assert ex["status"] == "execution_sent"
    assert ex["validation_status"] == "passed"
    assert ex["quantity_planned"] > 0
    # Broker was called with the planned quantity
    mock_broker.place_order.assert_called_once()
    call_args = mock_broker.place_order.call_args
    assert call_args.kwargs["quantity"] > 0 or call_args[1].get("quantity", call_args[0][2] if len(call_args[0]) > 2 else 0) > 0


# ---------------------------------------------------------------------------
# 4. Validation failure deja estado claro
# ---------------------------------------------------------------------------


def test_validation_failure_clear_state(db):
    """Validation failure sets validation_failed status with clear reason."""
    # Snapshot with zero-quantity position
    snap = PortfolioSnapshot(total_value=100000, cash=12000, currency="USD")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="AAPL", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="USD", quantity=0,
        market_value=0, avg_price=180, pnl_pct=0,
    ))
    db.flush()
    rec = _make_recommendation(db, symbol="AAPL", pct=-0.05)
    db.commit()

    from app.services.execution import approve_and_execute

    with patch("app.services.execution._get_execution_broker"):
        result = approve_and_execute(db, rec.id)

    ex = result["executions"][0]
    assert ex["status"] == "validation_failed"
    assert ex["blocked_reason"] != ""
    # Recommendation is still approved (decision made)
    updated = db.query(Recommendation).filter(Recommendation.id == rec.id).first()
    assert updated.status == "approved"


# ---------------------------------------------------------------------------
# 5. Scheduler nunca ejecuta órdenes
# ---------------------------------------------------------------------------


def test_scheduler_never_imports_execution():
    """Scheduler module should NEVER import from services.execution."""
    import inspect
    from app.scheduler import jobs
    source = inspect.getsource(jobs)
    assert "approve_and_execute" not in source
    assert "place_order" not in source
    assert "OrderExecution" not in source


# ---------------------------------------------------------------------------
# 6. Push sender only fires for qualifying alerts
# ---------------------------------------------------------------------------


def test_dispatch_alerts_respects_severity():
    """dispatch_alerts doesn't send for events below min severity."""
    from app.notifications.dispatcher import dispatch_alerts, _severity_passes

    assert _severity_passes("critical", "medium") is True
    assert _severity_passes("low", "high") is False
    assert _severity_passes("medium", "medium") is True


# ---------------------------------------------------------------------------
# 7. Cooldown anti-spam
# ---------------------------------------------------------------------------


def test_cooldown_respected():
    """dispatch_alerts respects cooldown period."""
    from app.notifications import dispatcher

    # Set last notification to now
    dispatcher._last_notification_at = datetime.now(timezone.utc)

    class MockEvent:
        severity = "critical"
        affected_symbols = ["AAPL"]
        trigger_type = "holding_risk"
        message = "Test"

    settings = get_settings_mock()
    with patch("app.notifications.dispatcher.get_settings", return_value=settings):
        result = dispatcher.dispatch_alerts(None, [MockEvent()])

    assert result["sent"] is False
    assert result["reason"] == "cooldown"

    # Reset
    dispatcher._last_notification_at = None


def get_settings_mock():
    s = MagicMock()
    s.notification_enabled = True
    s.notification_channel = "telegram"
    s.notification_min_severity = "medium"
    s.notification_cooldown_seconds = 300
    s.telegram_bot_token = ""
    s.telegram_chat_id = ""
    s.scheduler_market_open_hour = 11
    s.scheduler_market_close_hour = 20
    s.vapid_private_key = ""
    s.vapid_public_key = ""
    return s


# ---------------------------------------------------------------------------
# 8. Subscriptions invalidation
# ---------------------------------------------------------------------------


def test_push_subscription_model(db):
    """PushSubscription model works."""
    sub = PushSubscription(
        endpoint="https://fcm.googleapis.com/fcm/send/test",
        p256dh="key", auth="auth",
    )
    db.add(sub)
    db.flush()
    assert sub.id is not None


def test_send_web_push_no_vapid(db):
    """send_web_push_to_all returns early without VAPID keys."""
    from app.notifications.dispatcher import send_web_push_to_all

    with patch("app.notifications.dispatcher.get_settings") as mock_s:
        mock_s.return_value.vapid_private_key = ""
        mock_s.return_value.vapid_public_key = ""
        result = send_web_push_to_all(db, title="Test", body="Body")

    assert result["reason"] == "vapid_not_configured"


# ---------------------------------------------------------------------------
# 9. Settings persisted survive reload
# ---------------------------------------------------------------------------


def test_settings_persistence(db):
    """UserSettings can be written and read back."""
    db.add(UserSettings(key="investor_profile_target", value="aggressive"))
    db.add(UserSettings(key="notification_enabled", value="true"))
    db.add(UserSettings(key="notification_cooldown_seconds", value="600"))
    db.commit()

    rows = db.query(UserSettings).all()
    by_key = {r.key: r.value for r in rows}
    assert by_key["investor_profile_target"] == "aggressive"
    assert by_key["notification_enabled"] == "true"
    assert by_key["notification_cooldown_seconds"] == "600"


def test_settings_upsert(db):
    """Settings can be updated (upserted)."""
    db.add(UserSettings(key="investor_profile_target", value="moderate"))
    db.commit()

    existing = db.query(UserSettings).filter(UserSettings.key == "investor_profile_target").first()
    existing.value = "aggressive"
    db.commit()

    updated = db.query(UserSettings).filter(UserSettings.key == "investor_profile_target").first()
    assert updated.value == "aggressive"


# ---------------------------------------------------------------------------
# 10. Refresh catálogo crea instrumentos nuevos
# ---------------------------------------------------------------------------


def test_catalog_refresh_creates_instruments(db):
    """refresh_instrument_catalog creates new instruments from static seed."""
    from app.market.discovery import refresh_instrument_catalog

    result = refresh_instrument_catalog(db, force_seed=True)
    assert result["status"] == "ok"
    assert result["created"] > 0
    assert result["total_active"] > 0

    # Check some known instruments exist
    aapl = db.query(InstrumentCatalog).filter(InstrumentCatalog.symbol == "AAPL").first()
    assert aapl is not None
    assert aapl.asset_type == "CEDEAR"
    assert aapl.is_active is True

    ggal = db.query(InstrumentCatalog).filter(InstrumentCatalog.symbol == "GGAL").first()
    assert ggal is not None
    assert ggal.asset_type == "ACCIONES"


# ---------------------------------------------------------------------------
# 11. Deduplicación de símbolos funciona
# ---------------------------------------------------------------------------


def test_catalog_deduplication(db):
    """Running refresh twice doesn't duplicate instruments."""
    from app.market.discovery import refresh_instrument_catalog

    result1 = refresh_instrument_catalog(db, force_seed=True)
    created1 = result1["created"]

    result2 = refresh_instrument_catalog(db, force_seed=True)
    created2 = result2["created"]

    assert created1 > 0
    assert created2 == 0  # All were updates, no new creates
    assert result2["updated"] > 0


# ---------------------------------------------------------------------------
# 12. Universe dinámico desde instrument_catalog
# ---------------------------------------------------------------------------


def test_universe_from_catalog(db):
    """get_eligible_universe_symbols returns symbols from catalog."""
    from app.market.discovery import get_eligible_universe_symbols, refresh_instrument_catalog

    refresh_instrument_catalog(db, force_seed=True)
    symbols = get_eligible_universe_symbols(db)

    assert len(symbols) > 10
    assert "AAPL" in symbols
    assert "GGAL" in symbols
    assert "AL30" in symbols


# ---------------------------------------------------------------------------
# 13. external_opportunity de catálogo dinámico sin whitelist
# ---------------------------------------------------------------------------


def test_external_from_catalog_without_whitelist(db):
    """External universe includes catalog symbols even without manual whitelist/universe config."""
    from app.market.discovery import get_eligible_universe_symbols, refresh_instrument_catalog
    from app.recommendations.universe import build_allowed_assets

    refresh_instrument_catalog(db, force_seed=True)
    catalog_symbols = get_eligible_universe_symbols(db)

    # Holdings = just AAPL. No manual watchlist/universe.
    positions = [{"symbol": "AAPL"}]

    with patch("app.recommendations.universe.get_settings") as mock_s:
        mock_s.return_value.whitelist_assets = ["AAPL"]
        mock_s.return_value.watchlist_assets = []
        mock_s.return_value.market_universe_assets = []

        allowed = build_allowed_assets(positions, catalog_symbols=catalog_symbols)

    # Universe should be populated from catalog, not empty
    assert len(allowed["universe"]) > 10
    assert "GGAL" in allowed["universe"]
    assert "MSFT" in allowed["universe"]
    # Catalog dynamic count matches
    assert len(allowed["catalog_dynamic"]) == len(catalog_symbols)


# ---------------------------------------------------------------------------
# 14. Whitelist como override opcional
# ---------------------------------------------------------------------------


def test_whitelist_override_optional(db):
    """Whitelist still works as override but universe is primarily from catalog."""
    from app.market.discovery import get_eligible_universe_symbols, refresh_instrument_catalog
    from app.recommendations.universe import build_allowed_assets

    refresh_instrument_catalog(db, force_seed=True)
    catalog_symbols = get_eligible_universe_symbols(db)

    positions = [{"symbol": "AAPL"}]

    with patch("app.recommendations.universe.get_settings") as mock_s:
        mock_s.return_value.whitelist_assets = ["AAPL", "SPECIAL_OVERRIDE"]
        mock_s.return_value.watchlist_assets = []
        mock_s.return_value.market_universe_assets = []

        allowed = build_allowed_assets(positions, catalog_symbols=catalog_symbols)

    # Whitelist override is in main_allowed
    assert "SPECIAL_OVERRIDE" in allowed["main_allowed"]
    # Catalog fills universe
    assert len(allowed["universe"]) > 10
    # Universe is NOT mainly from whitelist
    assert "SPECIAL_OVERRIDE" not in allowed["universe"]


# ---------------------------------------------------------------------------
# 15. Existing logic preserved
# ---------------------------------------------------------------------------


def test_reject_does_not_execute(db):
    """Rejecting a recommendation does NOT create any OrderExecution."""
    rec = _make_recommendation(db)
    db.commit()

    from app.services.execution import reject_recommendation

    result = reject_recommendation(db, rec.id, note="not now")
    assert result["status"] == "rejected"

    oes = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).all()
    assert len(oes) == 0


def test_approve_mantener_no_orders(db):
    """Approving 'mantener' creates no orders."""
    rec = _make_recommendation(db, action="mantener")
    db.commit()

    from app.services.execution import approve_and_execute
    result = approve_and_execute(db, rec.id)
    assert result["status"] == "approved"
    assert len(result["executions"]) == 0


def test_order_plan_sell_quantity_calculation(db):
    """_plan_order calculates correct sell quantity from position data."""
    from app.services.execution import _plan_order

    snap = _make_snapshot(db)
    db.commit()

    action = MagicMock()
    action.symbol = "AAPL"
    action.target_change_pct = -0.05  # sell 5% of portfolio

    plan = _plan_order(action, snap)

    assert plan["valid"] is True
    assert plan["side"] == "sell"
    assert plan["quantity_planned"] > 0
    # 5% of 100000 = 5000. Price per unit = 38000/20 = 1900. Qty = 5000/1900 = 2.63 -> floor = 2
    assert plan["quantity_planned"] == 2
    assert plan["portfolio_value_used"] == 100000
    assert plan["position_value_used"] == 38000


def test_order_plan_no_snapshot():
    """_plan_order fails safely with no snapshot."""
    from app.services.execution import _plan_order

    action = MagicMock()
    action.symbol = "AAPL"
    action.target_change_pct = -0.05

    plan = _plan_order(action, None)
    assert plan["valid"] is False


def test_instrument_catalog_model(db):
    """InstrumentCatalog model creation and querying."""
    inst = InstrumentCatalog(
        symbol="TEST", name="Test Corp", asset_type="ACCIONES",
        market="BCBA", currency="ARS",
    )
    db.add(inst)
    db.flush()
    assert inst.id is not None

    found = db.query(InstrumentCatalog).filter(InstrumentCatalog.symbol == "TEST").first()
    assert found is not None
    assert found.asset_type == "ACCIONES"
    assert found.is_active is True


def test_catalog_asset_type_lookup(db):
    """get_catalog_asset_type returns the correct type from catalog."""
    from app.market.discovery import get_catalog_asset_type

    db.add(InstrumentCatalog(symbol="GGAL", asset_type="ACCIONES", is_active=True))
    db.flush()

    assert get_catalog_asset_type(db, "GGAL") == "ACCIONES"
    assert get_catalog_asset_type(db, "NONEXIST") is None
