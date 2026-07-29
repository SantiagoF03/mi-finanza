"""Execution Scope & Live Position Guard V1 + broker environment corrections.

Covers:
- structural HTTPS/URL validation for real and sandbox environments;
- the effective environment as the single source of truth (BROKER_MODE=real
  + IOL_USE_SANDBOX=true really behaves as sandbox everywhere);
- the per-instrument allowlist (identity, venue, per-symbol limits) with
  sell-only phase, all signed into the preview HMAC;
- the read-only live position guard executed BEFORE quoting, BEFORE
  'submitting' and BEFORE any POST, which can only block — never resize an
  order.

No test here touches the real IOL API.
"""

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.broker.environment import (
    effective_execution_environment,
    resolve_execution_environment,
)
from app.broker.instrument_scope import load_instrument_policies, validate_instrument_policy
from app.core.config import get_settings
from app.db.session import Base
from app.models.models import (
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
)
from app.services.execution import (
    approve_and_execute,
    build_execution_preview,
    confirmation_phrase,
    get_execution_readiness,
)

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"
SANDBOX_BASE = "https://sandbox.iol-test.local"

VALID_POLICY = {
    "asset_type": "CEDEAR", "instrument_type": "CEDEAR", "currency": "USD",
    "market": "bCBA", "settlement": "t1",
    "quantity_step": 1, "max_quantity": 100, "max_notional": 1_000_000,
}


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


def _make_snapshot(db: Session, asset_type="CEDEAR", instrument_type="CEDEAR", currency="USD") -> PortfolioSnapshot:
    snap = PortfolioSnapshot(total_value=100000, cash=12000, currency="USD")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="AAPL", asset_type=asset_type,
        instrument_type=instrument_type, currency=currency, quantity=20,
        market_value=38000, avg_price=180, pnl_pct=0.11,
    ))
    db.flush()
    return snap


def _make_recommendation(db: Session, symbol="AAPL", target_pct=-0.05) -> Recommendation:
    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.05, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(
        recommendation_id=rec.id, symbol=symbol,
        target_change_pct=target_pct, reason="test",
    ))
    db.flush()
    return rec


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


def _sandbox_settings(**extra):
    base = dict(
        broker_mode="sandbox", iol_use_sandbox=False,
        order_execution_enabled=False, sandbox_execution_enabled=True,
        iol_sandbox_api_base=SANDBOX_BASE,
        iol_sandbox_username="sbx-user", iol_sandbox_password="sbx-pass",
        execution_admin_key=TEST_ADMIN_KEY, execution_preview_secret=TEST_PREVIEW_SECRET,
        execution_preview_ttl_seconds=300, execution_max_recommendation_age_minutes=60,
        execution_max_order_value=1_000_000.0, execution_max_total_value=1_000_000.0,
        execution_max_portfolio_pct=0.50,
        iol_order_market="bCBA", iol_order_settlement="t1",
        iol_order_type="precioLimite", iol_order_validity_minutes=10,
        execution_max_quote_age_seconds=15, execution_max_price_deviation_pct=0.05,
        execution_sell_only=True, execution_require_live_position_check=True,
        execution_instrument_policies={"AAPL": dict(VALID_POLICY)},
    )
    base.update(extra)
    return base


def _live(quantity=20, asset_type="CEDEAR", instrument_type="CEDEAR", currency="USD", symbol="AAPL"):
    return {"positions": [{
        "symbol": symbol, "asset_type": asset_type, "instrument_type": instrument_type,
        "currency": currency, "quantity": quantity, "market_value": 38000,
    }]}


def _broker(live=None, quote_price=1900.0):
    b = mock.MagicMock()
    b.get_portfolio_snapshot.return_value = live if live is not None else _live()
    b.get_execution_quote.return_value = {
        "available": True, "price": quote_price, "source": "bid",
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "market": "bCBA", "settlement": "t1",
    }
    b.submit_order_request.return_value = {
        "outcome": "sent", "order_id": "SBX-9", "raw_response": {"numeroOperacion": "SBX-9"},
        "endpoint_used": "/api/v2/operar/Vender", "error": "",
    }
    return b


def _approve(db, rec, preview, broker):
    with mock.patch("app.services.execution._get_execution_broker", return_value=broker):
        return approve_and_execute(
            db, rec.id, execution_key=TEST_ADMIN_KEY,
            preview_hash=preview["preview_hash"],
            preview_generated_at=preview["generated_at"],
            confirmation_text=confirmation_phrase(rec.id),
        )


def _run(db, settings_extra=None, broker=None, snapshot_kwargs=None):
    _make_snapshot(db, **(snapshot_kwargs or {}))
    rec = _make_recommendation(db)
    db.commit()
    b = broker or _broker()
    with exec_settings(**_sandbox_settings(**(settings_extra or {}))):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, b)
    oe = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).first()
    return result, oe, b, preview


# ───────────────────────────────────────────────────────────────────
# Correcciones de ambiente: HTTPS y URL (tests 1-7)
# ───────────────────────────────────────────────────────────────────


def test_real_with_http_is_blocked():
    with exec_settings(broker_mode="real", iol_use_sandbox=False,
                       iol_real_api_base="http://api.invertironline.com",
                       iol_real_username="u", iol_real_password="p"):
        env = resolve_execution_environment(get_settings())
    assert "broker_environment_requires_https" in env["errors"]


def test_sandbox_with_http_is_blocked():
    with exec_settings(**_sandbox_settings(iol_sandbox_api_base="http://sandbox.iol-test.local")):
        env = resolve_execution_environment(get_settings())
    assert "broker_environment_requires_https" in env["errors"]


def test_url_with_embedded_credentials_is_blocked():
    with exec_settings(broker_mode="real", iol_use_sandbox=False,
                       iol_real_api_base="https://usuario:password@api.invertironline.com",
                       iol_real_username="u", iol_real_password="p"):
        env = resolve_execution_environment(get_settings())
    assert "broker_environment_url_invalid" in env["errors"]
    with exec_settings(**_sandbox_settings(iol_sandbox_api_base="https://u:p@sandbox.iol-test.local")):
        env = resolve_execution_environment(get_settings())
    assert "broker_environment_url_invalid" in env["errors"]


def test_url_with_query_or_fragment_is_blocked():
    with exec_settings(broker_mode="real", iol_use_sandbox=False,
                       iol_real_api_base="https://api.invertironline.com?token=abc",
                       iol_real_username="u", iol_real_password="p"):
        env = resolve_execution_environment(get_settings())
    assert "broker_environment_url_invalid" in env["errors"]
    with exec_settings(**_sandbox_settings(iol_sandbox_api_base="https://sandbox.iol-test.local#frag")):
        env = resolve_execution_environment(get_settings())
    assert "broker_environment_url_invalid" in env["errors"]


def test_lookalike_host_is_blocked_for_real():
    with exec_settings(broker_mode="real", iol_use_sandbox=False,
                       iol_real_api_base="https://api.invertironline.com.evil.example",
                       iol_real_username="u", iol_real_password="p"):
        env = resolve_execution_environment(get_settings())
    assert "real_environment_invalid" in env["errors"]


def test_real_https_exact_host_is_accepted():
    with exec_settings(broker_mode="real", iol_use_sandbox=False,
                       iol_real_api_base="https://api.invertironline.com",
                       iol_real_username="u", iol_real_password="p"):
        env = resolve_execution_environment(get_settings())
    assert env["errors"] == []
    # Explicit :443 is fine, any other port is not
    with exec_settings(broker_mode="real", iol_use_sandbox=False,
                       iol_real_api_base="https://api.invertironline.com:443",
                       iol_real_username="u", iol_real_password="p"):
        assert resolve_execution_environment(get_settings())["errors"] == []
    with exec_settings(broker_mode="real", iol_use_sandbox=False,
                       iol_real_api_base="https://api.invertironline.com:8443",
                       iol_real_username="u", iol_real_password="p"):
        assert "broker_environment_url_invalid" in resolve_execution_environment(get_settings())["errors"]


def test_sandbox_https_non_real_host_is_accepted():
    with exec_settings(**_sandbox_settings()):
        env = resolve_execution_environment(get_settings())
    assert env["errors"] == []
    assert env["api_base"] == SANDBOX_BASE


# ───────────────────────────────────────────────────────────────────
# Ambiente efectivo como única fuente de verdad (tests 8-12)
# ───────────────────────────────────────────────────────────────────


def _real_plus_use_sandbox(**extra):
    base = _sandbox_settings(**extra)
    base.update(broker_mode="real", iol_use_sandbox=True)
    return base


def test_real_plus_use_sandbox_resolves_to_sandbox():
    with exec_settings(**_real_plus_use_sandbox()):
        assert effective_execution_environment(get_settings()) == "sandbox"
        env = resolve_execution_environment(get_settings())
    assert env["environment"] == "sandbox"
    assert env["api_base"] == SANDBOX_BASE


def test_effective_sandbox_uses_sandbox_lock_only(db):
    """SANDBOX lock closed blocks even with ORDER_EXECUTION_ENABLED=true."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_real_plus_use_sandbox(sandbox_execution_enabled=False,
                                                order_execution_enabled=True)):
        preview = build_execution_preview(db, rec.id)
        result = approve_and_execute(db, rec.id)
    assert "execution_locked" in preview["blocking_reasons"]
    assert result["status_code"] == 423
    assert "SANDBOX_EXECUTION_ENABLED" in result["error"]


def test_effective_sandbox_never_uses_real_url_or_credentials():
    with exec_settings(**_real_plus_use_sandbox(
        iol_real_username="REAL-USER", iol_real_password="REAL-PASS",
        iol_username="LEGACY-USER", iol_password="LEGACY-PASS",
    )):
        env = resolve_execution_environment(get_settings())
    assert "api.invertironline.com" not in env["api_base"]
    assert env["username"] == "sbx-user"
    assert env["password"] == "sbx-pass"


def test_real_lock_does_not_enable_sandbox_and_vice_versa(db):
    """Real effective ignores the sandbox lock entirely."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(
        broker_mode="real", iol_use_sandbox=False,
        iol_real_username="u", iol_real_password="p",
        order_execution_enabled=False, sandbox_execution_enabled=True,
    )):
        preview = build_execution_preview(db, rec.id)
        result = approve_and_execute(db, rec.id)
    assert "execution_locked" in preview["blocking_reasons"]
    assert result["status_code"] == 423
    assert "ORDER_EXECUTION_ENABLED" in result["error"]


def test_preview_signs_effective_environment(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_real_plus_use_sandbox()):
        sandbox_preview = build_execution_preview(db, rec.id)
        gen = datetime.fromisoformat(sandbox_preview["generated_at"])
    assert sandbox_preview["configured_broker_mode"] == "real"
    assert sandbox_preview["effective_environment"] == "sandbox"

    with exec_settings(**_sandbox_settings(
        broker_mode="real", iol_use_sandbox=False,
        iol_real_username="u", iol_real_password="p", order_execution_enabled=True,
    )):
        real_preview = build_execution_preview(db, rec.id, generated_at=gen)
    assert real_preview["effective_environment"] == "real"
    # Same instant, same orders — only the effective environment changed.
    assert real_preview["preview_hash"] != sandbox_preview["preview_hash"]


# ───────────────────────────────────────────────────────────────────
# Execution scope (tests 13-30)
# ───────────────────────────────────────────────────────────────────


def test_empty_scope_blocks_real(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(
        broker_mode="real", iol_use_sandbox=False,
        iol_real_username="u", iol_real_password="p",
        execution_instrument_policies={},
    )):
        preview = build_execution_preview(db, rec.id)
    assert "execution_scope_not_configured" in preview["blocking_reasons"]
    assert preview["can_submit_approval"] is False


def test_empty_scope_blocks_sandbox(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(execution_instrument_policies={})):
        preview = build_execution_preview(db, rec.id)
        result = approve_and_execute(db, rec.id, execution_key=TEST_ADMIN_KEY,
                                     preview_hash=preview["preview_hash"],
                                     preview_generated_at=preview["generated_at"],
                                     confirmation_text=confirmation_phrase(rec.id))
    assert "execution_scope_not_configured" in preview["blocking_reasons"]
    assert result["status_code"] == 423
    assert db.query(OrderExecution).count() == 0


@pytest.mark.parametrize("bad_policy", [
    "not-a-dict",
    {"asset_type": "CEDEAR"},  # missing fields
    {**VALID_POLICY, "quantity_step": 0},
    {**VALID_POLICY, "max_quantity": -1},
    {**VALID_POLICY, "max_notional": float("inf")},
    {**VALID_POLICY, "market": "NYSE"},
    {**VALID_POLICY, "settlement": "t9"},
    {**VALID_POLICY, "currency": ""},
    {**VALID_POLICY, "surprise_field": 1},  # unknown field can't change semantics
])
def test_invalid_policy_is_rejected(bad_policy):
    normalized, error = validate_instrument_policy("AAPL", bad_policy)
    assert normalized is None
    assert error == "instrument_policy_invalid"


def test_invalid_policy_blocks_preview(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(execution_instrument_policies={"AAPL": {"asset_type": "X"}})):
        preview = build_execution_preview(db, rec.id)
    assert "instrument_policy_invalid" in preview["blocking_reasons"]


def test_symbol_without_policy_blocks(db):
    _make_snapshot(db)
    rec = _make_recommendation(db, symbol="AAPL")
    db.commit()
    with exec_settings(**_sandbox_settings(execution_instrument_policies={"MSFT": dict(VALID_POLICY)})):
        preview = build_execution_preview(db, rec.id)
    assert "instrument_policy_missing" in preview["blocking_reasons"]
    assert preview["orders_preview"][0]["valid"] is False


def test_buy_is_blocked_in_sell_only_phase(db):
    _make_snapshot(db)
    rec = _make_recommendation(db, target_pct=0.05)  # buy
    db.commit()
    with exec_settings(**_sandbox_settings()):
        preview = build_execution_preview(db, rec.id)
        result = approve_and_execute(db, rec.id, execution_key=TEST_ADMIN_KEY,
                                     preview_hash=preview["preview_hash"],
                                     preview_generated_at=preview["generated_at"],
                                     confirmation_text=confirmation_phrase(rec.id))
    assert preview["orders_preview"][0]["side"] == "buy"
    assert "buy_execution_disabled" in preview["blocking_reasons"]
    assert preview["orders_preview"][0]["valid"] is False
    assert preview["can_submit_approval"] is False
    assert result["status_code"] in (422, 423)
    assert db.query(OrderExecution).count() == 0


def test_allowed_sell_can_proceed(db):
    result, oe, broker, preview = _run(db)
    assert preview["can_submit_approval"] is True
    assert result["status"] == "approved"
    assert oe.status == "execution_sent"
    broker.submit_order_request.assert_called_once()


@pytest.mark.parametrize("snapshot_kwargs,expected", [
    ({"asset_type": "ACCIONES"}, "instrument_identity_mismatch"),
    ({"instrument_type": "ETF"}, "instrument_identity_mismatch"),
    ({"currency": "ARS"}, "instrument_currency_mismatch"),
])
def test_identity_mismatch_blocks(db, snapshot_kwargs, expected):
    _make_snapshot(db, **snapshot_kwargs)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings()):
        preview = build_execution_preview(db, rec.id)
    assert expected in preview["blocking_reasons"]
    assert preview["can_submit_approval"] is False


def test_market_and_settlement_mismatch_block(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(
        execution_instrument_policies={"AAPL": {**VALID_POLICY, "settlement": "t2"}},
    )):
        preview = build_execution_preview(db, rec.id)
    assert "instrument_settlement_mismatch" in preview["blocking_reasons"]

    with exec_settings(**_sandbox_settings(iol_order_market="bCBA", iol_order_settlement="t2")):
        # global says t2, policy says t1 → mismatch (never silently picked)
        preview2 = build_execution_preview(db, rec.id)
    assert "instrument_settlement_mismatch" in preview2["blocking_reasons"]


def test_quantity_step_mismatch_blocks(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(
        execution_instrument_policies={"AAPL": {**VALID_POLICY, "quantity_step": 3}},
    )):
        preview = build_execution_preview(db, rec.id)  # planned qty is 2
    assert "quantity_step_mismatch" in preview["blocking_reasons"]


def test_symbol_quantity_limit_blocks(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(
        execution_instrument_policies={"AAPL": {**VALID_POLICY, "max_quantity": 1}},
    )):
        preview = build_execution_preview(db, rec.id)
    assert "symbol_quantity_limit_exceeded" in preview["blocking_reasons"]


def test_symbol_notional_limit_blocks(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings(
        execution_instrument_policies={"AAPL": {**VALID_POLICY, "max_notional": 100}},
    )):
        preview = build_execution_preview(db, rec.id)
    assert "symbol_notional_limit_exceeded" in preview["blocking_reasons"]


def test_identity_and_scope_are_in_the_preview_and_signed(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_sandbox_settings()):
        preview = build_execution_preview(db, rec.id)
        gen = datetime.fromisoformat(preview["generated_at"])
        order = preview["orders_preview"][0]
        assert order["instrument_identity"] == {
            "symbol": "AAPL", "asset_type": "CEDEAR", "instrument_type": "CEDEAR",
            "currency": "USD", "market": "bCBA", "settlement": "t1",
        }
        assert order["execution_scope"] == {
            "sell_only": True, "quantity_step": 1.0,
            "max_quantity": 100.0, "max_notional": 1_000_000.0,
        }
        # Changing ONLY the policy invalidates the hash
        with exec_settings(execution_instrument_policies={"AAPL": {**VALID_POLICY, "max_quantity": 99}}):
            changed = build_execution_preview(db, rec.id, generated_at=gen)
    assert changed["preview_hash"] != preview["preview_hash"]


def test_readiness_reports_scope_state():
    with exec_settings(**_sandbox_settings(execution_instrument_policies={})):
        readiness = get_execution_readiness()
    assert readiness["execution_scope_configured"] is False
    assert readiness["allowed_symbols"] == []
    assert readiness["instrument_policy_count"] == 0
    assert "execution_scope_not_configured" in readiness["blocking_reasons"]
    assert readiness["ready_for_sandbox_execution"] is False

    with exec_settings(**_sandbox_settings()):
        ready = get_execution_readiness()
    assert ready["execution_scope_configured"] is True
    assert ready["allowed_symbols"] == ["AAPL"]
    assert ready["instrument_policy_count"] == 1
    assert ready["execution_sell_only"] is True
    assert ready["live_position_check_required"] is True
    assert ready["configured_broker_mode"] == "sandbox"
    assert ready["effective_environment"] == "sandbox"
    serialized = json.dumps(ready)
    for secret in ("sbx-user", "sbx-pass", TEST_ADMIN_KEY, TEST_PREVIEW_SECRET):
        assert secret not in serialized


# ───────────────────────────────────────────────────────────────────
# Live position guard (tests 31-45)
# ───────────────────────────────────────────────────────────────────


def test_live_position_checked_before_quote_and_submitting_and_post(db):
    """Ordering guard: portfolio read happens first, then the quote; the row
    is still execution_requested with quantity_sent=None at both points."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    calls = []
    broker = _broker()

    def spy_portfolio():
        row = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
        calls.append(("portfolio", row.status, row.quantity_sent))
        return _live()

    def spy_quote(*a, **k):
        row = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
        calls.append(("quote", row.status, row.quantity_sent))
        return {"available": True, "price": 1900.0, "source": "bid",
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "market": "bCBA", "settlement": "t1"}

    def spy_submit(req):
        row = db.query(OrderExecution).filter(OrderExecution.recommendation_id == rec.id).one()
        calls.append(("submit", row.status, row.quantity_sent))
        return {"outcome": "sent", "order_id": "S-1", "raw_response": {"numeroOperacion": "S-1"},
                "endpoint_used": "/api/v2/operar/Vender", "error": ""}

    broker.get_portfolio_snapshot.side_effect = spy_portfolio
    broker.get_execution_quote.side_effect = spy_quote
    broker.submit_order_request.side_effect = spy_submit

    with exec_settings(**_sandbox_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _approve(db, rec, preview, broker)

    assert result["status"] == "approved"
    assert [c[0] for c in calls] == ["portfolio", "quote", "submit"]
    assert calls[0][1] == "execution_requested" and calls[0][2] is None
    assert calls[1][1] == "execution_requested" and calls[1][2] is None
    assert calls[2][1] == "submitting" and calls[2][2] == 2


def test_sufficient_live_position_sends_signed_quantity(db):
    result, oe, broker, preview = _run(db, broker=_broker(live=_live(quantity=20)))
    signed_qty = preview["orders_preview"][0]["quantity_planned"]
    assert result["status"] == "approved"
    assert oe.quantity_sent == signed_qty == 2
    req = broker.submit_order_request.call_args[0][0]
    assert req["form_data"]["cantidad"] == "2"


def test_increased_live_position_does_not_increase_quantity(db):
    result, oe, broker, preview = _run(db, broker=_broker(live=_live(quantity=500)))
    assert result["status"] == "approved"
    assert oe.quantity_sent == 2  # signed quantity, never grown
    assert broker.submit_order_request.call_args[0][0]["form_data"]["cantidad"] == "2"


def test_reduced_live_position_blocks_and_never_shrinks(db):
    result, oe, broker, _ = _run(db, broker=_broker(live=_live(quantity=1)))
    assert result["status"] == "execution_failed"
    assert oe.status == "failed"
    assert "live_position_insufficient" in oe.error_message
    assert oe.quantity_sent is None
    broker.submit_order_request.assert_not_called()
    broker.get_execution_quote.assert_not_called()


def test_missing_live_position_blocks(db):
    result, oe, broker, _ = _run(db, broker=_broker(live={"positions": []}))
    assert oe.status == "failed"
    assert "live_position_missing" in oe.error_message
    assert oe.quantity_sent is None
    broker.submit_order_request.assert_not_called()


def test_portfolio_read_failure_blocks(db):
    broker = _broker()
    broker.get_portfolio_snapshot.side_effect = RuntimeError("IOL read timeout")
    result, oe, broker, _ = _run(db, broker=broker)
    assert oe.status == "failed"
    assert "live_position_verification_failed" in oe.error_message
    assert oe.quantity_sent is None
    broker.submit_order_request.assert_not_called()


@pytest.mark.parametrize("live_kwargs,expected", [
    ({"asset_type": "ACCIONES"}, "instrument_identity_mismatch"),
    ({"instrument_type": "ETF"}, "instrument_identity_mismatch"),
    ({"currency": "ARS"}, "instrument_currency_mismatch"),
])
def test_live_identity_mismatch_blocks(db, live_kwargs, expected):
    result, oe, broker, _ = _run(db, broker=_broker(live=_live(**live_kwargs)))
    assert oe.status == "failed"
    assert expected in oe.error_message
    assert oe.quantity_sent is None
    broker.submit_order_request.assert_not_called()


def test_pre_broker_failures_never_produce_reconciliation(db):
    """Every guard failure is definitive: failed, never uncertain."""
    for broker in (
        _broker(live={"positions": []}),
        _broker(live=_live(quantity=1)),
    ):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(bind=engine)
        session = sessionmaker(bind=engine)()
        try:
            result, oe, b, _ = _run(session, broker=broker)
            assert oe.status == "failed"
            assert oe.status != "manual_reconciliation_required"
            assert result["status"] == "execution_failed"
            assert result["status"] != "manual_reconciliation_required"
        finally:
            session.close()


def test_audit_stores_only_the_relevant_position_and_no_credentials(db):
    result, oe, broker, _ = _run(db, broker=_broker(live=_live(quantity=20)))
    audit = oe.broker_response["request_audit"]["live_position_check"]
    assert audit["symbol"] == "AAPL"
    assert audit["quantity_available"] == 20
    assert audit["quantity_required"] == 2
    assert audit["passed"] is True
    assert audit["checked_at"]
    # Only the relevant position — not the whole portfolio
    assert "positions" not in json.dumps(audit)
    serialized = json.dumps(oe.broker_response)
    for secret in ("sbx-user", "sbx-pass", TEST_ADMIN_KEY, TEST_PREVIEW_SECRET, "Bearer", "Authorization"):
        assert secret not in serialized


def test_live_check_can_be_disabled_for_mock_rehearsal(db):
    """With the check disabled the guard does not run (still no resize)."""
    broker = _broker()
    broker.get_portfolio_snapshot.side_effect = AssertionError("should not be called")
    result, oe, b, _ = _run(db, settings_extra={"execution_require_live_position_check": False}, broker=broker)
    assert result["status"] == "approved"
    assert oe.quantity_sent == 2


def test_second_approval_does_not_post_again(db):
    result, oe, broker, preview = _run(db)
    assert result["status"] == "approved"
    with exec_settings(**_sandbox_settings()):
        second = approve_and_execute(db, oe.recommendation_id)
    assert second["status_code"] == 409
    assert broker.submit_order_request.call_count == 1


def test_mock_legacy_flow_unaffected_by_scope(db):
    """Mock effective environment is exempt from scope/live-position gates."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(broker_mode="mock", iol_use_sandbox=False,
                       execution_instrument_policies={}, execution_sell_only=True):
        result = approve_and_execute(db, rec.id)
    assert result["status"] == "approved"
