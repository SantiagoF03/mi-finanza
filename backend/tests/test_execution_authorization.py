"""Execution Authorization V1 — mandatory test coverage.

Covers: unified safety lock semantics, canonical signed preview with expiry,
reinforced approve contract (execution key + confirmation phrase + preview
hash), execution limits, double-execution protection, and invariant checks
(scheduler/LLM/notifications have no route to approve).

NO real IOL calls happen here: broker real paths use fully mocked brokers and
IolBrokerClient is patched to explode if anything tries to instantiate it.
"""

import json
import re
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.db.session import Base
from app.models.models import (
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
    UserDecision,
)
from app.services.execution import (
    approve_and_execute,
    build_execution_preview,
    confirmation_phrase,
    preview_execution_plan,
)

BACKEND_DIR = Path(__file__).resolve().parent.parent

TEST_ADMIN_KEY = "test-execution-admin-key"
TEST_PREVIEW_SECRET = "test-preview-secret"


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _make_snapshot(db: Session, total_value=100000, cash=12000) -> PortfolioSnapshot:
    snap = PortfolioSnapshot(total_value=total_value, cash=cash, currency="USD")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="AAPL", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="USD", quantity=20,
        market_value=38000, avg_price=180, pnl_pct=0.11,
    ))
    db.flush()
    return snap


def _make_recommendation(db: Session, action="rebalancear", status="pending", target_pct=-0.05) -> Recommendation:
    rec = Recommendation(
        action=action, status=status, suggested_pct=0.05, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(
        recommendation_id=rec.id, symbol="AAPL",
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


def _full_auth_settings(**extra):
    """Settings for a fully-configured reinforced flow (mock by default)."""
    base = dict(
        broker_mode="mock",
        order_execution_enabled=False,
        execution_admin_key=TEST_ADMIN_KEY,
        execution_preview_secret=TEST_PREVIEW_SECRET,
        execution_preview_ttl_seconds=300,
        execution_max_recommendation_age_minutes=60,
        execution_max_order_value=1_000_000.0,
        execution_max_total_value=1_000_000.0,
        execution_max_portfolio_pct=0.50,
    )
    base.update(extra)
    return base


def _reinforced_approve(db, rec, preview, key=TEST_ADMIN_KEY, phrase=None, **overrides):
    kwargs = dict(
        execution_key=key,
        preview_hash=preview["preview_hash"],
        preview_generated_at=preview["generated_at"],
        confirmation_text=phrase if phrase is not None else confirmation_phrase(rec.id),
    )
    kwargs.update(overrides)
    return approve_and_execute(db, rec.id, **kwargs)


# ───────────────────────────────────────────────────────────────────
# Configuración y gates (tests 1-8)
# ───────────────────────────────────────────────────────────────────


def test_order_execution_enabled_declared_once():
    src = (BACKEND_DIR / "app" / "core" / "config.py").read_text()
    declarations = re.findall(r"^\s*order_execution_enabled\s*:", src, re.MULTILINE)
    assert len(declarations) == 1


def test_real_broker_lock_false_returns_423(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(broker_mode="real", order_execution_enabled=False):
        result = approve_and_execute(db, rec.id)
    assert result["status_code"] == 423
    assert "error" in result


def test_423_happens_before_any_state_change(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(broker_mode="real", order_execution_enabled=False):
        approve_and_execute(db, rec.id)
    db.expire_all()
    assert db.get(Recommendation, rec.id).status == "pending"


def test_423_creates_no_user_decision(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(broker_mode="real", order_execution_enabled=False):
        approve_and_execute(db, rec.id)
    assert db.query(UserDecision).count() == 0


def test_423_creates_no_order_execution(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(broker_mode="real", order_execution_enabled=False):
        approve_and_execute(db, rec.id)
    assert db.query(OrderExecution).count() == 0


def test_423_never_instantiates_real_broker(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(broker_mode="real", order_execution_enabled=False):
        with mock.patch("app.services.execution.IolBrokerClient", side_effect=AssertionError("IOL instantiated")) as m:
            result = approve_and_execute(db, rec.id)
    assert result["status_code"] == 423
    m.assert_not_called()


def test_mock_lock_false_legacy_flow_works(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(broker_mode="mock", order_execution_enabled=False):
        result = approve_and_execute(db, rec.id)
    assert result.get("status") == "approved"
    assert "error" not in result


def test_mock_flow_never_calls_iol_client(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(broker_mode="mock", order_execution_enabled=False):
        with mock.patch("app.services.execution.IolBrokerClient", side_effect=AssertionError("IOL instantiated")) as m:
            result = approve_and_execute(db, rec.id)
    assert result.get("status") == "approved"
    m.assert_not_called()


# ───────────────────────────────────────────────────────────────────
# Preview (tests 9-21)
# ───────────────────────────────────────────────────────────────────


def test_preview_does_not_modify_database(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings()):
        build_execution_preview(db, rec.id)
    db.expire_all()
    assert db.get(Recommendation, rec.id).status == "pending"
    assert db.query(OrderExecution).count() == 0
    assert db.query(UserDecision).count() == 0


def test_preview_never_calls_broker_or_place_order(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with mock.patch("app.services.execution._get_execution_broker") as broker_factory, \
         mock.patch("app.services.execution.IolBrokerClient") as iol, \
         mock.patch("app.services.execution.MockBrokerClient") as mock_client:
        build_execution_preview(db, rec.id)
    broker_factory.assert_not_called()
    iol.assert_not_called()
    mock_client.assert_not_called()


def test_preview_dry_run_and_would_execute(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    preview = build_execution_preview(db, rec.id)
    assert preview["dry_run"] is True
    assert preview["would_execute"] is False


def test_preview_includes_snapshot_and_recommendation_ids(db):
    snap = _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    preview = build_execution_preview(db, rec.id)
    assert preview["recommendation_id"] == rec.id
    assert preview["snapshot_id"] == snap.id
    assert preview["recommendation_created_at"] is not None


def test_preview_includes_estimated_notional_and_pct(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    preview = build_execution_preview(db, rec.id)
    order = preview["orders_preview"][0]
    # AAPL: price 1900, target 5% of 100k = 5000 → qty 2 → notional 3800
    assert order["estimated_notional"] == pytest.approx(3800.0)
    assert order["portfolio_pct"] == pytest.approx(0.038)


def test_preview_hmac_signature_when_secret_configured(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
    assert preview["preview_hash"]
    assert len(preview["preview_hash"]) == 64  # hex sha256
    # Deterministic: same generated_at → same hash
    with exec_settings(**_full_auth_settings()):
        again = build_execution_preview(
            db, rec.id, generated_at=datetime.fromisoformat(preview["generated_at"])
        )
    assert again["preview_hash"] == preview["preview_hash"]


def test_preview_without_secret_has_no_executable_signature(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings(execution_preview_secret="")):
        preview = build_execution_preview(db, rec.id)
    assert preview["preview_hash"] == ""
    assert preview["can_submit_approval"] is False
    assert "preview_signing_not_configured" in preview["blocking_reasons"]


def test_preview_includes_expires_at(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings(execution_preview_ttl_seconds=300)):
        preview = build_execution_preview(db, rec.id)
    gen = datetime.fromisoformat(preview["generated_at"])
    exp = datetime.fromisoformat(preview["expires_at"])
    assert (exp - gen).total_seconds() == 300


def test_preview_blocking_reasons_codes(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(
        broker_mode="real",
        order_execution_enabled=False,
        execution_admin_key="",
        execution_preview_secret="",
        execution_max_order_value=0.0,
        execution_max_total_value=0.0,
        execution_max_portfolio_pct=0.0,
    ):
        preview = build_execution_preview(db, rec.id)
    reasons = preview["blocking_reasons"]
    assert "execution_locked" in reasons
    assert "execution_admin_key_not_configured" in reasons
    assert "preview_signing_not_configured" in reasons
    assert "execution_limits_not_configured" in reasons
    assert preview["can_submit_approval"] is False


def test_preview_can_submit_true_when_fully_configured_mock(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
    assert preview["blocking_reasons"] == []
    assert preview["can_submit_approval"] is True


def test_preview_does_not_expose_secrets(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
    serialized = json.dumps(preview)
    assert TEST_ADMIN_KEY not in serialized
    assert TEST_PREVIEW_SECRET not in serialized


# ───────────────────────────────────────────────────────────────────
# Approve reforzado (tests 22-37)
# ───────────────────────────────────────────────────────────────────


def test_api_key_alone_without_execution_key_insufficient(db):
    """The normal API key gate passes at the route layer; without the
    execution credential the service still refuses (403)."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings(broker_mode="real", order_execution_enabled=True)):
        preview = build_execution_preview(db, rec.id)
        result = _reinforced_approve(db, rec, preview, key=None)
    assert result["status_code"] == 403
    assert db.query(UserDecision).count() == 0


def test_wrong_execution_key_403_no_changes(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings(broker_mode="real", order_execution_enabled=True)):
        preview = build_execution_preview(db, rec.id)
        result = _reinforced_approve(db, rec, preview, key="wrong-key")
    assert result["status_code"] == 403
    db.expire_all()
    assert db.get(Recommendation, rec.id).status == "pending"
    assert db.query(UserDecision).count() == 0
    assert db.query(OrderExecution).count() == 0


def test_lock_false_423_even_with_valid_keys(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings(broker_mode="real", order_execution_enabled=False)):
        preview = build_execution_preview(db, rec.id)
        result = _reinforced_approve(db, rec, preview)
    assert result["status_code"] == 423


def test_wrong_confirmation_phrase_422(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _reinforced_approve(db, rec, preview, phrase=f"ejecutar recomendacion {rec.id}")
    assert result["status_code"] == 422
    assert result["code"] == "confirmation_mismatch"


def test_expired_preview_409(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    stale_gen = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat()
    with exec_settings(**_full_auth_settings(execution_preview_ttl_seconds=300)):
        preview = build_execution_preview(db, rec.id)
        result = _reinforced_approve(db, rec, preview, preview_generated_at=stale_gen)
    assert result["status_code"] == 409
    assert result["code"] == "preview_expired"


def test_tampered_hash_409(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        bad_hash = ("0" if preview["preview_hash"][0] != "0" else "1") + preview["preview_hash"][1:]
        result = _reinforced_approve(db, rec, preview, preview_hash=bad_hash)
    assert result["status_code"] == 409
    assert result["code"] == "preview_mismatch"


def test_new_snapshot_after_preview_409(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        _make_snapshot(db, total_value=200000)  # portfolio changed after review
        db.commit()
        result = _reinforced_approve(db, rec, preview)
    assert result["status_code"] == 409
    assert result["code"] == "preview_mismatch"
    assert db.query(OrderExecution).count() == 0


def test_modified_actions_after_preview_409(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        action = db.query(RecommendationAction).filter(
            RecommendationAction.recommendation_id == rec.id
        ).first()
        action.target_change_pct = -0.10  # someone changed the plan
        db.commit()
        result = _reinforced_approve(db, rec, preview)
    assert result["status_code"] == 409
    assert result["code"] == "preview_mismatch"


def test_superseded_recommendation_409(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    rec.superseded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    db.commit()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        result = _reinforced_approve(db, rec, preview)
    assert result["status_code"] == 409
    assert result["code"] == "recommendation_superseded"


def test_stale_recommendation_409(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    rec.created_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=3)
    db.commit()
    with exec_settings(**_full_auth_settings(execution_max_recommendation_age_minutes=60)):
        preview = build_execution_preview(db, rec.id)
        assert "recommendation_stale" in preview["blocking_reasons"]
        result = _reinforced_approve(db, rec, preview)
    assert result["status_code"] == 409
    assert result["code"] == "recommendation_stale"


def test_invalid_order_422(db):
    _make_snapshot(db)
    rec = Recommendation(
        action="rebalancear", status="pending", suggested_pct=0.05, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json={},
    )
    db.add(rec)
    db.flush()
    db.add(RecommendationAction(
        recommendation_id=rec.id, symbol="GHOST",  # not in portfolio → invalid sell
        target_change_pct=-0.05, reason="test",
    ))
    db.commit()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        assert "invalid_order" in preview["blocking_reasons"]
        result = _reinforced_approve(db, rec, preview)
    assert result["status_code"] == 422
    assert result["code"] == "invalid_order"
    assert db.query(OrderExecution).count() == 0


def test_order_limit_exceeded_422(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings(execution_max_order_value=1000.0)):
        preview = build_execution_preview(db, rec.id)
        assert "order_limit_exceeded" in preview["blocking_reasons"]
        result = _reinforced_approve(db, rec, preview)
    assert result["status_code"] == 422
    assert result["code"] == "order_limit_exceeded"


def test_total_limit_exceeded_422(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings(execution_max_order_value=5000.0, execution_max_total_value=1000.0)):
        preview = build_execution_preview(db, rec.id)
        assert "total_limit_exceeded" in preview["blocking_reasons"]
        result = _reinforced_approve(db, rec, preview)
    assert result["status_code"] == 422
    assert result["code"] == "total_limit_exceeded"


def test_portfolio_pct_limit_exceeded_422(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings(execution_max_portfolio_pct=0.01)):
        preview = build_execution_preview(db, rec.id)
        assert "portfolio_pct_limit_exceeded" in preview["blocking_reasons"]
        result = _reinforced_approve(db, rec, preview)
    assert result["status_code"] == 422
    assert result["code"] == "portfolio_pct_limit_exceeded"


def test_zero_limits_block_reinforced_execution(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings(
        execution_max_order_value=0.0,
        execution_max_total_value=0.0,
        execution_max_portfolio_pct=0.0,
    )):
        preview = build_execution_preview(db, rec.id)
        assert "execution_limits_not_configured" in preview["blocking_reasons"]
        result = _reinforced_approve(db, rec, preview)
    assert result["status_code"] == 423
    assert result["code"] == "execution_limits_not_configured"


def test_all_validations_run_before_place_order(db):
    """A failing validation (wrong phrase) must never reach place_order."""
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    fake_broker = mock.MagicMock()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_execution_broker", return_value=fake_broker):
            result = _reinforced_approve(db, rec, preview, phrase="FRASE INCORRECTA")
    assert result["status_code"] == 422
    fake_broker.place_order.assert_not_called()


# ───────────────────────────────────────────────────────────────────
# Doble ejecución (tests 38-39)
# ───────────────────────────────────────────────────────────────────


def test_second_approval_does_not_call_broker_again(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    fake_broker = mock.MagicMock()
    fake_broker.place_order.return_value = {"status": "sent", "order_id": "MOCK-1", "raw_response": {}, "endpoint_used": "mock"}
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_execution_broker", return_value=fake_broker):
            first = _reinforced_approve(db, rec, preview)
            second = _reinforced_approve(db, rec, preview)
    assert first.get("status") == "approved"
    assert "error" in second
    assert second["status_code"] in (400, 409)
    assert fake_broker.place_order.call_count == 1


def test_repeated_requests_call_place_order_at_most_once(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    fake_broker = mock.MagicMock()
    fake_broker.place_order.return_value = {"status": "sent", "order_id": "MOCK-1", "raw_response": {}, "endpoint_used": "mock"}
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        with mock.patch("app.services.execution._get_execution_broker", return_value=fake_broker):
            for _ in range(5):
                approve_and_execute(
                    db, rec.id,
                    execution_key=TEST_ADMIN_KEY,
                    preview_hash=preview["preview_hash"],
                    preview_generated_at=preview["generated_at"],
                    confirmation_text=confirmation_phrase(rec.id),
                )
    assert fake_broker.place_order.call_count == 1
    assert db.query(UserDecision).filter(UserDecision.decision == "approved").count() == 1


def test_prior_order_execution_blocks_new_approval(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.add(OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=None, symbol="AAPL",
        side="sell", target_change_pct=-0.05, status="execution_sent",
    ))
    db.commit()
    with exec_settings(broker_mode="mock", order_execution_enabled=False):
        result = approve_and_execute(db, rec.id)
    assert result["status_code"] == 409
    assert result["code"] == "already_executed"


# ───────────────────────────────────────────────────────────────────
# Invariantes: nadie más tiene ruta hacia approve (tests 40-43)
# ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("module_path", [
    "app/scheduler/jobs.py",
    "app/services/orchestrator.py",
    "app/llm/explainer.py",
    "app/notifications/dispatcher.py",
])
def test_no_route_to_approve(module_path):
    src = (BACKEND_DIR / module_path).read_text()
    assert "approve_and_execute" not in src, f"{module_path} must not reference approve_and_execute"
    assert "place_order" not in src, f"{module_path} must not reference place_order"


# ───────────────────────────────────────────────────────────────────
# Flujos exitosos (tests 44-45)
# ───────────────────────────────────────────────────────────────────


def test_full_mock_flow_with_reinforced_contract(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    with exec_settings(**_full_auth_settings()):
        preview = build_execution_preview(db, rec.id)
        assert preview["can_submit_approval"] is True
        with mock.patch("app.services.execution.IolBrokerClient", side_effect=AssertionError("IOL touched")):
            result = _reinforced_approve(db, rec, preview)
    assert result.get("status") == "approved"
    assert len(result["executions"]) == 1
    db.expire_all()
    assert db.get(Recommendation, rec.id).status == "approved"
    assert db.query(UserDecision).count() == 1
    assert db.query(OrderExecution).count() == 1


def test_simulated_real_flow_reaches_place_order_only_after_all_validations(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    fake_broker = mock.MagicMock()
    fake_broker.place_order.return_value = {"status": "sent", "order_id": "SIM-1", "raw_response": {}, "endpoint_used": "sim"}
    with exec_settings(**_full_auth_settings(broker_mode="real", order_execution_enabled=True)):
        preview = build_execution_preview(db, rec.id)
        assert preview["can_submit_approval"] is True
        with mock.patch("app.services.execution._get_execution_broker", return_value=fake_broker):
            # First: failing validations never reach the broker
            bad = _reinforced_approve(db, rec, preview, key="nope")
            assert bad["status_code"] == 403
            fake_broker.place_order.assert_not_called()
            # Then: full valid contract reaches place_order exactly once
            result = _reinforced_approve(db, rec, preview)
    assert result.get("status") == "approved"
    assert fake_broker.place_order.call_count == 1


# ───────────────────────────────────────────────────────────────────
# Config validators y misc
# ───────────────────────────────────────────────────────────────────


def test_settings_reject_non_positive_ttl_and_age():
    from pydantic import ValidationError
    from app.core.config import Settings

    with pytest.raises(ValidationError):
        Settings(execution_preview_ttl_seconds=0)
    with pytest.raises(ValidationError):
        Settings(execution_max_recommendation_age_minutes=-5)


def test_preview_alias_still_works(db):
    _make_snapshot(db)
    rec = _make_recommendation(db)
    db.commit()
    result = preview_execution_plan(db, rec.id)
    assert result["dry_run"] is True
    assert result["recommendation_id"] == rec.id
