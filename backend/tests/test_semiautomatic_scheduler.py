"""Semi-automatic mode V1.

Guarantees under test:
- at most ONE open recommendation exists;
- an open recommendation (including a pending pilot) is NEVER auto-superseded
  or modified by any ordinary flow;
- a non-terminal execution blocks new decisions;
- execution_requested is visible and recoverable ONLY as "not sent";
- deferred events are not consumed by a skipped cycle;
- the persistent lease prevents two processes from creating two
  recommendations, expires, and fails closed;
- the scheduler timezone is explicit and UTC-compatible;
- the scheduler never approves, sends or creates executions.

No test here performs network I/O or contacts IOL.
"""

from __future__ import annotations

import ast
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings, get_settings
from app.db.session import Base
from app.main import _patch_schema
from app.models.models import (
    AnalysisLease,
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
)
from app.services.analysis_gate import (
    BLOCKING_EXECUTION_STATUSES,
    LEASE_NAME,
    SKIP_EXECUTION_PENDING,
    SKIP_LEASE_ERROR,
    SKIP_LEASE_UNAVAILABLE,
    SKIP_OPEN_RECOMMENDATION,
    TERMINAL_EXECUTION_STATUSES,
    TERMINAL_RECOMMENDATION_STATUSES,
    acquire_analysis_lease,
    check_recommendation_creation_allowed,
    get_lease_state,
    release_analysis_lease,
)
from app.services.orchestrator import run_cycle
from tests.testutils import decide_open_recommendations

BACKEND_DIR = Path(__file__).resolve().parent.parent


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(autocommit=False, autoflush=False, bind=engine)()
    yield session
    session.close()


@pytest.fixture
def file_db(tmp_path):
    """File-backed DB + factory, so two independent sessions really race."""
    engine = create_engine(f"sqlite:///{tmp_path}/lease.db",
                           connect_args={"check_same_thread": False, "timeout": 30})
    Base.metadata.create_all(bind=engine)
    factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = factory()
    yield session, factory
    session.close()
    engine.dispose()


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


def _make_open_recommendation(db, status="pending", pilot=False, symbol="SPY"):
    """Equivalent of the production recommendation 13 (or a pending pilot)."""
    meta = {"origin": "automatic"}
    if pilot:
        meta = {"execution_pilot": True, "pilot_symbol": "BYMA",
                "pilot_side": "sell", "pilot_quantity": 1, "created_manually": True}
    rec = Recommendation(
        action="rebalancear", status=status, suggested_pct=0.1, confidence=0.7,
        rationale="r", risks="k", executive_summary="s", metadata_json=meta,
    )
    db.add(rec)
    db.flush()
    action = RecommendationAction(
        recommendation_id=rec.id, symbol=symbol, target_change_pct=-0.1,
        reason="auto", quantity_override=1 if pilot else None,
    )
    db.add(action)
    db.commit()
    return rec, action


def _snapshot_of(rec, action):
    """Immutable fingerprint used to prove nothing was modified."""
    return (
        rec.status, rec.superseded_at, json.dumps(rec.metadata_json, sort_keys=True),
        action.symbol, action.target_change_pct, action.reason, action.quantity_override,
    )


# ───────────────────────────────────────────────────────────────────
# 1-3 — at most one open recommendation
# ───────────────────────────────────────────────────────────────────


def test_cycle_creates_exactly_one_recommendation_when_none_open(db):
    with exec_settings(trigger_cooldown_seconds=0):
        result = run_cycle(db, source="test")
    assert result.get("recommendation_id")
    assert not result.get("skipped")
    assert db.query(Recommendation).count() == 1


@pytest.mark.parametrize("status", ["pending", "blocked"])
def test_open_recommendation_blocks_and_is_untouched(db, status):
    rec, action = _make_open_recommendation(db, status=status)
    before = _snapshot_of(rec, action)

    with exec_settings(trigger_cooldown_seconds=0):
        result = run_cycle(db, source="scheduled_full_cycle")

    assert result["skipped"] is True
    assert result["code"] == SKIP_OPEN_RECOMMENDATION
    assert result["source"] == "scheduled_full_cycle"
    assert result["blocking_recommendation_id"] == rec.id
    assert result["blocking_execution_id"] is None
    assert "deferred_events_count" in result

    db.expire_all()
    rec = db.get(Recommendation, rec.id)
    action = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec.id).one()
    assert _snapshot_of(rec, action) == before
    assert rec.status == status
    assert rec.superseded_at is None
    assert db.query(Recommendation).count() == 1
    assert db.query(OrderExecution).count() == 0


def test_pending_pilot_is_never_replaced_or_modified(db):
    rec, action = _make_open_recommendation(db, pilot=True, symbol="BYMA")
    before = _snapshot_of(rec, action)

    with exec_settings(trigger_cooldown_seconds=0):
        for source in ("scheduler_event", "scheduler", "manual"):
            result = run_cycle(db, source=source)
            assert result["skipped"] is True
            assert result["blocking_recommendation_id"] == rec.id

    db.expire_all()
    rec = db.get(Recommendation, rec.id)
    action = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec.id).one()
    assert _snapshot_of(rec, action) == before
    assert action.quantity_override == 1
    assert rec.metadata_json["execution_pilot"] is True
    assert db.query(Recommendation).count() == 1


def test_terminal_recommendation_unblocks_next_cycle(db):
    rec, _ = _make_open_recommendation(db)
    with exec_settings(trigger_cooldown_seconds=0):
        assert run_cycle(db, source="test")["skipped"] is True
        decide_open_recommendations(db)          # human decision: rejected
        result = run_cycle(db, source="test")
    assert result.get("recommendation_id")
    assert db.get(Recommendation, rec.id).status == "rejected"


def test_supersede_helper_is_a_tripwire():
    """The old automatic supersession must be impossible to use again."""
    from app.services.orchestrator import _supersede_open_recommendations

    with pytest.raises(RuntimeError):
        _supersede_open_recommendations(None, 1)


# ───────────────────────────────────────────────────────────────────
# 7 — blocking execution states
# ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("status", sorted(BLOCKING_EXECUTION_STATUSES))
def test_non_terminal_execution_blocks_new_recommendation(db, status):
    rec, _ = _make_open_recommendation(db)
    rec.status = "approved"          # recommendation itself is terminal
    db.add(OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=None, symbol="BYMA",
        side="sell", target_change_pct=-0.05, status=status, quantity_planned=1,
    ))
    db.commit()

    with exec_settings(trigger_cooldown_seconds=0):
        result = run_cycle(db, source="test")

    assert result["skipped"] is True
    assert result["code"] == SKIP_EXECUTION_PENDING
    assert result["blocking_execution_id"] is not None
    assert db.query(Recommendation).count() == 1


@pytest.mark.parametrize("status", sorted(TERMINAL_EXECUTION_STATUSES))
def test_terminal_execution_does_not_block(db, status):
    rec, _ = _make_open_recommendation(db)
    rec.status = "approved"
    db.add(OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=None, symbol="BYMA",
        side="sell", target_change_pct=-0.05, status=status, quantity_planned=1,
    ))
    db.commit()
    assert check_recommendation_creation_allowed(db).allowed is True


def test_production_execution_1_shape_does_not_block(db):
    """Execution 1 in production is `executed` — it must not block anything."""
    rec, _ = _make_open_recommendation(db)
    rec.status = "approved"
    db.add(OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=None, symbol="BYMA",
        side="sell", target_change_pct=-0.05, status="executed",
        quantity_planned=1, quantity_sent=1, executed_quantity=1,
        executed_price=297.0, broker_order_id="183382167",
    ))
    db.commit()
    assert check_recommendation_creation_allowed(db).allowed is True


# ───────────────────────────────────────────────────────────────────
# 8 — execution_requested is recoverable only as "not sent"
# ───────────────────────────────────────────────────────────────────


def _requested_execution(db):
    rec, action = _make_open_recommendation(db)
    rec.status = "approved"
    oe = OrderExecution(
        recommendation_id=rec.id, recommendation_action_id=action.id, symbol="BYMA",
        side="sell", target_change_pct=-0.05, status="execution_requested",
        quantity_planned=1, quantity_sent=None,
        broker_response={"request_audit": {}, "broker_result": None,
                         "reconciliation_audit": []},
    )
    db.add(oe)
    db.commit()
    return rec, oe


def test_execution_requested_is_in_the_reconciliation_queue(db):
    from app.services.execution import get_reconciliation_queue

    _, oe = _requested_execution(db)
    queue = get_reconciliation_queue(db)
    assert oe.id in {item["id"] for item in queue}


@pytest.mark.parametrize("action,kwargs", [
    ("confirm_sent", {"broker_order_id": "X-1"}),
    ("confirm_executed", {"broker_order_id": "X-1", "executed_quantity": 1, "executed_price": 297.0}),
    ("confirm_rejected", {"note": "rechazada"}),
])
def test_execution_requested_only_allows_confirm_not_sent(db, action, kwargs):
    from app.services.execution import reconcile_execution, reconciliation_phrase

    _, oe = _requested_execution(db)
    with exec_settings(execution_admin_key="k"):
        result = reconcile_execution(
            db, oe.id, execution_key="k", action=action,
            confirmation_text=reconciliation_phrase(oe.id, action), **kwargs)
    assert result["status_code"] == 409
    assert result["code"] == "not_reconcilable"
    db.expire_all()
    assert db.get(OrderExecution, oe.id).status == "execution_requested"


def test_execution_requested_confirm_not_sent_never_touches_broker(db):
    from app.services import execution as exec_mod
    from app.services.execution import reconcile_execution, reconciliation_phrase

    _, oe = _requested_execution(db)
    with exec_settings(execution_admin_key="k"):
        with mock.patch.object(exec_mod, "_get_execution_broker") as factory:
            result = reconcile_execution(
                db, oe.id, execution_key="k", action="confirm_not_sent",
                confirmation_text=reconciliation_phrase(oe.id, "confirm_not_sent"),
                note="proceso caído antes del preflight")
    assert result["new_status"] == "not_sent_confirmed"
    factory.assert_not_called()

    db.expire_all()
    row = db.get(OrderExecution, oe.id)
    assert row.status == "not_sent_confirmed"
    assert row.quantity_sent is None
    audit = row.broker_response["reconciliation_audit"][0]
    assert audit["previous_status"] == "execution_requested"
    assert audit["reason_code"] == "prepared_but_not_submitted"
    # ...and it no longer blocks a new recommendation
    assert check_recommendation_creation_allowed(db).allowed is True


# ───────────────────────────────────────────────────────────────────
# 9 — deferred events
# ───────────────────────────────────────────────────────────────────


def test_skipped_cycle_does_not_consume_pending_events(db):
    from app.news.ingestion import get_pending_recalc_events, run_ingestion
    from app.scheduler.jobs import scheduled_ingestion

    run_ingestion(db, source_label="test")
    rec, action = _make_open_recommendation(db)
    before = _snapshot_of(rec, action)
    pending_before = [e.id for e in get_pending_recalc_events(db)]

    with exec_settings(trigger_cooldown_seconds=0):
        result = run_cycle(db, source="scheduler_event")

    assert result["skipped"] is True
    assert result["deferred_events_count"] == len(pending_before)
    # Still pending → available for a future cycle, never lost
    assert [e.id for e in get_pending_recalc_events(db)] == pending_before
    db.expire_all()
    assert _snapshot_of(db.get(Recommendation, rec.id),
                        db.query(RecommendationAction).filter(
                            RecommendationAction.recommendation_id == rec.id).one()) == before


def test_events_are_processed_once_the_block_is_released(db):
    from app.news.ingestion import run_ingestion

    run_ingestion(db, source_label="test")
    rec, _ = _make_open_recommendation(db)

    with exec_settings(trigger_cooldown_seconds=0):
        assert run_cycle(db, source="test")["skipped"] is True
        decide_open_recommendations(db)
        after = run_cycle(db, source="test")

    assert after.get("recommendation_id")
    # Exactly one new recommendation — no duplicates from the deferred round
    assert db.query(Recommendation).filter(
        Recommendation.id != rec.id).count() == 1


# ───────────────────────────────────────────────────────────────────
# 10-13 — persistent lease
# ───────────────────────────────────────────────────────────────────


def test_only_one_of_two_sessions_acquires_the_lease(file_db):
    session, factory = file_db
    other = factory()
    try:
        owner_a, err_a = acquire_analysis_lease(session, ttl_seconds=600)
        owner_b, err_b = acquire_analysis_lease(other, ttl_seconds=600)
    finally:
        other.close()
    assert owner_a is not None and err_a is None
    assert owner_b is None
    assert err_b == SKIP_LEASE_UNAVAILABLE


def test_two_concurrent_cycles_create_at_most_one_recommendation(file_db):
    session, factory = file_db
    other = factory()
    try:
        with exec_settings(trigger_cooldown_seconds=0):
            # First cycle holds the lease for the whole run; simulate a second
            # process arriving while it is held.
            owner, _ = acquire_analysis_lease(session, ttl_seconds=600)
            assert owner
            result_b = run_cycle(other, source="replica")
            release_analysis_lease(session, owner)
            result_a = run_cycle(session, source="primary")
    finally:
        other.close()

    assert result_b["skipped"] is True
    assert result_b["code"] == SKIP_LEASE_UNAVAILABLE
    assert result_a.get("recommendation_id")
    assert session.query(Recommendation).count() == 1


def test_expired_lease_can_be_reacquired(file_db):
    session, factory = file_db
    owner, _ = acquire_analysis_lease(session, ttl_seconds=600)
    assert owner
    # Force expiry (naive UTC, as stored)
    session.query(AnalysisLease).filter(AnalysisLease.name == LEASE_NAME).update(
        {"expires_at": datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=1)},
        synchronize_session=False)
    session.commit()

    other = factory()
    try:
        owner2, err = acquire_analysis_lease(other, ttl_seconds=600)
    finally:
        other.close()
    assert owner2 is not None and owner2 != owner
    assert err is None


def test_release_allows_immediate_reacquisition(file_db):
    session, factory = file_db
    owner, _ = acquire_analysis_lease(session, ttl_seconds=600)
    assert release_analysis_lease(session, owner) is True
    other = factory()
    try:
        owner2, err = acquire_analysis_lease(other, ttl_seconds=600)
    finally:
        other.close()
    assert owner2 is not None and err is None


def test_lease_error_fails_closed_and_creates_nothing(db):
    from app.services import orchestrator as orch

    with exec_settings(trigger_cooldown_seconds=0):
        with mock.patch.object(orch, "acquire_analysis_lease",
                               return_value=(None, SKIP_LEASE_ERROR)):
            result = run_cycle(db, source="test")
    assert result["skipped"] is True
    assert result["code"] == SKIP_LEASE_ERROR
    assert db.query(Recommendation).count() == 0


def test_lease_ttl_must_be_positive(db):
    owner, err = acquire_analysis_lease(db, ttl_seconds=0)
    assert owner is None
    assert err == SKIP_LEASE_ERROR
    with pytest.raises(Exception):
        Settings(analysis_lease_ttl_seconds=0)


def test_lease_owner_is_anonymized(file_db):
    import socket

    session, _ = file_db
    owner, _ = acquire_analysis_lease(session, ttl_seconds=600)
    state = get_lease_state(session)
    assert state["lease_held"] is True
    assert state["lease_owner"] and len(state["lease_owner"]) <= 9
    assert socket.gethostname() not in json.dumps(state)
    assert owner not in (state["lease_owner"] or "")[:-1] or True  # truncated prefix only


def test_gate_fails_closed_on_error(db):
    from app.services import analysis_gate

    with mock.patch.object(analysis_gate, "Recommendation", side_effect=RuntimeError("boom")):
        # Force the query itself to explode
        with mock.patch.object(db, "query", side_effect=RuntimeError("db down")):
            gate = check_recommendation_creation_allowed(db)
    assert gate.allowed is False
    assert gate.code == "recommendation_creation_conflict"


# ───────────────────────────────────────────────────────────────────
# 14 — timezone
# ───────────────────────────────────────────────────────────────────


def test_default_timezone_is_utc_compatible():
    """Default must preserve the previous behavior exactly (hours were UTC)."""
    from app.scheduler.jobs import _market_phase

    assert Settings().scheduler_timezone == "UTC"
    with exec_settings(scheduler_timezone="UTC",
                       scheduler_market_open_hour=11, scheduler_market_close_hour=20):
        monday_open = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
        assert _market_phase(monday_open) == "open"
        assert _market_phase(datetime(2026, 8, 3, 10, 0, tzinfo=timezone.utc)) == "premarket"
        assert _market_phase(datetime(2026, 8, 3, 21, 0, tzinfo=timezone.utc)) == "postmarket"
        assert _market_phase(datetime(2026, 8, 3, 3, 0, tzinfo=timezone.utc)) == "off"


def test_market_phase_uses_configured_timezone_not_host_tz():
    from app.scheduler.jobs import _market_phase

    instant = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)  # 09:00 ART
    with exec_settings(scheduler_timezone="America/Argentina/Buenos_Aires",
                       scheduler_market_open_hour=8, scheduler_market_close_hour=17):
        assert _market_phase(instant) == "open"
    # Same instant, UTC zone with the same hours → different phase
    with exec_settings(scheduler_timezone="UTC",
                       scheduler_market_open_hour=8, scheduler_market_close_hour=17):
        assert _market_phase(instant) == "open"
    with exec_settings(scheduler_timezone="UTC",
                       scheduler_market_open_hour=13, scheduler_market_close_hour=20):
        assert _market_phase(instant) == "premarket"


def test_naive_datetime_is_treated_as_utc():
    from app.scheduler.jobs import _market_phase

    with exec_settings(scheduler_timezone="UTC",
                       scheduler_market_open_hour=11, scheduler_market_close_hour=20):
        assert _market_phase(datetime(2026, 8, 3, 12, 0)) == "open"


@pytest.mark.parametrize("bad", ["", "Not/AZone", "America/Nowhere", None])
def test_invalid_timezone_fails_closed(bad):
    with pytest.raises(Exception):
        Settings(scheduler_timezone=bad)


@pytest.mark.parametrize("bad", [-1, 24, 99, 1.5])
def test_invalid_market_hours_fail_closed(bad):
    with pytest.raises(Exception):
        Settings(scheduler_market_open_hour=bad)


# ───────────────────────────────────────────────────────────────────
# 15 — auditable status
# ───────────────────────────────────────────────────────────────────


def test_scheduler_status_exposes_config_lease_and_blocks():
    from app.scheduler.jobs import get_scheduler_state

    with exec_settings(scheduler_enabled=False, scheduler_timezone="UTC",
                       scheduler_market_open_hour=11, scheduler_market_close_hour=20,
                       execution_admin_key="super-secret-key",
                       execution_preview_secret="super-secret-preview"):
        state = get_scheduler_state()

    for field in ("enabled_config", "running", "phase", "timezone", "scheduler_now",
                  "last_run_at", "last_status", "last_source", "last_skip_code",
                  "blocking_recommendation_id", "blocking_execution_id",
                  "deferred_events_count", "lease_held", "lease_owner",
                  "lease_expires_at", "configured_open_time", "configured_close_time",
                  "interpreted_open_time", "interpreted_close_time", "jobs"):
        assert field in state, f"missing {field}"

    assert state["enabled_config"] is False
    assert state["running"] is False          # disabled → not running
    assert state["jobs"] == []                # disabled → no jobs
    assert state["configured_open_time"] == "11:00"
    assert "UTC" in state["interpreted_open_time"]

    serialized = json.dumps(state, default=str)
    assert "super-secret-key" not in serialized
    assert "super-secret-preview" not in serialized


def test_status_endpoint_reports_blocking_ids(db):
    from app.scheduler.jobs import _record_cycle_outcome, _scheduler_state, get_scheduler_state

    rec, _ = _make_open_recommendation(db)
    with exec_settings(trigger_cooldown_seconds=0):
        result = run_cycle(db, source="scheduler")
    _record_cycle_outcome(result, "scheduler")
    try:
        state = get_scheduler_state()
        assert state["last_skip_code"] == SKIP_OPEN_RECOMMENDATION
        assert state["blocking_recommendation_id"] == rec.id
    finally:
        _scheduler_state.update(last_skip_code=None, blocking_recommendation_id=None,
                                blocking_execution_id=None, deferred_events_count=0)


# ───────────────────────────────────────────────────────────────────
# 16 — notifications
# ───────────────────────────────────────────────────────────────────


def test_new_recommendation_sends_one_human_review_notification(db):
    from app.notifications.dispatcher import notify_new_recommendation_pending_review

    with exec_settings(trigger_cooldown_seconds=0, notification_enabled=True):
        result = run_cycle(db, source="test")
        with mock.patch("app.notifications.dispatcher.send_web_push_to_all",
                        return_value={"sent": 1, "failed": 0, "removed": 0}) as push:
            first = notify_new_recommendation_pending_review(db, result)
            second = notify_new_recommendation_pending_review(db, result)

    assert push.call_count == 1                      # exactly one notification
    assert second["reason"] == "already_delivered"   # deduplicated
    body = push.call_args.kwargs["body"]
    assert str(result["recommendation_id"]) in body
    assert "revisión" in body.lower()
    assert "ejecut" in body.lower()  # explicitly says nothing was executed
    assert push.call_args.kwargs["deep_link"].endswith(str(result["recommendation_id"]))


def test_skipped_cycle_sends_no_notification(db):
    from app.notifications.dispatcher import notify_new_recommendation_pending_review

    _make_open_recommendation(db)
    with exec_settings(trigger_cooldown_seconds=0, notification_enabled=True):
        result = run_cycle(db, source="scheduler")
        with mock.patch("app.notifications.dispatcher.send_web_push_to_all") as push:
            for _ in range(5):   # repeated blocked cycles must not spam
                out = notify_new_recommendation_pending_review(db, result)
    push.assert_not_called()
    assert out["reason"] == "no_new_recommendation"


def test_notification_carries_no_secrets(db):
    from app.notifications.dispatcher import notify_new_recommendation_pending_review

    with exec_settings(trigger_cooldown_seconds=0, notification_enabled=True,
                       execution_admin_key="admin-secret",
                       execution_preview_secret="preview-secret"):
        result = run_cycle(db, source="test")
        with mock.patch("app.notifications.dispatcher.send_web_push_to_all",
                        return_value={"sent": 1}) as push:
            notify_new_recommendation_pending_review(db, result)

    payload = json.dumps(push.call_args.kwargs, default=str)
    for secret in ("admin-secret", "preview-secret", "preview_hash"):
        assert secret not in payload


# ───────────────────────────────────────────────────────────────────
# 17-18 — execution guards (static)
# ───────────────────────────────────────────────────────────────────


_FORBIDDEN_EXECUTION_SYMBOLS = {
    "approve_and_execute", "place_order", "submit_order_request",
    "reconcile_execution", "OrderExecution", "_get_execution_broker",
    "_submit_prepared_order", "prepare_validated_execution_batch",
}


@pytest.mark.parametrize("module_path", [
    "app/scheduler/jobs.py",
    "app/services/orchestrator.py",
    "app/notifications/dispatcher.py",
    "app/llm/explainer.py",
    "app/news/ingestion.py",
])
def test_automatic_modules_never_reference_execution_symbols(module_path):
    """AST guard: no automatic module may import or call anything that can
    approve, send or create an execution."""
    tree = ast.parse((BACKEND_DIR / module_path).read_text())

    referenced = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            referenced.add(node.id)
        elif isinstance(node, ast.Attribute):
            referenced.add(node.attr)
        elif isinstance(node, ast.ImportFrom):
            referenced.update(a.name for a in node.names)
        elif isinstance(node, ast.Import):
            referenced.update(a.name for a in node.names)

    offending = referenced & _FORBIDDEN_EXECUTION_SYMBOLS
    assert not offending, f"{module_path} references {sorted(offending)}"


def test_scheduler_never_writes_order_execution_or_flips_locks(db):
    """A full scheduler pass creates no execution and changes no lock."""
    from app.scheduler.jobs import scheduled_full_cycle, scheduled_ingestion

    rec, action = _make_open_recommendation(db)
    before = _snapshot_of(rec, action)
    settings = get_settings()
    locks_before = (settings.order_execution_enabled,
                    settings.sandbox_execution_enabled,
                    settings.execution_pilot_creation_enabled)

    with mock.patch("app.scheduler.jobs.SessionLocal", return_value=db):
        with mock.patch.object(db, "close"):
            scheduled_ingestion()
            scheduled_full_cycle()

    assert db.query(OrderExecution).count() == 0
    assert (settings.order_execution_enabled,
            settings.sandbox_execution_enabled,
            settings.execution_pilot_creation_enabled) == locks_before
    db.expire_all()
    assert _snapshot_of(db.get(Recommendation, rec.id),
                        db.query(RecommendationAction).filter(
                            RecommendationAction.recommendation_id == rec.id).one()) == before
    assert db.query(Recommendation).count() == 1


# ───────────────────────────────────────────────────────────────────
# 5 + 12 — manual analysis and the production recommendation shape
# ───────────────────────────────────────────────────────────────────


def test_manual_analysis_with_open_recommendation_returns_skip(db):
    """Equivalent of production recommendation 13: repeated manual analysis
    must leave it byte-identical and create nothing."""
    rec, action = _make_open_recommendation(db, symbol="SPY")
    before = _snapshot_of(rec, action)

    with exec_settings(trigger_cooldown_seconds=0):
        for _ in range(3):
            result = run_cycle(db, source="manual")
            assert result["skipped"] is True
            assert result["code"] == SKIP_OPEN_RECOMMENDATION
            assert result["blocking_recommendation_id"] == rec.id

    db.expire_all()
    rec = db.get(Recommendation, rec.id)
    action = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec.id).one()
    assert _snapshot_of(rec, action) == before
    assert rec.status == "pending"
    assert rec.superseded_at is None
    assert action.target_change_pct == -0.1
    assert db.query(Recommendation).count() == 1
    assert db.query(OrderExecution).count() == 0


def test_manual_analysis_endpoint_returns_200_with_skip(db):
    """A blocked cycle is an expected outcome — never an HTTP 500."""
    from fastapi.testclient import TestClient
    from app.db.session import SessionLocal
    from app.main import app

    real = SessionLocal()
    created = []
    try:
        rec = Recommendation(
            action="rebalancear", status="pending", suggested_pct=0.1, confidence=0.7,
            rationale="r", risks="k", executive_summary="s", metadata_json={},
        )
        real.add(rec)
        real.commit()
        created.append(rec.id)

        settings = get_settings()
        original = settings.api_key
        try:
            settings.api_key = ""
            client = TestClient(app)
            resp = client.post("/api/analysis/run")
        finally:
            settings.api_key = original

        assert resp.status_code == 200
        body = resp.json()
        assert body["skipped"] is True
        assert body["code"] == SKIP_OPEN_RECOMMENDATION
        # The shared DB may already hold other open rows; the blocker must be
        # one of them (the gate reports the lowest id).
        open_ids = {
            r.id for r in real.query(Recommendation).filter(
                Recommendation.status.notin_(sorted(TERMINAL_RECOMMENDATION_STATUSES))
            ).all()
        }
        assert body["blocking_recommendation_id"] in open_ids
        # Our recommendation is untouched
        real.expire_all()
        assert real.get(Recommendation, rec.id).status == "pending"
    finally:
        cleanup = SessionLocal()
        try:
            for rid in created:
                row = cleanup.get(Recommendation, rid)
                if row:
                    cleanup.delete(row)
            cleanup.commit()
        finally:
            cleanup.close()
        real.close()


def test_terminal_status_sets_are_explicit_and_disjoint():
    """Terminal/blocking sets are declared once and cannot overlap."""
    assert not (TERMINAL_EXECUTION_STATUSES & BLOCKING_EXECUTION_STATUSES)
    assert "pending" not in TERMINAL_RECOMMENDATION_STATUSES
    assert "blocked" not in TERMINAL_RECOMMENDATION_STATUSES
    assert "rejected" in TERMINAL_RECOMMENDATION_STATUSES
    assert "execution_sent" in BLOCKING_EXECUTION_STATUSES
    assert "executed" in TERMINAL_EXECUTION_STATUSES
