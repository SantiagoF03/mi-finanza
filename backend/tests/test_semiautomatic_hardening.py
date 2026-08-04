"""Semi-automatic mode V2 hardening — concurrency, events, notifications.

Closes the defects found auditing PR #135:
1. gate ran BEFORE the lease and was never re-checked before the INSERT;
2. the pilot bypassed the central lease/gate and left the previous
   recommendation `pending` while stamping superseded_at;
3. scheduled_full_cycle never consumed the events it used;
4. the review notification was marked delivered without evidence of delivery;
5. observability reported "ok" for skips and lost the real source.

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

from app.core.config import get_settings
from app.db.session import Base
from app.main import _patch_schema
from app.models.models import (
    AnalysisLease,
    MarketEvent,
    OrderExecution,
    PortfolioPosition,
    PortfolioSnapshot,
    Recommendation,
    RecommendationAction,
)
from app.services.analysis_gate import (
    LEASE_NAME,
    SKIP_COOLDOWN,
    SKIP_LEASE_UNAVAILABLE,
    SKIP_OPEN_RECOMMENDATION,
    acquire_analysis_lease,
    holds_analysis_lease,
    release_analysis_lease,
    renew_analysis_lease,
    verify_can_persist_recommendation,
)
from app.services.orchestrator import run_cycle
from tests.testutils import decide_open_recommendations

BACKEND_DIR = Path(__file__).resolve().parent.parent


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)
    session = sessionmaker(autocommit=False, autoflush=False, bind=engine)()
    yield session
    session.close()


@pytest.fixture
def file_db(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path}/hardening.db",
                           connect_args={"check_same_thread": False, "timeout": 30})
    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)
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


def _expire_lease(session):
    session.query(AnalysisLease).filter(AnalysisLease.name == LEASE_NAME).update(
        {"expires_at": datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=1)},
        synchronize_session=False)
    session.commit()


def _open_recommendation(db, status="pending", symbol="SPY", pilot=False):
    meta = ({"execution_pilot": True, "pilot_symbol": "BYMA", "pilot_side": "sell",
             "pilot_quantity": 1, "created_manually": True} if pilot
            else {"origin": "automatic"})
    rec = Recommendation(action="rebalancear", status=status, suggested_pct=0.1,
                         confidence=0.7, rationale="r", risks="k",
                         executive_summary="s", metadata_json=meta)
    db.add(rec)
    db.flush()
    action = RecommendationAction(recommendation_id=rec.id, symbol=symbol,
                                  target_change_pct=-0.1, reason="auto",
                                  quantity_override=1 if pilot else None)
    db.add(action)
    db.commit()
    return rec, action


def _fingerprint(rec, action):
    return (rec.status, rec.superseded_at, json.dumps(rec.metadata_json, sort_keys=True),
            action.symbol, action.target_change_pct, action.quantity_override)


# ───────────────────────────────────────────────────────────────────
# 1-3 — lease ownership, renewal and the final TOCTOU check
# ───────────────────────────────────────────────────────────────────


def test_gate_runs_inside_the_lease(db):
    """run_cycle must take the lease BEFORE evaluating the gate."""
    order = []
    from app.services import orchestrator as orch

    real_acquire = orch.acquire_analysis_lease
    real_gate = orch.check_recommendation_creation_allowed

    def spy_acquire(session, *a, **k):
        order.append("lease")
        return real_acquire(session, *a, **k)

    def spy_gate(session):
        order.append("gate")
        return real_gate(session)

    with exec_settings(trigger_cooldown_seconds=0):
        with mock.patch.object(orch, "acquire_analysis_lease", side_effect=spy_acquire), \
             mock.patch.object(orch, "check_recommendation_creation_allowed", side_effect=spy_gate):
            run_cycle(db, source="test")

    assert order[0] == "lease", f"gate must not precede the lease: {order}"
    assert "gate" in order


def test_only_the_current_owner_can_renew(file_db):
    session, factory = file_db
    owner, _ = acquire_analysis_lease(session, ttl_seconds=600)
    assert owner

    assert renew_analysis_lease(session, owner) is True
    assert renew_analysis_lease(session, "not-the-owner") is False   # rowcount 0
    assert renew_analysis_lease(session, "") is False
    assert holds_analysis_lease(session, owner) is True
    assert holds_analysis_lease(session, "not-the-owner") is False


def test_expired_lease_is_not_silently_renewed(file_db):
    session, _ = file_db
    owner, _ = acquire_analysis_lease(session, ttl_seconds=600)
    _expire_lease(session)
    assert renew_analysis_lease(session, owner) is False
    assert holds_analysis_lease(session, owner) is False


def test_final_verification_blocks_when_lease_was_taken_over(file_db):
    """Two sessions pass the initial check; only the current owner persists."""
    session, factory = file_db
    owner_a, _ = acquire_analysis_lease(session, ttl_seconds=600)
    assert owner_a

    # Lease expires mid-analysis and another process takes it over.
    _expire_lease(session)
    other = factory()
    try:
        owner_b, _ = acquire_analysis_lease(other, ttl_seconds=600)
        assert owner_b and owner_b != owner_a
        # The stale owner must fail the final check.
        result = verify_can_persist_recommendation(session, owner_a)
    finally:
        other.close()

    assert result.allowed is False
    assert result.code == SKIP_LEASE_UNAVAILABLE


def test_final_verification_blocks_when_a_recommendation_appeared(file_db):
    """A blocking recommendation created during the analysis stops the INSERT."""
    session, _ = file_db
    owner, _ = acquire_analysis_lease(session, ttl_seconds=600)
    _open_recommendation(session)
    result = verify_can_persist_recommendation(session, owner)
    assert result.allowed is False
    assert result.code == SKIP_OPEN_RECOMMENDATION


def test_analysis_exceeding_ttl_cannot_persist(file_db):
    """The lease expires during the cycle → nothing is persisted."""
    session, factory = file_db
    from app.services import orchestrator as orch

    real_verify = orch.verify_can_persist_recommendation

    def steal_lease_then_verify(db_, owner):
        # Simulate a long analysis: lease expires and a replica takes it.
        _expire_lease(db_)
        other = factory()
        try:
            acquire_analysis_lease(other, ttl_seconds=600)
        finally:
            other.close()
        return real_verify(db_, owner)

    with exec_settings(trigger_cooldown_seconds=0):
        with mock.patch.object(orch, "verify_can_persist_recommendation",
                               side_effect=steal_lease_then_verify):
            result = run_cycle(session, source="test")

    assert result["skipped"] is True
    assert result["code"] == SKIP_LEASE_UNAVAILABLE
    assert session.query(Recommendation).count() == 0


def test_at_most_one_recommendation_across_two_sessions(file_db):
    session, factory = file_db
    other = factory()
    try:
        with exec_settings(trigger_cooldown_seconds=0):
            owner, _ = acquire_analysis_lease(session, ttl_seconds=600)
            blocked = run_cycle(other, source="replica")
            release_analysis_lease(session, owner)
            created = run_cycle(session, source="primary")
    finally:
        other.close()
    assert blocked["skipped"] is True
    assert created.get("recommendation_id")
    assert session.query(Recommendation).count() == 1


def test_cooldown_has_stable_code_and_standard_payload(db):
    with exec_settings(trigger_cooldown_seconds=999999):
        first = run_cycle(db, source="test")
        assert first.get("recommendation_id")
        decide_open_recommendations(db)
        second = run_cycle(db, source="manual")

    assert second["skipped"] is True
    assert second["code"] == SKIP_COOLDOWN
    assert second["source"] == "manual"
    for field in ("blocking_recommendation_id", "blocking_execution_id",
                  "deferred_events_count", "snapshot_id"):
        assert field in second


# ───────────────────────────────────────────────────────────────────
# 4-5 — pilot under the central exclusion
# ───────────────────────────────────────────────────────────────────


def _byma_snapshot(db):
    snap = PortfolioSnapshot(total_value=200000, cash=10000, currency="ARS")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(snapshot_id=snap.id, symbol="BYMA", asset_type="ACCIONES",
                             instrument_type="ACCIONES", currency="ARS", quantity=306,
                             market_value=89734.5, avg_price=280, pnl_pct=0.05))
    db.commit()
    return snap


def _pilot_settings(**extra):
    base = dict(
        broker_mode="real", iol_use_sandbox=False,
        iol_real_username="u", iol_real_password="p",
        order_execution_enabled=False, execution_pilot_creation_enabled=True,
        sandbox_execution_enabled=False,
        execution_admin_key="k", execution_preview_secret="s",
        execution_preview_ttl_seconds=300, execution_max_recommendation_age_minutes=60,
        execution_max_order_value=500.0, execution_max_total_value=500.0,
        execution_max_portfolio_pct=0.50,
        iol_order_market="bCBA", iol_order_settlement="t1",
        iol_order_type="precioLimite", iol_order_validity_minutes=10,
        execution_max_quote_age_seconds=15, execution_max_price_deviation_pct=0.05,
        execution_sell_only=True, execution_require_live_position_check=True,
        execution_instrument_policies={"BYMA": {
            "asset_type": "ACCIONES", "instrument_type": "ACCIONES", "currency": "ARS",
            "market": "bCBA", "settlement": "t1",
            "quantity_step": 1, "price_tick": 0.25,
            "max_quantity": 1, "max_notional": 500}},
    )
    base.update(extra)
    return base


def _create_pilot(db):
    from app.services.execution_pilot import (
        PILOT_CONFIRMATION,
        create_execution_pilot_recommendation,
    )
    return create_execution_pilot_recommendation(
        db, execution_key="k", symbol="BYMA", side="sell", quantity=1,
        confirmation_text=PILOT_CONFIRMATION, note="piloto")


def test_pilot_is_blocked_by_open_recommendation_and_changes_nothing(db):
    _byma_snapshot(db)
    rec, action = _open_recommendation(db, symbol="SPY")
    before = _fingerprint(rec, action)
    count_before = db.query(Recommendation).count()

    with exec_settings(**_pilot_settings()):
        result = _create_pilot(db)

    assert result["status_code"] == 409
    assert result["code"] == SKIP_OPEN_RECOMMENDATION
    assert db.query(Recommendation).count() == count_before

    db.expire_all()
    rec = db.get(Recommendation, rec.id)
    action = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec.id).one()
    assert _fingerprint(rec, action) == before
    assert rec.status == "pending"          # NOT left half-superseded
    assert rec.superseded_at is None
    assert "superseded_by_execution_pilot" not in rec.metadata_json


def test_pilot_never_leaves_two_open_recommendations(db):
    _byma_snapshot(db)
    _open_recommendation(db, symbol="SPY")
    with exec_settings(**_pilot_settings()):
        _create_pilot(db)

    from app.services.analysis_gate import TERMINAL_RECOMMENDATION_STATUSES
    open_count = db.query(Recommendation).filter(
        Recommendation.status.notin_(sorted(TERMINAL_RECOMMENDATION_STATUSES))).count()
    assert open_count == 1


def test_pilot_and_run_cycle_are_mutually_exclusive(file_db):
    """A pilot cannot slip in while the analysis cycle holds the lease."""
    session, factory = file_db
    _byma_snapshot(session)
    owner, _ = acquire_analysis_lease(session, ttl_seconds=600)
    assert owner
    other = factory()
    try:
        with exec_settings(**_pilot_settings()):
            result = _create_pilot(other)
    finally:
        other.close()

    assert result["status_code"] == 409
    assert result["code"] == SKIP_LEASE_UNAVAILABLE
    assert session.query(Recommendation).count() == 0


def test_pilot_succeeds_once_the_recommendation_is_resolved(db):
    _byma_snapshot(db)
    _open_recommendation(db, symbol="SPY")
    with exec_settings(**_pilot_settings()):
        assert _create_pilot(db)["status_code"] == 409
        decide_open_recommendations(db)
        ok = _create_pilot(db)
    assert "error" not in ok
    assert ok["quantity_override"] == 1


def test_pilot_module_no_longer_supersedes():
    src = (BACKEND_DIR / "app" / "services" / "execution_pilot.py").read_text()
    tree = ast.parse(src)
    names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert "_supersede_other_open_recommendations" not in names


# ───────────────────────────────────────────────────────────────────
# 6-8 — transactional event consumption
# ───────────────────────────────────────────────────────────────────


def _pending_events(db):
    from app.news.ingestion import get_pending_recalc_events
    return get_pending_recalc_events(db)


def test_full_cycle_consumes_the_events_it_used(db):
    from app.news.ingestion import run_ingestion
    from app.scheduler.jobs import scheduled_full_cycle

    run_ingestion(db, source_label="test")
    pending_before = [e.id for e in _pending_events(db)]
    if not pending_before:
        pytest.skip("mock ingestion produced no trigger_recalc events")

    with exec_settings(trigger_cooldown_seconds=0, scheduler_postmarket_force_cycle=True):
        with mock.patch("app.scheduler.jobs.SessionLocal", return_value=db), \
             mock.patch.object(db, "close"):
            scheduled_full_cycle()

    rec = db.query(Recommendation).order_by(Recommendation.id.desc()).first()
    assert rec is not None
    for evt_id in pending_before:
        evt = db.get(MarketEvent, evt_id)
        assert evt.triggered_recalc is True
        assert evt.recalc_recommendation_id == rec.id
    assert [e.id for e in _pending_events(db)] == []


def test_skipped_full_cycle_consumes_nothing(db):
    from app.news.ingestion import run_ingestion
    from app.scheduler.jobs import scheduled_full_cycle

    run_ingestion(db, source_label="test")
    _open_recommendation(db)                     # blocks creation
    pending_before = [e.id for e in _pending_events(db)]

    with exec_settings(trigger_cooldown_seconds=0, scheduler_postmarket_force_cycle=True):
        with mock.patch("app.scheduler.jobs.SessionLocal", return_value=db), \
             mock.patch.object(db, "close"):
            scheduled_full_cycle()

    assert [e.id for e in _pending_events(db)] == pending_before
    for evt_id in pending_before:
        assert db.get(MarketEvent, evt_id).recalc_recommendation_id is None


def test_consumed_events_do_not_create_a_duplicate_recommendation(db):
    from app.news.ingestion import run_ingestion
    from app.scheduler.jobs import scheduled_full_cycle

    run_ingestion(db, source_label="test")
    with exec_settings(trigger_cooldown_seconds=0, scheduler_postmarket_force_cycle=True):
        with mock.patch("app.scheduler.jobs.SessionLocal", return_value=db), \
             mock.patch.object(db, "close"):
            scheduled_full_cycle()
            first_count = db.query(Recommendation).count()
            decide_open_recommendations(db)
            # Same (already consumed) events must not regenerate anything.
            assert _pending_events(db) == []

    # Second cycle NOT forced and with NO new input: ingestion is stubbed so
    # no fresh events appear, isolating the question under test — can an
    # ALREADY CONSUMED event regenerate a recommendation? (A forced cycle, or
    # one fed by new news, would legitimately create one; that is a different
    # scenario and must not mask a duplicate.)
    with exec_settings(trigger_cooldown_seconds=0, scheduler_postmarket_force_cycle=False):
        with mock.patch("app.scheduler.jobs.SessionLocal", return_value=db), \
             mock.patch.object(db, "close"), \
             mock.patch("app.scheduler.jobs.run_ingestion", return_value={"status": "ok"}), \
             mock.patch("app.scheduler.jobs.has_llm_eligible_news", return_value=False):
            scheduled_full_cycle()

    assert _pending_events(db) == [], "consumed events must not reappear as pending"

    # STRICT: the already-consumed events must not produce another
    # recommendation. Exact equality, not >=.
    for evt in db.query(MarketEvent).all():
        if evt.triggered_recalc:
            assert evt.recalc_recommendation_id is not None
    assert db.query(Recommendation).count() == first_count, (
        "an already-consumed event produced a duplicate recommendation"
    )


def test_no_separate_event_consumption_helper_exists():
    """The post-commit consumption window must be gone for good.

    consume_recalc_events committed events in a SECOND transaction after the
    recommendation was already committed. Association now happens inside
    run_cycle's single atomic commit, so the helper must not come back.
    """
    from app.scheduler import jobs

    assert not hasattr(jobs, "consume_recalc_events")
    src = (BACKEND_DIR / "app" / "scheduler" / "jobs.py").read_text()
    tree = ast.parse(src)
    fn_names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert "consume_recalc_events" not in fn_names


# ───────────────────────────────────────────────────────────────────
# 9-11 — notifications
# ───────────────────────────────────────────────────────────────────


def _fresh_recommendation(db):
    with exec_settings(trigger_cooldown_seconds=0):
        result = run_cycle(db, source="test")
    assert result.get("recommendation_id")
    return result


def _meta(db, rec_id):
    db.expire_all()
    return db.get(Recommendation, rec_id).metadata_json or {}


def test_disabled_notifications_are_not_recorded_as_delivered(db):
    from app.notifications.dispatcher import notify_new_recommendation_pending_review

    result = _fresh_recommendation(db)
    before = json.dumps(_meta(db, result["recommendation_id"]), sort_keys=True)

    with exec_settings(notification_enabled=False):
        out = notify_new_recommendation_pending_review(db, result)

    assert out["review_notification_status"] == "disabled"
    meta = _meta(db, result["recommendation_id"])
    assert meta.get("review_notification_sent") is not True
    assert "review_notification_delivered_at" not in meta
    # A never-notified recommendation is left byte-identical: disabled
    # notifications must not fabricate state nor consume an attempt.
    assert json.dumps(meta, sort_keys=True) == before
    assert meta.get("review_notification_attempts") in (None, 0)

    # But a recommendation that ALREADY has notification state records the
    # disabled outcome, so a later retry can pick it up.
    rec = db.get(Recommendation, result["recommendation_id"])
    rec.metadata_json = {**(rec.metadata_json or {}),
                         "review_notification_status": "failed",
                         "review_notification_attempts": 1}
    db.commit()
    with exec_settings(notification_enabled=False):
        notify_new_recommendation_pending_review(db, result)
    assert _meta(db, result["recommendation_id"])["review_notification_status"] == "disabled"


def test_failed_dispatch_is_not_recorded_as_delivered(db):
    from app.notifications.dispatcher import notify_new_recommendation_pending_review

    result = _fresh_recommendation(db)
    with exec_settings(notification_enabled=True):
        with mock.patch("app.notifications.dispatcher.send_web_push_to_all",
                        side_effect=RuntimeError("transport down")):
            out = notify_new_recommendation_pending_review(db, result)

    assert out["review_notification_status"] == "failed"
    meta = _meta(db, result["recommendation_id"])
    assert meta["review_notification_status"] == "failed"
    assert meta.get("review_notification_sent") is not True
    assert meta["review_notification_attempts"] == 1
    assert meta["review_notification_last_attempt_at"]


def test_zero_sent_is_not_recorded_as_delivered(db):
    from app.notifications.dispatcher import notify_new_recommendation_pending_review

    result = _fresh_recommendation(db)
    with exec_settings(notification_enabled=True):
        with mock.patch("app.notifications.dispatcher.send_web_push_to_all",
                        return_value={"sent": 0, "failed": 1, "removed": 0}):
            out = notify_new_recommendation_pending_review(db, result)

    assert out["review_notification_status"] == "failed"
    assert _meta(db, result["recommendation_id"]).get("review_notification_sent") is not True


def test_failed_notification_can_be_retried_after_cooldown_without_spam(db):
    from app.notifications import dispatcher
    from app.notifications.dispatcher import notify_new_recommendation_pending_review

    result = _fresh_recommendation(db)
    rec_id = result["recommendation_id"]

    with exec_settings(notification_enabled=True):
        with mock.patch.object(dispatcher, "send_web_push_to_all",
                               return_value={"sent": 0}) as push:
            notify_new_recommendation_pending_review(db, result)
            # Immediate retry is refused by the cooldown → no spam.
            again = notify_new_recommendation_pending_review(db, result)
            assert again["reason"] == "retry_cooldown"
            assert push.call_count == 1

        # After the cooldown window, a retry is allowed and can succeed.
        meta = _meta(db, rec_id)
        rec = db.get(Recommendation, rec_id)
        meta["review_notification_last_attempt_at"] = (
            datetime.now(timezone.utc)
            - timedelta(seconds=dispatcher.REVIEW_NOTIFICATION_RETRY_COOLDOWN_SECONDS + 1)
        ).isoformat()
        rec.metadata_json = dict(meta)
        db.commit()

        with mock.patch.object(dispatcher, "send_web_push_to_all",
                               return_value={"sent": 1}) as push2:
            ok = notify_new_recommendation_pending_review(db, result)
    assert ok["review_notification_status"] == "delivered"
    assert push2.call_count == 1
    final = _meta(db, rec_id)
    assert final["review_notification_sent"] is True
    assert final["review_notification_delivered_at"]


def test_retries_are_bounded(db):
    from app.notifications import dispatcher
    from app.notifications.dispatcher import notify_new_recommendation_pending_review

    result = _fresh_recommendation(db)
    rec_id = result["recommendation_id"]
    with exec_settings(notification_enabled=True):
        with mock.patch.object(dispatcher, "send_web_push_to_all", return_value={"sent": 0}):
            for _ in range(dispatcher.REVIEW_NOTIFICATION_MAX_ATTEMPTS + 2):
                rec = db.get(Recommendation, rec_id)
                meta = dict(rec.metadata_json or {})
                meta.pop("review_notification_last_attempt_at", None)  # bypass cooldown
                rec.metadata_json = meta
                db.commit()
                out = notify_new_recommendation_pending_review(db, result)
    assert out["reason"] == "max_attempts_reached"
    assert _meta(db, rec_id)["review_notification_attempts"] == \
        dispatcher.REVIEW_NOTIFICATION_MAX_ATTEMPTS


def test_successful_cycle_emits_at_most_one_main_push(db):
    """The creating cycle must produce ONE push, not two."""
    from app.notifications import dispatcher

    result = _fresh_recommendation(db)
    with exec_settings(notification_enabled=True, notification_channel="web_push"):
        with mock.patch.object(dispatcher, "send_web_push_to_all",
                               return_value={"sent": 1}) as push:
            dispatcher.dispatch_recommendation_alerts(db, result, suppress_push=True)
            dispatcher.notify_new_recommendation_pending_review(db, result)
    assert push.call_count == 1


def test_suppress_push_keeps_the_audit_trail(db):
    from app.notifications import dispatcher

    result = _fresh_recommendation(db)
    with exec_settings(notification_enabled=True, notification_channel="web_push"):
        with mock.patch.object(dispatcher, "send_web_push_to_all") as push:
            out = dispatcher.dispatch_recommendation_alerts(db, result, suppress_push=True)
    push.assert_not_called()
    # The audit trail is still persisted on the recommendation
    assert (_meta(db, result["recommendation_id"])).get("notification_audit") is not None
    assert isinstance(out, dict)


# ───────────────────────────────────────────────────────────────────
# 12 — observability
# ───────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_scheduler_state():
    from app.scheduler.jobs import _scheduler_state
    snapshot = dict(_scheduler_state)
    yield
    _scheduler_state.clear()
    _scheduler_state.update(snapshot)


def test_status_reports_created_then_clears_blocking_fields(db):
    from app.scheduler.jobs import _record_cycle_outcome, _scheduler_state

    _record_cycle_outcome({"skipped": True, "code": SKIP_OPEN_RECOMMENDATION,
                           "blocking_recommendation_id": 13,
                           "blocking_execution_id": None,
                           "deferred_events_count": 4}, "scheduler")
    assert _scheduler_state["last_status"] == "skipped"
    assert _scheduler_state["last_source"] == "scheduler"
    assert _scheduler_state["blocking_recommendation_id"] == 13
    assert _scheduler_state["deferred_events_count"] == 4

    # Once unblocked, stale blocking info must disappear.
    _record_cycle_outcome({"recommendation_id": 99}, "scheduler_event")
    assert _scheduler_state["last_status"] == "created"
    assert _scheduler_state["last_source"] == "scheduler_event"
    assert _scheduler_state["last_skip_code"] is None
    assert _scheduler_state["blocking_recommendation_id"] is None
    assert _scheduler_state["blocking_execution_id"] is None
    assert _scheduler_state["deferred_events_count"] == 0


def test_status_reports_no_cycle_needed():
    from app.scheduler.jobs import _record_cycle_outcome, _scheduler_state

    _record_cycle_outcome(None, "scheduler")
    assert _scheduler_state["last_status"] == "no_cycle_needed"
    assert _scheduler_state["last_source"] == "scheduler"
    assert _scheduler_state["blocking_recommendation_id"] is None


def test_status_reports_error_without_losing_source(db):
    from app.scheduler.jobs import _scheduler_state, scheduled_full_cycle

    with mock.patch("app.scheduler.jobs.SessionLocal", side_effect=RuntimeError("db down")):
        scheduled_full_cycle()
    assert _scheduler_state["last_status"] == "error"
    assert _scheduler_state["last_error"]
    assert _scheduler_state["last_job"] == "full_cycle"


def test_real_source_is_preserved_per_job(db):
    from app.scheduler.jobs import _scheduler_state, scheduled_ingestion

    _open_recommendation(db)
    with exec_settings(trigger_cooldown_seconds=0):
        with mock.patch("app.scheduler.jobs.SessionLocal", return_value=db), \
             mock.patch.object(db, "close"):
            scheduled_ingestion()
    assert _scheduler_state["last_source"] in {"scheduler_event"}
    assert _scheduler_state["last_status"] in {"skipped", "no_cycle_needed"}


# ───────────────────────────────────────────────────────────────────
# 13-14 — timezone portability and execution guards
# ───────────────────────────────────────────────────────────────────


def test_argentina_timezone_is_available():
    """python:3.11-slim ships no system tzdata — the tzdata package must
    provide the IANA database so ART can be configured in production."""
    import importlib.util
    from zoneinfo import ZoneInfo

    assert importlib.util.find_spec("tzdata") is not None, \
        "tzdata must be a declared dependency for zoneinfo on slim images"
    tz = ZoneInfo("America/Argentina/Buenos_Aires")
    instant = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
    assert instant.astimezone(tz).hour == 9      # UTC-3


def test_tzdata_is_declared_in_requirements():
    reqs = (BACKEND_DIR / "requirements.txt").read_text().lower()
    assert "tzdata" in reqs


def test_market_phase_works_with_argentina_timezone():
    from app.scheduler.jobs import _market_phase

    with exec_settings(scheduler_timezone="America/Argentina/Buenos_Aires",
                       scheduler_market_open_hour=8, scheduler_market_close_hour=17):
        assert _market_phase(datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)) == "open"


_FORBIDDEN = {
    "approve_and_execute", "place_order", "submit_order_request",
    "reconcile_execution", "OrderExecution", "_get_execution_broker",
    "_submit_prepared_order", "prepare_validated_execution_batch",
}


@pytest.mark.parametrize("module_path", [
    "app/scheduler/jobs.py",
    "app/notifications/dispatcher.py",
    "app/services/orchestrator.py",
    "app/services/analysis_gate.py",
])
def test_scheduler_and_notifications_cannot_execute_orders(module_path):
    forbidden = set(_FORBIDDEN)
    if module_path.endswith("analysis_gate.py"):
        # The gate must READ OrderExecution to detect blocking executions; it
        # can never create or send one (covered by the other symbols).
        forbidden.discard("OrderExecution")
    tree = ast.parse((BACKEND_DIR / module_path).read_text())
    referenced = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            referenced.add(node.id)
        elif isinstance(node, ast.Attribute):
            referenced.add(node.attr)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            referenced.update(a.name for a in node.names)
    offending = referenced & forbidden
    assert not offending, f"{module_path} references {sorted(offending)}"


def test_production_recommendation_13_shape_survives_everything(db):
    """End-to-end regression on the production shape: pending + one action."""
    from app.scheduler.jobs import scheduled_full_cycle, scheduled_ingestion

    _byma_snapshot(db)
    rec, action = _open_recommendation(db, symbol="SPY")
    before = _fingerprint(rec, action)

    with exec_settings(trigger_cooldown_seconds=0, **_pilot_settings()):
        with mock.patch("app.scheduler.jobs.SessionLocal", return_value=db), \
             mock.patch.object(db, "close"):
            scheduled_ingestion()
            scheduled_full_cycle()
        run_cycle(db, source="manual")
        _create_pilot(db)

    db.expire_all()
    rec = db.get(Recommendation, rec.id)
    action = db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec.id).one()
    assert _fingerprint(rec, action) == before
    assert rec.status == "pending"
    assert rec.superseded_at is None
    assert db.query(Recommendation).count() == 1
    assert db.query(OrderExecution).count() == 0
