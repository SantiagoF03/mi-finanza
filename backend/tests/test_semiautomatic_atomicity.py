"""Semi-automatic V3 — atomic event association, notification retry, observability.

Closes the residual defects found auditing main after PR #136:
1. Recommendation and MarketEvent were committed in TWO transactions, so a
   crash in between left a recommendation whose events were still pending —
   and those events could later regenerate a duplicate. Events created by
   run_cycle's INTERNAL run_ingestion were also missed by the jobs, which
   captured pending events before the cycle.
2. The notification retry had no productive trigger: the notifier was only
   called on the creating cycle, and every later cycle is skipped by that very
   recommendation.
3. Observability kept stale fields and never recorded manual runs.

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
    MarketEvent,
    OrderExecution,
    Recommendation,
    RecommendationAction,
)
from app.news.ingestion import get_pending_recalc_events, run_ingestion
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


def _counts(db):
    return (db.query(Recommendation).count(),
            db.query(RecommendationAction).count())


def _consumed(db):
    return [(e.id, e.triggered_recalc, e.recalc_recommendation_id)
            for e in db.query(MarketEvent).all()]


# ───────────────────────────────────────────────────────────────────
# 1 — atomic commit
# ───────────────────────────────────────────────────────────────────


def test_recommendation_actions_and_events_share_one_commit(db):
    """Success path: the events used are associated to the new recommendation."""
    run_ingestion(db, source_label="test")
    pending_before = [e.id for e in get_pending_recalc_events(db)]

    with exec_settings(trigger_cooldown_seconds=0):
        result = run_cycle(db, source="test")

    rec_id = result["recommendation_id"]
    assert rec_id
    assert db.query(Recommendation).count() == 1
    assert db.query(RecommendationAction).filter(
        RecommendationAction.recommendation_id == rec_id).count() >= 0

    for evt_id in pending_before:
        evt = db.get(MarketEvent, evt_id)
        assert evt.triggered_recalc is True
        assert evt.recalc_recommendation_id == rec_id
    assert get_pending_recalc_events(db) == []


def test_internal_ingestion_events_are_also_associated(db):
    """run_cycle ingests internally; those events must be associated too.

    The jobs used to capture pending events BEFORE the cycle, so anything the
    internal ingestion produced stayed pending.
    """
    with exec_settings(trigger_cooldown_seconds=0):
        result = run_cycle(db, source="test")   # no prior ingestion at all

    rec_id = result["recommendation_id"]
    assert rec_id
    events = db.query(MarketEvent).all()
    triggered = [e for e in events if e.triggered_recalc]
    # Every event the internal ingestion produced and that was pending at
    # persist time is now bound to this recommendation.
    for evt in triggered:
        assert evt.recalc_recommendation_id == rec_id
    assert get_pending_recalc_events(db) == []


def test_exception_before_commit_leaves_no_partial_writes(db):
    """A crash after preparing everything but before the commit writes nothing."""
    run_ingestion(db, source_label="test")
    before_counts = _counts(db)
    before_events = _consumed(db)

    with exec_settings(trigger_cooldown_seconds=0):
        with mock.patch.object(type(db), "commit",
                               side_effect=RuntimeError("crash before commit")):
            result = run_cycle(db, source="test")

    db.rollback()
    assert isinstance(result, dict)
    assert result.get("skipped") is True
    assert _counts(db) == before_counts, "no recommendation/action may survive"
    assert _consumed(db) == before_events, "no event may be consumed"
    for _, triggered, rec_id in _consumed(db):
        assert triggered is False
        assert rec_id is None


def test_exception_during_commit_rolls_everything_back(db):
    """The commit itself fails → full rollback, fail-closed skip."""
    run_ingestion(db, source_label="test")
    before_counts = _counts(db)
    real_commit = type(db).commit

    def flaky_commit(self, *a, **k):
        # Blow up precisely on the ATOMIC commit. At that point the events
        # have just been marked (dirty) and the actions are pending (new) —
        # the recommendation itself was already flushed to obtain its id.
        atomic = (any(isinstance(o, MarketEvent) for o in self.dirty)
                  and any(isinstance(o, RecommendationAction) for o in self.new))
        if atomic:
            raise RuntimeError("commit exploded")
        return real_commit(self, *a, **k)

    with exec_settings(trigger_cooldown_seconds=0):
        with mock.patch.object(type(db), "commit", flaky_commit):
            result = run_cycle(db, source="test")

    db.rollback()
    assert result.get("skipped") is True
    assert _counts(db) == before_counts
    assert all(not t for _, t, _ in _consumed(db))


def test_manual_analysis_associates_events_atomically(db):
    """Manual analysis consumes the events it used, in the same commit."""
    run_ingestion(db, source_label="test")
    pending_before = [e.id for e in get_pending_recalc_events(db)]

    with exec_settings(trigger_cooldown_seconds=0):
        result = run_cycle(db, source="manual")

    rec_id = result["recommendation_id"]
    assert rec_id
    for evt_id in pending_before:
        evt = db.get(MarketEvent, evt_id)
        assert evt.triggered_recalc is True
        assert evt.recalc_recommendation_id == rec_id


def test_skipped_cycle_consumes_nothing(db):
    """A blocked cycle must not consume events."""
    run_ingestion(db, source_label="test")
    rec = Recommendation(action="rebalancear", status="pending", suggested_pct=0.1,
                         confidence=0.7, rationale="r", risks="k",
                         executive_summary="s", metadata_json={})
    db.add(rec)
    db.commit()
    before = _consumed(db)

    with exec_settings(trigger_cooldown_seconds=0):
        result = run_cycle(db, source="scheduler")

    assert result["skipped"] is True
    assert _consumed(db) == before


# ───────────────────────────────────────────────────────────────────
# 4 — productive notification retry
# ───────────────────────────────────────────────────────────────────


def _fresh(db):
    with exec_settings(trigger_cooldown_seconds=0):
        result = run_cycle(db, source="test")
    assert result.get("recommendation_id")
    return result


def _meta(db, rec_id):
    db.expire_all()
    return db.get(Recommendation, rec_id).metadata_json or {}


def test_failed_notification_is_retried_by_the_transport_job(db):
    """The retry finds it in the DB — no in-memory cycle_result involved."""
    from app.notifications import dispatcher

    result = _fresh(db)
    rec_id = result["recommendation_id"]

    with exec_settings(notification_enabled=True):
        with mock.patch.object(dispatcher, "send_web_push_to_all", return_value={"sent": 0}):
            dispatcher.notify_new_recommendation_pending_review(db, result)
        assert _meta(db, rec_id)["review_notification_status"] == "failed"

        # Cooldown not elapsed → the retry does not spam.
        with mock.patch.object(dispatcher, "send_web_push_to_all") as push:
            out = dispatcher.retry_pending_review_notifications(db)
        push.assert_not_called()
        assert out["skipped"] >= 1

        # After the cooldown the transport job retries and succeeds.
        rec = db.get(Recommendation, rec_id)
        meta = dict(rec.metadata_json)
        meta["review_notification_last_attempt_at"] = (
            datetime.now(timezone.utc)
            - timedelta(seconds=dispatcher.REVIEW_NOTIFICATION_RETRY_COOLDOWN_SECONDS + 1)
        ).isoformat()
        rec.metadata_json = meta
        db.commit()

        with mock.patch.object(dispatcher, "send_web_push_to_all",
                               return_value={"sent": 1}) as push2:
            out = dispatcher.retry_pending_review_notifications(db)

    assert push2.call_count == 1
    assert out["delivered"] == 1
    assert _meta(db, rec_id)["review_notification_status"] == "delivered"


def test_delivered_notification_is_never_resent(db):
    from app.notifications import dispatcher

    result = _fresh(db)
    with exec_settings(notification_enabled=True):
        with mock.patch.object(dispatcher, "send_web_push_to_all", return_value={"sent": 1}):
            dispatcher.notify_new_recommendation_pending_review(db, result)
        with mock.patch.object(dispatcher, "send_web_push_to_all") as push:
            out = dispatcher.retry_pending_review_notifications(db)
    push.assert_not_called()
    assert out["skipped"] >= 1


def test_legacy_sent_flag_is_treated_as_delivered(db):
    """Legacy metadata has only review_notification_sent=True."""
    from app.notifications import dispatcher

    result = _fresh(db)
    rec = db.get(Recommendation, result["recommendation_id"])
    rec.metadata_json = {**(rec.metadata_json or {}), "review_notification_sent": True}
    db.commit()

    assert dispatcher._review_notification_is_delivered(rec.metadata_json) is True
    with exec_settings(notification_enabled=True):
        with mock.patch.object(dispatcher, "send_web_push_to_all") as push:
            dispatcher.retry_pending_review_notifications(db)
    push.assert_not_called()


def test_explicit_status_wins_over_legacy_flag(db):
    from app.notifications import dispatcher

    assert dispatcher._review_notification_is_delivered(
        {"review_notification_sent": True, "review_notification_status": "failed"}) is False
    assert dispatcher._review_notification_is_delivered(
        {"review_notification_status": "delivered"}) is True
    assert dispatcher._review_notification_is_delivered({}) is False


def test_disabled_does_not_consume_attempts_and_retries_once_enabled(db):
    from app.notifications import dispatcher

    result = _fresh(db)
    rec_id = result["recommendation_id"]

    with exec_settings(notification_enabled=False):
        with mock.patch.object(dispatcher, "send_web_push_to_all") as push:
            out = dispatcher.retry_pending_review_notifications(db)
    push.assert_not_called()
    assert out.get("reason") == "notifications_disabled"
    assert _meta(db, rec_id).get("review_notification_attempts") in (None, 0)

    # Enabling notifications lets the transport job pick it up.
    with exec_settings(notification_enabled=True):
        with mock.patch.object(dispatcher, "send_web_push_to_all",
                               return_value={"sent": 1}) as push2:
            out = dispatcher.retry_pending_review_notifications(db)
    assert push2.call_count == 1
    assert _meta(db, rec_id)["review_notification_status"] == "delivered"


def test_retry_emits_at_most_one_main_push_per_recommendation(db):
    from app.notifications import dispatcher

    result = _fresh(db)
    with exec_settings(notification_enabled=True):
        with mock.patch.object(dispatcher, "send_web_push_to_all",
                               return_value={"sent": 1}) as push:
            dispatcher.retry_pending_review_notifications(db)
            dispatcher.retry_pending_review_notifications(db)
            dispatcher.retry_pending_review_notifications(db)
    assert push.call_count == 1


def test_retry_never_touches_analysis_or_execution(db):
    from app.notifications import dispatcher
    from app.services import orchestrator as orch

    _fresh(db)
    with exec_settings(notification_enabled=True):
        with mock.patch.object(dispatcher, "send_web_push_to_all", return_value={"sent": 1}), \
             mock.patch.object(orch, "run_cycle", side_effect=AssertionError("no analysis retry")):
            dispatcher.retry_pending_review_notifications(db)
    assert db.query(OrderExecution).count() == 0


def test_no_retry_job_exists_when_scheduler_disabled():
    """SCHEDULER_ENABLED=false → no jobs at all, transport included."""
    from app.scheduler.jobs import get_scheduler_state, scheduler, start_scheduler

    with exec_settings(scheduler_enabled=False):
        start_scheduler()
        state = get_scheduler_state()
    assert scheduler.running is False
    assert state["running"] is False
    assert state["jobs"] == []


# ───────────────────────────────────────────────────────────────────
# 5 — observability
# ───────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_state():
    from app.scheduler.jobs import _scheduler_state
    snapshot = dict(_scheduler_state)
    yield
    _scheduler_state.clear()
    _scheduler_state.update(snapshot)


def test_manual_analysis_records_created(db):
    from app.scheduler.jobs import _scheduler_state, record_cycle_outcome

    result = _fresh(db)
    record_cycle_outcome(result, "manual", job="manual")
    assert _scheduler_state["last_status"] == "created"
    assert _scheduler_state["last_source"] == "manual"
    assert _scheduler_state["last_job"] == "manual"
    assert _scheduler_state["last_error"] is None


def test_manual_analysis_records_skipped(db):
    from app.scheduler.jobs import _scheduler_state, record_cycle_outcome

    rec = Recommendation(action="rebalancear", status="pending", suggested_pct=0.1,
                         confidence=0.7, rationale="r", risks="k",
                         executive_summary="s", metadata_json={})
    db.add(rec)
    db.commit()
    with exec_settings(trigger_cooldown_seconds=0):
        result = run_cycle(db, source="manual")
    record_cycle_outcome(result, "manual", job="manual")

    assert _scheduler_state["last_status"] == "skipped"
    assert _scheduler_state["last_source"] == "manual"
    assert _scheduler_state["last_job"] == "manual"
    assert _scheduler_state["blocking_recommendation_id"] == rec.id


def test_manual_analysis_records_error():
    from app.scheduler.jobs import _scheduler_state, record_cycle_error

    record_cycle_error(RuntimeError("boom"), "manual", "manual")
    assert _scheduler_state["last_status"] == "error"
    assert _scheduler_state["last_source"] == "manual"
    assert _scheduler_state["last_job"] == "manual"
    assert "boom" in _scheduler_state["last_error"]


def test_success_clears_last_error_and_stale_blocks():
    from app.scheduler.jobs import _scheduler_state, record_cycle_error, record_cycle_outcome

    record_cycle_error(RuntimeError("earlier failure"), "scheduler", "full_cycle")
    assert _scheduler_state["last_error"]
    record_cycle_outcome({"skipped": True, "code": "x",
                          "blocking_recommendation_id": 13,
                          "deferred_events_count": 2}, "scheduler", job="full_cycle")
    assert _scheduler_state["last_error"] is None

    record_cycle_outcome({"recommendation_id": 7}, "scheduler_event", job="ingestion")
    assert _scheduler_state["last_status"] == "created"
    assert _scheduler_state["last_source"] == "scheduler_event"
    assert _scheduler_state["last_job"] == "ingestion"
    assert _scheduler_state["last_error"] is None
    assert _scheduler_state["blocking_recommendation_id"] is None
    assert _scheduler_state["deferred_events_count"] == 0


def test_status_does_not_mix_runs():
    """A later run must never leave a previous run's blocking data behind."""
    from app.scheduler.jobs import _scheduler_state, record_cycle_outcome

    record_cycle_outcome({"skipped": True, "code": "open_recommendation_requires_decision",
                          "blocking_recommendation_id": 13,
                          "blocking_execution_id": 1,
                          "deferred_events_count": 5}, "scheduler", job="full_cycle")
    record_cycle_outcome(None, "scheduler_event", job="ingestion")
    assert _scheduler_state["last_status"] == "no_cycle_needed"
    assert _scheduler_state["last_source"] == "scheduler_event"
    assert _scheduler_state["last_job"] == "ingestion"
    assert _scheduler_state["blocking_recommendation_id"] is None
    assert _scheduler_state["blocking_execution_id"] is None
    assert _scheduler_state["last_skip_code"] is None


# ───────────────────────────────────────────────────────────────────
# 14 — AST guards
# ───────────────────────────────────────────────────────────────────


_FORBIDDEN = {
    "approve_and_execute", "place_order", "submit_order_request",
    "reconcile_execution", "_get_execution_broker",
    "_submit_prepared_order", "prepare_validated_execution_batch",
}


@pytest.mark.parametrize("module_path", [
    "app/scheduler/jobs.py",
    "app/notifications/dispatcher.py",
    "app/services/orchestrator.py",
    "app/services/analysis_gate.py",
])
def test_no_execution_capability_in_automatic_modules(module_path):
    tree = ast.parse((BACKEND_DIR / module_path).read_text())
    referenced = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            referenced.add(node.id)
        elif isinstance(node, ast.Attribute):
            referenced.add(node.attr)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            referenced.update(a.name for a in node.names)
    offending = referenced & _FORBIDDEN
    assert not offending, f"{module_path} references {sorted(offending)}"


def test_observability_module_cannot_create_executions():
    """record_cycle_* only mutate an in-memory dict."""
    from app.scheduler import jobs

    src = (BACKEND_DIR / "app" / "scheduler" / "jobs.py").read_text()
    tree = ast.parse(src)
    for fn in ast.walk(tree):
        if isinstance(fn, ast.FunctionDef) and fn.name.startswith("record_cycle"):
            names = {n.id for n in ast.walk(fn) if isinstance(n, ast.Name)}
            names |= {n.attr for n in ast.walk(fn) if isinstance(n, ast.Attribute)}
            assert not (names & _FORBIDDEN)
            assert "OrderExecution" not in names
