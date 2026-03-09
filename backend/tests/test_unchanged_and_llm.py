"""Tests for unchanged detection (Part A) and LLM fallback (Part B)."""

from unittest.mock import patch, MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.db.session import Base
from app.models.models import Recommendation
from app.recommendations.unchanged import detect_unchanged
from app.services.orchestrator import run_cycle, get_current_recommendation


def make_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


# ---------------------------------------------------------------------------
# Part A: Unchanged detection
# ---------------------------------------------------------------------------


def test_two_identical_cycles_produce_unchanged_true():
    """Two consecutive cycles with same mock data => unchanged=true on the second."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    first = run_cycle(db, source="test")
    second = run_cycle(db, source="test")

    assert first["recommendation_id"] != second["recommendation_id"]
    assert second.get("unchanged") is True
    assert second.get("unchanged_reason")

    # Verify it's persisted in metadata
    rec = db.query(Recommendation).filter(Recommendation.id == second["recommendation_id"]).first()
    meta = rec.metadata_json or {}
    assert meta["unchanged"] is True
    assert meta["unchanged_reason"]


def test_material_change_produces_unchanged_false():
    """If the underlying recommendation changes materially, unchanged=false."""
    new_rec = {
        "action": "reducir riesgo",
        "suggested_pct": 0.08,
        "actions": [{"symbol": "AAPL", "target_change_pct": -0.08, "reason": "test"}],
        "blocked_reason": "",
        "external_opportunities": [],
        "_news_items": [],
    }
    # Build a fake previous recommendation row
    prev = MagicMock()
    prev.action = "mantener"
    prev.suggested_pct = 0.0
    prev.actions = []
    prev.blocked_reason = ""
    prev.metadata_json = {"analysis": {"risk_score": 0.3, "concentration_score": 0.2, "alerts": []}, "news_used": 0, "external_opportunities": []}

    unchanged, reason = detect_unchanged(new_rec, prev, {"risk_score": 0.3, "concentration_score": 0.2, "alerts": []})
    assert unchanged is False
    assert "Acción cambió" in reason


def test_unchanged_false_when_suggested_pct_changes():
    new_rec = {
        "action": "mantener",
        "suggested_pct": 0.05,
        "actions": [],
        "blocked_reason": "",
        "external_opportunities": [],
        "_news_items": [],
    }
    prev = MagicMock()
    prev.action = "mantener"
    prev.suggested_pct = 0.0
    prev.actions = []
    prev.blocked_reason = ""
    prev.metadata_json = {"analysis": {"risk_score": 0.3, "concentration_score": 0.2, "alerts": []}, "news_used": 0, "external_opportunities": []}

    unchanged, reason = detect_unchanged(new_rec, prev, {"risk_score": 0.3, "concentration_score": 0.2, "alerts": []})
    assert unchanged is False
    assert "Porcentaje sugerido" in reason


def test_unchanged_false_when_alerts_change():
    new_rec = {
        "action": "mantener",
        "suggested_pct": 0.0,
        "actions": [],
        "blocked_reason": "",
        "external_opportunities": [],
        "_news_items": [],
    }
    prev = MagicMock()
    prev.action = "mantener"
    prev.suggested_pct = 0.0
    prev.actions = []
    prev.blocked_reason = ""
    prev.metadata_json = {"analysis": {"risk_score": 0.3, "concentration_score": 0.2, "alerts": []}, "news_used": 0, "external_opportunities": []}

    unchanged, reason = detect_unchanged(
        new_rec, prev, {"risk_score": 0.3, "concentration_score": 0.2, "alerts": ["Sobreconcentración en un activo > 40%."]}
    )
    assert unchanged is False
    assert "Alertas" in reason


def test_unchanged_true_when_no_prev_returns_false():
    new_rec = {"action": "mantener", "suggested_pct": 0.0, "actions": [], "_news_items": []}
    unchanged, reason = detect_unchanged(new_rec, None, {"risk_score": 0.3, "alerts": []})
    assert unchanged is False
    assert "No hay recomendación previa" in reason


def test_unchanged_reason_populated():
    """unchanged_reason should always be a non-empty string."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    first = run_cycle(db, source="test")
    second = run_cycle(db, source="test")
    rec = db.query(Recommendation).filter(Recommendation.id == second["recommendation_id"]).first()
    assert isinstance((rec.metadata_json or {}).get("unchanged_reason"), str)
    assert len((rec.metadata_json or {}).get("unchanged_reason")) > 0


# ---------------------------------------------------------------------------
# Part B: LLM fallback
# ---------------------------------------------------------------------------


def test_llm_disabled_fallback_cycle_completes():
    """With LLM disabled, cycle completes and news_summary/explanation are null."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0
    s.llm_enabled = False

    result = run_cycle(db, source="test")
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}

    # Cycle completed successfully
    assert result["recommendation_id"]
    assert result["status"] in {"pending", "blocked"}

    # LLM fields are null
    assert meta.get("news_summary") is None
    assert meta.get("recommendation_explanation_llm") is None


def test_llm_error_fallback_cycle_completes():
    """If LLM call raises, cycle still completes with null LLM fields."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0
    s.llm_enabled = True
    s.llm_api_key = "fake-key"

    with patch("app.services.orchestrator.llm_summarize", side_effect=RuntimeError("timeout")), \
         patch("app.services.orchestrator.llm_explain", side_effect=RuntimeError("timeout")):
        result = run_cycle(db, source="test")

    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}

    assert result["recommendation_id"]
    assert meta.get("news_summary") is None
    assert meta.get("recommendation_explanation_llm") is None

    # Reset
    s.llm_enabled = False


def test_llm_does_not_alter_structured_recommendation():
    """Even when LLM returns content, the rule-based recommendation fields are unchanged."""
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0
    s.llm_enabled = True
    s.llm_api_key = "fake-key"

    with patch("app.services.orchestrator.llm_summarize", return_value="LLM news summary text"), \
         patch("app.services.orchestrator.llm_explain", return_value="LLM explanation text here"):
        result = run_cycle(db, source="test")

    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}

    # LLM fields are populated
    assert meta.get("news_summary") == "LLM news summary text"
    assert meta.get("recommendation_explanation_llm") == "LLM explanation text here"

    # Rule-based fields are NOT altered by LLM
    assert rec.action in {"mantener", "reducir riesgo", "rebalancear", "aumentar posición"}
    assert isinstance(rec.suggested_pct, float)
    assert isinstance(rec.confidence, float)
    assert rec.status in {"pending", "blocked"}

    # Reset
    s.llm_enabled = False
