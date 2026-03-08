from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.models.models import Recommendation
from app.portfolio.analyzer import analyze_portfolio
from app.recommendations.engine import generate_recommendation
from app.rules.engine import enforce_rules
from app.services.orchestrator import get_current_recommendation, run_cycle
from app.core.config import get_settings


def make_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


def test_rule_blocks_non_whitelisted_assets():
    rec = {
        "action": "aumentar posición",
        "suggested_pct": 0.2,
        "confidence": 0.8,
        "rationale": "test",
        "risks": "test",
        "executive_summary": "test",
        "actions": [{"symbol": "TSLA", "target_change_pct": 0.2, "reason": "foo"}],
    }
    out = enforce_rules(rec, whitelist=["AAPL"], max_move=0.1)
    assert out["status"] == "blocked"
    assert "whitelist" in out["blocked_reason"]


def test_recommendation_respects_max_move():
    snapshot = {
        "total_value": 100,
        "cash": 10,
        "currency": "USD",
        "positions": [{"symbol": "AAPL", "market_value": 60, "pnl_pct": 0.01}],
    }
    analysis = {
        "alerts": ["Sobreconcentración en un activo > 40%."],
        "weights_by_asset": {"AAPL": 0.6},
        "rebalance_deviation": {},
    }
    rec = generate_recommendation(snapshot, analysis, [], max_move=0.1)
    assert rec["suggested_pct"] <= 0.1


def test_current_recommendation_selection_and_superseded():
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    first = run_cycle(db)
    first_id = first["recommendation_id"]
    second = run_cycle(db)
    second_id = second["recommendation_id"]

    assert first_id != second_id
    current = get_current_recommendation(db)
    assert current.id == second_id

    old = db.query(Recommendation).filter(Recommendation.id == first_id).first()
    assert old.status == "superseded"


def test_idempotency_cooldown_skips_duplicate():
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 999999

    first = run_cycle(db)
    second = run_cycle(db)

    assert second["skipped"] is True
    assert second["status"] == "cooldown"
    assert second["cooldown_remaining_seconds"] > 0
    assert second["cooldown_remaining_minutes"] > 0
    assert second["recommendation_id"] == first["recommendation_id"]


def test_analyzer_edge_cases_empty_and_negative_cash():
    empty = analyze_portfolio({"positions": [], "cash": 0, "currency": "USD", "total_value": 0})
    assert "Portfolio vacío o sin valor." in empty["alerts"]

    neg = analyze_portfolio({"positions": [], "cash": -5, "currency": "USD", "total_value": -5})
    assert "Cash negativo detectado." in neg["alerts"]


def test_contradictory_news_lowers_confidence():
    snapshot = {"total_value": 100, "cash": 10, "currency": "USD", "positions": [{"symbol": "AAPL", "market_value": 60, "pnl_pct": 0.01}]}
    analysis = {"alerts": [], "weights_by_asset": {"AAPL": 0.6}, "rebalance_deviation": {"AAPL": 0.0}}
    news = [
        {"impact": "positivo", "related_assets": ["AAPL"]},
        {"impact": "negativo", "related_assets": ["AAPL"]},
    ]
    rec = generate_recommendation(snapshot, analysis, news, max_move=0.1)
    assert rec["confidence"] <= 0.45


def test_recommendation_actions_symbols_always_in_snapshot():
    snapshot = {
        "total_value": 100,
        "cash": 20,
        "currency": "USD",
        "positions": [
            {"symbol": "GGAL", "market_value": 40, "pnl_pct": 0.01},
            {"symbol": "YPFD", "market_value": 40, "pnl_pct": 0.02},
        ],
    }
    analysis = {
        "alerts": ["Sobreconcentración en un activo > 40%."],
        "weights_by_asset": {"GGAL": 0.41, "AAPL": 0.49},
        "rebalance_deviation": {"GGAL": 0.1, "AAPL": 0.2},
    }
    news = [{"impact": "positivo", "related_assets": ["AAPL"]}]

    rec = generate_recommendation(snapshot, analysis, news, max_move=0.1)
    symbols = {p["symbol"] for p in snapshot["positions"]}
    assert all(a["symbol"] in symbols for a in rec["actions"])


def test_positive_news_outside_snapshot_falls_back_to_maintain():
    snapshot = {
        "total_value": 100,
        "cash": 20,
        "currency": "USD",
        "positions": [{"symbol": "GGAL", "market_value": 80, "pnl_pct": 0.01}],
    }
    analysis = {"alerts": [], "weights_by_asset": {"GGAL": 0.8}, "rebalance_deviation": {"GGAL": 0.0}}
    news = [{"impact": "positivo", "related_assets": ["AAPL"]}]

    rec = generate_recommendation(snapshot, analysis, news, max_move=0.1)
    assert rec["action"] == "mantener"
    assert rec["actions"] == []


def test_mock_news_not_duplicated_between_cycles():
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    run_cycle(db)
    from app.models.models import NewsEvent

    first_count = db.query(NewsEvent).count()
    run_cycle(db)
    second_count = db.query(NewsEvent).count()

    assert first_count == 3
    assert second_count == 3
