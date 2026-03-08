from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.models.models import NewsEvent
from app.news.pipeline import classify_news_event, deduplicate_news_items
from app.services import orchestrator
from app.core.config import get_settings


class FakeNewsProvider:
    def get_recent_news(self, portfolio_symbols):
        return [
            {
                "title": "GGAL reporta resultados fuertes y mejora guidance",
                "event_type": "earnings",
                "impact": "positivo",
                "confidence": 0.8,
                "related_assets": ["GGAL"],
                "summary": "La empresa supera expectativas.",
                "created_at": datetime.utcnow(),
            },
            {
                "title": "GGAL reporta resultados fuertes y mejora guidance",
                "event_type": "earnings",
                "impact": "positivo",
                "confidence": 0.8,
                "related_assets": ["GGAL"],
                "summary": "La empresa supera expectativas.",
                "created_at": datetime.utcnow(),
            },
        ]


def make_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


def test_classification_minimum_rule_based():
    c = classify_news_event(
        "Banco central sube tasas por inflación",
        "La medida busca contener inflación",
        ["GGAL", "AL30"],
    )
    assert c["event_type"] in {"tasas", "inflación", "inflación"}
    assert c["impact"] in {"positivo", "negativo", "neutro"}
    assert 0.5 <= c["confidence"] <= 0.95


def test_deduplicate_news_items_by_title_and_summary():
    items = [
        {"title": "A", "summary": "X"},
        {"title": "A", "summary": "X"},
        {"title": "A", "summary": "Y"},
    ]
    out = deduplicate_news_items(items)
    assert len(out) == 2


def test_news_pipeline_integration_dedup_persisted(monkeypatch):
    db = make_db()
    s = get_settings()
    s.trigger_cooldown_seconds = 0

    monkeypatch.setattr(orchestrator, "get_news_provider", lambda: FakeNewsProvider())

    orchestrator.run_cycle(db, source="manual")
    orchestrator.run_cycle(db, source="manual")

    count = db.query(NewsEvent).count()
    assert count == 1
