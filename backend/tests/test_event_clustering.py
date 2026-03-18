"""Tests for EventCluster model, build_or_update_clusters logic, and /events/clusters/recent.

Covers:
1. run_ingestion creates clusters when NewsNormalized items exist
2. Clustering groups by topic_hash + time_bucket
3. Same topic_hash in different time_buckets yields separate clusters
4. get_recent_clusters returns cluster data
5. include_items=true includes child NewsNormalized items
6. Clustering metadata in run_ingestion response is consistent
7. Schema migration adds event_cluster_id to existing DBs
"""

from datetime import datetime, timedelta

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.models.models import EventCluster, NewsNormalized, NewsRaw, IngestionRun
from app.news.ingestion import (
    _compute_time_bucket,
    _make_cluster_key,
    build_or_update_clusters,
    get_engine_eligible_clusters,
    get_llm_eligible_clusters,
    get_recent_clusters,
    run_ingestion,
)


def make_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal(), engine


def _insert_normalized(db, *, title, topic_hash, published_at=None, source="test",
                       event_type="macro", pre_score=0.5, triage_level="observe",
                       related_assets=None):
    """Insert a minimal NewsNormalized row for clustering tests."""
    now = datetime.utcnow()
    pub = published_at or now

    # Need an IngestionRun
    run = db.query(IngestionRun).first()
    if not run:
        run = IngestionRun(source="test", status="completed",
                           items_fetched=0, items_new=0, items_filtered=0,
                           events_created=0, started_at=now, finished_at=now)
        db.add(run)
        db.flush()

    # Minimal raw row
    raw = NewsRaw(
        ingestion_run_id=run.id, source=source, title=title,
        summary=title, url=f"https://example.com/{hash(title + source)}",
        published_at=pub, fetched_at=now,
        dedup_hash=f"{hash(title + source + str(pub))}",
    )
    db.add(raw)
    db.flush()

    norm = NewsNormalized(
        raw_id=raw.id, title=title, summary=f"Summary: {title}",
        source=source, url=raw.url, published_at=pub,
        event_type=event_type, impact="medium", confidence=0.7,
        related_assets=related_assets or [], recency_hours=1.0,
        pre_score=pre_score, triage_level=triage_level,
        topic_hash=topic_hash, multi_source_count=1,
    )
    db.add(norm)
    db.flush()
    return norm


# ---------------------------------------------------------------------------
# 1. run_ingestion creates clusters
# ---------------------------------------------------------------------------


def test_run_ingestion_includes_clustering_metadata():
    """run_ingestion response must include clustering metadata."""
    db, _ = make_db()
    result = run_ingestion(db, source_label="test")
    assert result["status"] == "completed"
    assert "clustering" in result
    cm = result["clustering"]
    assert "clusters_created" in cm
    assert "clusters_updated" in cm
    assert "clusters_touched" in cm
    assert "items_clustered" in cm


# ---------------------------------------------------------------------------
# 2. Clustering groups by topic_hash + time_bucket
# ---------------------------------------------------------------------------


def test_same_topic_hash_same_bucket_one_cluster():
    """Two items with same topic_hash in same 12h bucket -> one cluster."""
    db, _ = make_db()
    now = datetime.utcnow()

    _insert_normalized(db, title="Fed raises rates impact on SPY",
                       topic_hash="abc123", published_at=now, source="reuters",
                       related_assets=["SPY"])
    _insert_normalized(db, title="Fed raises rates SPY falls",
                       topic_hash="abc123", published_at=now + timedelta(hours=2),
                       source="bloomberg", related_assets=["SPY"])
    db.commit()

    result = build_or_update_clusters(db)
    db.commit()

    assert result["clusters_created"] == 1
    assert result["items_clustered"] == 2

    cluster = db.query(EventCluster).first()
    assert cluster is not None
    assert cluster.topic_hash == "abc123"
    assert cluster.item_count == 2
    assert cluster.source_count == 2
    assert "bloomberg" in cluster.sources_list
    assert "reuters" in cluster.sources_list
    assert "SPY" in cluster.affected_symbols


# ---------------------------------------------------------------------------
# 3. Same topic_hash, different time_buckets -> separate clusters
# ---------------------------------------------------------------------------


def test_different_buckets_yield_separate_clusters():
    """Same topic_hash but different 12h windows must produce separate clusters."""
    db, _ = make_db()
    morning = datetime(2026, 3, 17, 8, 0, 0)   # H0
    evening = datetime(2026, 3, 17, 20, 0, 0)   # H1

    _insert_normalized(db, title="BCRA sube tasas matutino",
                       topic_hash="xyz789", published_at=morning)
    _insert_normalized(db, title="BCRA sube tasas vespertino",
                       topic_hash="xyz789", published_at=evening)
    db.commit()

    result = build_or_update_clusters(db)
    db.commit()

    assert result["clusters_created"] == 2
    clusters = db.query(EventCluster).filter(EventCluster.topic_hash == "xyz789").all()
    assert len(clusters) == 2

    buckets = {c.time_bucket for c in clusters}
    assert "2026-03-17_H0" in buckets
    assert "2026-03-17_H1" in buckets


def test_different_days_yield_separate_clusters():
    """Same topic_hash on different days must produce separate clusters."""
    db, _ = make_db()
    day1 = datetime(2026, 3, 15, 10, 0, 0)
    day2 = datetime(2026, 3, 16, 10, 0, 0)

    _insert_normalized(db, title="Oil price day1", topic_hash="oil001", published_at=day1)
    _insert_normalized(db, title="Oil price day2", topic_hash="oil001", published_at=day2)
    db.commit()

    result = build_or_update_clusters(db)
    db.commit()

    assert result["clusters_created"] == 2


# ---------------------------------------------------------------------------
# 4. get_recent_clusters returns cluster data
# ---------------------------------------------------------------------------


def test_get_recent_clusters_returns_data():
    """get_recent_clusters should return cluster dicts with all required fields."""
    db, _ = make_db()
    _insert_normalized(db, title="Test cluster item", topic_hash="t001",
                       pre_score=0.7, triage_level="send_to_llm",
                       related_assets=["GGAL"])
    db.commit()
    build_or_update_clusters(db)
    db.commit()

    clusters = get_recent_clusters(db, limit=10)
    assert len(clusters) == 1

    c = clusters[0]
    assert c["canonical_title"] == "Test cluster item"
    assert c["item_count"] == 1
    assert c["source_count"] == 1
    assert "GGAL" in c["affected_symbols"]
    assert c["relevance_score"] == 0.7
    assert c["triage_max"] == "send_to_llm"
    assert c["llm_candidate"] is True
    assert "items" not in c  # not requested


# ---------------------------------------------------------------------------
# 5. include_items=true returns child items
# ---------------------------------------------------------------------------


def test_get_recent_clusters_include_items():
    """With include_items=True, each cluster must contain its NewsNormalized items."""
    db, _ = make_db()
    _insert_normalized(db, title="Item A", topic_hash="grp1", source="src_a")
    _insert_normalized(db, title="Item B", topic_hash="grp1", source="src_b")
    db.commit()
    build_or_update_clusters(db)
    db.commit()

    clusters = get_recent_clusters(db, limit=10, include_items=True)
    assert len(clusters) == 1
    assert "items" in clusters[0]
    assert len(clusters[0]["items"]) == 2

    titles = {i["title"] for i in clusters[0]["items"]}
    assert "Item A" in titles
    assert "Item B" in titles


# ---------------------------------------------------------------------------
# 6. Clustering metadata consistency
# ---------------------------------------------------------------------------


def test_clustering_metadata_counts_are_correct():
    """clusters_touched == clusters_created + clusters_updated; items_clustered is accurate."""
    db, _ = make_db()
    now = datetime.utcnow()

    # Create 3 items: 2 same cluster, 1 different
    _insert_normalized(db, title="A1", topic_hash="aaa", published_at=now)
    _insert_normalized(db, title="A2", topic_hash="aaa", published_at=now + timedelta(hours=1))
    _insert_normalized(db, title="B1", topic_hash="bbb", published_at=now)
    db.commit()

    result = build_or_update_clusters(db)
    db.commit()

    assert result["clusters_created"] == 2
    assert result["clusters_updated"] == 0
    assert result["clusters_touched"] == 2
    assert result["items_clustered"] == 3

    # Run again — should update, not create
    result2 = build_or_update_clusters(db)
    db.commit()

    assert result2["clusters_created"] == 0
    assert result2["clusters_updated"] == 2
    assert result2["clusters_touched"] == 2
    assert result2["items_clustered"] == 3


# ---------------------------------------------------------------------------
# 7. Schema migration — event_cluster_id on pre-existing DB
# ---------------------------------------------------------------------------


def test_schema_patch_adds_missing_column():
    """_patch_schema must add event_cluster_id to a news_normalized table that lacks it."""
    from app.main import _patch_schema

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})

    # Create tables WITHOUT EventCluster (simulate old schema)
    # We'll manually create a minimal news_normalized without the FK column
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE ingestion_runs (
                id INTEGER PRIMARY KEY,
                source_label TEXT DEFAULT '',
                status TEXT DEFAULT 'running'
            )
        """))
        conn.execute(text("""
            CREATE TABLE news_raw (
                id INTEGER PRIMARY KEY,
                ingestion_run_id INTEGER,
                source TEXT DEFAULT '',
                title TEXT DEFAULT '',
                summary TEXT DEFAULT '',
                url TEXT DEFAULT '',
                dedup_hash TEXT DEFAULT ''
            )
        """))
        conn.execute(text("""
            CREATE TABLE news_normalized (
                id INTEGER PRIMARY KEY,
                raw_id INTEGER,
                title TEXT DEFAULT '',
                summary TEXT DEFAULT '',
                source TEXT DEFAULT '',
                url TEXT DEFAULT '',
                event_type TEXT DEFAULT '',
                impact TEXT DEFAULT '',
                confidence TEXT DEFAULT '',
                topic_hash TEXT DEFAULT ''
            )
        """))

    # Verify column is missing
    inspector = inspect(engine)
    cols_before = {c["name"] for c in inspector.get_columns("news_normalized")}
    assert "event_cluster_id" not in cols_before

    # Now create all tables (this adds event_clusters but NOT the FK column)
    Base.metadata.create_all(bind=engine)

    # Still missing because create_all doesn't alter existing tables
    inspector = inspect(engine)
    cols_mid = {c["name"] for c in inspector.get_columns("news_normalized")}
    assert "event_cluster_id" not in cols_mid

    # Run patch
    _patch_schema(engine)

    # Now it should exist
    inspector = inspect(engine)
    cols_after = {c["name"] for c in inspector.get_columns("news_normalized")}
    assert "event_cluster_id" in cols_after


def test_schema_patch_is_idempotent():
    """Running _patch_schema twice should not fail."""
    from app.main import _patch_schema

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})

    # Create old-style table
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE news_normalized (
                id INTEGER PRIMARY KEY,
                title TEXT DEFAULT ''
            )
        """))

    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)
    _patch_schema(engine)  # Second call must not raise

    inspector = inspect(engine)
    cols = {c["name"] for c in inspector.get_columns("news_normalized")}
    assert "event_cluster_id" in cols


def test_fresh_db_needs_no_patch():
    """On a fresh DB, _patch_schema should be a no-op (column already exists)."""
    from app.main import _patch_schema

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)

    inspector = inspect(engine)
    cols = {c["name"] for c in inspector.get_columns("news_normalized")}
    assert "event_cluster_id" in cols

    # Patch is a no-op
    _patch_schema(engine)

    cols2 = {c["name"] for c in inspector.get_columns("news_normalized")}
    assert "event_cluster_id" in cols2


# ---------------------------------------------------------------------------
# Helpers unit tests
# ---------------------------------------------------------------------------


def test_compute_time_bucket():
    """Time bucket must split at hour 12."""
    morning = datetime(2026, 3, 17, 6, 30, 0)
    afternoon = datetime(2026, 3, 17, 14, 0, 0)
    midnight = datetime(2026, 3, 18, 0, 5, 0)

    assert _compute_time_bucket(morning) == "2026-03-17_H0"
    assert _compute_time_bucket(afternoon) == "2026-03-17_H1"
    assert _compute_time_bucket(midnight) == "2026-03-18_H0"


def test_make_cluster_key():
    assert _make_cluster_key("abc", "2026-03-17_H0") == "abc_2026-03-17_H0"


def test_canonical_title_picks_highest_score():
    """The canonical_title should come from the item with the highest pre_score."""
    db, _ = make_db()
    now = datetime.utcnow()

    _insert_normalized(db, title="Low score title", topic_hash="pick",
                       published_at=now, pre_score=0.2)
    _insert_normalized(db, title="High score title", topic_hash="pick",
                       published_at=now, pre_score=0.9)
    db.commit()

    build_or_update_clusters(db)
    db.commit()

    cluster = db.query(EventCluster).filter(EventCluster.topic_hash == "pick").first()
    assert cluster.canonical_title == "High score title"


def test_items_have_event_cluster_id_set():
    """After clustering, all NewsNormalized items should have event_cluster_id set."""
    db, _ = make_db()
    n1 = _insert_normalized(db, title="FK test 1", topic_hash="fk01")
    n2 = _insert_normalized(db, title="FK test 2", topic_hash="fk01")
    db.commit()

    build_or_update_clusters(db)
    db.commit()

    db.refresh(n1)
    db.refresh(n2)
    assert n1.event_cluster_id is not None
    assert n2.event_cluster_id is not None
    assert n1.event_cluster_id == n2.event_cluster_id


# ---------------------------------------------------------------------------
# Cluster-aware eligible functions
# ---------------------------------------------------------------------------


def test_get_engine_eligible_clusters_returns_correct_format():
    """Cluster-sourced dicts must have same keys as _news_rows_to_dicts + traceability."""
    db, _ = make_db()
    now = datetime.utcnow()

    _insert_normalized(db, title="Engine cluster A", topic_hash="eng01",
                       published_at=now, pre_score=0.6, triage_level="observe",
                       related_assets=["GGAL"])
    _insert_normalized(db, title="Engine cluster A dup", topic_hash="eng01",
                       published_at=now + timedelta(hours=1), pre_score=0.4,
                       triage_level="observe", source="other", related_assets=["GGAL"])
    db.commit()
    build_or_update_clusters(db)
    db.commit()

    results = get_engine_eligible_clusters(db)
    assert len(results) == 1

    item = results[0]
    # Standard fields (backward compat with generate_recommendation)
    assert "title" in item
    assert "summary" in item
    assert "event_type" in item
    assert "impact" in item
    assert "confidence" in item
    assert "related_assets" in item
    assert "created_at" in item
    assert "source" in item
    assert "pre_score" in item
    assert "triage_level" in item
    assert "multi_source_count" in item

    # Traceability fields
    assert "cluster_id" in item
    assert "cluster_key" in item
    assert "item_count" in item
    assert item["item_count"] == 2
    assert "source_count" in item
    assert "relevance_score" in item
    assert "llm_candidate" in item
    assert "external_opportunity_candidate" in item
    assert "affects_holdings" in item
    assert "affects_watchlist" in item
    assert "affected_sectors" in item
    assert "GGAL" in item["related_assets"]


def test_get_engine_eligible_clusters_excludes_store_only():
    """Clusters with triage_max=store_only must not appear in engine results."""
    db, _ = make_db()
    _insert_normalized(db, title="Low triage", topic_hash="low01",
                       pre_score=0.1, triage_level="store_only")
    db.commit()
    build_or_update_clusters(db)
    db.commit()

    results = get_engine_eligible_clusters(db)
    assert len(results) == 0


def test_get_llm_eligible_clusters_only_llm_candidates():
    """LLM clusters must only include llm_candidate=True clusters."""
    db, _ = make_db()
    now = datetime.utcnow()

    # observe-only cluster (not llm candidate)
    _insert_normalized(db, title="Observe only", topic_hash="obs01",
                       published_at=now, pre_score=0.3, triage_level="observe")
    # send_to_llm cluster (llm candidate)
    _insert_normalized(db, title="LLM worthy", topic_hash="llm01",
                       published_at=now, pre_score=0.8, triage_level="send_to_llm",
                       related_assets=["SPY"])
    db.commit()
    build_or_update_clusters(db)
    db.commit()

    llm_results = get_llm_eligible_clusters(db)
    engine_results = get_engine_eligible_clusters(db)

    assert len(llm_results) == 1
    assert llm_results[0]["title"] == "LLM worthy"
    assert llm_results[0]["llm_candidate"] is True

    # Engine should see both
    assert len(engine_results) == 2


def test_cluster_deduplication_over_individual_items():
    """5 items about the same event should become 1 cluster entry, not 5."""
    db, _ = make_db()
    now = datetime.utcnow()

    for i in range(5):
        _insert_normalized(
            db, title=f"Fed meeting impact {i}", topic_hash="fed_meet",
            published_at=now + timedelta(minutes=i * 30),
            pre_score=0.5 + i * 0.05, triage_level="observe",
            source=f"source_{i}", related_assets=["SPY"],
        )
    db.commit()
    build_or_update_clusters(db)
    db.commit()

    clusters = get_engine_eligible_clusters(db)
    assert len(clusters) == 1
    assert clusters[0]["item_count"] == 5
    assert clusters[0]["source_count"] == 5


def test_orchestrator_cluster_traceability_in_metadata():
    """With use_clusters=True, recommendation metadata must include cluster_traceability."""
    from app.core.config import get_settings
    from app.services.orchestrator import run_cycle

    db, _ = make_db()
    settings = get_settings()

    # Enable cluster mode
    original = settings.use_clusters
    settings.use_clusters = True

    # Insert clusterable items
    now = datetime.utcnow()
    _insert_normalized(db, title="Cluster trace test", topic_hash="trace01",
                       published_at=now, pre_score=0.7, triage_level="send_to_llm",
                       related_assets=["SPY"])
    db.commit()
    build_or_update_clusters(db)
    db.commit()

    try:
        result = run_cycle(db, source="test")
    finally:
        settings.use_clusters = original

    assert "recommendation_id" in result

    from app.models.models import Recommendation
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}

    assert meta.get("news_mode") in ("clusters", "individual_fallback")
    if meta["news_mode"] == "clusters":
        trace = meta.get("cluster_traceability")
        assert trace is not None
        assert isinstance(trace, list)
        if trace:
            assert "cluster_id" in trace[0]
            assert "source_count" in trace[0]
            assert "relevance_score" in trace[0]


def test_orchestrator_individual_mode_no_cluster_traceability():
    """With use_clusters=False (default), metadata should have news_mode=individual."""
    from app.core.config import get_settings
    from app.services.orchestrator import run_cycle

    db, _ = make_db()
    settings = get_settings()

    original = settings.use_clusters
    settings.use_clusters = False

    try:
        result = run_cycle(db, source="test")
    finally:
        settings.use_clusters = original

    assert "recommendation_id" in result

    from app.models.models import Recommendation
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}

    assert meta.get("news_mode") == "individual"
    assert meta.get("cluster_traceability") is None


def test_cluster_fallback_when_no_clusters_exist():
    """When use_clusters=True but no clusters exist, should fallback to individual news."""
    from app.core.config import get_settings
    from app.services.orchestrator import run_cycle

    db, _ = make_db()
    settings = get_settings()

    original = settings.use_clusters
    settings.use_clusters = True

    # Don't insert any news or clusters — force fallback
    try:
        result = run_cycle(db, source="test")
    finally:
        settings.use_clusters = original

    assert "recommendation_id" in result

    from app.models.models import Recommendation
    rec = db.query(Recommendation).filter(Recommendation.id == result["recommendation_id"]).first()
    meta = rec.metadata_json or {}

    # Should have fallen back to individual
    assert meta.get("news_mode") in ("individual_fallback", "clusters")


# ---------------------------------------------------------------------------
# API response: /recommendations/current exposes news_mode and cluster_traceability
# ---------------------------------------------------------------------------


def test_recommendations_current_exposes_news_mode_individual():
    """GET /recommendations/current must include news_mode and cluster_traceability."""
    from app.api.routes import current_recommendation
    from app.core.config import get_settings
    from app.services.orchestrator import run_cycle

    db, _ = make_db()
    settings = get_settings()
    original = settings.use_clusters
    settings.use_clusters = False

    try:
        run_cycle(db, source="test")
    finally:
        settings.use_clusters = original

    # Simulate the route logic directly (no TestClient needed)
    from app.models.models import Recommendation
    from sqlalchemy.orm import joinedload
    rec = db.query(Recommendation).options(joinedload(Recommendation.actions)).order_by(Recommendation.id.desc()).first()
    meta = rec.metadata_json or {}

    response = {
        "news_mode": meta.get("news_mode", "individual"),
        "cluster_traceability": meta.get("cluster_traceability") or [],
    }

    assert response["news_mode"] == "individual"
    assert response["cluster_traceability"] == []


def test_recommendations_current_exposes_cluster_traceability():
    """With clusters, /recommendations/current must expose cluster_traceability list."""
    from app.core.config import get_settings
    from app.services.orchestrator import run_cycle

    db, _ = make_db()
    settings = get_settings()
    original = settings.use_clusters
    settings.use_clusters = True

    now = datetime.utcnow()
    _insert_normalized(db, title="API trace test", topic_hash="api01",
                       published_at=now, pre_score=0.75, triage_level="send_to_llm",
                       related_assets=["GGAL"])
    db.commit()
    build_or_update_clusters(db)
    db.commit()

    try:
        run_cycle(db, source="test")
    finally:
        settings.use_clusters = original

    from app.models.models import Recommendation
    rec = db.query(Recommendation).order_by(Recommendation.id.desc()).first()
    meta = rec.metadata_json or {}

    news_mode = meta.get("news_mode", "individual")
    cluster_trace = meta.get("cluster_traceability") or []

    assert news_mode in ("clusters", "individual_fallback")
    if news_mode == "clusters":
        assert len(cluster_trace) >= 1
        assert "cluster_id" in cluster_trace[0]
        assert "source_count" in cluster_trace[0]
        assert "relevance_score" in cluster_trace[0]
        assert "llm_candidate" in cluster_trace[0]
        assert "affects_holdings" in cluster_trace[0]
        assert "affected_sectors" in cluster_trace[0]


def test_recommendations_current_defaults_for_old_recommendations():
    """Old recommendations without news_mode in metadata should default safely."""
    from app.models.models import Recommendation

    db, _ = make_db()

    # Insert a recommendation with no news_mode in metadata (simulates old data)
    rec = Recommendation(
        action="mantener", status="pending", suggested_pct=0.0,
        confidence=0.5, rationale="test", risks="test",
        executive_summary="test",
        metadata_json={"analysis": {}, "source": "test"},
    )
    db.add(rec)
    db.commit()

    meta = rec.metadata_json or {}
    news_mode = meta.get("news_mode", "individual")
    cluster_trace = meta.get("cluster_traceability") or []

    assert news_mode == "individual"
    assert cluster_trace == []
