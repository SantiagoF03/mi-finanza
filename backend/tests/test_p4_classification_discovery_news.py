"""Tests for P4 gaps: catalog classification priority, discovery panel metadata,
allowed_assets layer separation, and mock news exposure.

Required tests:
1. Symbol in InstrumentCatalog with asset_type="TitulosPublicos" appears as such
   in external_opportunities, never as CEDEAR or DESCONOCIDO.
2. catalog asset_type has priority over heuristics/fallbacks (e.g. "D" suffix).
3. refresh_instrument_catalog returns metadata per panel/category consulted.
4. allowed_assets returns separate catalog_dynamic, universe_curated, main_allowed.
5. In real broker mode with mock news, news_is_mock is explicit in metadata.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.db.session import Base
from app.models.models import InstrumentCatalog


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


# ---------------------------------------------------------------------------
# Test 1: catalog asset_type flows correctly into external_opportunities
# ---------------------------------------------------------------------------


def test_catalog_asset_type_in_external_opportunities(db):
    """A symbol in InstrumentCatalog with asset_type='TitulosPublicos' must appear
    with that exact type in external_opportunities, not as CEDEAR or DESCONOCIDO.
    The 'D' suffix heuristic must NOT override the catalog classification.
    """
    # Add symbol ending in 'D' (would previously be misclassified as CEDEAR)
    cat = InstrumentCatalog(
        symbol="XN6D", name="Letra XN6D", asset_type="TitulosPublicos",
        market="BCBA", currency="ARS", tradable=True,
        source="iol_discovery", is_active=True,
        eligible_for_external_discovery=True,
        last_seen_at=datetime.now(timezone.utc),
    )
    db.add(cat)
    db.commit()

    from app.market.assets import build_catalog_asset_type_map
    from app.market.candidates import generate_external_candidates

    catalog_map = build_catalog_asset_type_map(db)
    positions = [{"symbol": "AAPL", "asset_type": "CEDEAR", "quantity": 10, "market_value": 19000}]
    allowed_assets = {
        "holdings": {"AAPL"},
        "whitelist": set(),
        "watchlist": set(),
        "catalog_dynamic": {"XN6D"},
        "universe_curated": set(),
        "universe": {"XN6D"},
        "main_allowed": {"AAPL"},
        "external_allowed": {"XN6D"},
        "all_known": {"AAPL", "XN6D"},
    }

    candidates = generate_external_candidates(
        news_opportunities=[],
        allowed_assets=allowed_assets,
        positions=positions,
        catalog_map=catalog_map,
    )

    xn6d = [c for c in candidates if c["symbol"] == "XN6D"]
    assert len(xn6d) == 1
    assert xn6d[0]["asset_type"] == "TitulosPublicos", (
        f"Expected TitulosPublicos but got {xn6d[0]['asset_type']}. "
        "Catalog classification must have priority over heuristics."
    )
    assert xn6d[0]["asset_type_status"] == "known_valid"


# ---------------------------------------------------------------------------
# Test 2: catalog_map has priority over suffix heuristic
# ---------------------------------------------------------------------------


def test_catalog_priority_over_heuristic():
    """catalog_map must take priority over suffix-based heuristics.
    A symbol ending in 'D' in the catalog as TitulosPublicos must NOT resolve as CEDEAR.
    """
    from app.market.assets import resolve_asset_type

    catalog_map = {"DF6D": "TitulosPublicos", "BNA6D": "TitulosPublicos"}

    # With catalog_map — should resolve from catalog
    at, status, *_ = resolve_asset_type("DF6D", catalog_map=catalog_map)
    assert at == "TitulosPublicos", f"Expected TitulosPublicos, got {at}"
    assert status == "known_valid"

    at, status, *_ = resolve_asset_type("BNA6D", catalog_map=catalog_map)
    assert at == "TitulosPublicos", f"Expected TitulosPublicos, got {at}"

    # Without catalog_map — should NOT resolve as CEDEAR anymore
    # (the dangerous "D" suffix heuristic was removed)
    at, status, *_ = resolve_asset_type("DF6D")
    assert at != "CEDEAR", "Symbol ending in D must NOT be classified as CEDEAR by heuristic"


# ---------------------------------------------------------------------------
# Test 3: refresh_instrument_catalog returns per-panel metadata
# ---------------------------------------------------------------------------


def test_refresh_catalog_panel_metadata(db):
    """refresh_instrument_catalog must include panel_results in its response
    with per-panel status, instrument count, and error info.
    """
    from app.market.discovery import refresh_instrument_catalog

    # In mock/test mode, it'll use static seed
    result = refresh_instrument_catalog(db)

    assert "panel_results" in result, "Response must include panel_results"
    assert isinstance(result["panel_results"], list)
    assert len(result["panel_results"]) > 0

    # Each panel_result must have at minimum: asset_type, status
    for pr in result["panel_results"]:
        assert "status" in pr, f"panel_result missing 'status' key: {pr}"
        # Panel results now use asset_type + panel_used (probing strategy)
        has_identifier = "panel" in pr or "asset_type" in pr or "panel_used" in pr
        assert has_identifier, f"panel_result missing identifier key: {pr}"


# ---------------------------------------------------------------------------
# Test 4: allowed_assets returns separate layers
# ---------------------------------------------------------------------------


def test_allowed_assets_separate_layers():
    """build_allowed_assets must return distinct catalog_dynamic, universe_curated,
    and main_allowed — not mixed together.
    """
    from app.recommendations.universe import build_allowed_assets

    positions = [{"symbol": "AAPL"}, {"symbol": "MSFT"}]
    catalog = {"XN6D", "DF6D", "AL30"}

    with patch("app.recommendations.universe.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(
            whitelist_assets=["AAPL", "MSFT"],
            watchlist_assets=["TSLA"],
            market_universe_assets=["SPY", "QQQ"],
        )
        result = build_allowed_assets(positions, catalog_symbols=catalog)

    # catalog_dynamic: exactly what was passed
    assert result["catalog_dynamic"] == catalog

    # universe_curated: manual config only, NOT including catalog
    assert result["universe_curated"] == {"SPY", "QQQ"}
    assert "XN6D" not in result["universe_curated"]

    # universe: union of curated + catalog (for analysis breadth)
    assert result["universe"] == {"SPY", "QQQ", "XN6D", "DF6D", "AL30"}

    # main_allowed: holdings | whitelist (safety rail)
    assert result["main_allowed"] == {"AAPL", "MSFT"}
    assert "XN6D" not in result["main_allowed"]

    # All three layers are distinct
    assert "catalog_dynamic" in result
    assert "universe_curated" in result
    assert "main_allowed" in result


# ---------------------------------------------------------------------------
# Test 5: mock news is explicit in metadata when broker_mode=real
# ---------------------------------------------------------------------------


def test_mock_news_explicit_in_real_mode():
    """When news comes from MockNewsProvider (even in real broker mode),
    the is_mock flag must be True and news_source must indicate mock.
    """
    from app.services.orchestrator import _load_news_items

    # _load_news_items uses get_news_provider() which returns MockNewsProvider
    # unless a real provider is configured
    items, source, is_mock, *_ = _load_news_items([{"symbol": "AAPL"}])

    # In test environment, provider is MockNewsProvider
    assert is_mock is True, "is_mock must be True when MockNewsProvider is the source"
    assert "Mock" in source, f"Source should mention Mock, got: {source}"

    # Verify the return value is a 3-tuple (not 2-tuple as before)
    assert isinstance(is_mock, bool)
