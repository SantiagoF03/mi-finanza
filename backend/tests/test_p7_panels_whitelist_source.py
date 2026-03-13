"""Tests for P7: panel probing, whitelist asset_type resolution,
resolution source tracking, and catalog priority over whitelist.

Required tests:
1. Invalid acciones/cedears panels produce explicit panel_results without fake success.
2. Whitelist symbol VZ/JNJ resolves valid asset_type from KNOWN_ASSET_TYPES, investable=true.
3. Resolution source is exposed in external_opportunities output.
4. Catalog has priority over known_assets if both have the symbol.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.models.models import InstrumentCatalog  # noqa: F401


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


# ---------------------------------------------------------------------------
# Test 1: invalid panels report errors explicitly
# ---------------------------------------------------------------------------


def test_invalid_panels_report_explicit_errors(db):
    """When acciones/cedears panels return errors (400/403), panel_results
    must report each variant attempted with its status, not fake success.
    """
    from app.market.discovery import refresh_instrument_catalog

    def mock_discover():
        # Simulate: acciones all fail, letras ok
        instruments = [
            {"symbol": f"LETRA{i}", "asset_type": "TitulosPublicos",
             "source": "iol", "market": "BCBA"} for i in range(5)
        ]
        panel_results = [
            {"asset_type": "ACCIONES", "status": "error",
             "instruments_found": 0, "error": "400 Bad Request",
             "panel_used": None,
             "variants_tried": [
                 {"path": "Cotizaciones/acciones/argentina/Todos", "status": "error", "error": "400 Bad Request"},
                 {"path": "Cotizaciones/acciones/argentina/Merval", "status": "error", "error": "400 Bad Request"},
             ]},
            {"asset_type": "CEDEAR", "status": "error",
             "instruments_found": 0, "error": "400 Bad Request",
             "panel_used": None,
             "variants_tried": [
                 {"path": "Cotizaciones/cedears/argentina/Todos", "status": "error", "error": "400 Bad Request"},
             ]},
            {"asset_type": "TitulosPublicos", "status": "ok",
             "instruments_found": 5, "panel_used": "Cotizaciones/letras/argentina/Todos",
             "variants_tried": [
                 {"path": "Cotizaciones/letras/argentina/Todos", "status": "ok", "instruments_found": 5},
             ]},
        ]
        return instruments, panel_results

    with patch("app.market.discovery.get_settings") as mock_settings:
        s = MagicMock()
        s.broker_mode = "real"
        s.iol_username = "test_user"
        mock_settings.return_value = s

        with patch("app.market.discovery._discover_from_iol", side_effect=mock_discover):
            result = refresh_instrument_catalog(db)

    # Coverage should be partial (some ok, some error)
    assert result["coverage_status"] == "partial"
    assert result["discovery_source_effective"] == "iol"

    # Verify each panel has explicit status
    acciones_panel = [pr for pr in result["panel_results"] if pr["asset_type"] == "ACCIONES"]
    assert len(acciones_panel) == 1
    assert acciones_panel[0]["status"] == "error"
    assert acciones_panel[0]["panel_used"] is None
    assert len(acciones_panel[0]["variants_tried"]) == 2  # tried 2 variants

    # Letras should show success
    letras_panel = [pr for pr in result["panel_results"] if pr["asset_type"] == "TitulosPublicos"]
    assert len(letras_panel) == 1
    assert letras_panel[0]["status"] == "ok"
    assert letras_panel[0]["panel_used"] is not None


# ---------------------------------------------------------------------------
# Test 2: whitelist symbols resolve from KNOWN_ASSET_TYPES, investable
# ---------------------------------------------------------------------------


def test_whitelist_symbol_resolves_and_investable():
    """VZ and JNJ must resolve to CEDEAR from KNOWN_ASSET_TYPES.
    When in main_allowed, they must be investable=true.
    """
    from app.market.assets import KNOWN_ASSET_TYPES, resolve_asset_type
    from app.market.candidates import generate_external_candidates

    # Verify VZ and JNJ are in KNOWN_ASSET_TYPES
    assert "VZ" in KNOWN_ASSET_TYPES, "VZ must be in KNOWN_ASSET_TYPES"
    assert "JNJ" in KNOWN_ASSET_TYPES, "JNJ must be in KNOWN_ASSET_TYPES"
    assert KNOWN_ASSET_TYPES["VZ"] == "CEDEAR"
    assert KNOWN_ASSET_TYPES["JNJ"] == "CEDEAR"

    # resolve_asset_type must return valid
    at, status, source = resolve_asset_type("VZ")
    assert at == "CEDEAR"
    assert status == "known_valid"
    assert source == "known_assets"

    # When VZ is in main_allowed + whitelist, it must be investable
    allowed = {
        "holdings": set(),
        "whitelist": {"VZ", "JNJ"},
        "watchlist": {"VZ", "JNJ"},
        "catalog_dynamic": set(),
        "universe_curated": set(),
        "universe": set(),
        "main_allowed": {"VZ", "JNJ"},
        "external_allowed": {"VZ", "JNJ"},
        "all_known": {"VZ", "JNJ"},
    }

    candidates = generate_external_candidates(
        news_opportunities=[],
        allowed_assets=allowed,
        positions=[],
    )

    vz = [c for c in candidates if c["symbol"] == "VZ"]
    assert len(vz) == 1
    assert vz[0]["asset_type"] == "CEDEAR"
    assert vz[0]["asset_type_status"] == "known_valid"
    assert vz[0]["investable"] is True, (
        f"VZ should be investable (in_main_allowed + known_valid). Got: {vz[0]}"
    )


# ---------------------------------------------------------------------------
# Test 3: asset_type resolution source exposed in output
# ---------------------------------------------------------------------------


def test_resolution_source_in_candidates_output():
    """external_opportunities must include asset_type_source field
    showing which layer resolved the asset_type.
    """
    from app.market.candidates import generate_external_candidates

    catalog_map = {"XN6D": "TitulosPublicos"}
    allowed = {
        "holdings": set(),
        "whitelist": set(),
        "watchlist": {"TSLA", "XN6D"},
        "catalog_dynamic": {"XN6D"},
        "universe_curated": set(),
        "universe": {"XN6D"},
        "main_allowed": set(),
        "external_allowed": {"TSLA", "XN6D"},
        "all_known": {"TSLA", "XN6D"},
    }

    candidates = generate_external_candidates(
        news_opportunities=[],
        allowed_assets=allowed,
        positions=[],
        catalog_map=catalog_map,
    )

    # TSLA should resolve from known_assets (static KNOWN_ASSET_TYPES)
    tsla = [c for c in candidates if c["symbol"] == "TSLA"]
    assert len(tsla) == 1
    assert "asset_type_source" in tsla[0], "asset_type_source must be in output"
    assert tsla[0]["asset_type_source"] == "known_assets"

    # XN6D should resolve from catalog (priority over heuristic)
    xn6d = [c for c in candidates if c["symbol"] == "XN6D"]
    assert len(xn6d) == 1
    assert xn6d[0]["asset_type_source"] == "catalog"
    assert xn6d[0]["asset_type"] == "TitulosPublicos"


# ---------------------------------------------------------------------------
# Test 4: catalog priority over known_assets when both exist
# ---------------------------------------------------------------------------


def test_catalog_priority_over_known_assets():
    """If a symbol exists in both InstrumentCatalog (as ACCIONES) and
    KNOWN_ASSET_TYPES (as CEDEAR), catalog must win.
    """
    from app.market.assets import KNOWN_ASSET_TYPES, resolve_asset_type

    # Pick a symbol that's in KNOWN_ASSET_TYPES as CEDEAR
    assert "AAPL" in KNOWN_ASSET_TYPES
    assert KNOWN_ASSET_TYPES["AAPL"] == "CEDEAR"

    # With catalog_map saying it's ACCIONES, catalog must win
    catalog_map = {"AAPL": "ACCIONES"}
    at, status, source = resolve_asset_type("AAPL", catalog_map=catalog_map)
    assert at == "ACCIONES", f"Catalog should override known_assets. Got {at}"
    assert source == "catalog"
    assert status == "known_valid"

    # Without catalog_map, it falls to known_assets
    at2, status2, source2 = resolve_asset_type("AAPL")
    assert at2 == "CEDEAR"
    assert source2 == "known_assets"
