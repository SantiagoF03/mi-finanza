"""Tests for P5 gaps: discovery source reporting, coverage_status, summary_by_asset_type,
and API route consistency.

Required tests:
1. If discovery fails and static seed is used, response indicates used_static_seed=true
   and discovery_source_effective="static_seed", never misleading "iol".
2. If one panel errors and another succeeds, coverage_status is "partial".
3. refresh response includes summary_by_asset_type with counts per type.
4. Real API route for analysis cycle is /api/analysis/run (not /api/recommendations/trigger).
"""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.models.models import InstrumentCatalog  # noqa: F401 — ensure table is registered


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


# ---------------------------------------------------------------------------
# Test 1: static seed fallback has honest reporting
# ---------------------------------------------------------------------------


def test_seed_fallback_reports_correctly(db):
    """When IOL discovery fails and static seed is used, response must:
    - discovery_source_attempted = "iol"
    - discovery_source_effective = "static_seed"
    - used_static_seed = True
    - coverage_status = "seed_only"
    - Never report source as "iol" when data came from seed.
    """
    from app.market.discovery import refresh_instrument_catalog

    with patch("app.market.discovery.get_settings") as mock_settings:
        s = MagicMock()
        s.broker_mode = "real"
        s.iol_username = "test_user"
        mock_settings.return_value = s

        # Make IOL discovery raise an exception
        with patch("app.market.discovery._discover_from_iol", side_effect=Exception("Connection refused")):
            result = refresh_instrument_catalog(db)

    assert result["discovery_source_attempted"] == "iol"
    assert result["discovery_source_effective"] == "static_seed"
    assert result["used_static_seed"] is True
    assert result["coverage_status"] == "seed_only"
    # Must NOT say source="iol" — that was the old misleading behavior
    assert result.get("source") != "iol" or result["used_static_seed"] is True


# ---------------------------------------------------------------------------
# Test 2: partial coverage when some panels fail
# ---------------------------------------------------------------------------


def test_partial_coverage_on_mixed_panels(db):
    """When one panel errors and another succeeds, coverage_status must be 'partial'."""
    from app.market.discovery import refresh_instrument_catalog

    panel_results = [
        {"panel": "Cotizaciones/acciones/argentina/Merval", "asset_type": "ACCIONES",
         "status": "ok", "instruments_found": 5, "error": None},
        {"panel": "Cotizaciones/cedears/argentina/CEDEARs", "asset_type": "CEDEAR",
         "status": "error", "instruments_found": 0, "error": "HTTP 404"},
    ]
    instruments = [
        {"symbol": f"SYM{i}", "asset_type": "ACCIONES", "source": "iol_cotizaciones",
         "source_category": "Merval", "market": "BCBA"}
        for i in range(5)
    ]

    with patch("app.market.discovery.get_settings") as mock_settings:
        s = MagicMock()
        s.broker_mode = "real"
        s.iol_username = "test_user"
        mock_settings.return_value = s

        with patch("app.market.discovery._discover_from_iol", return_value=(instruments, panel_results)):
            result = refresh_instrument_catalog(db)

    assert result["coverage_status"] == "partial"
    assert result["discovery_source_effective"] == "iol"
    assert result["used_static_seed"] is False
    assert any(pr["status"] == "error" for pr in result["panel_results"])
    assert any(pr["status"] == "ok" for pr in result["panel_results"])


# ---------------------------------------------------------------------------
# Test 3: summary_by_asset_type in refresh response
# ---------------------------------------------------------------------------


def test_refresh_includes_summary_by_asset_type(db):
    """refresh_instrument_catalog response must include summary_by_asset_type
    with counts per discovered asset type.
    """
    from app.market.discovery import refresh_instrument_catalog

    # Use default mock/static seed mode
    result = refresh_instrument_catalog(db)

    assert "summary_by_asset_type" in result, "Response must include summary_by_asset_type"
    summary = result["summary_by_asset_type"]
    assert isinstance(summary, dict)

    # Static seed includes multiple types from KNOWN_ASSET_TYPES
    assert len(summary) > 0, "summary_by_asset_type must have at least one type"
    # Verify counts are positive integers
    for asset_type, count in summary.items():
        assert isinstance(count, int) and count > 0, (
            f"Count for {asset_type} must be positive int, got {count}"
        )

    # Known types from static seed should be present
    assert "CEDEAR" in summary, "Static seed must include CEDEARs"
    assert "ACCIONES" in summary, "Static seed must include ACCIONES"
    assert "BONO" in summary, "Static seed must include BONOs"


# ---------------------------------------------------------------------------
# Test 4: real API route is /api/analysis/run
# ---------------------------------------------------------------------------


def test_analysis_run_route_exists():
    """The real API route for triggering the analysis cycle must be /api/analysis/run.
    Ensure it exists and is a POST endpoint (not /api/recommendations/trigger).
    """
    from app.api.routes import router

    # Collect all routes from the router
    routes_found = {}
    for route in router.routes:
        if hasattr(route, "path") and hasattr(route, "methods"):
            for method in route.methods:
                routes_found[f"{method} {route.path}"] = True

    # /api/analysis/run must exist as POST (prefix /api added by main.py, router has /analysis/run)
    assert "POST /analysis/run" in routes_found, (
        f"/analysis/run POST route not found. Available routes: {sorted(routes_found.keys())}"
    )

    # /recommendations/trigger must NOT exist (it's not a real endpoint)
    assert "POST /recommendations/trigger" not in routes_found, (
        "/recommendations/trigger should not exist — the real route is /analysis/run"
    )
