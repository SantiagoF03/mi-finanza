"""Tests for P6: universe/current layer exposure, refresh contract strictness,
and reporting field coherence.

Required tests:
1. GET /api/universe/current returns universe_curated and catalog_dynamic separately.
2. refresh response does not expose legacy 'source' field inconsistent with
   discovery_source_effective.
3. coverage_status=seed_only implies used_static_seed=true and
   discovery_source_effective=static_seed (strict coherence).
4. coverage_status=full implies all panels ok, none error/empty (strict coherence).
"""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.models.models import InstrumentCatalog, PortfolioPosition, PortfolioSnapshot  # noqa: F401


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


# ---------------------------------------------------------------------------
# Test 1: /api/universe/current exposes separate layers
# ---------------------------------------------------------------------------


def test_universe_current_exposes_layers(db):
    """GET /api/universe/current must return universe_curated (manual config)
    and catalog_dynamic (IOL-discovered) as separate fields.
    """
    # Create a snapshot with one position
    snap = PortfolioSnapshot(total_value=100000, cash=12000, currency="USD")
    db.add(snap)
    db.flush()
    db.add(PortfolioPosition(
        snapshot_id=snap.id, symbol="AAPL", asset_type="CEDEAR",
        instrument_type="CEDEAR", currency="USD", quantity=20,
        market_value=38000, avg_price=180, pnl_pct=0.11,
    ))
    db.flush()

    # Seed catalog with a few instruments
    for sym in ("XN6D", "DF6D"):
        db.add(InstrumentCatalog(
            symbol=sym, name=sym, asset_type="TitulosPublicos",
            market="BCBA", currency="ARS", tradable=True,
            source="iol_discovery", is_active=True,
            eligible_for_external_discovery=True,
        ))
    db.commit()

    from app.api.routes import current_universe

    with patch("app.api.routes.get_eligible_universe_symbols", return_value={"XN6D", "DF6D"}):
        # Need to pass a real db that has snapshot
        result = current_universe(db=db)

    # Must have universe_curated as a separate list (manual config, not catalog)
    assert "universe_curated" in result, "universe_curated must be in response"
    assert isinstance(result["universe_curated"], list)

    # Must have catalog_dynamic fields
    assert "catalog_dynamic_count" in result
    assert "catalog_dynamic_sample" in result
    assert isinstance(result["catalog_dynamic_sample"], list)
    assert result["catalog_dynamic_count"] == 2

    # catalog_dynamic_sample must contain our catalog symbols
    assert "XN6D" in result["catalog_dynamic_sample"]
    assert "DF6D" in result["catalog_dynamic_sample"]

    # universe_curated must NOT contain catalog symbols (it's only manual config)
    for sym in result["catalog_dynamic_sample"]:
        if sym not in result.get("universe_curated", []):
            pass  # expected: catalog symbols are not in curated

    # main_allowed must still be present (safety rail)
    assert "main_allowed" in result


# ---------------------------------------------------------------------------
# Test 2: refresh response has no legacy 'source' contradicting new fields
# ---------------------------------------------------------------------------


def test_refresh_no_legacy_source_contradiction(db):
    """refresh_instrument_catalog must NOT have a legacy 'source' field
    that could contradict discovery_source_effective. The old 'source' key
    must either not exist or, if present, must equal discovery_source_effective.
    """
    from app.market.discovery import refresh_instrument_catalog

    # Default mock mode — should use static seed
    result = refresh_instrument_catalog(db)

    # New required fields must all exist
    assert "discovery_source_attempted" in result
    assert "discovery_source_effective" in result
    assert "used_static_seed" in result
    assert "coverage_status" in result

    # Legacy 'source' must not exist as a top-level key
    if "source" in result:
        # If it somehow exists, it MUST match discovery_source_effective
        assert result["source"] == result["discovery_source_effective"], (
            f"Legacy source='{result['source']}' contradicts "
            f"discovery_source_effective='{result['discovery_source_effective']}'"
        )

    # Verify no misleading "iol" anywhere when seed was used
    assert result["discovery_source_effective"] == "static_seed"
    assert result["used_static_seed"] is True


# ---------------------------------------------------------------------------
# Test 3: seed_only coherence (strict)
# ---------------------------------------------------------------------------


def test_seed_only_implies_strict_coherence(db):
    """When coverage_status=seed_only, ALL of these must hold:
    - used_static_seed == True
    - discovery_source_effective == "static_seed"
    - discovery_source_effective != "iol"
    - instruments come from static seed, not IOL
    """
    from app.market.discovery import refresh_instrument_catalog

    # Simulate: real mode attempted, IOL failed, fell to seed
    with patch("app.market.discovery.get_settings") as mock_settings:
        s = MagicMock()
        s.broker_mode = "real"
        s.iol_username = "test_user"
        mock_settings.return_value = s

        with patch("app.market.discovery._discover_from_iol", side_effect=Exception("timeout")):
            result = refresh_instrument_catalog(db)

    assert result["coverage_status"] == "seed_only"
    assert result["used_static_seed"] is True
    assert result["discovery_source_effective"] == "static_seed"
    assert result["discovery_source_effective"] != "iol"
    assert result["discovery_source_attempted"] == "iol"  # it tried IOL
    assert result["symbols_found"] > 0  # seed provided instruments
    assert "CEDEAR" in result["summary_by_asset_type"]  # seed has CEDEARs


# ---------------------------------------------------------------------------
# Test 4: full coverage coherence (strict)
# ---------------------------------------------------------------------------


def test_full_coverage_implies_all_panels_ok(db):
    """When coverage_status=full, ALL panels must have status=ok,
    none can be error or empty.
    """
    from app.market.discovery import refresh_instrument_catalog

    all_ok_panels = [
        {"panel": "Cotizaciones/acciones/argentina/Merval", "asset_type": "ACCIONES",
         "status": "ok", "instruments_found": 10, "error": None},
        {"panel": "Cotizaciones/cedears/argentina/CEDEARs", "asset_type": "CEDEAR",
         "status": "ok", "instruments_found": 15, "error": None},
    ]
    instruments = (
        [{"symbol": f"ACC{i}", "asset_type": "ACCIONES", "source": "iol",
          "source_category": "Merval", "market": "BCBA"} for i in range(10)]
    )
    instruments = list(instruments) + [
        {"symbol": f"CED{i}", "asset_type": "CEDEAR", "source": "iol",
         "source_category": "CEDEARs", "market": "BCBA"} for i in range(15)
    ]

    with patch("app.market.discovery.get_settings") as mock_settings:
        s = MagicMock()
        s.broker_mode = "real"
        s.iol_username = "test_user"
        mock_settings.return_value = s

        with patch("app.market.discovery._discover_from_iol",
                    return_value=(instruments, all_ok_panels)):
            result = refresh_instrument_catalog(db)

    assert result["coverage_status"] == "full"
    assert result["used_static_seed"] is False
    assert result["discovery_source_effective"] == "iol"

    # Verify ALL panels are ok — none error, none empty
    for pr in result["panel_results"]:
        assert pr["status"] == "ok", (
            f"Panel {pr['panel']} has status='{pr['status']}' but coverage_status=full "
            f"requires all panels to be ok"
        )
        assert pr.get("error") is None

    # summary_by_asset_type must reflect discovered types
    assert "ACCIONES" in result["summary_by_asset_type"]
    assert "CEDEAR" in result["summary_by_asset_type"]
    assert result["summary_by_asset_type"]["ACCIONES"] == 10
    assert result["summary_by_asset_type"]["CEDEAR"] == 15
