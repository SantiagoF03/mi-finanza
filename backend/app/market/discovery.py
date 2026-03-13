"""IOL instrument catalog discovery service (Priority 1).

Discovers tradeable instruments from IOL Argentina and persists them
to the instrument_catalog table.

Strategy:
- Query IOL /api/v2/Cotizaciones/{instrumentType}/{pais}/{panel} endpoints
  to enumerate available instruments across multiple asset types.
- If IOL credentials are not available or broker_mode=mock, seed from
  the static KNOWN_ASSET_TYPES map in market/assets.py as bootstrap data.
- Deduplicate by symbol, normalize asset types, update last_seen_at.
- Mark instruments not seen in latest refresh as potentially inactive.

Assumption: IOL's cotizaciones endpoint returns a list of instruments
with at least {simbolo, descripcion, ultimoPrecio, moneda} fields.
The exact panel names may vary — we try the most common ones.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.broker.clients import IolBrokerClient, _normalize_asset_type, _map_currency
from app.core.config import get_settings
from app.market.assets import KNOWN_ASSET_TYPES
from app.models.models import InstrumentCatalog
from app.recommendations.universe import VALID_ASSET_TYPES

logger = logging.getLogger(__name__)

# IOL cotizaciones panels to query per instrument type.
# Format: (instrument_type_path, pais, panel, our_asset_type)
# Assumption: IOL exposes GET /api/v2/{Cotizaciones}/{tipo}/{pais}/{panel}
_IOL_PANELS = [
    ("Cotizaciones/acciones/argentina/Merval", "ACCIONES"),
    ("Cotizaciones/acciones/argentina/Panel%20General", "ACCIONES"),
    ("Cotizaciones/cedears/argentina/CEDEARs", "CEDEAR"),
    ("Cotizaciones/bonos/argentina/Todos", "BONO"),
    ("Cotizaciones/letras/argentina/Todos", "TitulosPublicos"),
    ("Cotizaciones/obligaciones_negociables/argentina/Todos", "ON"),
    ("Cotizaciones/etf/argentina/Todos", "ETF"),
    ("Cotizaciones/fci/argentina/Todos", "FondoComundeInversion"),
]

# Asset types allowed for the catalog
_CATALOG_ALLOWED_TYPES = VALID_ASSET_TYPES

# Minimum volume filter (0 = no filter)
_MIN_VOLUME_FILTER = 0


def refresh_instrument_catalog(db: Session, force_seed: bool = False) -> dict:
    """Refresh the instrument catalog from IOL or static seed.

    Returns summary dict with:
    - discovery_source_attempted: what we tried ("iol", "static_seed", "none")
    - discovery_source_effective: what actually provided data ("iol", "static_seed")
    - used_static_seed: bool — True if static seed was the actual data source
    - coverage_status: "full" | "partial" | "seed_only" | "failed"
    - panel_results: per-panel observability
    - summary_by_asset_type: counts per asset type discovered
    """
    settings = get_settings()
    now = datetime.now(timezone.utc)

    instruments_found: list[dict] = []
    panel_results: list[dict] = []
    discovery_source_attempted = "none"
    discovery_source_effective = "none"
    used_static_seed = False

    # Try IOL real discovery if credentials are available
    if settings.broker_mode == "real" and settings.iol_username and not force_seed:
        discovery_source_attempted = "iol"
        try:
            instruments_found, panel_results = _discover_from_iol()
            if instruments_found:
                discovery_source_effective = "iol"
        except Exception as exc:
            logger.warning("IOL discovery failed, falling back to static seed: %s", exc)
            instruments_found = []
            panel_results = [{"panel": "all", "status": "error", "error": str(exc)[:200]}]

    # Fallback or supplement: static seed from KNOWN_ASSET_TYPES
    if not instruments_found or force_seed:
        if discovery_source_attempted == "none":
            discovery_source_attempted = "static_seed"
        instruments_found = _seed_from_static()
        discovery_source_effective = "static_seed"
        used_static_seed = True
        if not panel_results:
            panel_results = [{"panel": "static_seed", "status": "ok",
                              "instruments_found": len(instruments_found)}]

    # Compute coverage_status from panel_results
    if discovery_source_effective == "static_seed":
        coverage_status = "seed_only"
    elif panel_results:
        statuses = {pr.get("status") for pr in panel_results}
        if statuses == {"error"}:
            coverage_status = "failed"
        elif "error" in statuses or "empty" in statuses:
            coverage_status = "partial"
        else:
            coverage_status = "full"
    else:
        coverage_status = "failed"

    # Upsert into DB
    created = 0
    updated = 0
    seen_symbols = set()

    for inst in instruments_found:
        symbol = inst.get("symbol", "").strip().upper()
        if not symbol:
            continue

        asset_type = inst.get("asset_type", "DESCONOCIDO")
        if asset_type not in _CATALOG_ALLOWED_TYPES and asset_type != "DESCONOCIDO":
            continue

        seen_symbols.add(symbol)

        existing = db.query(InstrumentCatalog).filter(
            InstrumentCatalog.symbol == symbol
        ).first()

        if existing:
            existing.last_seen_at = now
            existing.is_active = True
            if inst.get("name"):
                existing.name = inst["name"]
            if inst.get("last_price") is not None:
                existing.last_price = inst["last_price"]
            if inst.get("avg_volume") is not None:
                existing.avg_volume = inst["avg_volume"]
            if asset_type != "DESCONOCIDO" and existing.asset_type == "DESCONOCIDO":
                existing.asset_type = asset_type
            if inst.get("currency"):
                existing.currency = inst["currency"]
            if inst.get("source_category"):
                existing.source_category = inst["source_category"]
            updated += 1
        else:
            new_inst = InstrumentCatalog(
                symbol=symbol,
                name=inst.get("name", ""),
                asset_type=asset_type,
                market=inst.get("market", "BCBA"),
                currency=inst.get("currency", "ARS"),
                tradable=True,
                source=inst.get("source", "iol_discovery"),
                source_category=inst.get("source_category", ""),
                last_seen_at=now,
                is_active=True,
                avg_volume=inst.get("avg_volume"),
                last_price=inst.get("last_price"),
                investable_local=True,
                eligible_for_external_discovery=True,
                metadata_json=inst.get("metadata", {}),
            )
            db.add(new_inst)
            created += 1

    db.flush()

    # Mark not-seen instruments as potentially inactive (soft deactivation)
    # Only if we found a meaningful number of instruments
    deactivated = 0
    if len(seen_symbols) > 10:
        stale = db.query(InstrumentCatalog).filter(
            InstrumentCatalog.is_active == True,
            InstrumentCatalog.symbol.notin_(seen_symbols),
        ).all()
        for inst in stale:
            inst.is_active = False
            deactivated += 1

    db.commit()

    total = db.query(InstrumentCatalog).filter(InstrumentCatalog.is_active == True).count()

    # Build summary_by_asset_type from what was actually discovered
    summary_by_asset_type: dict[str, int] = {}
    for inst in instruments_found:
        at = inst.get("asset_type", "DESCONOCIDO")
        summary_by_asset_type[at] = summary_by_asset_type.get(at, 0) + 1

    return {
        "status": "ok",
        "discovery_source_attempted": discovery_source_attempted,
        "discovery_source_effective": discovery_source_effective,
        "used_static_seed": used_static_seed,
        "coverage_status": coverage_status,
        "created": created,
        "updated": updated,
        "deactivated": deactivated,
        "total_active": total,
        "symbols_found": len(seen_symbols),
        "panel_results": panel_results,
        "summary_by_asset_type": summary_by_asset_type,
    }


def _discover_from_iol() -> tuple[list[dict], list[dict]]:
    """Query IOL cotizaciones panels to discover instruments.

    Returns (instruments, panel_results) where panel_results is a list of
    per-panel metadata dicts for observability.
    """
    client = IolBrokerClient()
    instruments = []
    panel_results = []

    for panel_path, our_type in _IOL_PANELS:
        panel_meta = {
            "panel": panel_path,
            "asset_type": our_type,
            "status": "pending",
            "instruments_found": 0,
            "error": None,
        }
        try:
            resp = client._authorized_get(f"/api/v2/{panel_path}")
            data = resp.json()

            # IOL returns either a dict with "titulos" or a list directly
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("titulos", [])

            panel_count = 0
            for item in items:
                if not isinstance(item, dict):
                    continue
                symbol = (item.get("simbolo") or item.get("symbol") or "").strip()
                if not symbol:
                    continue

                instruments.append({
                    "symbol": symbol,
                    "name": item.get("descripcion", ""),
                    "asset_type": our_type,
                    "currency": _map_currency(item.get("moneda")),
                    "last_price": _safe_float(item.get("ultimoPrecio")),
                    "avg_volume": _safe_float(item.get("volumen")),
                    "source": "iol_cotizaciones",
                    "source_category": panel_path.split("/")[-1] if "/" in panel_path else "",
                    "market": "BCBA",
                    "metadata": {
                        "panel": panel_path,
                        "variacion": item.get("variacionPorcentual"),
                    },
                })
                panel_count += 1

            panel_meta["status"] = "ok" if panel_count > 0 else "empty"
            panel_meta["instruments_found"] = panel_count
        except Exception as exc:
            panel_meta["status"] = "error"
            panel_meta["error"] = str(exc)[:200]
            logger.warning("Failed to fetch IOL panel %s: %s", panel_path, exc)

        panel_results.append(panel_meta)

    return instruments, panel_results


def _seed_from_static() -> list[dict]:
    """Bootstrap catalog from the static KNOWN_ASSET_TYPES map."""
    instruments = []
    for symbol, asset_type in KNOWN_ASSET_TYPES.items():
        instruments.append({
            "symbol": symbol,
            "name": "",
            "asset_type": asset_type,
            "currency": "USD" if asset_type in ("CEDEAR", "ETF") else "ARS",
            "source": "static_seed",
            "source_category": "known_asset_types",
            "market": "BCBA",
        })
    return instruments


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def get_catalog_instruments(
    db: Session,
    *,
    active_only: bool = True,
    eligible_only: bool = False,
    asset_types: list[str] | None = None,
    min_volume: float | None = None,
) -> list[dict]:
    """Get instruments from catalog with optional filters."""
    q = db.query(InstrumentCatalog)

    if active_only:
        q = q.filter(InstrumentCatalog.is_active == True)
    if eligible_only:
        q = q.filter(InstrumentCatalog.eligible_for_external_discovery == True)
    if asset_types:
        q = q.filter(InstrumentCatalog.asset_type.in_(asset_types))
    if min_volume is not None and min_volume > 0:
        q = q.filter(
            (InstrumentCatalog.avg_volume >= min_volume)
            | (InstrumentCatalog.avg_volume == None)  # noqa: E711
        )

    instruments = q.order_by(InstrumentCatalog.symbol).all()
    return [_catalog_to_dict(inst) for inst in instruments]


def get_eligible_universe_symbols(db: Session) -> set[str]:
    """Get the set of symbols eligible for external discovery from the catalog."""
    results = (
        db.query(InstrumentCatalog.symbol)
        .filter(
            InstrumentCatalog.is_active == True,
            InstrumentCatalog.eligible_for_external_discovery == True,
            InstrumentCatalog.tradable == True,
            InstrumentCatalog.asset_type.in_(list(VALID_ASSET_TYPES)),
        )
        .all()
    )
    return {r[0] for r in results}


def get_catalog_asset_type(db: Session, symbol: str) -> str | None:
    """Lookup asset_type for a symbol from the catalog."""
    result = db.query(InstrumentCatalog.asset_type).filter(
        InstrumentCatalog.symbol == symbol,
        InstrumentCatalog.is_active == True,
    ).first()
    return result[0] if result else None


def _catalog_to_dict(inst: InstrumentCatalog) -> dict:
    return {
        "id": inst.id,
        "symbol": inst.symbol,
        "name": inst.name,
        "asset_type": inst.asset_type,
        "market": inst.market,
        "currency": inst.currency,
        "tradable": inst.tradable,
        "source": inst.source,
        "source_category": inst.source_category,
        "last_seen_at": inst.last_seen_at.isoformat() if inst.last_seen_at else None,
        "is_active": inst.is_active,
        "avg_volume": inst.avg_volume,
        "last_price": inst.last_price,
        "investable_local": inst.investable_local,
        "eligible_for_external_discovery": inst.eligible_for_external_discovery,
    }
