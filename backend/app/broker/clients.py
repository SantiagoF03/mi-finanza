from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.core.config import get_settings


class BrokerClient(ABC):
    @abstractmethod
    def get_portfolio_snapshot(self) -> dict:
        ...

    def ping(self) -> dict:
        return {"status": "ok", "mode": "mock"}


def map_iol_portfolio_to_snapshot(payload: dict) -> dict:
    """
    Normaliza el JSON de IOL /api/v2/portafolio/{pais} al formato interno del MVP.

    Estructura esperada (según tu Postman):
      {
        "pais": "argentina",
        "activos": [
          {
            "cantidad": ...,
            "valorizado": ...,
            "ppc": ...,
            "gananciaPorcentaje": ...,
            "titulo": {
                "simbolo": "...",
                "tipo": "...",
                "moneda": "peso_Argentino|dolar_Estadounidense"
            }
          }
        ]
      }
    """
    activos = payload.get("activos") or []
    positions: list[dict] = []

    # En tu JSON no aparece cash, así que lo dejamos 0 por ahora
    cash = 0.0

    # Snapshot currency general (para Argentina: ARS).
    # La moneda real por activo va en cada position.
    snapshot_currency = "ARS"

    def map_currency(iol_moneda: str) -> str:
        m = (iol_moneda or "").strip().lower()
        if m in {"peso_argentino", "peso argentino", "ars"}:
            return "ARS"
        if m in {"dolar_estadounidense", "dólar estadounidense", "usd", "u$s"}:
            return "USD"
        return "ARS"

    for a in activos:
        titulo = a.get("titulo") or {}

        symbol = titulo.get("simbolo") or ""
        if not symbol:
            continue

        iol_tipo = (titulo.get("tipo") or "").strip()
        iol_moneda = (titulo.get("moneda") or "").strip()

        quantity = float(a.get("cantidad") or 0.0)
        market_value = float(a.get("valorizado") or 0.0)
        avg_price = float(a.get("ppc") or 0.0)

        asset_type = iol_tipo or "DESCONOCIDO"
        currency = map_currency(iol_moneda)

        pnl_pct_raw = a.get("gananciaPorcentaje")
        try:
            pnl_pct = float(pnl_pct_raw) / 100.0 if pnl_pct_raw is not None else 0.0
        except (TypeError, ValueError):
            pnl_pct = 0.0

        positions.append(
            {
                "symbol": symbol,
                "asset_type": asset_type,
                "instrument_type": iol_tipo,
                "currency": currency,
                "quantity": quantity,
                "market_value": market_value,
                "avg_price": avg_price,
                "pnl_pct": pnl_pct,
            }
        )

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "currency": snapshot_currency,
        "cash": cash,
        "positions": positions,
    }


class IolBrokerClient(BrokerClient):
    def __init__(self) -> None:
        settings = get_settings()
        self.api_base = settings.iol_api_base.rstrip("/")
        self.username = settings.iol_username
        self.password = settings.iol_password
        self.country = settings.iol_portfolio_country
        self.timeout = settings.iol_timeout_seconds
        self._client = httpx.Client(timeout=self.timeout)
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._expires_at: datetime | None = None

    def _token_expired(self) -> bool:
        if not self._access_token or not self._expires_at:
            return True
        return datetime.now(timezone.utc) >= self._expires_at

    def _set_tokens(self, payload: dict[str, Any]) -> None:
        self._access_token = payload.get("access_token")
        self._refresh_token = payload.get("refresh_token")
        expires_in = int(payload.get("expires_in") or 0)
        self._expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=max(expires_in - 30, 0)
        )

    def _authenticate_password(self) -> None:
        if not self.username or not self.password:
            raise RuntimeError("IOL credentials missing")

        resp = self._client.post(
            f"{self.api_base}/token",
            data={
                "username": self.username,
                "password": self.password,
                "grant_type": "password",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        self._set_tokens(resp.json())

    def _refresh_access_token(self) -> bool:
        if not self._refresh_token:
            return False

        resp = self._client.post(
            f"{self.api_base}/token",
            data={
                "refresh_token": self._refresh_token,
                "grant_type": "refresh_token",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        if resp.status_code >= 400:
            return False

        self._set_tokens(resp.json())
        return True

    def _ensure_auth(self) -> None:
        if self._token_expired():
            if not self._refresh_access_token():
                self._authenticate_password()

    def _authorized_get(self, path: str) -> httpx.Response:
        self._ensure_auth()
        assert self._access_token

        resp = self._client.get(
            f"{self.api_base}{path}",
            headers={"Authorization": f"Bearer {self._access_token}"},
        )

        if resp.status_code in {401, 403}:
            if self._refresh_access_token():
                assert self._access_token
                resp = self._client.get(
                    f"{self.api_base}{path}",
                    headers={"Authorization": f"Bearer {self._access_token}"},
                )

        resp.raise_for_status()
        return resp

    def ping(self) -> dict:
        try:
            resp = self._authorized_get("/api/v2/estadocuenta")
            return {"status": "ok", "mode": "real", "http_status": resp.status_code}
        except Exception as exc:
            return {"status": "error", "mode": "real", "message": str(exc)}

    def get_portfolio_snapshot(self) -> dict:
        resp = self._authorized_get(f"/api/v2/portafolio/{self.country}")
        return map_iol_portfolio_to_snapshot(resp.json())


class MockBrokerClient(BrokerClient):
    def get_portfolio_snapshot(self) -> dict:
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "currency": "USD",
            "cash": 12000,
            "positions": [
                {
                    "symbol": "AAPL",
                    "asset_type": "CEDEAR",
                    "instrument_type": "CEDEAR",
                    "currency": "USD",
                    "quantity": 20,
                    "market_value": 38000,
                    "avg_price": 180,
                    "pnl_pct": 0.11,
                },
                {
                    "symbol": "MSFT",
                    "asset_type": "CEDEAR",
                    "instrument_type": "CEDEAR",
                    "currency": "USD",
                    "quantity": 12,
                    "market_value": 28000,
                    "avg_price": 340,
                    "pnl_pct": 0.08,
                },
                {
                    "symbol": "SPY",
                    "asset_type": "ETF",
                    "instrument_type": "ETF",
                    "currency": "USD",
                    "quantity": 15,
                    "market_value": 17000,
                    "avg_price": 510,
                    "pnl_pct": 0.05,
                },
                {
                    "symbol": "AL30",
                    "asset_type": "BONO",
                    "instrument_type": "BONO",
                    "currency": "ARS",
                    "quantity": 3000,
                    "market_value": 9000,
                    "avg_price": 42,
                    "pnl_pct": -0.02,
                },
            ],
        }