from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.core.config import get_settings


class BrokerClient(ABC):
    @abstractmethod
    def get_portfolio_snapshot(self) -> dict: ...

    def ping(self) -> dict:
        return {"status": "ok", "mode": "mock"}


def map_iol_estadocuenta_cash(payload: dict[str, Any]) -> float:
    """Extrae cash disponible desde /estadocuenta con fallbacks robustos."""
    candidates = [
        payload.get("disponible"),
        payload.get("saldoDisponible"),
        payload.get("cuentas", {}).get("disponible"),
        payload.get("cuenta", {}).get("disponible"),
        payload.get("cash"),
    ]
    for value in candidates:
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return 0.0


def map_iol_portfolio_to_snapshot(
    payload: dict[str, Any],
    default_currency: str = "ARS",
    cash_override: float | None = None,
) -> dict:
    """Mapea respuesta variada de IOL al formato interno del MVP."""

    titulos = payload.get("titulos") or payload.get("activos") or payload.get("positions") or []
    portfolio_cash = payload.get("disponible")
    if portfolio_cash is None:
        portfolio_cash = payload.get("cuentas", {}).get("disponible")
    if portfolio_cash is None:
        portfolio_cash = payload.get("cash")
    cash = float(cash_override if cash_override is not None else (portfolio_cash or 0))

    currency = payload.get("moneda") or payload.get("currency") or default_currency

    positions: list[dict[str, Any]] = []
    for item in titulos:
        symbol = item.get("simbolo") or item.get("ticker") or item.get("symbol")
        if not symbol:
            continue

        quantity = float(item.get("cantidad") or item.get("quantity") or 0)
        market_value = (
            item.get("valorizado")
            or item.get("valuado")
            or item.get("marketValue")
            or item.get("market_value")
            or (quantity * float(item.get("ultimoPrecio") or item.get("lastPrice") or 0))
        )
        avg_price = item.get("precioPromedio") or item.get("averagePrice") or item.get("avg_price")
        instrument_type = item.get("tipo") or item.get("tipoInstrumento") or item.get("instrumentType") or "UNKNOWN"

        positions.append(
            {
                "symbol": symbol,
                "quantity": quantity,
                "market_value": float(market_value or 0),
                "avg_price": float(avg_price) if avg_price is not None else None,
                "instrument_type": instrument_type,
                "asset_type": instrument_type,
                "currency": item.get("moneda") or item.get("currency") or currency,
                "pnl_pct": float(item.get("rentabilidad") or item.get("pnlPct") or 0),
            }
        )

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "currency": currency,
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
        self._expires_at = datetime.now(timezone.utc) + timedelta(seconds=max(expires_in - 30, 0))

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
        resp = self._client.get(f"{self.api_base}{path}", headers={"Authorization": f"Bearer {self._access_token}"})
        if resp.status_code in {401, 403}:
            if self._refresh_access_token():
                assert self._access_token
                resp = self._client.get(f"{self.api_base}{path}", headers={"Authorization": f"Bearer {self._access_token}"})
        resp.raise_for_status()
        return resp

    def ping(self) -> dict:
        try:
            resp = self._authorized_get("/api/v2/estadocuenta")
            return {"status": "ok", "mode": "real", "http_status": resp.status_code}
        except Exception as exc:  # pragma: no cover
            return {"status": "error", "mode": "real", "message": str(exc)}

    def get_portfolio_snapshot(self) -> dict:
        portfolio_resp = self._authorized_get(f"/api/v2/portafolio/{self.country}")
        estado_resp = self._authorized_get("/api/v2/estadocuenta")
        real_cash = map_iol_estadocuenta_cash(estado_resp.json())
        return map_iol_portfolio_to_snapshot(portfolio_resp.json(), cash_override=real_cash)


class MockBrokerClient(BrokerClient):
    def get_portfolio_snapshot(self) -> dict:
        return {
            "timestamp": datetime.utcnow().isoformat(),
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
