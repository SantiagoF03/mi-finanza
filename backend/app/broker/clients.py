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


def map_iol_estadocuenta_cash(payload: dict) -> float:
    """
    Extrae cash/disponible desde /api/v2/estadocuenta con varios fallbacks.
    Soporta estructuras tipo dict y listas en 'cuentas'.
    """
    def to_float(value) -> float | None:
        try:
            if value is None or value == "":
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    # Fallbacks directos en raíz
    direct_candidates = [
        payload.get("disponible"),
        payload.get("saldoDisponible"),
        payload.get("cash"),
    ]

    for candidate in direct_candidates:
        parsed = to_float(candidate)
        if parsed is not None:
            return parsed

    # Caso cuenta simple como dict
    cuenta = payload.get("cuenta")
    if isinstance(cuenta, dict):
        for key in ["disponible", "saldoDisponible", "cash"]:
            parsed = to_float(cuenta.get(key))
            if parsed is not None:
                return parsed

    # Caso cuentas como dict
    cuentas = payload.get("cuentas")
    if isinstance(cuentas, dict):
        for key in ["disponible", "saldoDisponible", "cash"]:
            parsed = to_float(cuentas.get(key))
            if parsed is not None:
                return parsed

    # Caso cuentas como lista
    if isinstance(cuentas, list):
        total = 0.0
        found_any = False

        for item in cuentas:
            if not isinstance(item, dict):
                continue

            for key in ["disponible", "saldoDisponible", "cash"]:
                parsed = to_float(item.get(key))
                if parsed is not None:
                    total += parsed
                    found_any = True
                    break

        if found_any:
            return total

    return 0.0

def map_iol_portfolio_to_snapshot(payload: dict, cash_override: float | None = None) -> dict:
    """
    Normaliza el JSON real de IOL /api/v2/portafolio/{pais} al formato interno del MVP.

    Soporta:
    - estructura real V2 con "activos"
    - estructura vieja/mock con "titulos" o "positions"
    """
    positions: list[dict] = []

    def map_currency(iol_moneda: str | None) -> str:
        m = (iol_moneda or "").strip().lower()
        if m in {"peso_argentino", "peso argentino", "ars"}:
            return "ARS"
        if m in {"dolar_estadounidense", "dólar estadounidense", "usd", "u$s"}:
            return "USD"
        return "ARS"

    # ===== Caso real IOL V2: activos =====
    activos = payload.get("activos") or []
    if isinstance(activos, list) and activos:
        for a in activos:
            if not isinstance(a, dict):
                continue

            titulo = a.get("titulo") or {}
            if not isinstance(titulo, dict):
                titulo = {}

            symbol = titulo.get("simbolo") or ""
            if not symbol:
                continue

            iol_tipo = (titulo.get("tipo") or "").strip()
            iol_moneda = titulo.get("moneda")

            try:
                quantity = float(a.get("cantidad") or 0.0)
            except (TypeError, ValueError):
                quantity = 0.0

            try:
                market_value = float(a.get("valorizado") or 0.0)
            except (TypeError, ValueError):
                market_value = 0.0

            try:
                avg_price = float(a.get("ppc") or 0.0)
            except (TypeError, ValueError):
                avg_price = 0.0

            try:
                pnl_pct = float(a.get("gananciaPorcentaje") or 0.0) / 100.0
            except (TypeError, ValueError):
                pnl_pct = 0.0

            positions.append(
                {
                    "symbol": symbol,
                    "asset_type": iol_tipo or "DESCONOCIDO",
                    "instrument_type": iol_tipo or "DESCONOCIDO",
                    "currency": map_currency(iol_moneda),
                    "quantity": quantity,
                    "market_value": market_value,
                    "avg_price": avg_price,
                    "pnl_pct": pnl_pct,
                }
            )

    # ===== Fallback legacy: titulos =====
    elif isinstance(payload.get("titulos"), list):
        for t in payload.get("titulos", []):
            if not isinstance(t, dict):
                continue
            symbol = t.get("simbolo") or ""
            if not symbol:
                continue
            positions.append(
                {
                    "symbol": symbol,
                    "asset_type": t.get("tipo") or "DESCONOCIDO",
                    "instrument_type": t.get("tipo") or "DESCONOCIDO",
                    "currency": payload.get("moneda", "ARS"),
                    "quantity": float(t.get("cantidad") or 0.0),
                    "market_value": float(t.get("valorizado") or 0.0),
                    "avg_price": float(t.get("precioPromedio") or 0.0),
                    "pnl_pct": 0.0,
                }
            )

    # ===== Fallback extra: positions ya normalizadas =====
    elif isinstance(payload.get("positions"), list):
        for p in payload.get("positions", []):
            if isinstance(p, dict):
                positions.append(p)

    cash = cash_override if cash_override is not None else float(payload.get("disponible") or 0.0)

    return {
        "timestamp": datetime.utcnow().isoformat() + "+00:00",
        "currency": "ARS",
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
        assert self._access_token is not None

        resp = self._client.get(
            f"{self.api_base}{path}",
            headers={"Authorization": f"Bearer {self._access_token}"},
        )

        if resp.status_code in {401, 403}:
            if self._refresh_access_token():
                assert self._access_token is not None
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
        portfolio_resp = self._authorized_get(f"/api/v2/portafolio/{self.country}")
        portfolio_payload = portfolio_resp.json()

        real_cash = 0.0
        try:
            estado_resp = self._authorized_get("/api/v2/estadocuenta")
            real_cash = map_iol_estadocuenta_cash(estado_resp.json())
        except Exception:
            real_cash = 0.0

        return map_iol_portfolio_to_snapshot(portfolio_payload, cash_override=real_cash)

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
