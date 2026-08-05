from __future__ import annotations

import logging
import math
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from app.core.config import get_settings

logger = logging.getLogger(__name__)

# Every IOL operar endpoint — securities and funds alike — is form-urlencoded.
IOL_FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"


class BrokerClient(ABC):
    @abstractmethod
    def get_portfolio_snapshot(self) -> dict:
        ...

    def ping(self) -> dict:
        return {"status": "ok", "mode": "mock"}

    def get_execution_quote(self, symbol: str, side: str, market: str, settlement: str) -> dict:
        """Executable quote for a limit order.

        Contract: sell uses the best bid (precioCompra), buy uses the best
        ask (precioVenta). ultimoPrecio is NEVER an execution fallback. When
        the needed side of the book is missing → available=False (never a
        market order).

        Returns {available, price, source, retrieved_at, market, settlement}.
        """
        return {
            "available": False,
            "price": None,
            "source": "none",
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "market": market,
            "settlement": settlement,
        }

    def get_live_cash(self, currency: str) -> dict:
        """Live AVAILABLE balance in ONE currency, read immediately before a buy.

        Contract: {available, cash, currency, committed, retrieved_at, source}.
        `available` False means the balance could not be established — the
        caller must block, never fall back to a snapshot value.
        """
        return {
            "available": False,
            "cash": None,
            "currency": currency,
            "committed": None,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "source": "none",
        }


def _extract_iol_validation_errors(data: Any) -> str:
    """Summarize an IOL validation-error body, or "" if it is not one.

    IOL can answer 2xx (observed: 202) with a list — or a wrapped list — of
    {title, description} entries when it refuses an order. Detecting this
    shape is what separates a DEFINITIVE rejection from a genuinely
    uncertain submission.
    """
    candidates = None
    if isinstance(data, list):
        candidates = data
    elif isinstance(data, dict):
        for key in ("errors", "Errors", "mensajes", "messages"):
            value = data.get(key)
            if isinstance(value, list):
                candidates = value
                break
        if candidates is None and (data.get("title") or data.get("description")):
            candidates = [data]

    if not candidates:
        return ""

    parts = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or item.get("Title") or "").strip()
        description = str(item.get("description") or item.get("Description") or "").strip()
        if title or description:
            parts.append(f"{title}: {description}".strip(": ").strip())
    return " | ".join(parts)[:400]


def _extract_fund_operation_id(data: Any) -> str:
    """Operation identifier from an FCI response.

    Only field names that are documented for orders (`numeroOperacion`) or
    that are their obvious direct casings are read. If none is present we say
    so instead of inventing one — an operation we cannot identify is an
    operation we cannot reconcile.
    """
    if not isinstance(data, dict):
        return ""
    for key in ("numeroOperacion", "numerooperacion", "NumeroOperacion"):
        value = data.get(key)
        if value not in (None, "", 0):
            return str(value)
    return ""


def _positive_price(value) -> float | None:
    """Finite positive price, or None. Rejects NaN/Infinity/junk."""
    if value is None or isinstance(value, bool):
        return None
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(price) or price <= 0:
        return None
    return price


def _extract_book_price(data: Any, field: str) -> float | None:
    """Read one side of the order book from an IOL quote payload.

    The real /Cotizacion response carries precioCompra/precioVenta at the top
    level; `puntas` may additionally be a dict or a list of book levels
    (best level first). ultimoPrecio is NEVER used — a missing side of the
    book must fail closed.
    """
    if not isinstance(data, dict):
        return None

    top_level = _positive_price(data.get(field))
    if top_level is not None:
        return top_level

    puntas = data.get("puntas")
    if isinstance(puntas, dict):
        return _positive_price(puntas.get(field))
    if isinstance(puntas, list):
        for level in puntas:
            if isinstance(level, dict):
                price = _positive_price(level.get(field))
                if price is not None:
                    return price
    return None


def map_iol_estadocuenta_cash(payload: dict[str, Any]) -> float:
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

    for candidate in [
        payload.get("disponible"),
        payload.get("saldoDisponible"),
        payload.get("cash"),
    ]:
        parsed = to_float(candidate)
        if parsed is not None:
            return parsed

    cuenta = payload.get("cuenta")
    if isinstance(cuenta, dict):
        for key in ["disponible", "saldoDisponible", "cash"]:
            parsed = to_float(cuenta.get(key))
            if parsed is not None:
                return parsed

    cuentas = payload.get("cuentas")
    if isinstance(cuentas, dict):
        for key in ["disponible", "saldoDisponible", "cash"]:
            parsed = to_float(cuentas.get(key))
            if parsed is not None:
                return parsed

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


def map_iol_estadocuenta_by_currency(payload: dict[str, Any], currency: str) -> dict:
    """Available balance for ONE currency from /api/v2/estadocuenta.

    Deliberately NOT the same as map_iol_estadocuenta_cash, which sums every
    account regardless of currency. Summing pesos and dollars produces a
    number that means nothing, and using it to authorise a buy would be a
    silent overdraft. Here an account only counts when its `moneda` maps to
    the requested currency; if none does, we fail closed.

    Prefers the most conservative figure IOL exposes:
      disponibleOperar  (what may actually be used to operate)
      → disponible
    and reports `committed` separately so the caller can show it.
    """
    target = (currency or "").strip().upper()
    result = {
        "available": False,
        "cash": None,
        "currency": target,
        "committed": None,
        "matched_accounts": 0,
        "source": "estadocuenta",
    }
    if not target or not isinstance(payload, dict):
        return result

    def to_float(value) -> float | None:
        if value is None or value == "" or isinstance(value, bool):
            return None
        try:
            num = float(value)
        except (TypeError, ValueError):
            return None
        return num if math.isfinite(num) else None

    cuentas = payload.get("cuentas")
    if isinstance(cuentas, dict):
        cuentas = [cuentas]
    if not isinstance(cuentas, list):
        return result

    total: float | None = None
    committed_total: float | None = None
    matched = 0

    for cuenta in cuentas:
        if not isinstance(cuenta, dict):
            continue
        if _map_currency(cuenta.get("moneda")) != target:
            continue
        matched += 1

        account_available: float | None = None
        saldos = cuenta.get("saldos")
        if isinstance(saldos, list):
            for saldo in saldos:
                if not isinstance(saldo, dict):
                    continue
                candidate = to_float(saldo.get("disponibleOperar"))
                if candidate is None:
                    candidate = to_float(saldo.get("disponible"))
                if candidate is None:
                    continue
                # Immediate settlement is the only bucket usable right now.
                liquidacion = str(saldo.get("liquidacion") or "").strip().lower()
                if liquidacion and liquidacion not in {"inmediato", "immediate", "0", "t0"}:
                    continue
                account_available = candidate if account_available is None else min(
                    account_available, candidate
                )
                comp = to_float(saldo.get("comprometido"))
                if comp is not None:
                    committed_total = comp if committed_total is None else committed_total + comp

        if account_available is None:
            for key in ("disponibleOperar", "disponible", "saldoDisponible"):
                account_available = to_float(cuenta.get(key))
                if account_available is not None:
                    break
        if account_available is None:
            # An account in the right currency whose balance we cannot read
            # makes the whole answer untrustworthy — never treat it as zero.
            return {**result, "matched_accounts": matched}

        total = account_available if total is None else total + account_available

    if matched == 0 or total is None:
        return {**result, "matched_accounts": matched}

    return {
        **result,
        "available": True,
        "cash": total,
        "committed": committed_total,
        "matched_accounts": matched,
    }


def _normalize_asset_type(iol_tipo: str) -> str:
    """Normalize IOL's titulo.tipo to our internal asset type format.

    IOL V2 returns lowercase with underscores (e.g. "acciones",
    "fondos_comunes_de_inversion", "cedears"). We map these to our
    canonical types: CEDEAR, ACCIONES, BONO, ON, TitulosPublicos,
    FondoComundeInversion, ETF.
    """
    t = (iol_tipo or "").strip()
    t_lower = t.lower().replace(" ", "_")

    _IOL_TIPO_MAP: dict[str, str] = {
        "cedears": "CEDEAR", "cedear": "CEDEAR",
        "acciones": "ACCIONES", "accion": "ACCIONES",
        "acciones_argentina": "ACCIONES",
        "bonos": "BONO", "bono": "BONO",
        "titulos_publicos": "TitulosPublicos",
        "titulo_publico": "TitulosPublicos",
        "letras": "TitulosPublicos", "letra": "TitulosPublicos",
        "obligaciones_negociables": "ON",
        "obligacion_negociable": "ON", "on": "ON",
        "fondos_comunes_de_inversion": "FondoComundeInversion",
        "fondo_comun_de_inversion": "FondoComundeInversion",
        "fci": "FondoComundeInversion",
        "etf": "ETF", "etfs": "ETF",
    }

    if t_lower in _IOL_TIPO_MAP:
        return _IOL_TIPO_MAP[t_lower]

    # Already in canonical format
    _CANONICAL = {"CEDEAR", "ACCIONES", "BONO", "ON", "TitulosPublicos",
                  "FondoComundeInversion", "ETF"}
    if t in _CANONICAL:
        return t

    return t or "DESCONOCIDO"


def _map_currency(iol_moneda: str | None) -> str:
    """Normalize IOL currency to ARS/USD."""
    m = (iol_moneda or "").strip().lower().replace(" ", "_")
    if m in {"peso_argentino", "pesos", "peso", "ars", "$"}:
        return "ARS"
    if m in {"dolar_estadounidense", "dólar_estadounidense",
             "dolar_eeuu", "dólar_eeuu", "usd", "u$s",
             "dolar", "dólar", "dolares", "dólares",
             "dolar_billete", "dolar_mep", "dolar_ccl",
             "dolar_cable", "us$"}:
        return "USD"
    return "ARS"


def map_iol_portfolio_to_snapshot(
    payload: dict[str, Any],
    cash_override: float | None = None,
) -> dict:
    """
    Normaliza el JSON real de IOL /api/v2/portafolio/{pais} al formato interno del MVP.

    Soporta:
    - estructura real V2 con "activos"
    - estructura vieja con "titulos"
    - estructura ya normalizada con "positions"
    """
    positions: list[dict[str, Any]] = []

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
            normalized_tipo = _normalize_asset_type(iol_tipo)
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
                    "asset_type": normalized_tipo,
                    "instrument_type": normalized_tipo,
                    "currency": _map_currency(iol_moneda),
                    "quantity": quantity,
                    "market_value": market_value,
                    "avg_price": avg_price,
                    "pnl_pct": pnl_pct,
                }
            )

    elif isinstance(payload.get("titulos"), list):
        for t in payload.get("titulos", []):
            if not isinstance(t, dict):
                continue

            symbol = t.get("simbolo") or ""
            if not symbol:
                continue

            raw_tipo = t.get("tipo") or "DESCONOCIDO"
            normalized_tipo = _normalize_asset_type(raw_tipo)

            positions.append(
                {
                    "symbol": symbol,
                    "asset_type": normalized_tipo,
                    "instrument_type": normalized_tipo,
                    "currency": _map_currency(payload.get("moneda")),
                    "quantity": float(t.get("cantidad") or 0.0),
                    "market_value": float(t.get("valorizado") or 0.0),
                    "avg_price": float(t.get("precioPromedio") or 0.0),
                    "pnl_pct": 0.0,
                }
            )

    elif isinstance(payload.get("positions"), list):
        for p in payload.get("positions", []):
            if isinstance(p, dict):
                positions.append(p)

    cash = cash_override if cash_override is not None else float(payload.get("disponible") or 0.0)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "currency": "ARS",
        "cash": cash,
        "positions": positions,
    }


class IolBrokerClient(BrokerClient):
    def __init__(
        self,
        api_base: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        settings = get_settings()
        # Explicit environment injection (sandbox vs real) — defaults keep the
        # legacy read-path behavior (real credentials from settings).
        self.api_base = (api_base or settings.iol_api_base).rstrip("/")
        self.username = username if username is not None else settings.iol_username
        self.password = password if password is not None else settings.iol_password
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

        real_cash = 0.0
        try:
            estado_resp = self._authorized_get("/api/v2/estadocuenta")
            real_cash = map_iol_estadocuenta_cash(estado_resp.json())
        except Exception:
            real_cash = 0.0

        return map_iol_portfolio_to_snapshot(
            portfolio_resp.json(),
            cash_override=real_cash,
        )


    def _authorized_post(self, path: str, json_body: dict | None = None) -> httpx.Response:
        self._ensure_auth()
        assert self._access_token is not None

        resp = self._client.post(
            f"{self.api_base}{path}",
            json=json_body,
            headers={"Authorization": f"Bearer {self._access_token}"},
        )

        if resp.status_code in {401, 403}:
            if self._refresh_access_token():
                assert self._access_token is not None
                resp = self._client.post(
                    f"{self.api_base}{path}",
                    json=json_body,
                    headers={"Authorization": f"Bearer {self._access_token}"},
                )

        resp.raise_for_status()
        return resp

    # Mapping from internal side to IOL-specific endpoint
    _IOL_SIDE_ENDPOINTS: dict[str, str] = {
        "sell": "/api/v2/operar/Vender",
        "buy": "/api/v2/operar/Comprar",
    }

    def place_order(self, symbol: str, side: str, quantity: float, price: float | None = None) -> dict:
        """DISABLED legacy path — refuses to send, always.

        This method used to POST a JSON body with a hardcoded `plazo` and an
        implicit `precioMercado` when no price was given. None of that
        matches the documented personal-order contract, and market orders are
        banned in this version.

        Real/sandbox execution MUST go through:
            build_iol_order_request()  → application/x-www-form-urlencoded
            → submit_order_request()

        Raising here is defense in depth: approve_and_execute already routes
        every non-mock environment to the canonical contract, so if this is
        ever reached it is a bug — and it must fail closed instead of
        sending a malformed real order.
        """
        raise RuntimeError(
            "IolBrokerClient.place_order is disabled: real orders must use "
            "build_iol_order_request() + submit_order_request() "
            "(application/x-www-form-urlencoded, precioLimite only)."
        )


    def get_order_status(self, order_id: str) -> dict:
        """Check order status via IOL API."""
        try:
            resp = self._authorized_get(f"/api/v2/operaciones/{order_id}")
            data = resp.json()
            # The real API returns "estadoActual" (e.g. "iniciada",
            # "cancelada"); "estado" is kept as a fallback for older shapes.
            status = "unknown"
            if isinstance(data, dict):
                status = data.get("estadoActual") or data.get("estado") or "unknown"
            return {"order_id": order_id, "raw_response": data, "status": status}
        except Exception as exc:
            return {"order_id": order_id, "status": "error", "error": str(exc)}

    def cancel_order(self, order_id: str) -> dict:
        """DELETE /api/v2/operaciones/{numero} — cancel ONE live order.

        Sent AT MOST ONCE by the caller; this method never retries. Typed
        outcome, mirroring submit_order_request:

        - "cancelled": 2xx, IOL accepted the cancellation
        - "rejected":  4xx after auth handling — evaluated and refused
          (already executed, already cancelled, unknown operation)
        - "cancellation_unknown": timeout / connection error / 5xx — the
          DELETE may have been applied. NEVER re-send; reconcile manually.
        """
        endpoint = f"/api/v2/operaciones/{order_id}"
        url = f"{self.api_base}{endpoint}"

        # --- BEFORE the point of no return: authentication is fully resolved.
        # A DELETE is not idempotent from our side: IOL may apply it even if we
        # never see the response, and by the time we could retry, the operation
        # number may already belong to a DIFFERENT, later order. So the token
        # is renewed HERE, where nothing has been sent yet and a failure is
        # definitively "not sent".
        try:
            self._ensure_auth()
        except Exception as exc:
            return {
                "outcome": "rejected",
                "endpoint_used": endpoint,
                "raw_response": {},
                "http_requests_sent": 0,
                "error": f"Auth failed before cancellation: {str(exc)[:200]}",
            }

        # --- POINT OF NO RETURN: exactly ONE HTTP request from here on. ---
        # There is deliberately no refresh-and-retry: a 401/403 arriving AFTER
        # the request was written is classified without sending anything else.
        try:
            resp = self._client.delete(
                url, headers={"Authorization": f"Bearer {self._access_token}"}
            )
        except httpx.HTTPError as exc:
            # ONLY transport errors are ambiguous — the request may have been
            # written before the failure. A local programming error sent
            # nothing and must not be disguised as an uncertain cancellation.
            return {
                "outcome": "cancellation_unknown",
                "endpoint_used": endpoint,
                "raw_response": {},
                "http_requests_sent": 1,
                "error": f"Network failure during cancellation: {str(exc)[:200]}",
            }

        try:
            raw = resp.json()
        except Exception:
            raw = {"status_code": resp.status_code}

        base = {
            "endpoint_used": endpoint,
            "raw_response": raw,
            "http_requests_sent": 1,
        }

        if 200 <= resp.status_code < 300:
            return {**base, "outcome": "cancelled", "error": ""}
        if resp.status_code in {401, 403}:
            # Authentication was valid when we sent this, so a 401/403 now is
            # ambiguous: it may have been evaluated and refused, or the token
            # may have expired mid-flight after IOL already applied it. We do
            # NOT re-send to find out.
            return {
                **base,
                "outcome": "cancellation_unknown",
                "error": (
                    f"HTTP {resp.status_code} after the cancellation was sent — "
                    "not retried. Outcome unknown."
                ),
            }
        if 400 <= resp.status_code < 500:
            return {
                **base,
                "outcome": "rejected",
                "error": f"HTTP {resp.status_code}: cancellation not accepted.",
            }
        return {
            **base,
            "outcome": "cancellation_unknown",
            "error": f"HTTP {resp.status_code}: cancellation outcome unknown.",
        }

    def submit_fund_request(self, request: dict, *, lifecycle: dict | None = None) -> dict:
        """POST an FCI subscription/redemption EXACTLY as built.

        Contract: application/x-www-form-urlencoded with Simbolo / Monto /
        soloValidar. Never JSON, never retried.

        LIFECYCLE
        ---------
        The caller may pass a mutable `lifecycle` dict. It is stamped as the
        call progresses:

            before_send       reached the method, nothing sent yet
            request_started   we began `self._client.post` — from here on we
                              CANNOT prove nothing left the process
            response_received a response object came back
            response_parsed   the body was read

        This exists so that an exception escaping this method is not guesswork
        for the caller. Everything before `request_started` is definitively
        "not sent" and must NOT become `submission_unknown`; stranding an
        operation in a state that requires human reconciliation because of a
        local bug is a worse failure than the bug.

        Outcomes depend on `soloValidar`:

        soloValidar=true  → "validated" | "rejected" | "validation_unknown"
          A validation NEVER creates an operation, so it can never come back
          as submitted, and an ambiguous validation is simply not validated
          (safe: the caller stays in `prepared`).

        soloValidar=false → "submitted" | "rejected" | "submission_unknown"
          A subscription is not idempotent — a duplicate is real money — so
          anything we cannot prove is `submission_unknown` and reconciled by
          hand.

        `local_error` is the one outcome that means "nothing was sent, and it
        was our fault". It carries http_requests_sent=0.
        """
        marks = lifecycle if isinstance(lifecycle, dict) else {}
        marks.update({
            "before_send": True,
            "request_started": False,
            "response_received": False,
            "response_parsed": False,
        })

        def _envelope(**extra) -> dict:
            return {
                "operation_id": "",
                "raw_response": {},
                "solo_validar": bool(request.get("solo_validar")),
                "request_started": marks["request_started"],
                "response_received": marks["response_received"],
                "response_parsed": marks["response_parsed"],
                **extra,
            }

        # --- Everything here is BEFORE the point of no return. A failure is
        #     provably "not sent", so it is a local error, never ambiguity.
        #
        #     The request OBJECT is built here too, deliberately. Building and
        #     sending used to be one call, so a serialization bug — bad form
        #     data, an unencodable header — was indistinguishable from a
        #     network failure and became `submission_unknown`, stranding the
        #     operation until a human checked IOL for a request that was never
        #     assembled, let alone sent. Now `build_request` failing is a
        #     local error, and only `send` can be ambiguous.
        try:
            endpoint = request["endpoint"]
            form_data = request["form_data"]
            solo_validar = bool(request.get("solo_validar"))
            content_type = request.get("content_type", IOL_FORM_CONTENT_TYPE)
            url = f"{self.api_base}{endpoint}"
            self._ensure_auth()
            request_obj = self._client.build_request(
                "POST",
                url,
                data=form_data,
                headers={
                    "Authorization": f"Bearer {self._access_token}",
                    "Content-Type": content_type,
                },
            )
        except Exception as exc:
            logger.exception("submit_fund_request failed before sending anything")
            return _envelope(
                outcome="local_error",
                endpoint_used=str(request.get("endpoint") or ""),
                http_requests_sent=0,
                error=f"Failed before sending: {type(exc).__name__}: {str(exc)[:200]}",
            )

        # --- POINT OF NO RETURN: exactly ONE request. No refresh-and-retry.
        #     The request is fully built; the only thing left is to put it on
        #     the wire, so anything raised from here on is ambiguous by
        #     construction.
        marks["request_started"] = True
        try:
            resp = self._client.send(request_obj)
            marks["response_received"] = True
        except Exception as exc:
            return _envelope(
                outcome="validation_unknown" if solo_validar else "submission_unknown",
                endpoint_used=endpoint,
                http_requests_sent=1,
                error=f"Failure after the request started: {str(exc)[:200]}",
            )

        try:
            data = resp.json()
            marks["response_parsed"] = True
        except Exception:
            # A response arrived but its body is not JSON. `response_received`
            # stays true and `response_parsed` stays false, so the caller can
            # tell "IOL answered something we could not read" from "IOL never
            # answered" — the status code below still classifies the outcome.
            data = None

        base = _envelope(
            endpoint_used=endpoint,
            raw_response=data if isinstance(data, (dict, list)) else
                         {"status_code": resp.status_code},
            http_requests_sent=1,
            status_code=resp.status_code,
        )

        rejection = _extract_iol_validation_errors(data)

        if 200 <= resp.status_code < 300:
            if rejection:
                # IOL answers 2xx with a body of validation errors when it
                # refuses — a DEFINITIVE rejection, not an uncertain send.
                return {**base, "outcome": "rejected",
                        "error": f"HTTP {resp.status_code}: {rejection}"}
            if solo_validar:
                return {**base, "outcome": "validated", "error": ""}
            numero = _extract_fund_operation_id(data)
            if numero:
                return {**base, "outcome": "submitted",
                        "operation_id": str(numero), "error": ""}
            # 2xx but we cannot tell whether the operation was created.
            return {
                **base,
                "outcome": "submission_unknown",
                "error": (
                    f"HTTP {resp.status_code} without an operation identifier — "
                    "outcome unknown."
                ),
            }

        if 400 <= resp.status_code < 500:
            # Evaluated and refused. Definitive for both modes.
            return {
                **base,
                "outcome": "rejected",
                "error": (
                    f"HTTP {resp.status_code}: not accepted."
                    + (f" {rejection}" if rejection else "")
                ),
            }

        # 5xx or anything else: no contract says the operation was NOT created.
        return {
            **base,
            "outcome": "validation_unknown" if solo_validar else "submission_unknown",
            "error": f"HTTP {resp.status_code}: outcome unknown.",
        }

    def get_execution_quote(self, symbol: str, side: str, market: str, settlement: str) -> dict:
        """Executable quote: best bid for sell, best ask for buy.

        ultimoPrecio is NEVER used as an execution fallback — a missing side
        of the book means available=False, and no order is sent.
        """
        retrieved_at = datetime.now(timezone.utc).isoformat()
        base = {
            "available": False,
            "price": None,
            "source": "none",
            "retrieved_at": retrieved_at,
            "market": market,
            "settlement": settlement,
        }
        # Verified against the real API: /api/v2/Cotizaciones/detalle/{market}/
        # {symbol} answers HTTP 400. The working contract is
        # /api/v2/{market}/Titulos/{symbol}/Cotizacion.
        try:
            resp = self._authorized_get(f"/api/v2/{market}/Titulos/{symbol}/Cotizacion")
            data = resp.json()
        except Exception:
            return base

        field = "precioCompra" if side == "sell" else "precioVenta"
        raw_price = _extract_book_price(data, field)
        if raw_price is None:
            # No usable side of the book → never fall back to ultimoPrecio,
            # never turn this into a market order.
            return base
        return {
            **base,
            "available": True,
            "price": raw_price,
            "source": "bid" if side == "sell" else "ask",
        }

    def get_live_cash(self, currency: str) -> dict:
        """Live available balance in ONE currency, read right before a buy.

        Any failure (network, auth, unreadable body, currency not present in
        the account) yields available=False. The caller must block with
        live_cash_unavailable — a snapshot cash figure is never a substitute.
        """
        retrieved_at = datetime.now(timezone.utc).isoformat()
        base = {
            "available": False,
            "cash": None,
            "currency": (currency or "").strip().upper(),
            "committed": None,
            "retrieved_at": retrieved_at,
            "source": "estadocuenta",
        }
        try:
            resp = self._authorized_get("/api/v2/estadocuenta")
            data = resp.json()
        except Exception:
            return base
        if not isinstance(data, dict):
            return base
        from app.broker.account_status import available_for_currency

        parsed = available_for_currency(data, currency)
        return {**base, **parsed, "retrieved_at": retrieved_at}

    def get_account_status_raw(self) -> dict:
        """READ-ONLY estadocuenta payload, for contract diagnosis only.

        Returned to the diagnostic endpoint, which normalizes it and never
        echoes it back verbatim.
        """
        resp = self._authorized_get("/api/v2/estadocuenta")
        data = resp.json()
        return data if isinstance(data, dict) else {}

    def submit_order_request(self, order_request: dict) -> dict:
        """Submit a canonical form-urlencoded order request (built by
        app.broker.order_request.build_iol_order_request) EXACTLY as given.

        Typed outcome classification:
        - "sent": HTTP 2xx + valid JSON + non-empty numeroOperacion
        - "rejected": DEFINITIVE non-acceptance (4xx after auth handling —
          the request was evaluated and not accepted)
        - "submission_uncertain": the request may have reached IOL but we
          cannot prove the outcome (timeout / connection error during send,
          5xx, 2xx without numeroOperacion, unreadable 2xx body). The caller
          must route this to manual reconciliation — NEVER auto-retry.
        """
        endpoint = order_request["endpoint"]
        form_data = order_request["form_data"]
        url = f"{self.api_base}{endpoint}"

        # Auth failures BEFORE the order POST are definitive: nothing sent.
        try:
            self._ensure_auth()
        except Exception as exc:
            return {
                "outcome": "rejected",
                "order_id": "",
                "endpoint_used": endpoint,
                "raw_response": {},
                "error": f"Auth failed before submission: {str(exc)[:200]}",
            }

        def _post() -> httpx.Response:
            return self._client.post(
                url,
                data=form_data,
                headers={
                    "Authorization": f"Bearer {self._access_token}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )

        try:
            resp = _post()
            if resp.status_code in {401, 403} and self._refresh_access_token():
                resp = _post()
        except Exception as exc:
            # The POST may have been written before the failure — uncertain.
            return {
                "outcome": "submission_uncertain",
                "order_id": "",
                "endpoint_used": endpoint,
                "raw_response": {},
                "error": f"Network failure during submission: {str(exc)[:200]}",
            }

        if 200 <= resp.status_code < 300:
            try:
                data = resp.json()
            except Exception:
                return {
                    "outcome": "submission_uncertain",
                    "order_id": "",
                    "endpoint_used": endpoint,
                    "raw_response": {"status_code": resp.status_code, "unparseable_body": True},
                    "error": "2xx response with unreadable body — outcome unknown.",
                }
            numero = data.get("numeroOperacion") if isinstance(data, dict) else None
            if numero:
                return {
                    "outcome": "sent",
                    "order_id": str(numero),
                    "endpoint_used": endpoint,
                    "raw_response": data,
                    "error": "",
                }

            # Verified against the real API: IOL answers HTTP 202 with a body
            # of validation errors (e.g. [{"title": "PrecioLimite",
            # "description": "Los decimales indicados no son compatibles con
            # la alteración mínima permitida..."}]) when it refuses an order.
            # That is a DEFINITIVE rejection — the order was never created —
            # so it must not be treated as an uncertain submission.
            rejection = _extract_iol_validation_errors(data)
            if rejection:
                return {
                    "outcome": "rejected",
                    "order_id": "",
                    "endpoint_used": endpoint,
                    "raw_response": data if isinstance(data, (dict, list)) else {"body": str(data)[:500]},
                    "error": f"HTTP {resp.status_code}: order rejected by IOL validation — {rejection}",
                }

            return {
                "outcome": "submission_uncertain",
                "order_id": "",
                "endpoint_used": endpoint,
                "raw_response": data if isinstance(data, (dict, list)) else {"body": str(data)[:500]},
                "error": f"HTTP {resp.status_code} without numeroOperacion — outcome unknown.",
            }

        raw = {}
        try:
            raw = resp.json()
        except Exception:
            raw = {"status_code": resp.status_code}

        if 400 <= resp.status_code < 500:
            # Definitive: IOL evaluated the request and did not accept it.
            return {
                "outcome": "rejected",
                "order_id": "",
                "endpoint_used": endpoint,
                "raw_response": raw,
                "error": f"HTTP {resp.status_code}: request not accepted.",
            }

        # 5xx or anything else: no contract guarantees the order was NOT
        # created server-side → uncertain.
        return {
            "outcome": "submission_uncertain",
            "order_id": "",
            "endpoint_used": endpoint,
            "raw_response": raw,
            "error": f"HTTP {resp.status_code}: outcome unknown.",
        }


class MockBrokerClient(BrokerClient):
    _mock_orders: list[dict] = []

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

    def place_order(self, symbol: str, side: str, quantity: float, price: float | None = None) -> dict:
        """Mock order placement — always succeeds."""
        import uuid
        endpoint = f"/api/v2/operar/{'Vender' if side == 'sell' else 'Comprar'}"
        order_id = f"MOCK-{uuid.uuid4().hex[:8].upper()}"
        order = {
            "order_id": order_id,
            "status": "sent",
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price or 100.0,
            "endpoint_used": endpoint,
            "raw_response": {"mock": True, "numeroOperacion": order_id},
        }
        self._mock_orders.append(order)
        return order

    def get_order_status(self, order_id: str) -> dict:
        """Mock order status — always returns executed."""
        return {"order_id": order_id, "status": "terminada", "raw_response": {"mock": True}}

    def get_execution_quote(self, symbol: str, side: str, market: str, settlement: str) -> dict:
        """Mock quote — price None keeps the historical mock market-order
        semantics (mock never builds IOL requests, so precioLimite rules do
        not apply here)."""
        return {
            "available": True,
            "price": None,
            "source": "market_order",
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "market": market,
            "settlement": settlement,
        }

    def get_live_cash(self, currency: str) -> dict:
        """Mock balance — matches the mock snapshot's own currency only, so a
        currency mismatch is still exercised end to end without IOL."""
        target = (currency or "").strip().upper()
        snapshot = self.get_portfolio_snapshot()
        matches = target == (snapshot.get("currency") or "").strip().upper()
        return {
            "available": matches,
            "cash": float(snapshot.get("cash") or 0.0) if matches else None,
            "currency": target,
            "committed": 0.0 if matches else None,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "source": "mock",
        }