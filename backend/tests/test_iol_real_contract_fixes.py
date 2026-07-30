"""Corrections derived from a controlled test against the REAL IOL API.

Evidence gathered manually against https://api.invertironline.com:

1. GET /api/v2/Cotizaciones/detalle/bCBA/BYMA        → HTTP 400 Bad Request
   GET /api/v2/bCBA/Titulos/BYMA/Cotizacion          → OK
   (ultimoPrecio 298, precioCompra 298, precioVenta 299)

2. A sell limit order whose price decimals were incompatible with the
   minimum tick got HTTP **202** with a body of validation errors:
   [{"title": "PrecioLimite",
     "description": "Los decimales indicados no son compatibles con la
                     alteración mínima permitida..."}]
   → a DEFINITIVE rejection, NOT an uncertain submission.

These tests pin the corrected behavior. They use httpx.MockTransport and
never contact the real API.
"""

from contextlib import contextmanager

import httpx
import pytest

from app.broker.clients import IolBrokerClient
from app.broker.instrument_scope import validate_instrument_policy
from app.broker.order_request import build_iol_order_request
from app.core.config import get_settings

VALIDITY = "2026-07-29T12:00:00"


@contextmanager
def exec_settings(**overrides):
    s = get_settings()
    saved = {k: getattr(s, k) for k in overrides}
    try:
        for k, v in overrides.items():
            setattr(s, k, v)
        yield s
    finally:
        for k, v in saved.items():
            setattr(s, k, v)


def _broker(handler) -> IolBrokerClient:
    broker = IolBrokerClient(
        api_base="https://sandbox.iol-test.local", username="u", password="p"
    )
    broker._client = httpx.Client(transport=httpx.MockTransport(handler), timeout=10)
    return broker


def _token(request: httpx.Request) -> httpx.Response | None:
    if request.url.path == "/token":
        return httpx.Response(200, json={
            "access_token": "t", "refresh_token": "r", "expires_in": 3600,
        })
    return None


# ───────────────────────────────────────────────────────────────────
# Evidencia 1 — endpoint de cotización corregido
# ───────────────────────────────────────────────────────────────────


def test_quote_uses_the_working_titulos_endpoint():
    """The app must call /api/v2/{market}/Titulos/{symbol}/Cotizacion.
    The old /Cotizaciones/detalle/... path answers 400 on the real API."""
    seen = []

    def handler(request):
        token = _token(request)
        if token:
            return token
        seen.append(request.url.path)
        if request.url.path == "/api/v2/bCBA/Titulos/BYMA/Cotizacion":
            return httpx.Response(200, json={
                "ultimoPrecio": 298, "precioCompra": 298, "precioVenta": 299,
            })
        # Mirror the real API: the legacy path is a 400.
        return httpx.Response(400, json={"message": "Bad Request"})

    quote = _broker(handler).get_execution_quote("BYMA", "sell", "bCBA", "t1")
    assert "/api/v2/bCBA/Titulos/BYMA/Cotizacion" in seen
    assert not any("Cotizaciones/detalle" in p for p in seen)
    assert quote["available"] is True
    assert quote["price"] == 298
    assert quote["source"] == "bid"


def test_quote_reads_real_response_shape_for_both_sides():
    """Real payload keeps book prices at the top level: sell → precioCompra
    (298), buy → precioVenta (299)."""
    payload = {"ultimoPrecio": 298, "precioCompra": 298, "precioVenta": 299}

    def handler(request):
        return _token(request) or httpx.Response(200, json=payload)

    broker = _broker(handler)
    sell = broker.get_execution_quote("BYMA", "sell", "bCBA", "t1")
    buy = broker.get_execution_quote("BYMA", "buy", "bCBA", "t1")
    assert (sell["price"], sell["source"]) == (298, "bid")
    assert (buy["price"], buy["source"]) == (299, "ask")


@pytest.mark.parametrize("puntas", [
    {"precioCompra": 298, "precioVenta": 299},                      # dict shape
    [{"precioCompra": 298, "precioVenta": 299}],                    # list shape
    [{"precioVenta": 299}, {"precioCompra": 298}],                  # best level scan
])
def test_quote_also_supports_puntas_shapes(puntas):
    def handler(request):
        return _token(request) or httpx.Response(200, json={"ultimoPrecio": 298, "puntas": puntas})

    quote = _broker(handler).get_execution_quote("BYMA", "sell", "bCBA", "t1")
    assert quote["available"] is True
    assert quote["price"] == 298


def test_quote_never_falls_back_to_ultimo_precio():
    """Only ultimoPrecio present → NOT executable (never a market order)."""
    def handler(request):
        return _token(request) or httpx.Response(200, json={"ultimoPrecio": 298})

    quote = _broker(handler).get_execution_quote("BYMA", "sell", "bCBA", "t1")
    assert quote["available"] is False
    assert quote["price"] is None


def test_quote_http_400_fails_closed():
    def handler(request):
        return _token(request) or httpx.Response(400, json={"message": "Bad Request"})

    quote = _broker(handler).get_execution_quote("BYMA", "sell", "bCBA", "t1")
    assert quote["available"] is False
    assert quote["retrieved_at"]


# ───────────────────────────────────────────────────────────────────
# Evidencia 2 — HTTP 202 con errores = rechazo definitivo
# ───────────────────────────────────────────────────────────────────


def _submit(handler) -> dict:
    request, err = build_iol_order_request(
        side="sell", symbol="BYMA", quantity=1, price=298,
        market="bCBA", settlement="t1", order_type="precioLimite", validity=VALIDITY,
    )
    assert err is None
    return _broker(handler).submit_order_request(request)


def test_http_202_with_validation_errors_is_definitive_rejection():
    """The exact body observed on the real API must classify as rejected —
    never submission_uncertain, which would trigger needless manual
    reconciliation for an order that was never created."""
    body = [{
        "title": "PrecioLimite",
        "description": ("Los decimales indicados no son compatibles con la "
                        "alteración mínima permitida para el instrumento."),
    }]

    def handler(request):
        return _token(request) or httpx.Response(202, json=body)

    result = _submit(handler)
    assert result["outcome"] == "rejected"
    assert result["order_id"] == ""
    assert "PrecioLimite" in result["error"]
    assert "alteración mínima" in result["error"]
    assert result["raw_response"] == body


@pytest.mark.parametrize("body", [
    {"errors": [{"title": "PrecioLimite", "description": "decimales inválidos"}]},
    {"title": "Cantidad", "description": "cantidad inválida"},
    {"mensajes": [{"title": "Plazo", "description": "plazo inválido"}]},
])
def test_wrapped_validation_error_shapes_are_rejections(body):
    def handler(request):
        return _token(request) or httpx.Response(202, json=body)

    assert _submit(handler)["outcome"] == "rejected"


def test_202_with_numero_operacion_is_still_sent():
    """A 2xx that DOES carry numeroOperacion remains a successful send."""
    def handler(request):
        return _token(request) or httpx.Response(202, json={"numeroOperacion": 987654})

    result = _submit(handler)
    assert result["outcome"] == "sent"
    assert result["order_id"] == "987654"


def test_2xx_without_operation_or_errors_stays_uncertain():
    """An unrecognized 2xx body is still uncertain — we must not claim it
    was rejected when we cannot prove it."""
    def handler(request):
        return _token(request) or httpx.Response(200, json={"ok": True})

    result = _submit(handler)
    assert result["outcome"] == "submission_uncertain"


def test_unparseable_2xx_stays_uncertain():
    def handler(request):
        return _token(request) or httpx.Response(200, content=b"<html>nope</html>")

    assert _submit(handler)["outcome"] == "submission_uncertain"


# ───────────────────────────────────────────────────────────────────
# Evidencia 2b — tick de precio (alteración mínima) antes de enviar
# ───────────────────────────────────────────────────────────────────


def test_price_incompatible_with_tick_is_blocked_before_sending():
    """The order that IOL refused must never leave the app again."""
    request, err = build_iol_order_request(
        side="sell", symbol="BYMA", quantity=1, price="298.37",
        market="bCBA", settlement="t1", order_type="precioLimite",
        validity=VALIDITY, price_tick="0.5",
    )
    assert request is None
    assert err == "price_tick_mismatch"


def test_price_compatible_with_tick_is_accepted():
    request, err = build_iol_order_request(
        side="sell", symbol="BYMA", quantity=1, price="298.5",
        market="bCBA", settlement="t1", order_type="precioLimite",
        validity=VALIDITY, price_tick="0.5",
    )
    assert err is None
    assert request["form_data"]["precio"] == "298.5"


def test_price_is_never_rounded_to_fit_the_tick():
    """Rounding would silently change the reviewed limit price."""
    request, err = build_iol_order_request(
        side="sell", symbol="BYMA", quantity=1, price="298.24",
        market="bCBA", settlement="t1", order_type="precioLimite",
        validity=VALIDITY, price_tick="0.1",
    )
    assert request is None
    assert err == "price_tick_mismatch"


@pytest.mark.parametrize("bad_tick", [0, -1, "abc", float("nan"), float("inf")])
def test_invalid_tick_fails_closed(bad_tick):
    request, err = build_iol_order_request(
        side="sell", symbol="BYMA", quantity=1, price=298,
        market="bCBA", settlement="t1", order_type="precioLimite",
        validity=VALIDITY, price_tick=bad_tick,
    )
    assert request is None
    assert err == "invalid_price_tick"


def test_price_tick_is_a_required_policy_field():
    """Without a known tick we cannot guarantee an acceptable price."""
    base = {
        "asset_type": "CEDEAR", "instrument_type": "CEDEAR", "currency": "ARS",
        "market": "bCBA", "settlement": "t1",
        "quantity_step": 1, "max_quantity": 5, "max_notional": 200000,
    }
    policy, err = validate_instrument_policy("BYMA", base)
    assert policy is None
    assert err == "instrument_policy_invalid"

    policy, err = validate_instrument_policy("BYMA", {**base, "price_tick": 0.5})
    assert err is None
    assert policy["price_tick"] == 0.5

    for bad in (0, -0.5, float("nan"), float("inf"), "x"):
        policy, err = validate_instrument_policy("BYMA", {**base, "price_tick": bad})
        assert policy is None
        assert err == "instrument_policy_invalid"
