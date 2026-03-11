from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.broker.clients import IolBrokerClient, map_iol_estadocuenta_cash, map_iol_portfolio_to_snapshot
from app.core.config import get_settings
from app.db.session import Base
from app.services import orchestrator


class FakeResponse:
    def __init__(self, status_code=200, json_data=None):
        self.status_code = status_code
        self._json_data = json_data or {}

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeHttpClient:
    def __init__(self):
        self.get_calls = 0
        self.post_calls = []

    def post(self, url, data=None, headers=None):
        self.post_calls.append(data)
        if data.get("grant_type") == "password":
            return FakeResponse(200, {"access_token": "token-1", "refresh_token": "ref-1", "expires_in": 1})
        if data.get("grant_type") == "refresh_token":
            return FakeResponse(200, {"access_token": "token-2", "refresh_token": "ref-2", "expires_in": 3600})
        return FakeResponse(400, {})

    def get(self, url, headers=None):
        self.get_calls += 1
        if self.get_calls == 1:
            return FakeResponse(401, {})
        if "/portafolio/" in url:
            return FakeResponse(200, {"moneda": "ARS", "disponible": 0, "titulos": []})
        if "/estadocuenta" in url:
            return FakeResponse(200, {"disponible": 4321.5})
        return FakeResponse(404, {})


class BrokenRealClient:
    def get_portfolio_snapshot(self):
        raise RuntimeError("Auth failed")


def make_db():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    return TestingSessionLocal()


def test_map_iol_portfolio_to_snapshot():
    payload = {
        "moneda": "ARS",
        "disponible": 5000,
        "titulos": [
            {"simbolo": "GGAL", "cantidad": 10, "valorizado": 25000, "precioPromedio": 2000, "tipo": "ACCION"}
        ],
    }
    out = map_iol_portfolio_to_snapshot(payload)
    assert out["currency"] == "ARS"
    assert out["cash"] == 5000
    assert out["positions"][0]["symbol"] == "GGAL"
    assert out["positions"][0]["avg_price"] == 2000
    # "ACCION" gets normalized to "ACCIONES" by _normalize_asset_type
    assert out["positions"][0]["instrument_type"] == "ACCIONES"


def test_map_iol_estadocuenta_cash_with_fallbacks():
    assert map_iol_estadocuenta_cash({"disponible": 1500}) == 1500
    assert map_iol_estadocuenta_cash({"saldoDisponible": "2400.5"}) == 2400.5
    assert map_iol_estadocuenta_cash({"cuentas": {"disponible": 300}}) == 300
    assert map_iol_estadocuenta_cash({}) == 0.0


def test_refresh_token_flow_and_real_cash_from_estadocuenta():
    settings = get_settings()
    settings.iol_username = "u"
    settings.iol_password = "p"

    client = IolBrokerClient()
    fake = FakeHttpClient()
    client._client = fake

    snapshot = client.get_portfolio_snapshot()

    assert snapshot["currency"] == "ARS"
    assert snapshot["cash"] == 4321.5
    assert len(fake.post_calls) >= 2
    assert fake.post_calls[0]["grant_type"] == "password"
    assert fake.post_calls[1]["grant_type"] == "refresh_token"


def test_fallback_to_mock_if_auth_fails(monkeypatch):
    db = make_db()
    settings = get_settings()
    settings.broker_mode = "real"
    settings.trigger_cooldown_seconds = 0

    orchestrator._broker_singletons.clear()
    monkeypatch.setattr(orchestrator, "IolBrokerClient", lambda: BrokenRealClient())

    out = orchestrator.run_cycle(db, source="manual")
    assert out["broker_mode"] == "mock_fallback"
