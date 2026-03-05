from abc import ABC, abstractmethod
from datetime import datetime


class BrokerClient(ABC):
    @abstractmethod
    def get_portfolio_snapshot(self) -> dict: ...


class IolBrokerClient(BrokerClient):
    def get_portfolio_snapshot(self) -> dict:
        raise NotImplementedError("TODO: integrar credenciales OAuth/REST de IOL")


class MockBrokerClient(BrokerClient):
    def get_portfolio_snapshot(self) -> dict:
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "currency": "USD",
            "cash": 12000,
            "positions": [
                {"symbol": "AAPL", "asset_type": "CEDEAR", "currency": "USD", "quantity": 20, "market_value": 38000, "pnl_pct": 0.11},
                {"symbol": "MSFT", "asset_type": "CEDEAR", "currency": "USD", "quantity": 12, "market_value": 28000, "pnl_pct": 0.08},
                {"symbol": "SPY", "asset_type": "ETF", "currency": "USD", "quantity": 15, "market_value": 17000, "pnl_pct": 0.05},
                {"symbol": "AL30", "asset_type": "BONO", "currency": "ARS", "quantity": 3000, "market_value": 9000, "pnl_pct": -0.02},
            ],
        }
