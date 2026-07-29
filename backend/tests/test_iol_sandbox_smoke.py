"""OPTIONAL IOL sandbox smoke test — DISABLED by default.

This is the ONLY test in the repository allowed to make an external call,
and it only runs when explicitly armed:

    RUN_IOL_SANDBOX_TESTS=true

With the flag unset/false (the default, including CI) it is skipped and no
network is touched.

When armed it fails closed unless EVERYTHING is explicitly configured for
sandbox: broker_mode=sandbox, a non-real sandbox base URL, sandbox
credentials, sandbox_execution_enabled=true and a dedicated smoke symbol +
quantity. It never uses production values, never uses the real portfolio,
and never talks to api.invertironline.com.

The official IOL sandbox URL is NOT documented in this repository — do not
guess it. Obtain it (and sandbox credentials) from IOL and configure:
    IOL_SANDBOX_API_BASE=...
    IOL_SANDBOX_USERNAME=...
    IOL_SANDBOX_PASSWORD=...
    IOL_SANDBOX_SMOKE_SYMBOL=...
    IOL_SANDBOX_SMOKE_QUANTITY=1
"""

import os

import pytest

RUN_SANDBOX = os.environ.get("RUN_IOL_SANDBOX_TESTS", "false").strip().lower() == "true"


@pytest.mark.skipif(not RUN_SANDBOX, reason="RUN_IOL_SANDBOX_TESTS != true — sandbox smoke disabled (no external calls)")
def test_iol_sandbox_smoke_readonly_and_quote():
    """Armed sandbox smoke: auth + read-only quote against the OFFICIAL IOL
    sandbox only. Refuses to run against anything that looks like the real
    API or with production credentials. Does NOT place an order — order
    submission remains a deliberate, manual step through the app flow."""
    from app.broker.clients import IolBrokerClient
    from app.broker.environment import resolve_execution_environment
    from app.core.config import get_settings

    settings = get_settings()

    if settings.broker_mode != "sandbox":
        pytest.fail("Sandbox smoke requires BROKER_MODE=sandbox (explicitly).")
    env = resolve_execution_environment(settings)
    if env["errors"]:
        pytest.fail(f"Sandbox environment not usable (fail closed): {env['errors']}. "
                    "Configure IOL_SANDBOX_API_BASE and sandbox credentials from IOL.")
    if "api.invertironline.com" in env["api_base"]:
        pytest.fail("Sandbox smoke must NEVER point at the real IOL API.")
    if not settings.sandbox_execution_enabled:
        pytest.fail("Sandbox smoke requires SANDBOX_EXECUTION_ENABLED=true (explicit opt-in).")

    smoke_symbol = os.environ.get("IOL_SANDBOX_SMOKE_SYMBOL", "").strip()
    smoke_quantity = os.environ.get("IOL_SANDBOX_SMOKE_QUANTITY", "").strip()
    if not smoke_symbol or not smoke_quantity:
        pytest.fail("Configure IOL_SANDBOX_SMOKE_SYMBOL and IOL_SANDBOX_SMOKE_QUANTITY "
                    "expressly for sandbox — production assets are not used here.")

    broker = IolBrokerClient(
        api_base=env["api_base"], username=env["username"], password=env["password"]
    )
    ping = broker.ping()
    assert ping.get("status") == "ok", f"Sandbox auth/ping failed: {ping}"

    quote = broker.get_execution_quote(
        smoke_symbol, "sell", settings.iol_order_market, settings.iol_order_settlement
    )
    # Read-only smoke: we only assert the quote contract shape.
    assert "available" in quote and "retrieved_at" in quote
    print(f"[SANDBOX SMOKE] host={env['api_base']} symbol={smoke_symbol} quote={quote}")
