"""FCI (Fondos Comunes de Inversión) — capability declaration and analysis.

PHASE 0 RESULT: `FCI_EXECUTION_SUPPORTED = False`.

This repository has a verified, first-party contract for SECURITIES orders
(POST /api/v2/operar/Comprar | /Vender, form-urlencoded, precioLimite,
numeroOperacion in the response) because those calls were exercised against
the real API and pinned by tests. It has NO such evidence for an FCI
subscription or redemption endpoint:

  * no official IOL documentation for an FCI subscribe/redeem contract is
    available to this codebase (the documentation host is not reachable from
    the build environment, and an endpoint may never be inferred);
  * no request/response shape, no operation identifier, no per-fund cutoff,
    no settlement delay and no minimum amount have been verified;
  * the private web-site endpoints are explicitly out of scope — scraping or
    click automation must never stand in for a missing public API.

Consequences, enforced by this module and covered by tests:

  * every FCI subscription/redemption request is refused with the stable code
    `fci_not_supported_by_iol_api`;
  * FCI positions are still ANALYSED, displayed and may still be
    RECOMMENDED — the user simply performs the operation manually in IOL;
  * an informational preview is produced so the user knows exactly what to
    do by hand, and it is explicitly marked as non-executable;
  * FCI never borrows the securities contract: no limit price, no bid/ask, no
    OrderExecution row, no quantity step.

Re-enabling is a deliberate act: verify the official contract, record it in
docs/IOL_FCI_CAPABILITY.md with request/response evidence, implement the
FundInstrument/FundOperation models with their own state machine, and only
then flip this constant.
"""

from __future__ import annotations

from app.broker.execution_class import CLASS_FCI, FAMILY_FUND

# THE single source of truth for FCI execution capability.
FCI_EXECUTION_SUPPORTED = False

# Stable blocking code returned everywhere an FCI operation is requested.
FCI_NOT_SUPPORTED_CODE = "fci_not_supported_by_iol_api"

FCI_MANUAL_INSTRUCTION = (
    "Esta operación de FCI debe realizarse manualmente en IOL. La API oficial "
    "de IOL no expone un contrato verificado de suscripción ni de rescate, por "
    "lo que la aplicación no puede enviarla."
)

# The capabilities we would need to verify before enabling execution. Kept
# explicit so the gap is auditable instead of implicit.
REQUIRED_FCI_CAPABILITIES = (
    "fund_catalog",
    "subscription",
    "redemption",
    "operation_status",
    "cancellation",
    "cutoff",
    "settlement_delay",
    "currency",
    "minimum_amount",
    "sandbox",
)


def get_fci_capability() -> dict:
    """Read-only capability report for FCI. Contains no secrets."""
    return {
        "execution_class": CLASS_FCI,
        "execution_family": FAMILY_FUND,
        "fci_execution_supported": FCI_EXECUTION_SUPPORTED,
        "subscription_supported": False,
        "redemption_supported": False,
        "cancellation_supported": False,
        "blocking_code": FCI_NOT_SUPPORTED_CODE,
        # What the application CAN legitimately do with FCI today.
        "analysis_supported": True,
        "display_supported": True,
        "recommendation_supported": True,
        "informational_preview_supported": True,
        "manual_operation_required": True,
        "unverified_capabilities": list(REQUIRED_FCI_CAPABILITIES),
        "reason": (
            "No official IOL contract for FCI subscription/redemption has been "
            "verified by this repository. Endpoints are never inferred and the "
            "private web-site endpoints are out of scope."
        ),
        "documentation": "docs/IOL_FCI_CAPABILITY.md",
    }


def fci_execution_blocked(operation: str) -> dict:
    """The refusal returned for ANY attempted FCI subscribe/redeem.

    Always a refusal: there is deliberately no code path where this function
    can answer "allowed". A flag alone can never authorise an operation whose
    contract is unknown.
    """
    return {
        "error": FCI_MANUAL_INSTRUCTION,
        "code": FCI_NOT_SUPPORTED_CODE,
        "status_code": 501,
        "operation": operation,
        "fci_execution_supported": False,
        "manual_operation_required": True,
    }


def build_fci_informational_preview(
    *,
    symbol: str,
    side: str,
    currency: str = "",
    position_quantity=None,
    position_value=None,
    fund_cutoff_local_time: str | None = None,
    settlement_delay_days: int | None = None,
    fund_minimum_amount=None,
) -> dict:
    """Informational-only FCI preview.

    Deliberately carries NO limit price, NO bid/ask and NO quantity step: a
    fund is subscribed/redeemed by amount or by cuotapartes at an unknown
    end-of-day value, so presenting a price would be a lie. Every field that
    would authorise sending is absent by construction, and the payload is
    self-describing about being non-executable.

    `side` is normalised to the FUND vocabulary (subscribe/redeem), never
    buy/sell, so an FCI intent can never be mistaken for a securities order.
    """
    operation = "subscribe" if str(side or "").strip().lower() in {"buy", "subscribe", "suscripcion"} else "redeem"
    return {
        "execution_family": FAMILY_FUND,
        "execution_class": CLASS_FCI,
        "symbol": str(symbol or "").strip().upper(),
        "operation": operation,
        "currency": str(currency or "").strip().upper(),
        "current_quantity": position_quantity,
        "current_value": position_value,
        "fund_cutoff_local_time": fund_cutoff_local_time,
        "settlement_delay_days": settlement_delay_days,
        "fund_minimum_amount": fund_minimum_amount,
        # Explicitly absent — an FCI operation has none of these.
        "limit_price": None,
        "best_bid": None,
        "best_ask": None,
        "quantity_step": None,
        # Explicitly non-executable.
        "executable": False,
        "would_execute": False,
        "informational_only": True,
        "immediate": False,
        "blocking_reasons": [FCI_NOT_SUPPORTED_CODE],
        "manual_operation_required": True,
        "manual_instruction": FCI_MANUAL_INSTRUCTION,
    }
