"""Broker environment resolution — mock | sandbox | real, fail closed.

Single source of truth for which API host and credentials an execution
broker may use. Sandbox and real can never be confused:
- sandbox NEVER uses (or contains) the real IOL host;
- real NEVER uses sandbox credentials;
- mock never touches the network at all.

The deprecated `iol_use_sandbox` flag is honored by mapping it to
broker_mode=sandbox so it cannot remain dead configuration.
"""

from __future__ import annotations

from urllib.parse import urlparse

_REAL_IOL_HOST = "api.invertironline.com"

_VALID_MODES = {"mock", "sandbox", "real"}


def resolve_execution_environment(settings) -> dict:
    """Resolve the effective execution environment from settings.

    Returns:
    {
        "environment": "mock" | "sandbox" | "real",
        "api_base": str,          # empty for mock
        "username": str,          # empty for mock (never returned to clients)
        "password": str,          # empty for mock (never returned to clients)
        "errors": [str, ...],     # stable blocking codes; non-empty = unusable
    }
    """
    mode = (settings.broker_mode or "").strip().lower()

    # DEPRECATED mapping: iol_use_sandbox=true forces sandbox resolution so
    # the flag can never silently do nothing.
    if mode == "real" and getattr(settings, "iol_use_sandbox", False):
        mode = "sandbox"

    if mode not in _VALID_MODES:
        return {
            "environment": mode or "unknown",
            "api_base": "",
            "username": "",
            "password": "",
            "errors": ["unsupported_broker_mode"],
        }

    if mode == "mock":
        return {"environment": "mock", "api_base": "", "username": "", "password": "", "errors": []}

    if mode == "sandbox":
        errors = []
        base = (settings.iol_sandbox_api_base or "").strip().rstrip("/")
        if not base:
            errors.append("sandbox_environment_not_configured")
        elif _REAL_IOL_HOST in base:
            # Sandbox may NEVER point at (or through) the real IOL host.
            errors.append("sandbox_environment_invalid")
        username = (settings.iol_sandbox_username or "").strip()
        password = settings.iol_sandbox_password or ""
        if not username or not password:
            errors.append("sandbox_credentials_not_configured")
        return {
            "environment": "sandbox",
            "api_base": base,
            "username": username,
            "password": password,
            "errors": errors,
        }

    # real
    errors = []
    base = (settings.iol_real_api_base or settings.iol_api_base or "").strip().rstrip("/")
    host = urlparse(base).netloc if base else ""
    if not base:
        errors.append("real_environment_not_configured")
    elif host != _REAL_IOL_HOST:
        # Real execution may ONLY talk to the real IOL API.
        errors.append("real_environment_invalid")
    username = (settings.iol_real_username or settings.iol_username or "").strip()
    password = settings.iol_real_password or settings.iol_password or ""
    if not username or not password:
        errors.append("real_credentials_not_configured")
    return {
        "environment": "real",
        "api_base": base,
        "username": username,
        "password": password,
        "errors": errors,
    }


def api_host_of(env: dict) -> str:
    """Host (netloc) of the resolved environment — safe to expose."""
    base = env.get("api_base") or ""
    return urlparse(base).netloc if base else ""
