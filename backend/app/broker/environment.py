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


def _validate_network_url(base: str) -> tuple[str | None, str | None]:
    """Structurally validate a broker API base URL.

    Returns (hostname, error_code). Any network environment must be HTTPS,
    have a real hostname, carry no embedded credentials, no query string and
    no fragment, and use the default TLS port.
    """
    try:
        parsed = urlparse(base)
    except (ValueError, TypeError):
        return None, "broker_environment_url_invalid"

    if parsed.scheme != "https":
        return None, "broker_environment_requires_https"

    try:
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        # Malformed port (e.g. "https://host:notaport")
        return None, "broker_environment_url_invalid"

    if not hostname:
        return None, "broker_environment_url_invalid"
    if parsed.username or parsed.password:
        return None, "broker_environment_url_invalid"
    if parsed.query or parsed.fragment:
        return None, "broker_environment_url_invalid"
    if port is not None and port != 443:
        return None, "broker_environment_url_invalid"

    return hostname, None


def effective_execution_environment(settings) -> str:
    """THE authoritative execution environment: mock | sandbox | real.

    Never decide locks, factories or branching from settings.broker_mode
    directly — the deprecated iol_use_sandbox flag can make the effective
    environment differ from the configured mode.
    """
    return resolve_execution_environment(settings)["environment"]


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
        else:
            hostname, url_error = _validate_network_url(base)
            if url_error:
                errors.append(url_error)
            elif hostname == _REAL_IOL_HOST:
                # Sandbox may NEVER point at the real IOL host.
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
    if not base:
        errors.append("real_environment_not_configured")
    else:
        hostname, url_error = _validate_network_url(base)
        if url_error:
            errors.append(url_error)
        elif hostname != _REAL_IOL_HOST:
            # Exact hostname match — never a substring check, so lookalikes
            # such as api.invertironline.com.evil.example are rejected.
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
