"""GET /api/v2/estadocuenta — normalized parsing and contract diagnosis.

The balance is what authorises a purchase, so reading it wrongly is the most
expensive parsing bug in the system. Two failure modes matter:

1. **Reading the wrong number.** `disponible` is not the same as
   `disponibleOperar`; a settlement bucket that is not immediate is not cash
   we can spend right now.
2. **Reading a number that isn't there.** If a field we expect is absent, the
   honest answer is "unknown" — never 0, which reads as "no money" and
   silently blocks, or worse, a partial sum that reads as "enough".

Everything here is pure: it parses a payload the broker client already
fetched, and diagnoses whether the payload matches the shape we understand.
It never opens a connection and never places an order.
"""

from __future__ import annotations

import math

# Field names we read, grouped by role. These are the names this codebase has
# evidence for. An unrecognised payload is REPORTED as unrecognised rather
# than guessed at — `diagnose_account_status` exists precisely so a contract
# drift is visible before it becomes a bad order.
CURRENCY_FIELDS = ("moneda",)
ACCOUNT_LIST_FIELDS = ("cuentas",)
# Preferred first: "what may actually be used to operate".
AVAILABLE_FIELDS = ("disponibleOperar", "disponible", "saldoDisponible")
COMMITTED_FIELDS = ("comprometido",)
SETTLEMENT_BUCKET_FIELDS = ("saldos",)
SETTLEMENT_LABEL_FIELDS = ("liquidacion",)

# Settlement labels that mean "usable right now". Anything else is money that
# exists but cannot fund today's purchase.
IMMEDIATE_SETTLEMENTS = frozenset({"inmediato", "immediate", "0", "t0"})


def _to_float(value) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _first_field(payload: dict, names: tuple[str, ...]) -> tuple[str | None, float | None]:
    for name in names:
        if name in payload:
            parsed = _to_float(payload.get(name))
            if parsed is not None:
                return name, parsed
    return None, None


def parse_account_status(payload: dict) -> dict:
    """Normalize an estadocuenta payload into per-currency availability.

    Returns:
        {
          "recognized": bool,
          "currencies": {"ARS": {"available": float|None, "committed": float|None,
                                 "buckets": [...], "fields_used": [...] }},
          "recognized_fields": [...],
          "missing_fields": [...],
          "accounts_seen": int,
        }

    `available=None` means "present but unreadable" and must be treated as
    unknown by the caller — never as zero.
    """
    from app.broker.clients import _map_currency

    result = {
        "recognized": False,
        "currencies": {},
        "recognized_fields": [],
        "missing_fields": [],
        "accounts_seen": 0,
    }
    if not isinstance(payload, dict):
        result["missing_fields"] = list(ACCOUNT_LIST_FIELDS)
        return result

    accounts = None
    for name in ACCOUNT_LIST_FIELDS:
        value = payload.get(name)
        if isinstance(value, dict):
            accounts = [value]
            result["recognized_fields"].append(name)
            break
        if isinstance(value, list):
            accounts = value
            result["recognized_fields"].append(name)
            break
    if accounts is None:
        result["missing_fields"] = list(ACCOUNT_LIST_FIELDS)
        return result

    recognized_fields: set[str] = set(result["recognized_fields"])
    missing_fields: set[str] = set()

    for account in accounts:
        if not isinstance(account, dict):
            continue
        result["accounts_seen"] += 1

        currency_field = next((f for f in CURRENCY_FIELDS if f in account), None)
        if currency_field is None:
            missing_fields.update(CURRENCY_FIELDS)
            continue
        recognized_fields.add(currency_field)
        currency = _map_currency(account.get(currency_field))

        entry = result["currencies"].setdefault(currency, {
            "available": None,
            "committed": None,
            "buckets": [],
            "fields_used": [],
            "accounts": 0,
        })
        entry["accounts"] += 1

        account_available: float | None = None
        bucket_field = next((f for f in SETTLEMENT_BUCKET_FIELDS if f in account), None)
        if bucket_field is not None:
            recognized_fields.add(bucket_field)
            for bucket in account.get(bucket_field) or []:
                if not isinstance(bucket, dict):
                    continue
                label_field = next(
                    (f for f in SETTLEMENT_LABEL_FIELDS if f in bucket), None
                )
                label = str(bucket.get(label_field) or "").strip().lower() if label_field else ""
                if label_field:
                    recognized_fields.add(label_field)

                used_field, value = _first_field(bucket, AVAILABLE_FIELDS)
                committed_field, committed = _first_field(bucket, COMMITTED_FIELDS)
                if committed_field:
                    recognized_fields.add(committed_field)
                    entry["committed"] = (entry["committed"] or 0.0) + committed

                # A bucket funds a purchase ONLY when its settlement label is
                # explicitly present AND recognised as immediate. An empty or
                # missing label is NOT immediate: it used to slip through the
                # `label and ...` guard and be spent as if it were cash. An
                # unrecognised label is not immediate either — we do not guess
                # what "72hs" or a future label means.
                is_immediate = bool(label) and label in IMMEDIATE_SETTLEMENTS
                entry["buckets"].append({
                    "settlement": label or None,
                    "immediate": is_immediate,
                    "recognized": bool(label),
                    "available": value,
                    "field": used_field,
                })
                if used_field:
                    recognized_fields.add(used_field)
                if not is_immediate or value is None:
                    continue
                account_available = (
                    value if account_available is None else min(account_available, value)
                )
                if used_field and used_field not in entry["fields_used"]:
                    entry["fields_used"].append(used_field)

            if account_available is None:
                # `saldos` was present but produced no verifiable immediate
                # bucket. Falling back to the account-level `disponible` here
                # would silently spend deferred or unlabelled money — the very
                # thing the buckets exist to distinguish. Fail closed.
                entry["available"] = None
                entry["unreadable"] = True
                entry["no_immediate_bucket"] = True
                continue
        else:
            missing_fields.update(SETTLEMENT_BUCKET_FIELDS)

        if account_available is None:
            # No `saldos` collection at all. The account-level field is the
            # only thing on offer, and the documented contract presents it as
            # what is available — so it is used, but ONLY in the absence of
            # any (possibly contradicting) bucket data.
            used_field, value = _first_field(account, AVAILABLE_FIELDS)
            if used_field:
                recognized_fields.add(used_field)
                account_available = value
                if used_field not in entry["fields_used"]:
                    entry["fields_used"].append(used_field)
            else:
                missing_fields.update(AVAILABLE_FIELDS)

        if account_available is None:
            # Present but unreadable: mark the whole currency unknown rather
            # than contributing a silent zero.
            entry["available"] = None
            entry["unreadable"] = True
        elif not entry.get("unreadable"):
            entry["available"] = (entry["available"] or 0.0) + account_available

    result["recognized_fields"] = sorted(recognized_fields)
    result["missing_fields"] = sorted(missing_fields)
    result["recognized"] = bool(result["currencies"]) and not any(
        c.get("unreadable") for c in result["currencies"].values()
    )
    return result


def available_for_currency(payload: dict, currency: str) -> dict:
    """Available balance for ONE currency, in the broker client's shape."""
    target = (currency or "").strip().upper()
    parsed = parse_account_status(payload)
    entry = parsed["currencies"].get(target)
    if entry is None or entry.get("available") is None:
        return {
            "available": False,
            "cash": None,
            "currency": target,
            "committed": None,
            "matched_accounts": (entry or {}).get("accounts", 0),
            "source": "estadocuenta",
        }
    return {
        "available": True,
        "cash": entry["available"],
        "currency": target,
        "committed": entry.get("committed"),
        "matched_accounts": entry.get("accounts", 0),
        "source": "estadocuenta",
    }


def diagnose_account_status(payload: dict) -> dict:
    """Contract diagnosis — safe to expose, contains no balances-as-secrets.

    Reports which fields were recognised and which are missing, so a contract
    drift on IOL's side is visible as a configuration problem instead of
    surfacing later as an unexplained `live_cash_unavailable`.
    """
    parsed = parse_account_status(payload)
    blocking: list[str] = []
    if not parsed["currencies"]:
        blocking.append("no_currency_accounts_found")
    for currency, entry in parsed["currencies"].items():
        if entry.get("no_immediate_bucket"):
            blocking.append(f"no_immediate_settlement_bucket_{currency}")
        elif entry.get("available") is None:
            blocking.append(f"unreadable_balance_{currency}")
    if parsed["missing_fields"]:
        blocking.append("unrecognized_account_contract")

    return {
        "contract_valid": not blocking,
        "currencies_found": sorted(parsed["currencies"].keys()),
        "available_by_currency": {
            currency: entry.get("available")
            for currency, entry in sorted(parsed["currencies"].items())
        },
        "committed_by_currency": {
            currency: entry.get("committed")
            for currency, entry in sorted(parsed["currencies"].items())
        },
        "settlement_buckets": {
            currency: [
                {"settlement": b["settlement"], "immediate": b["immediate"]}
                for b in entry.get("buckets", [])
            ]
            for currency, entry in sorted(parsed["currencies"].items())
        },
        "recognized_fields": parsed["recognized_fields"],
        "missing_fields": parsed["missing_fields"],
        "accounts_seen": parsed["accounts_seen"],
        "blocking_reasons": blocking,
    }
