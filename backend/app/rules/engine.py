def enforce_rules(recommendation: dict, whitelist: list[str], max_move: float, holdings: set[str] | None = None) -> dict:
    """Apply hard rules to a recommendation.

    `whitelist` is the manual WHITELIST_ASSETS from config.
    `holdings` (optional) are the real snapshot symbols — always auto-permitted.
    The effective allowed set is holdings | whitelist.
    """
    allowed = set(whitelist)
    if holdings:
        allowed = allowed | holdings

    adjusted = recommendation.copy()
    filtered_actions = []
    blocked_reasons = []

    for action in recommendation.get("actions", []):
        symbol = action.get("symbol")
        if symbol not in allowed:
            blocked_reasons.append(f"{symbol} fuera de whitelist")
            continue
        clamped = max(min(action.get("target_change_pct", 0), max_move), -max_move)
        if clamped != action.get("target_change_pct", 0):
            blocked_reasons.append(f"{symbol} ajustado por max_move")
        filtered_actions.append({**action, "target_change_pct": clamped})

    adjusted["actions"] = filtered_actions
    adjusted["suggested_pct"] = min(abs(adjusted.get("suggested_pct", 0)), max_move)
    adjusted["blocked_reasons"] = blocked_reasons

    if adjusted.get("confidence", 0) < 0.45:
        blocked_reasons.append("confianza insuficiente")

    if not filtered_actions or blocked_reasons:
        adjusted["status"] = "blocked"
        adjusted["action"] = "mantener"
        adjusted["suggested_pct"] = 0.0
        adjusted["rationale"] += " Señal degradada por reglas hard."
        adjusted["blocked_reason"] = "; ".join(blocked_reasons) if blocked_reasons else "Sin acciones válidas"
    else:
        adjusted["status"] = "pending"
        adjusted["blocked_reason"] = ""

    return adjusted
