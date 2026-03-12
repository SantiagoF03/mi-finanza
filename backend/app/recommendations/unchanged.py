from __future__ import annotations


def detect_unchanged(
    new_rec: dict,
    prev_rec_row,
    new_analysis: dict,
    pct_threshold: float = 0.01,
    risk_threshold: float = 0.03,
) -> tuple[bool, str]:
    """Compare a freshly generated recommendation against the previous one.

    Returns (unchanged: bool, reason: str).
    *prev_rec_row* is the SQLAlchemy Recommendation row (or None).
    """
    if prev_rec_row is None:
        return False, "No hay recomendación previa para comparar."

    reasons: list[str] = []

    # 1. action
    if new_rec.get("action") != prev_rec_row.action:
        reasons.append(f"Acción cambió: {prev_rec_row.action} -> {new_rec.get('action')}")

    # 2. suggested_pct difference
    pct_diff = abs((new_rec.get("suggested_pct", 0) or 0) - (prev_rec_row.suggested_pct or 0))
    if pct_diff > pct_threshold:
        reasons.append(f"Porcentaje sugerido cambió en {round(pct_diff * 100, 2)}%")

    # 3. main symbols in actions
    new_symbols = sorted({a.get("symbol", "") for a in new_rec.get("actions", [])})
    prev_symbols = sorted({a.symbol for a in prev_rec_row.actions}) if prev_rec_row.actions else []
    if new_symbols != prev_symbols:
        reasons.append(f"Símbolos afectados cambiaron: {prev_symbols} -> {new_symbols}")

    # 4. blocked_reason
    new_blocked = new_rec.get("blocked_reason", "") or ""
    prev_blocked = prev_rec_row.blocked_reason or ""
    if new_blocked != prev_blocked:
        reasons.append("Razón de bloqueo cambió.")

    # 5. analysis signals
    prev_meta = prev_rec_row.metadata_json or {}
    prev_analysis = prev_meta.get("analysis", {})

    risk_diff = abs(new_analysis.get("risk_score", 0) - prev_analysis.get("risk_score", 0))
    if risk_diff > risk_threshold:
        reasons.append(f"Risk score cambió en {round(risk_diff, 3)}")

    conc_diff = abs(new_analysis.get("concentration_score", 0) - prev_analysis.get("concentration_score", 0))
    if conc_diff > risk_threshold:
        reasons.append(f"Concentration score cambió en {round(conc_diff, 3)}")

    new_alerts = sorted(new_analysis.get("alerts", []))
    prev_alerts = sorted(prev_analysis.get("alerts", []))
    if new_alerts != prev_alerts:
        reasons.append("Alertas de análisis cambiaron.")

    # 6. news fingerprint (count of news used changed meaningfully)
    prev_news_used = prev_meta.get("news_used_engine", prev_meta.get("news_used", 0))
    new_news_used = len(new_rec.get("_news_items", []))  # caller attaches this temporarily
    if abs(new_news_used - prev_news_used) >= 2:
        reasons.append(f"Cantidad de noticias cambió: {prev_news_used} -> {new_news_used}")

    # 7. external opportunities material change
    prev_ext = prev_meta.get("external_opportunities", [])
    new_ext = new_rec.get("external_opportunities", [])
    prev_ext_symbols = sorted({o.get("symbol", "") for o in prev_ext})
    new_ext_symbols = sorted({o.get("symbol", "") for o in new_ext})
    if prev_ext_symbols != new_ext_symbols:
        reasons.append(f"Oportunidades externas cambiaron: {prev_ext_symbols} -> {new_ext_symbols}")

    if reasons:
        return False, " | ".join(reasons)

    return True, "Se mantiene la recomendación anterior. No hubo cambios materiales."
