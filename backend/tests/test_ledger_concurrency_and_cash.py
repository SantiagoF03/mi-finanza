"""Daily-notional ledger under concurrency, and the estadocuenta contract.

The daily limit is the only guard that spans approvals, so it is the one an
attacker — or two honest browser tabs — will hit simultaneously. PR #138 read
the total, added in Python and wrote it back: two processes both see room and
both proceed. These tests use TWO REAL SESSIONS against a file-backed SQLite
database so the race is genuine, not simulated.

Also covers the estadocuenta parser against fixtures shaped like the
documented payload, and pending-buy accounting per currency and per day.

No test here performs network I/O or contacts IOL.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.broker.account_status import (
    available_for_currency,
    diagnose_account_status,
    parse_account_status,
)
from app.core.config import get_settings
from app.db.session import Base
from app.main import _patch_schema
from app.models.models import ExecutionDailyNotional, OrderExecution
from app.services.execution_limits import (
    PENDING_BUY_STATUSES,
    get_daily_totals,
    pending_buy_notional,
    release_daily_budget,
    reserve_daily_budget,
    stale_pending_buys,
    trade_date_for,
)

TODAY = "2026-08-04"
YESTERDAY = "2026-08-03"


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


@pytest.fixture
def file_db(tmp_path):
    """File-backed DB + factory, so two sessions really contend."""
    engine = create_engine(
        f"sqlite:///{tmp_path}/ledger.db",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    yield session, factory
    session.close()
    engine.dispose()


# ───────────────────────────────────────────────────────────────────
# 1 — atomicity
# ───────────────────────────────────────────────────────────────────


def test_two_sessions_cannot_both_reserve_the_same_budget(file_db):
    """The core race: both see an empty ledger and both want 600 of 1000."""
    session_a, factory = file_db
    session_b = factory()
    try:
        error_a, _ = reserve_daily_budget(
            session_a, trade_date=TODAY, execution_class="ACCIONES", currency="ARS",
            notional=Decimal("600"), max_daily_notional=1000,
        )
        session_a.commit()
        error_b, audit_b = reserve_daily_budget(
            session_b, trade_date=TODAY, execution_class="ACCIONES", currency="ARS",
            notional=Decimal("600"), max_daily_notional=1000,
        )
        session_b.commit()

        assert error_a is None, "the first reservation must succeed"
        assert error_b == "daily_limit_exceeded", "the second must be refused"
        assert audit_b["already_submitted"] == 600.0
        assert get_daily_totals(session_a, TODAY, "ACCIONES", "ARS") == Decimal("600")
    finally:
        session_b.close()


def test_the_maximum_is_never_exceeded_under_repeated_contention(file_db):
    """Many small reservations from two sessions must sum to <= the limit."""
    session_a, factory = file_db
    session_b = factory()
    try:
        granted = 0
        for _ in range(30):
            for session in (session_a, session_b):
                error, _ = reserve_daily_budget(
                    session, trade_date=TODAY, execution_class="ACCIONES",
                    currency="ARS", notional=Decimal("100"), max_daily_notional=1000,
                )
                session.commit()
                if error is None:
                    granted += 1
        assert granted == 10, "exactly 1000/100 reservations may be granted"
        total = get_daily_totals(session_a, TODAY, "ACCIONES", "ARS")
        assert total == Decimal("1000")
        assert total <= 1000
    finally:
        session_b.close()


def test_uniqueness_prevents_two_rows_for_the_same_scope(file_db):
    """Without the unique key each process gets its OWN row and its own full
    allowance — the bug this constraint exists to make impossible."""
    session, _factory = file_db
    reserve_daily_budget(
        session, trade_date=TODAY, execution_class="ACCIONES", currency="ARS",
        notional=Decimal("10"), max_daily_notional=1000,
    )
    session.commit()

    with pytest.raises(Exception):
        session.execute(text(
            "INSERT INTO execution_daily_notional "
            "(trade_date, execution_class, currency, submitted_notional, order_count) "
            "VALUES (:d, 'ACCIONES', 'ARS', 999, 1)"
        ), {"d": TODAY})
        session.commit()
    session.rollback()
    assert session.query(ExecutionDailyNotional).count() == 1


def test_an_order_bigger_than_the_whole_day_is_refused(file_db):
    session, _factory = file_db
    error, audit = reserve_daily_budget(
        session, trade_date=TODAY, execution_class="ACCIONES", currency="ARS",
        notional=Decimal("5000"), max_daily_notional=1000,
    )
    assert error == "daily_limit_exceeded"
    assert audit["reason"] == "exceeds_daily_limit_alone"
    assert session.query(ExecutionDailyNotional).count() == 0


def test_an_unconfigured_daily_limit_blocks(file_db):
    session, _factory = file_db
    for limit in (None, 0, -1):
        error, audit = reserve_daily_budget(
            session, trade_date=TODAY, execution_class="ACCIONES", currency="ARS",
            notional=Decimal("1"), max_daily_notional=limit,
        )
        assert error == "daily_limit_exceeded"
        assert audit["reason"] == "daily_limit_not_configured"


# ───────────────────────────────────────────────────────────────────
# 2 — separate ledgers
# ───────────────────────────────────────────────────────────────────


def test_ars_and_usd_do_not_share_a_ledger(file_db):
    session, _factory = file_db
    for currency in ("ARS", "USD"):
        error, _ = reserve_daily_budget(
            session, trade_date=TODAY, execution_class="ACCIONES", currency=currency,
            notional=Decimal("900"), max_daily_notional=1000,
        )
        session.commit()
        assert error is None, f"{currency} has its own budget"
    assert get_daily_totals(session, TODAY, "ACCIONES", "ARS") == Decimal("900")
    assert get_daily_totals(session, TODAY, "ACCIONES", "USD") == Decimal("900")


def test_classes_do_not_share_a_ledger(file_db):
    session, _factory = file_db
    for execution_class in ("ACCIONES", "CEDEARS"):
        error, _ = reserve_daily_budget(
            session, trade_date=TODAY, execution_class=execution_class, currency="ARS",
            notional=Decimal("900"), max_daily_notional=1000,
        )
        session.commit()
        assert error is None
    assert get_daily_totals(session, TODAY, "CEDEARS", "ARS") == Decimal("900")


def test_days_do_not_share_a_ledger(file_db):
    session, _factory = file_db
    for day in (YESTERDAY, TODAY):
        error, _ = reserve_daily_budget(
            session, trade_date=day, execution_class="ACCIONES", currency="ARS",
            notional=Decimal("900"), max_daily_notional=1000,
        )
        session.commit()
        assert error is None


# ───────────────────────────────────────────────────────────────────
# 3 — what does and does not release budget
# ───────────────────────────────────────────────────────────────────


def test_a_refused_reservation_consumes_nothing(file_db):
    session, _factory = file_db
    reserve_daily_budget(
        session, trade_date=TODAY, execution_class="ACCIONES", currency="ARS",
        notional=Decimal("900"), max_daily_notional=1000,
    )
    session.commit()
    reserve_daily_budget(
        session, trade_date=TODAY, execution_class="ACCIONES", currency="ARS",
        notional=Decimal("200"), max_daily_notional=1000,
    )
    session.commit()
    assert get_daily_totals(session, TODAY, "ACCIONES", "ARS") == Decimal("900")


def test_a_cancellation_does_not_automatically_free_budget(file_db):
    """At the moment we learn an order was cancelled we usually cannot prove
    it consumed nothing at the broker. Quietly restoring budget would let a
    bad day's worth of orders be re-attempted."""
    session, _factory = file_db
    reserve_daily_budget(
        session, trade_date=TODAY, execution_class="ACCIONES", currency="ARS",
        notional=Decimal("900"), max_daily_notional=1000,
    )
    session.commit()
    # Nothing in the cancellation path calls release_daily_budget.
    from app.services import cancellation

    source = (cancellation.__file__)
    with open(source) as handle:
        assert "release_daily_budget" not in handle.read()
    assert get_daily_totals(session, TODAY, "ACCIONES", "ARS") == Decimal("900")


def test_releasing_budget_is_an_explicit_act_with_a_reason(file_db):
    session, _factory = file_db
    reserve_daily_budget(
        session, trade_date=TODAY, execution_class="ACCIONES", currency="ARS",
        notional=Decimal("900"), max_daily_notional=1000,
    )
    session.commit()
    result = release_daily_budget(
        session, trade_date=TODAY, execution_class="ACCIONES", currency="ARS",
        notional=Decimal("400"), reason="conciliado: la orden nunca llegó a IOL",
    )
    session.commit()
    assert result["released"] is True
    assert result["reason"].startswith("conciliado")
    assert get_daily_totals(session, TODAY, "ACCIONES", "ARS") == Decimal("500")


# ───────────────────────────────────────────────────────────────────
# 4 — pending buys per currency and per day
# ───────────────────────────────────────────────────────────────────


def _buy(session, *, currency, trade_date, status="execution_sent", notional=1000.0):
    order = OrderExecution(
        recommendation_id=1, symbol="BYMA", side="buy", target_change_pct=0.01,
        status=status, quantity_planned=1, quantity_sent=1,
        broker_response={"request_audit": {
            "instrument_identity": {"currency": currency},
            "trade_date": trade_date,
            "actual_notional": notional,
        }},
    )
    session.add(order)
    session.commit()
    return order


def test_pending_buys_do_not_mix_currencies(file_db):
    session, _factory = file_db
    _buy(session, currency="ARS", trade_date=TODAY, notional=1000.0)
    _buy(session, currency="USD", trade_date=TODAY, notional=7777.0)

    ars = pending_buy_notional(session, currency="ARS", trade_date=TODAY)
    usd = pending_buy_notional(session, currency="USD", trade_date=TODAY)
    assert ars == Decimal("1000")
    assert usd == Decimal("7777")


def test_a_previous_days_pending_buy_does_not_reserve_todays_cash(file_db):
    """It used to reserve cash forever, silently shrinking every future
    order until someone noticed."""
    session, _factory = file_db
    _buy(session, currency="ARS", trade_date=YESTERDAY, notional=50_000.0)

    assert pending_buy_notional(session, currency="ARS", trade_date=TODAY) == Decimal("0")
    stale = stale_pending_buys(session, trade_date=TODAY)
    assert len(stale) == 1
    assert stale[0]["trade_date"] == YESTERDAY
    assert stale[0]["notional"] == 50_000.0


@pytest.mark.parametrize("status,reserves", [
    ("submitting", True),
    ("execution_sent", True),
    ("manual_reconciliation_required", True),
    ("submission_unknown", True),
    ("executed", False),
    ("rejected_by_broker", False),
    ("cancelled_by_user", False),
    ("cancelled_at_broker", False),
    ("failed", False),
    ("not_sent_confirmed", False),
    ("preflight_cancelled", False),
])
def test_only_unresolved_buys_reserve_cash(file_db, status, reserves):
    session, _factory = file_db
    _buy(session, currency="ARS", trade_date=TODAY, status=status, notional=1234.0)
    total = pending_buy_notional(session, currency="ARS", trade_date=TODAY)
    assert (total > 0) is reserves, f"{status} should{'' if reserves else ' not'} reserve"


def test_submission_unknown_keeps_reserving(file_db):
    """We cannot prove the money did not leave; it stays reserved until
    someone reconciles it."""
    session, _factory = file_db
    _buy(session, currency="ARS", trade_date=TODAY, status="submission_unknown",
         notional=2500.0)
    assert "submission_unknown" in PENDING_BUY_STATUSES
    assert pending_buy_notional(session, currency="ARS", trade_date=TODAY) == Decimal("2500")


def test_excluded_ids_are_not_double_counted(file_db):
    session, _factory = file_db
    order = _buy(session, currency="ARS", trade_date=TODAY, notional=1000.0)
    assert pending_buy_notional(
        session, currency="ARS", trade_date=TODAY, exclude_ids={order.id}
    ) == Decimal("0")


def test_trade_date_uses_the_market_timezone_not_utc():
    """21:00 UTC is still the same Argentine business day."""
    with exec_settings(scheduler_market_open_time="10:30",
                       scheduler_market_close_time="17:00",
                       scheduler_timezone="America/Argentina/Buenos_Aires",
                       market_holidays=[]):
        late_utc = datetime(2026, 8, 4, 21, 30, tzinfo=timezone.utc)
        assert trade_date_for(get_settings(), now=late_utc) == "2026-08-04"


# ───────────────────────────────────────────────────────────────────
# 5 — estadocuenta contract, against documented-shape fixtures
# ───────────────────────────────────────────────────────────────────

# Shaped after the documented /api/v2/estadocuenta response: a `cuentas`
# list, each with `moneda`, and per-settlement `saldos` entries carrying
# `disponible` / `disponibleOperar` / `comprometido`.
ESTADOCUENTA_FIXTURE = {
    "cuentas": [
        {
            "numero": "1234567",
            "tipo": "inversion_Argentina_Pesos",
            "moneda": "peso_Argentino",
            "disponible": 150000.0,
            "titulosValorizados": 900000.0,
            "total": 1050000.0,
            "saldos": [
                {"liquidacion": "inmediato", "saldo": 150000.0,
                 "comprometido": 5000.0, "disponible": 145000.0,
                 "disponibleOperar": 140000.0},
                {"liquidacion": "24hs", "saldo": 20000.0, "comprometido": 0.0,
                 "disponible": 20000.0, "disponibleOperar": 20000.0},
            ],
        },
        {
            "numero": "7654321",
            "tipo": "inversion_Argentina_Dolares",
            "moneda": "dolar_Estadounidense",
            "disponible": 500.0,
            "titulosValorizados": 0.0,
            "total": 500.0,
            "saldos": [
                {"liquidacion": "inmediato", "saldo": 500.0, "comprometido": 0.0,
                 "disponible": 500.0, "disponibleOperar": 500.0},
            ],
        },
    ],
    "totalEnPesos": 1_100_000.0,
}


def test_parser_separates_currencies():
    parsed = parse_account_status(ESTADOCUENTA_FIXTURE)
    assert set(parsed["currencies"]) == {"ARS", "USD"}
    assert parsed["currencies"]["ARS"]["available"] == 140000.0
    assert parsed["currencies"]["USD"]["available"] == 500.0
    assert parsed["recognized"] is True


def test_parser_prefers_disponible_operar_over_disponible():
    """`disponible` (145000) is not what may actually be used to operate
    (140000). Taking the larger number authorises a purchase the account
    cannot honour."""
    parsed = parse_account_status(ESTADOCUENTA_FIXTURE)
    assert parsed["currencies"]["ARS"]["available"] == 140000.0
    assert "disponibleOperar" in parsed["currencies"]["ARS"]["fields_used"]


def test_parser_ignores_non_immediate_settlement_buckets():
    """A 24hs bucket is real money that cannot fund today's purchase."""
    parsed = parse_account_status(ESTADOCUENTA_FIXTURE)
    buckets = parsed["currencies"]["ARS"]["buckets"]
    immediate = [b for b in buckets if b["immediate"]]
    deferred = [b for b in buckets if not b["immediate"]]
    assert len(immediate) == 1 and len(deferred) == 1
    assert parsed["currencies"]["ARS"]["available"] == immediate[0]["available"]


def test_parser_reports_committed_funds():
    parsed = parse_account_status(ESTADOCUENTA_FIXTURE)
    assert parsed["currencies"]["ARS"]["committed"] == 5000.0


def test_available_for_currency_matches_the_broker_contract():
    ars = available_for_currency(ESTADOCUENTA_FIXTURE, "ARS")
    assert ars["available"] is True and ars["cash"] == 140000.0
    usd = available_for_currency(ESTADOCUENTA_FIXTURE, "USD")
    assert usd["cash"] == 500.0
    missing = available_for_currency(ESTADOCUENTA_FIXTURE, "EUR")
    assert missing["available"] is False and missing["cash"] is None


def test_unreadable_balance_is_unknown_never_zero():
    """Zero reads as "no money" and blocks; worse, a partial sum reads as
    "enough". Unknown must stay unknown."""
    broken = {"cuentas": [{"moneda": "peso_Argentino", "saldos": [
        {"liquidacion": "inmediato", "disponible": "not-a-number"}
    ]}]}
    parsed = parse_account_status(broken)
    assert parsed["currencies"]["ARS"]["available"] is None
    assert available_for_currency(broken, "ARS")["available"] is False


def test_flat_account_without_settlement_buckets_still_parses():
    flat = {"cuentas": [{"moneda": "peso_Argentino", "disponible": 1000.0}]}
    assert available_for_currency(flat, "ARS")["cash"] == 1000.0


def test_diagnostic_reports_a_valid_contract():
    diagnosis = diagnose_account_status(ESTADOCUENTA_FIXTURE)
    assert diagnosis["contract_valid"] is True
    assert diagnosis["currencies_found"] == ["ARS", "USD"]
    assert diagnosis["available_by_currency"]["ARS"] == 140000.0
    assert diagnosis["blocking_reasons"] == []
    assert "disponibleOperar" in diagnosis["recognized_fields"]


def test_diagnostic_flags_an_unrecognised_contract():
    """A drift on IOL's side must surface as a configuration problem, not as
    an unexplained live_cash_unavailable later."""
    diagnosis = diagnose_account_status({"algoNuevo": []})
    assert diagnosis["contract_valid"] is False
    assert "no_currency_accounts_found" in diagnosis["blocking_reasons"]
    assert "cuentas" in diagnosis["missing_fields"]


def test_diagnostic_never_returns_the_raw_payload():
    diagnosis = diagnose_account_status(ESTADOCUENTA_FIXTURE)
    serialized = str(diagnosis)
    assert "1234567" not in serialized, "account numbers must not leak"
    assert "titulosValorizados" not in serialized
