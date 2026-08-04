"""Migration safety on a production-shaped database, and security invariants.

The production database is a persistent SQLite file holding real history:
Recommendation 13 (pending, reduce SPY) and OrderExecution 1 (executed sell,
IOL operation 183382167). These tests build a copy of that shape with the
PREVIOUS schema, run the startup migration against it, and assert that:

- the migration is idempotent and purely additive;
- no pre-existing row, column or status is altered;
- Recommendation 13 stays pending and OrderExecution 1 stays executed;
- rolling back to the previous code leaves the old tables intact.

Plus the security invariants that must survive every new capability.

No test here performs network I/O or contacts IOL.
"""

from __future__ import annotations

import ast
import json
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from app.core.config import Settings, get_settings
from app.db.session import Base
from app.main import _patch_schema
from app.models.models import OrderExecution, Recommendation

BACKEND_DIR = Path(__file__).resolve().parent.parent

# The exact production rows we must never disturb.
PROD_RECOMMENDATION_ID = 13
PROD_EXECUTION_ID = 1
PROD_IOL_OPERATION = "183382167"


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
def legacy_db(tmp_path):
    """A file-backed DB carrying the pre-change schema AND production rows."""
    path = tmp_path / "prod_copy.db"
    engine = create_engine(f"sqlite:///{path}")

    # Build the OLD schema: every table except the two new ones.
    tables = [
        t for name, t in Base.metadata.tables.items()
        if name not in ("execution_instruments", "execution_daily_notional")
    ]
    Base.metadata.create_all(bind=engine, tables=tables)

    # Simulate an even older DB that predates quantity_override.
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE recommendation_actions DROP COLUMN quantity_override"))

    session = sessionmaker(bind=engine)()
    session.add(Recommendation(
        id=PROD_RECOMMENDATION_ID, action="rebalancear", status="pending",
        suggested_pct=0.1, confidence=0.7, rationale="reducir SPY",
        risks="k", executive_summary="reducir SPY", metadata_json={"origin": "automatic"},
    ))
    session.add(Recommendation(
        id=12, action="rebalancear", status="approved", suggested_pct=0.1,
        confidence=0.7, rationale="r", risks="k", executive_summary="s",
        metadata_json={},
    ))
    session.add(OrderExecution(
        id=PROD_EXECUTION_ID, recommendation_id=12, symbol="BYMA", side="sell",
        target_change_pct=-0.1, status="executed", broker_order_id=PROD_IOL_OPERATION,
        quantity=1, quantity_planned=1, quantity_sent=1,
        executed_quantity=1, executed_price=297.0,
        endpoint_used="/api/v2/operar/Vender", broker_response={},
    ))
    session.commit()
    yield engine, session, sessionmaker(bind=engine)
    session.close()
    engine.dispose()


def _fingerprint(session):
    """Immutable fingerprint of the production rows."""
    rec = session.get(Recommendation, PROD_RECOMMENDATION_ID)
    execution = session.get(OrderExecution, PROD_EXECUTION_ID)
    return (
        rec.id, rec.status, rec.action, rec.rationale, rec.superseded_at,
        json.dumps(rec.metadata_json, sort_keys=True),
        execution.id, execution.status, execution.symbol, execution.side,
        execution.broker_order_id, execution.executed_quantity, execution.executed_price,
        execution.quantity_sent,
    )


# ───────────────────────────────────────────────────────────────────
# 1 — migration safety
# ───────────────────────────────────────────────────────────────────


def test_migration_is_additive_and_preserves_production_rows(legacy_db):
    engine, session, _factory = legacy_db
    before = _fingerprint(session)

    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)

    session.expire_all()
    assert _fingerprint(session) == before

    rec = session.get(Recommendation, PROD_RECOMMENDATION_ID)
    execution = session.get(OrderExecution, PROD_EXECUTION_ID)
    assert rec.status == "pending", "Recommendation 13 must stay pending"
    assert execution.status == "executed", "OrderExecution 1 must stay executed"
    assert execution.broker_order_id == PROD_IOL_OPERATION
    assert execution.executed_price == 297.0


def test_migration_adds_the_new_tables_and_the_missing_column(legacy_db):
    engine, _session, _factory = legacy_db
    inspector = inspect(engine)
    assert "execution_instruments" not in inspector.get_table_names()

    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)

    inspector = inspect(engine)
    assert "execution_instruments" in inspector.get_table_names()
    assert "execution_daily_notional" in inspector.get_table_names()
    columns = {c["name"] for c in inspector.get_columns("recommendation_actions")}
    assert "quantity_override" in columns


def test_migration_is_idempotent(legacy_db):
    engine, session, _factory = legacy_db
    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)
    before = _fingerprint(session)

    for _ in range(3):
        Base.metadata.create_all(bind=engine)
        _patch_schema(engine)

    session.expire_all()
    assert _fingerprint(session) == before


def test_migration_never_drops_or_rewrites_anything():
    """Static guard: the startup path contains no destructive SQL."""
    source = (BACKEND_DIR / "app" / "main.py").read_text()
    tree = ast.parse(source)
    statements = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]
    for statement in statements:
        upper = statement.upper()
        assert "DROP TABLE" not in upper
        assert "DROP COLUMN" not in upper
        assert "DELETE FROM" not in upper
        assert "UPDATE " not in upper
        assert "TRUNCATE" not in upper


def test_new_tables_are_unused_by_previous_behaviour(legacy_db):
    """Rollback story: the new tables start empty and nothing reads them
    until the new code paths are configured."""
    engine, session, _factory = legacy_db
    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)

    from app.models.models import ExecutionDailyNotional, ExecutionInstrument

    assert session.query(ExecutionInstrument).count() == 0
    assert session.query(ExecutionDailyNotional).count() == 0
    # And the old data is still exactly where it was.
    assert session.get(Recommendation, PROD_RECOMMENDATION_ID).status == "pending"


# ───────────────────────────────────────────────────────────────────
# 2 — fail-closed defaults
# ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("field", [
    "securities_buy_enabled",
    "securities_sell_enabled",
    "fci_subscription_enabled",
    "fci_redemption_enabled",
    "order_execution_enabled",
    "sandbox_execution_enabled",
    "execution_pilot_creation_enabled",
    "execution_allow_override_limit_increase",
])
def test_every_capability_defaults_to_false(field):
    assert getattr(Settings(), field) is False, f"{field} must default to false"


@pytest.mark.parametrize("field", [
    "execution_class_policies",
    "execution_instrument_overrides",
    "execution_instrument_ticks",
    "execution_instrument_policies",
])
def test_no_policy_ships_configured(field):
    assert getattr(Settings(), field) == {}


def test_denylist_and_holidays_default_empty():
    settings = Settings()
    assert settings.execution_denylist == []
    assert settings.market_holidays == []


def test_live_guards_default_on():
    """The guards are on by default; only the capabilities are off."""
    settings = Settings()
    assert settings.execution_require_live_position_check is True
    assert settings.execution_require_live_cash_check is True


def test_new_json_settings_reject_malformed_input():
    with pytest.raises(ValueError):
        Settings(execution_class_policies="{not json")
    with pytest.raises(ValueError):
        Settings(execution_instrument_overrides="[]")


# ───────────────────────────────────────────────────────────────────
# 3 — no test may reach the real IOL API
# ───────────────────────────────────────────────────────────────────


def _string_literals(node) -> list[str]:
    return [
        child.value
        for child in ast.walk(node)
        if isinstance(child, ast.Constant) and isinstance(child.value, str)
    ]


def test_no_test_builds_a_broker_client_against_the_real_iol_host():
    """Mentioning the real host is fine — several tests configure it precisely
    to assert it is REJECTED by a pure validator. What must never happen is a
    broker client being *constructed* against it, because that is the only
    shape that could actually reach IOL.
    """
    offenders = []
    for path in sorted((BACKEND_DIR / "tests").glob("test_*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = getattr(func, "id", None) or getattr(func, "attr", None)
            if name != "IolBrokerClient":
                continue
            if any("api.invertironline.com" in literal for literal in _string_literals(node)):
                offenders.append(f"{path.name}:{node.lineno}")
    assert not offenders, f"broker client built against the real IOL API: {offenders}"


def test_no_test_opens_a_real_http_client():
    """Every httpx.Client in the suite must carry a MockTransport."""
    offenders = []
    for path in sorted((BACKEND_DIR / "tests").glob("test_*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if getattr(node.func, "attr", None) != "Client":
                continue
            if not any(kw.arg == "transport" for kw in node.keywords):
                offenders.append(f"{path.name}:{node.lineno}")
    assert not offenders, f"unmocked httpx client in tests: {offenders}"


def test_sandbox_smoke_stays_opt_in():
    source = (BACKEND_DIR / "tests" / "test_iol_sandbox_smoke.py").read_text()
    assert "RUN_IOL_SANDBOX_TESTS" in source
    assert "skipif" in source
    # Sandbox credentials are separate settings; real ones are never reused.
    assert "iol_sandbox_username" in source or "IOL_SANDBOX_USERNAME" in source


def test_new_modules_never_import_httpx_directly():
    """Only the broker client speaks HTTP. Nothing new opens its own socket."""
    for module in (
        "app/broker/execution_class.py",
        "app/broker/execution_scope.py",
        "app/broker/instrument_catalog.py",
        "app/market/calendar.py",
        "app/services/execution_limits.py",
        "app/services/fci.py",
        "app/services/instrument_capabilities.py",
    ):
        tree = ast.parse((BACKEND_DIR / module).read_text())
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(a.name for a in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add(node.module or "")
        assert "httpx" not in imported, f"{module} must not open its own connection"


def test_fci_never_uses_the_securities_execution_path():
    """FCI has its own official contract, but it must never borrow the
    securities one: no OrderExecution, no place_order, no limit price."""
    from app.services import fci

    source = (BACKEND_DIR / "app" / "services" / "fci.py").read_text()
    tree = ast.parse(source)
    referenced = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported.update(a.name for a in node.names)

    assert not (referenced & {"place_order", "submit_order_request"})
    assert "OrderExecution" not in names and "OrderExecution" not in imported
    # The fund models are the ONLY persistence it may touch.
    assert {"FundOperation", "FundInstrument"} <= imported

    # Only the official documented endpoints appear.
    assert fci.FCI_SUBSCRIBE_ENDPOINT == "/api/v2/operar/suscripcion/fci"
    assert fci.FCI_REDEEM_ENDPOINT == "/api/v2/operar/rescate/fci"
    assert fci.FCI_CATALOG_ENDPOINT == "/api/v2/Titulos/FCI"


def test_fci_request_fields_are_never_invented():
    """The endpoint PATHS are documented; the request FIELD NAMES are not
    recorded here. Guessing them would produce a malformed real subscription,
    so the builder fails closed until an operator verifies the mapping."""
    from app.models.models import FundOperation
    from app.services import fci

    assert fci.FCI_REQUEST_CONTRACT_VERIFIED is False
    assert fci.FCI_REQUEST_FIELDS == {}

    operation = FundOperation(
        fund_symbol="FCIX", operation=fci.OPERATION_SUBSCRIBE,
        currency="ARS", amount=10_000.0, status=fci.STATE_PREPARED,
    )
    for solo_validar in (True, False):
        request, error = fci.build_fund_request(operation, solo_validar=solo_validar)
        assert request is None
        assert error == fci.FCI_CONTRACT_UNVERIFIED_CODE


# ───────────────────────────────────────────────────────────────────
# 4 — explicit manual cancellation
# ───────────────────────────────────────────────────────────────────


@pytest.fixture
def mem_db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _sent_order(session, status="execution_sent"):
    session.add(Recommendation(
        id=1, action="rebalancear", status="execution_pending", suggested_pct=0.1,
        confidence=0.7, rationale="r", risks="k", executive_summary="s",
        metadata_json={},
    ))
    order = OrderExecution(
        recommendation_id=1, symbol="BYMA", side="sell", target_change_pct=-0.1,
        status=status, quantity_planned=1, quantity_sent=1,
        broker_order_id="OP-1", broker_response={},
    )
    session.add(order)
    session.commit()
    return order


def test_manual_cancellation_records_a_human_decision(mem_db):
    """The app cannot cancel at IOL; it records that a human did."""
    from app.services.execution import reconcile_execution, reconciliation_phrase

    order = _sent_order(mem_db)
    with exec_settings(execution_admin_key="k"):
        result = reconcile_execution(
            mem_db, order.id, execution_key="k", action="confirm_cancelled",
            confirmation_text=reconciliation_phrase(order.id, "confirm_cancelled"),
            note="Cancelada en el panel de IOL, operación OP-1",
        )

    assert result["new_status"] == "cancelled_by_user"
    mem_db.refresh(order)
    assert order.status == "cancelled_by_user"
    # It WAS sent, so quantity_sent is deliberately preserved: claiming
    # nothing was sent is a different statement (confirm_not_sent).
    assert order.quantity_sent == 1
    assert result["reconciliation_audit"][-1]["action"] == "confirm_cancelled"


def test_manual_cancellation_requires_evidence(mem_db):
    from app.services.execution import reconcile_execution, reconciliation_phrase

    order = _sent_order(mem_db)
    with exec_settings(execution_admin_key="k"):
        result = reconcile_execution(
            mem_db, order.id, execution_key="k", action="confirm_cancelled",
            confirmation_text=reconciliation_phrase(order.id, "confirm_cancelled"),
            note="",
        )
    assert result["code"] == "note_required"
    mem_db.refresh(order)
    assert order.status == "execution_sent"


def test_cancellation_needs_the_exact_phrase_and_credential(mem_db):
    from app.services.execution import reconcile_execution, reconciliation_phrase

    order = _sent_order(mem_db)
    with exec_settings(execution_admin_key="k"):
        wrong_key = reconcile_execution(
            mem_db, order.id, execution_key="nope", action="confirm_cancelled",
            confirmation_text=reconciliation_phrase(order.id, "confirm_cancelled"),
            note="x",
        )
        wrong_phrase = reconcile_execution(
            mem_db, order.id, execution_key="k", action="confirm_cancelled",
            confirmation_text="CANCELAR", note="x",
        )
    assert wrong_key["code"] == "invalid_execution_key"
    assert wrong_phrase["code"] == "confirmation_mismatch"
    mem_db.refresh(order)
    assert order.status == "execution_sent"


def test_a_prepared_order_cannot_be_marked_cancelled_at_iol(mem_db):
    """Nothing was ever sent, so 'cancelled in IOL' would be false."""
    from app.services.execution import reconcile_execution, reconciliation_phrase

    order = _sent_order(mem_db, status="execution_ready")
    with exec_settings(execution_admin_key="k"):
        result = reconcile_execution(
            mem_db, order.id, execution_key="k", action="confirm_cancelled",
            confirmation_text=reconciliation_phrase(order.id, "confirm_cancelled"),
            note="x",
        )
    assert result["code"] == "not_reconcilable"


def test_cancellation_never_sends_or_retries_anything(mem_db):
    from unittest import mock

    from app.services.execution import reconcile_execution, reconciliation_phrase

    order = _sent_order(mem_db)
    with exec_settings(execution_admin_key="k"):
        with mock.patch("app.services.execution._get_execution_broker") as broker_factory:
            reconcile_execution(
                mem_db, order.id, execution_key="k", action="confirm_cancelled",
                confirmation_text=reconciliation_phrase(order.id, "confirm_cancelled"),
                note="cancelada en IOL",
            )
    broker_factory.assert_not_called()
    assert mem_db.query(OrderExecution).count() == 1


# ───────────────────────────────────────────────────────────────────
# 5 — upgrading a database that PR #138 already created
# ───────────────────────────────────────────────────────────────────


@pytest.fixture
def pr138_db(tmp_path):
    """A DB in the exact shape PR #138 left behind.

    That is the real production starting point for this change: the new
    tables exist, but WITHOUT the verification columns and WITHOUT the
    uniqueness that makes the daily ledger safe.
    """
    from sqlalchemy import inspect as sa_inspect

    path = tmp_path / "pr138.db"
    engine = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(bind=engine)

    with engine.begin() as conn:
        # Undo this change's additions, leaving PR #138's shape. The index on
        # verification_status must go first — SQLite refuses to drop a column
        # an index still references.
        conn.execute(text("DROP INDEX IF EXISTS ix_execution_instruments_verification_status"))
        for column in ("verification_status", "field_provenance", "pending_identity",
                       "previous_identity", "identity_changed_at", "verification_audit"):
            conn.execute(text(f"ALTER TABLE execution_instruments DROP COLUMN {column}"))

        # PR #138 created execution_daily_notional with NO uniqueness at all.
        # SQLite cannot drop a table-level constraint, so recreate the table
        # exactly as that version shipped it — this is what production has.
        conn.execute(text("DROP TABLE execution_daily_notional"))
        conn.execute(text(
            "CREATE TABLE execution_daily_notional ("
            " id INTEGER NOT NULL PRIMARY KEY,"
            " trade_date VARCHAR(10),"
            " execution_class VARCHAR(20),"
            " currency VARCHAR(10),"
            " submitted_notional FLOAT,"
            " order_count INTEGER,"
            " updated_at DATETIME NOT NULL)"
        ))

    inspector = sa_inspect(engine)
    assert "verification_status" not in {
        c["name"] for c in inspector.get_columns("execution_instruments")
    }

    session = sessionmaker(bind=engine)()
    session.add(Recommendation(
        id=PROD_RECOMMENDATION_ID, action="rebalancear", status="pending",
        suggested_pct=0.1, confidence=0.7, rationale="reducir SPY",
        risks="k", executive_summary="reducir SPY", metadata_json={"origin": "automatic"},
    ))
    session.add(Recommendation(
        id=12, action="rebalancear", status="approved", suggested_pct=0.1,
        confidence=0.7, rationale="r", risks="k", executive_summary="s",
        metadata_json={},
    ))
    session.add(OrderExecution(
        id=PROD_EXECUTION_ID, recommendation_id=12, symbol="BYMA", side="sell",
        target_change_pct=-0.1, status="executed", broker_order_id=PROD_IOL_OPERATION,
        quantity=1, quantity_planned=1, quantity_sent=1,
        executed_quantity=1, executed_price=297.0,
        endpoint_used="/api/v2/operar/Vender", broker_response={},
    ))
    # A PR #138 catalog row: fields present, nobody ever verified them.
    session.execute(text(
        "INSERT INTO execution_instruments "
        "(broker_symbol, display_symbol, description, country, market, settlement, "
        " asset_type, instrument_type, execution_family, execution_class, currency, "
        " quantity_step, price_tick, active, buy_supported, sell_supported, "
        " quote_supported, cancellation_supported, source, verified_at, "
        " raw_identity_hash, raw_identity, created_at, updated_at) "
        "VALUES ('BYMA','BYMA','','argentina','bCBA','t1','ACCIONES','ACCIONES',"
        " 'securities','ACCIONES','ARS',1.0,0.01,1,1,1,1,0,'iol_portfolio',"
        " :now,'oldhash','{}', :now, :now)"
    ), {"now": datetime.utcnow()})
    session.commit()
    yield engine, session
    session.close()
    engine.dispose()


def test_upgrading_a_pr138_database_preserves_production_rows(pr138_db):
    engine, session = pr138_db
    before = _fingerprint(session)

    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)

    session.expire_all()
    assert _fingerprint(session) == before
    assert session.get(Recommendation, PROD_RECOMMENDATION_ID).status == "pending"
    execution = session.get(OrderExecution, PROD_EXECUTION_ID)
    assert execution.status == "executed"
    assert execution.broker_order_id == PROD_IOL_OPERATION
    assert execution.executed_price == 297.0


def test_pr138_catalog_rows_are_not_grandfathered_into_verified(pr138_db):
    """Their tick came from a class default; nobody verified anything."""
    from app.broker.instrument_catalog import (
        STATUS_CANDIDATE,
        catalog_entry_status,
        get_instrument,
        verification_status_of,
    )

    engine, session = pr138_db
    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)
    session.expire_all()

    entry = get_instrument(session, "BYMA")
    assert entry is not None
    assert verification_status_of(entry) == STATUS_CANDIDATE
    assert catalog_entry_status(entry, max_age_seconds=86400)[0] == "instrument_not_verified"


def test_upgrade_adds_ledger_uniqueness(pr138_db):
    from sqlalchemy import inspect as sa_inspect

    engine, _session = pr138_db
    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)

    indexes = {ix["name"] for ix in sa_inspect(engine).get_indexes("execution_daily_notional")}
    assert "uq_execution_daily_notional_scope" in indexes


def test_upgrade_is_idempotent_on_a_pr138_database(pr138_db):
    engine, session = pr138_db
    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)
    before = _fingerprint(session)

    for _ in range(3):
        Base.metadata.create_all(bind=engine)
        _patch_schema(engine)

    session.expire_all()
    assert _fingerprint(session) == before


def test_duplicate_ledger_rows_fail_closed_instead_of_being_merged(pr138_db):
    """These numbers gate real spending. Silently collapsing them could hide
    spent budget or restore allowance that was legitimately consumed."""
    engine, session = pr138_db
    with engine.begin() as conn:
        for _ in range(2):
            conn.execute(text(
                "INSERT INTO execution_daily_notional "
                "(trade_date, execution_class, currency, submitted_notional, "
                " order_count, updated_at) "
                "VALUES ('2026-08-04','ACCIONES','ARS',500,1,:now)"
            ), {"now": datetime.utcnow()})

    with pytest.raises(RuntimeError, match="duplicate"):
        Base.metadata.create_all(bind=engine)
        _patch_schema(engine)

    # Nothing was merged or deleted: both rows are still there for a human.
    with engine.begin() as conn:
        remaining = conn.execute(text(
            "SELECT COUNT(*) FROM execution_daily_notional"
        )).scalar()
    assert remaining == 2


def test_new_fund_tables_start_empty(pr138_db):
    from app.models.models import FundInstrument, FundOperation

    engine, session = pr138_db
    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)
    assert session.query(FundInstrument).count() == 0
    assert session.query(FundOperation).count() == 0
    # And production history is still intact alongside them.
    assert session.get(Recommendation, PROD_RECOMMENDATION_ID).status == "pending"
