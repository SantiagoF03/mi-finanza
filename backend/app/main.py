from sqlalchemy import inspect, text

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router
from app.core.config import get_settings
from app.db.session import Base, engine
from app.scheduler.jobs import start_scheduler

app = FastAPI(title="Mi Finanza MVP")

_settings = get_settings()
_origins = [o.strip() for o in _settings.cors_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _patch_schema(engine_ref):
    """Ensure schema matches models for DBs created before EventCluster existed.

    SQLAlchemy's create_all() creates missing *tables* but never adds columns
    to existing tables.  This helper detects the gap and patches it.
    """
    inspector = inspect(engine_ref)
    existing_tables = inspector.get_table_names()

    # 1. event_clusters table — create_all handles this for new AND old DBs
    #    (no extra work needed; included here for clarity)

    # 2. news_normalized.event_cluster_id — must be added if missing
    if "news_normalized" in existing_tables:
        columns = {c["name"] for c in inspector.get_columns("news_normalized")}
        if "event_cluster_id" not in columns:
            with engine_ref.begin() as conn:
                conn.execute(text(
                    "ALTER TABLE news_normalized "
                    "ADD COLUMN event_cluster_id INTEGER REFERENCES event_clusters(id)"
                ))

    # 3. recommendation_actions.quantity_override — nullable, added for the
    #    execution pilot. Existing rows stay NULL, so automatic
    #    recommendations keep their current behavior unchanged.
    if "recommendation_actions" in existing_tables:
        columns = {c["name"] for c in inspector.get_columns("recommendation_actions")}
        if "quantity_override" not in columns:
            with engine_ref.begin() as conn:
                conn.execute(text(
                    "ALTER TABLE recommendation_actions ADD COLUMN quantity_override INTEGER"
                ))

    # 3b. execution_instruments verification columns (added after PR #138
    #     shipped the table without them). Nullable/defaulted, so existing
    #     rows keep working — they simply start as `candidate`, i.e. NOT
    #     executable until verified. That is the fail-closed direction.
    if "execution_instruments" in existing_tables:
        columns = {c["name"] for c in inspector.get_columns("execution_instruments")}
        additions = {
            "verification_status": "VARCHAR(20) DEFAULT 'candidate'",
            "field_provenance": "JSON",
            "pending_identity": "JSON",
            "previous_identity": "JSON",
            "identity_changed_at": "DATETIME",
            "verification_audit": "JSON",
        }
        for column, ddl in additions.items():
            if column not in columns:
                with engine_ref.begin() as conn:
                    conn.execute(text(
                        f"ALTER TABLE execution_instruments ADD COLUMN {column} {ddl}"
                    ))
        # Pre-existing rows must NOT be grandfathered into `verified`: an
        # entry whose tick came from a class default was never verified by
        # anyone. SQLite's ADD COLUMN ... DEFAULT backfills existing rows, and
        # the read path treats NULL/empty as `candidate` anyway — so no
        # rewrite of existing data is needed, and none is performed.

    # 3c. execution_daily_notional uniqueness (trade_date, execution_class,
    #     currency). The table may already exist from PR #138 WITHOUT the
    #     constraint, and SQLite cannot ADD CONSTRAINT — a unique INDEX is the
    #     additive, idempotent equivalent.
    #
    #     If duplicates already exist we FAIL CLOSED and report, rather than
    #     merging or deleting rows: those numbers gate real money, and
    #     silently collapsing them could either hide spent budget or restore
    #     allowance that was legitimately consumed.
    if "execution_daily_notional" in existing_tables:
        index_names = {ix["name"] for ix in inspector.get_indexes("execution_daily_notional")}
        if "uq_execution_daily_notional_scope" not in index_names:
            with engine_ref.begin() as conn:
                duplicates = conn.execute(text(
                    "SELECT trade_date, execution_class, currency, COUNT(*) AS n "
                    "FROM execution_daily_notional "
                    "GROUP BY trade_date, execution_class, currency HAVING n > 1"
                )).fetchall()
                if duplicates:
                    raise RuntimeError(
                        "execution_daily_notional has duplicate "
                        "(trade_date, execution_class, currency) rows and cannot be "
                        "made unique automatically. These rows gate real spending "
                        f"limits and must be reconciled by hand: {duplicates!r}"
                    )
                conn.execute(text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_daily_notional_scope "
                    "ON execution_daily_notional (trade_date, execution_class, currency)"
                ))

    # 3d. fund_operations validation columns. Nullable/empty defaults, so an
    #     existing operation simply reads as "not validated" — which blocks
    #     submission. That is the fail-closed direction.
    if "fund_operations" in existing_tables:
        columns = {c["name"] for c in inspector.get_columns("fund_operations")}
        for column, ddl in (
            ("validated_at", "DATETIME"),
            ("validated_payload_hash", "VARCHAR(64) DEFAULT ''"),
            # Empty means "the fund's verified state was never recorded for
            # this validation", which reads as changed and forces a new
            # validation. Fail-closed, and correct: it genuinely was not
            # checked.
            ("validated_fund_hash", "VARCHAR(64) DEFAULT ''"),
        ):
            if column not in columns:
                with engine_ref.begin() as conn:
                    conn.execute(text(
                        f"ALTER TABLE fund_operations ADD COLUMN {column} {ddl}"
                    ))

    # 3d-bis. FCI daily-ledger keys moved from "FCI:subscribe"/"FCI:redeem" to
    #     the explicit FCI_SUBSCRIBE / FCI_REDEEM classes. Deliberately NO
    #     rewrite: the old rows stay exactly as they are, readable as audit.
    #     Renaming them would move already-spent budget into today's key, and
    #     copying them would charge the same money twice. Queries filter on the
    #     exact class, so the legacy rows are simply never counted again.

    # 3e. fund_instruments identity columns. Empty identity_hash means "never
    #     hashed"; the first refresh computes it and — because there is no
    #     previous hash to differ from — does NOT report an identity change.
    #     Reading a missing hash as a change would freeze every existing fund
    #     at once for a schema migration nobody made.
    if "fund_instruments" in existing_tables:
        columns = {c["name"] for c in inspector.get_columns("fund_instruments")}
        for column, ddl in (
            ("identity_hash", "VARCHAR(64) DEFAULT ''"),
            ("previous_identity", "JSON"),
            ("pending_identity", "JSON"),
        ):
            if column not in columns:
                with engine_ref.begin() as conn:
                    conn.execute(text(
                        f"ALTER TABLE fund_instruments ADD COLUMN {column} {ddl}"
                    ))

    # 4. execution_instruments / execution_daily_notional /
    #    fund_instrument_verifications — new tables only.
    #    create_all() already made them; nothing here rewrites or drops data.
    #
    #    MIGRATION SAFETY: every step in this function is additive and
    #    idempotent — CREATE TABLE for tables that do not exist, ADD COLUMN
    #    for columns that do not exist. There is no DROP, no UPDATE and no
    #    DELETE anywhere in the startup path, so existing rows (including
    #    Recommendation 13 and OrderExecution 1) are never touched.
    #
    #    ROLLBACK: reverting the application code is sufficient. The new
    #    tables are simply left unused and unread by the previous version;
    #    no old column changed type, meaning or nullability. If the tables
    #    must also be removed, that is a manual, offline
    #    `DROP TABLE execution_instruments; DROP TABLE execution_daily_notional;`
    #    which cannot affect any pre-existing table.


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)
    _patch_schema(engine)

    # Load persisted settings from DB (P4)
    from app.db.session import SessionLocal
    try:
        db = SessionLocal()
        from app.api.routes import _load_persisted_settings
        _load_persisted_settings(db)
        db.close()
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning(
            "Failed to load persisted settings at startup (using .env defaults): %s", exc
        )

    start_scheduler()


app.include_router(router, prefix="/api")
