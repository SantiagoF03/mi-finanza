from sqlalchemy import inspect, text

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router
from app.db.session import Base, engine
from app.scheduler.jobs import start_scheduler

app = FastAPI(title="Mi Finanza MVP")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
    except Exception:
        pass

    start_scheduler()


app.include_router(router, prefix="/api")
