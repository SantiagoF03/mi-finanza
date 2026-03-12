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


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

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
