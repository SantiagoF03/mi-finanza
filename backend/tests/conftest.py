"""Shared test setup.

Ensures the real (default) database has its schema created AND patched
before any test touches SessionLocal or TestClient. Locally the tables
usually already exist; on a fresh checkout (CI) they don't, and TestClient
does not always fire the startup hook that would create them.

_patch_schema is applied for the same reason production applies it at
startup: create_all() creates missing tables but never adds columns to
existing ones (e.g. recommendation_actions.quantity_override).
"""

import app.models.models  # noqa: F401 — register all models on Base
from app.db.session import Base, engine
from app.main import _patch_schema

Base.metadata.create_all(bind=engine)
_patch_schema(engine)
