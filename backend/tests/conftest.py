"""Shared test setup.

Ensures the real (default) database has its schema created before any test
touches SessionLocal or TestClient. Locally the tables usually already exist;
on a fresh checkout (CI) they don't, and TestClient does not always fire the
startup hook that would create them.
"""

import app.models.models  # noqa: F401 — register all models on Base
from app.db.session import Base, engine

Base.metadata.create_all(bind=engine)
