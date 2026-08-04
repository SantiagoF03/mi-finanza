"""Shared test setup.

Ensures the real (default) database has its schema created AND patched
before any test touches SessionLocal or TestClient. Locally the tables
usually already exist; on a fresh checkout (CI) they don't, and TestClient
does not always fire the startup hook that would create them.

_patch_schema is applied for the same reason production applies it at
startup: create_all() creates missing tables but never adds columns to
existing ones (e.g. recommendation_actions.quantity_override).
"""

from unittest import mock

import pytest

import app.models.models  # noqa: F401 — register all models on Base
from app.db.session import Base, engine
from app.main import _patch_schema

Base.metadata.create_all(bind=engine)
_patch_schema(engine)


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "real_market_calendar: use the real trading calendar instead of the "
        "suite-wide open-session default",
    )


_OPEN_SESSION = {
    "open": True,
    "code": None,
    "timezone": "America/Argentina/Buenos_Aires",
    "open_time": "10:30",
    "close_time": "17:00",
    "deprecated_settings_in_use": False,
    "local_time": None,
    "local_date": None,
}


@pytest.fixture(autouse=True)
def _default_open_market_session(request):
    """Default the securities session to OPEN for the whole suite.

    Order submission now requires an open BYMA session, which depends on the
    wall clock and on the day of the week. Letting that leak into every
    execution test would make the suite fail on evenings and weekends for
    reasons unrelated to what those tests assert.

    The calendar itself is covered deterministically in
    tests/test_market_calendar.py, which passes an explicit `now` and marks
    itself with @pytest.mark.real_market_calendar to opt out of this default.
    """
    if request.node.get_closest_marker("real_market_calendar"):
        yield
        return
    with mock.patch(
        "app.services.execution.market_session_state", return_value=dict(_OPEN_SESSION)
    ):
        yield
