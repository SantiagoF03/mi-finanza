from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from app.core.config import get_settings

settings = get_settings()

# ONE engine for the whole application.
#
# This module used to build `engine` twice: SessionLocal was bound to the
# first one (no busy timeout) while the exported `engine` — used by
# create_all/_patch_schema — was a second, different object. Both pointed at
# the same file, so it "worked", but the analysis lease depends on
# conditional UPDATEs racing against other sessions, and that guarantee must
# not rest on two engines with different connect args. They are now one.
#
# `timeout` is a sqlite3-only connect arg: it gives writers a busy-wait
# window instead of failing immediately under concurrent access (and the
# previous unconditional version would have broken a non-sqlite URL).
if settings.database_url.startswith("sqlite"):
    connect_args = {"check_same_thread": False, "timeout": 30}
else:
    connect_args = {}

engine = create_engine(settings.database_url, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
