from sqlalchemy.orm import Session

from app.models.models import AppLog


def app_log(db: Session, message: str, level: str = "INFO", context: dict | None = None) -> None:
    log = AppLog(level=level, message=message, context=context or {})
    db.add(log)
    db.commit()
