from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from app.core.config import settings

engine_kwargs: dict[str, object] = {"pool_pre_ping": True}
if settings.database_url.startswith("sqlite"):
    engine_kwargs["connect_args"] = {"check_same_thread": False}

engine = create_engine(settings.database_url, **engine_kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    from app.db.models import AnalysisCacheEntry, AnalysisReport, AnalysisReportCacheLink, User  # noqa: F401

    Base.metadata.create_all(bind=engine)
    ensure_analysis_cache_entry_columns()


def ensure_analysis_cache_entry_columns() -> None:
    type_map = {
        "sqlite": {
            "storage_backend": "TEXT DEFAULT 'local'",
            "storage_key": "TEXT",
            "celery_task_id": "TEXT",
            "started_at": "TEXT",
            "completed_at": "TEXT",
            "failed_at": "TEXT",
        },
        "postgresql": {
            "storage_backend": "VARCHAR(20) DEFAULT 'local'",
            "storage_key": "TEXT",
            "celery_task_id": "VARCHAR(255)",
            "started_at": "TIMESTAMP WITH TIME ZONE",
            "completed_at": "TIMESTAMP WITH TIME ZONE",
            "failed_at": "TIMESTAMP WITH TIME ZONE",
        },
    }

    dialect = engine.dialect.name
    column_definitions = type_map.get(dialect, type_map["postgresql"])

    with engine.begin() as connection:
        inspector = inspect(connection)
        existing_columns = {column["name"] for column in inspector.get_columns("analysis_cache_entries")}
        for column_name, column_ddl in column_definitions.items():
            if column_name in existing_columns:
                continue
            connection.execute(
                text(f"ALTER TABLE analysis_cache_entries ADD COLUMN {column_name} {column_ddl}")
            )
