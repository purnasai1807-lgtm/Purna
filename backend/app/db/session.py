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
    from app.db.models import (  # noqa: F401
        AnalysisCacheEntry,
        AnalysisReport,
        AnalysisReportCacheLink,
        AnalysisUploadSession,
        User,
    )

    Base.metadata.create_all(bind=engine)
    ensure_analysis_cache_entry_columns()
    ensure_analysis_upload_session_columns()


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


def ensure_analysis_upload_session_columns() -> None:
    type_map = {
        "sqlite": {
            "target_column": "TEXT",
            "content_type": "TEXT",
            "file_size_bytes": "INTEGER DEFAULT 0",
            "processing_mode": "TEXT DEFAULT 'small'",
            "storage_backend": "TEXT DEFAULT 'local'",
            "storage_key": "TEXT",
            "s3_upload_id": "TEXT",
            "upload_strategy": "TEXT DEFAULT 'single_part'",
            "status": "TEXT DEFAULT 'created'",
            "error_message": "TEXT",
            "report_id": "TEXT",
            "job_id": "TEXT",
            "expires_at": "TEXT",
        },
        "postgresql": {
            "target_column": "VARCHAR(120)",
            "content_type": "VARCHAR(255)",
            "file_size_bytes": "INTEGER DEFAULT 0",
            "processing_mode": "VARCHAR(20) DEFAULT 'small'",
            "storage_backend": "VARCHAR(20) DEFAULT 'local'",
            "storage_key": "TEXT",
            "s3_upload_id": "VARCHAR(255)",
            "upload_strategy": "VARCHAR(20) DEFAULT 'single_part'",
            "status": "VARCHAR(20) DEFAULT 'created'",
            "error_message": "TEXT",
            "report_id": "VARCHAR(36)",
            "job_id": "VARCHAR(36)",
            "expires_at": "TIMESTAMP WITH TIME ZONE",
        },
    }

    dialect = engine.dialect.name
    column_definitions = type_map.get(dialect, type_map["postgresql"])

    with engine.begin() as connection:
        inspector = inspect(connection)
        existing_tables = set(inspector.get_table_names())
        if "analysis_upload_sessions" not in existing_tables:
            return

        existing_columns = {column["name"] for column in inspector.get_columns("analysis_upload_sessions")}
        for column_name, column_ddl in column_definitions.items():
            if column_name in existing_columns:
                continue
            connection.execute(
                text(f"ALTER TABLE analysis_upload_sessions ADD COLUMN {column_name} {column_ddl}")
            )
