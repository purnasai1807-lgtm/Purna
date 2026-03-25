from __future__ import annotations

import secrets
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.session import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        onupdate=utcnow,
    )


class User(TimestampMixin, Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    full_name: Mapped[str] = mapped_column(String(120))
    password_hash: Mapped[str] = mapped_column(String(255))

    reports: Mapped[list["AnalysisReport"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
    )


class AnalysisReport(TimestampMixin, Base):
    __tablename__ = "analysis_reports"
    __table_args__ = (
        Index("ix_analysis_reports_user_created_at", "user_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), index=True)
    dataset_name: Mapped[str] = mapped_column(String(180))
    source_type: Mapped[str] = mapped_column(String(30))
    target_column: Mapped[str | None] = mapped_column(String(120), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="completed")
    row_count: Mapped[int] = mapped_column(Integer)
    column_count: Mapped[int] = mapped_column(Integer)
    share_token: Mapped[str] = mapped_column(
        String(64),
        unique=True,
        index=True,
        default=lambda: secrets.token_urlsafe(18),
    )
    report_payload: Mapped[dict] = mapped_column(JSON)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    user: Mapped[User] = relationship(back_populates="reports")
    cache_link: Mapped["AnalysisReportCacheLink | None"] = relationship(
        back_populates="report",
        cascade="all, delete-orphan",
        uselist=False,
    )


class AnalysisCacheEntry(TimestampMixin, Base):
    __tablename__ = "analysis_cache_entries"
    __table_args__ = (
        UniqueConstraint("content_hash", "target_column", name="uq_analysis_cache_entries_hash_target"),
        Index("ix_analysis_cache_entries_status_updated_at", "status", "updated_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    content_hash: Mapped[str] = mapped_column(String(128), index=True)
    original_filename: Mapped[str] = mapped_column(String(255))
    file_type: Mapped[str] = mapped_column(String(30))
    target_column: Mapped[str | None] = mapped_column(String(120), nullable=True)
    processing_mode: Mapped[str] = mapped_column(String(20), default="small")
    status: Mapped[str] = mapped_column(String(20), default="queued")
    progress: Mapped[int] = mapped_column(Integer, default=0)
    progress_message: Mapped[str | None] = mapped_column(String(255), nullable=True)
    file_size_bytes: Mapped[int] = mapped_column(Integer, default=0)
    row_count: Mapped[int] = mapped_column(Integer, default=0)
    column_count: Mapped[int] = mapped_column(Integer, default=0)
    storage_backend: Mapped[str] = mapped_column(String(20), default="local")
    storage_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    storage_path: Mapped[str] = mapped_column(Text)
    parquet_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    celery_task_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    preview_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    full_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    sections_ready: Mapped[dict] = mapped_column(JSON, default=dict)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    report_links: Mapped[list["AnalysisReportCacheLink"]] = relationship(
        back_populates="cache_entry",
        cascade="all, delete-orphan",
    )


class AnalysisUploadSession(TimestampMixin, Base):
    __tablename__ = "analysis_upload_sessions"
    __table_args__ = (
        Index("ix_analysis_upload_sessions_user_created_at", "user_id", "created_at"),
        Index("ix_analysis_upload_sessions_status_updated_at", "status", "updated_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id"), index=True)
    dataset_name: Mapped[str] = mapped_column(String(180))
    target_column: Mapped[str | None] = mapped_column(String(120), nullable=True)
    original_filename: Mapped[str] = mapped_column(String(255))
    content_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    file_size_bytes: Mapped[int] = mapped_column(Integer, default=0)
    processing_mode: Mapped[str] = mapped_column(String(20), default="small")
    storage_backend: Mapped[str] = mapped_column(String(20), default="local")
    storage_key: Mapped[str] = mapped_column(Text)
    s3_upload_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    upload_strategy: Mapped[str] = mapped_column(String(20), default="single_part")
    status: Mapped[str] = mapped_column(String(20), default="created")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    report_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("analysis_reports.id", ondelete="SET NULL"),
        nullable=True,
    )
    job_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("analysis_cache_entries.id", ondelete="SET NULL"),
        nullable=True,
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped[User] = relationship()
    report: Mapped[AnalysisReport | None] = relationship(foreign_keys=[report_id])
    job: Mapped[AnalysisCacheEntry | None] = relationship(foreign_keys=[job_id])


class AnalysisReportCacheLink(Base):
    __tablename__ = "analysis_report_cache_links"
    __table_args__ = (
        Index("ix_analysis_report_cache_links_cache_entry_id", "cache_entry_id"),
    )

    report_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("analysis_reports.id", ondelete="CASCADE"),
        primary_key=True,
    )
    cache_entry_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("analysis_cache_entries.id", ondelete="CASCADE"),
    )

    report: Mapped[AnalysisReport] = relationship(back_populates="cache_link")
    cache_entry: Mapped[AnalysisCacheEntry] = relationship(back_populates="report_links")
