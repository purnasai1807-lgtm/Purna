from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class ManualEntryRequest(BaseModel):
    dataset_name: str = Field(min_length=2, max_length=180)
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    target_column: str | None = Field(default=None, max_length=120)


class AnalysisHistoryItem(BaseModel):
    id: str
    job_id: str | None = None
    job_status_url: str | None = None
    dataset_name: str
    source_type: str
    target_column: str | None
    row_count: int
    column_count: int
    status: str
    progress: int = 100
    progress_message: str | None = None
    processing_mode: str | None = None
    file_type: str | None = None
    file_size_bytes: int | None = None
    error_message: str | None = None
    share_token: str
    share_url: str
    created_at: datetime


class AnalysisJobStatusRead(BaseModel):
    job_id: str
    report_id: str
    dataset_name: str
    status: str
    progress: int = 100
    message: str | None = None
    progress_message: str | None = None
    processing_mode: str | None = None
    file_type: str | None = None
    file_size_bytes: int | None = None
    error_message: str | None = None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    failed_at: datetime | None = None
    result: dict[str, Any] | None = None


class AnalysisReportRead(AnalysisHistoryItem):
    report: dict[str, Any]


class ShareLinkResponse(BaseModel):
    share_token: str
    share_url: str


class AnalysisSectionRead(BaseModel):
    section: str
    data: Any


class ReportRowsPageRead(BaseModel):
    page: int
    page_size: int
    total_rows: int
    total_pages: int
    columns: list[str]
    rows: list[dict[str, Any]]
    is_preview: bool = False
