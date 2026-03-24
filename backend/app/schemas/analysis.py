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
    dataset_name: str
    source_type: str
    target_column: str | None
    row_count: int
    column_count: int
    status: str
    share_token: str
    share_url: str
    created_at: datetime


class AnalysisReportRead(AnalysisHistoryItem):
    report: dict[str, Any]


class ShareLinkResponse(BaseModel):
    share_token: str
    share_url: str

