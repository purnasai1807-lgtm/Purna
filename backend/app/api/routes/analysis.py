from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.config import settings
from app.db.models import AnalysisReport, User
from app.db.session import get_db
from app.schemas.analysis import (
    AnalysisHistoryItem,
    AnalysisReportRead,
    ManualEntryRequest,
    ShareLinkResponse,
)
from app.services.analytics import analyze_dataframe, parse_manual_dataframe, parse_uploaded_dataframe
from app.services.reporting import build_pdf_report

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.post("/upload", response_model=AnalysisReportRead)
async def upload_analysis(
    file: UploadFile = File(...),
    dataset_name: str | None = Form(default=None),
    target_column: str | None = Form(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AnalysisReportRead:
    filename = file.filename or ""
    if not filename:
        raise HTTPException(status_code=400, detail="Please select a CSV or Excel file.")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="The uploaded file is empty.")

    try:
        dataframe = parse_uploaded_dataframe(filename, content)
        payload = analyze_dataframe(
            dataframe=dataframe,
            dataset_name=dataset_name or filename.rsplit(".", maxsplit=1)[0],
            source_type="upload",
            target_column=target_column,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {exc}") from exc

    report = persist_report(
        db=db,
        current_user=current_user,
        dataset_name=payload["dataset_name"],
        source_type="upload",
        target_column=payload.get("target_column"),
        payload=payload,
    )
    return serialize_report(report)


@router.post("/manual", response_model=AnalysisReportRead)
def manual_analysis(
    request: ManualEntryRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AnalysisReportRead:
    try:
        dataframe = parse_manual_dataframe(request.columns, request.rows)
        payload = analyze_dataframe(
            dataframe=dataframe,
            dataset_name=request.dataset_name,
            source_type="manual",
            target_column=request.target_column,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {exc}") from exc

    report = persist_report(
        db=db,
        current_user=current_user,
        dataset_name=payload["dataset_name"],
        source_type="manual",
        target_column=payload.get("target_column"),
        payload=payload,
    )
    return serialize_report(report)


@router.get("/history", response_model=list[AnalysisHistoryItem])
def list_history(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[AnalysisHistoryItem]:
    statement = (
        select(AnalysisReport)
        .where(AnalysisReport.user_id == current_user.id)
        .order_by(desc(AnalysisReport.created_at))
    )
    reports = db.scalars(statement).all()
    return [serialize_history_item(report) for report in reports]


@router.get("/reports/{report_id}", response_model=AnalysisReportRead)
def get_report(
    report_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AnalysisReportRead:
    report = get_owned_report(report_id, current_user.id, db)
    return serialize_report(report)


@router.post("/reports/{report_id}/share", response_model=ShareLinkResponse)
def create_share_link(
    report_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ShareLinkResponse:
    report = get_owned_report(report_id, current_user.id, db)
    return ShareLinkResponse(share_token=report.share_token, share_url=build_share_url(report.share_token))


@router.get("/shared/{share_token}", response_model=AnalysisReportRead)
def get_shared_report(share_token: str, db: Session = Depends(get_db)) -> AnalysisReportRead:
    statement = select(AnalysisReport).where(AnalysisReport.share_token == share_token)
    report = db.scalar(statement)
    if not report:
        raise HTTPException(status_code=404, detail="Shared report not found.")
    return serialize_report(report)


@router.get("/reports/{report_id}/download-pdf")
def download_report_pdf(
    report_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    report = get_owned_report(report_id, current_user.id, db)
    pdf_buffer = build_pdf_report(report)
    safe_name = report.dataset_name.lower().replace(" ", "-")
    return StreamingResponse(
        pdf_buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{safe_name}-report.pdf"'},
    )


def persist_report(
    db: Session,
    current_user: User,
    dataset_name: str,
    source_type: str,
    target_column: str | None,
    payload: dict,
) -> AnalysisReport:
    overview = payload.get("overview", {})
    report = AnalysisReport(
        user_id=current_user.id,
        dataset_name=dataset_name,
        source_type=source_type,
        target_column=target_column,
        row_count=overview.get("row_count", 0),
        column_count=overview.get("column_count", 0),
        report_payload=payload,
    )
    db.add(report)
    db.commit()
    db.refresh(report)
    return report


def get_owned_report(report_id: str, user_id: str, db: Session) -> AnalysisReport:
    report = db.scalar(
        select(AnalysisReport).where(
            AnalysisReport.id == report_id,
            AnalysisReport.user_id == user_id,
        )
    )
    if not report:
        raise HTTPException(status_code=404, detail="Report not found.")
    return report


def serialize_history_item(report: AnalysisReport) -> AnalysisHistoryItem:
    return AnalysisHistoryItem(
        id=report.id,
        dataset_name=report.dataset_name,
        source_type=report.source_type,
        target_column=report.target_column,
        row_count=report.row_count,
        column_count=report.column_count,
        status=report.status,
        share_token=report.share_token,
        share_url=build_share_url(report.share_token),
        created_at=report.created_at,
    )


def serialize_report(report: AnalysisReport) -> AnalysisReportRead:
    return AnalysisReportRead(
        **serialize_history_item(report).model_dump(),
        report=report.report_payload or {},
    )


def build_share_url(share_token: str) -> str:
    return f"{settings.report_base_url.rstrip('/')}/share/{share_token}"

