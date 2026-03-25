from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import desc, select
from sqlalchemy.orm import Session, selectinload

from app.api.deps import get_current_user
from app.core.config import settings
from app.db.models import AnalysisCacheEntry, AnalysisReport, AnalysisReportCacheLink, User
from app.db.session import get_db
from app.schemas.analysis import (
    AnalysisHistoryItem,
    AnalysisJobStatusRead,
    AnalysisReportRead,
    AnalysisSectionRead,
    ManualEntryRequest,
    ReportRowsPageRead,
    ShareLinkResponse,
)
from app.services.analytics import analyze_dataframe, normalize_column_name, parse_manual_dataframe
from app.services.job_manager import job_manager
from app.services.processing import (
    attach_report_to_existing_cache,
    build_preview_payload,
    build_small_file_payload,
    create_cache_entry,
    create_upload_report,
    ensure_report_section,
    find_cache_entry,
    get_preview_ready_state,
    get_report_rows_page,
    materialize_report_payload,
)
from app.services.reporting import build_pdf_report
from app.services.storage import delete_storage_artifacts, delete_stored_upload, save_upload_to_storage

router = APIRouter(prefix="/analysis", tags=["analysis"])
logger = logging.getLogger(__name__)


@router.post("/upload", response_model=AnalysisReportRead)
async def upload_analysis(
    file: UploadFile = File(...),
    dataset_name: str | None = Form(default=None),
    target_column: str | None = Form(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AnalysisReportRead:
    normalized_target = normalize_column_name(target_column) if target_column else None
    stored_upload = None

    try:
        stored_upload = await save_upload_to_storage(file)
        resolved_dataset_name = dataset_name or Path(stored_upload.original_filename).stem or "Dataset"
        logger.info(
            "Upload received: user_id=%s filename=%s size_bytes=%s mode=%s type=%s",
            current_user.id,
            stored_upload.original_filename,
            stored_upload.file_size_bytes,
            stored_upload.processing_mode,
            stored_upload.file_type,
        )

        cache_entry = find_cache_entry(
            db,
            content_hash=stored_upload.content_hash,
            target_column=normalized_target,
        )
        if cache_entry and cache_entry.status == "failed":
            logger.info(
                "Rebuilding failed cache entry: cache_entry_id=%s filename=%s mode=%s",
                cache_entry.id,
                stored_upload.original_filename,
                stored_upload.processing_mode,
            )
            previous_storage_path = cache_entry.storage_path
            previous_storage_backend = cache_entry.storage_backend
            previous_storage_key = cache_entry.storage_key
            preview_payload = (
                build_small_file_payload(
                    stored_upload=stored_upload,
                    dataset_name=resolved_dataset_name,
                    target_column=normalized_target,
                )
                if stored_upload.processing_mode == "small"
                else build_preview_payload(
                    stored_upload=stored_upload,
                    dataset_name=resolved_dataset_name,
                    target_column=normalized_target,
                )
            )
            cache_entry.original_filename = stored_upload.original_filename
            cache_entry.file_type = stored_upload.file_type
            cache_entry.processing_mode = stored_upload.processing_mode
            cache_entry.status = "completed" if stored_upload.processing_mode == "small" else "preview_ready"
            cache_entry.progress = get_preview_ready_state(stored_upload.processing_mode)[0]
            cache_entry.progress_message = get_preview_ready_state(stored_upload.processing_mode)[1]
            cache_entry.file_size_bytes = stored_upload.file_size_bytes
            cache_entry.row_count = preview_payload.get("overview", {}).get("row_count", 0)
            cache_entry.column_count = preview_payload.get("overview", {}).get("column_count", 0)
            cache_entry.storage_backend = stored_upload.storage_backend
            cache_entry.storage_key = stored_upload.storage_key
            cache_entry.storage_path = str(stored_upload.storage_path)
            cache_entry.parquet_path = None
            cache_entry.celery_task_id = None
            cache_entry.started_at = None
            cache_entry.completed_at = None
            cache_entry.failed_at = None
            cache_entry.preview_payload = preview_payload
            cache_entry.full_payload = preview_payload if stored_upload.processing_mode == "small" else None
            cache_entry.sections_ready = preview_payload.get("sections", {})
            cache_entry.error_message = None
            report = create_upload_report(
                db,
                current_user=current_user,
                dataset_name=resolved_dataset_name,
                target_column=normalized_target,
                cache_entry=cache_entry,
            )
            db.commit()
            db.refresh(report)
            if stored_upload.processing_mode != "small":
                job_manager.submit(cache_entry.id)
            if (
                previous_storage_path != str(stored_upload.storage_path)
                or previous_storage_key != stored_upload.storage_key
            ):
                delete_storage_artifacts(
                    storage_path=previous_storage_path,
                    storage_backend=previous_storage_backend,
                    storage_key=previous_storage_key,
                )
            logger.info(
                "Upload report created from rebuilt cache entry: report_id=%s cache_entry_id=%s status=%s",
                report.id,
                cache_entry.id,
                cache_entry.status,
            )
            return serialize_report(report)

        if cache_entry:
            delete_stored_upload(stored_upload)
            logger.info(
                "Cache hit for upload: cache_entry_id=%s filename=%s status=%s",
                cache_entry.id,
                stored_upload.original_filename,
                cache_entry.status,
            )
            report = attach_report_to_existing_cache(
                db,
                current_user=current_user,
                dataset_name=resolved_dataset_name,
                target_column=normalized_target,
                cache_entry=cache_entry,
            )
            db.commit()
            db.refresh(report)
            if cache_entry.status in {"queued", "preview_ready", "processing"}:
                job_manager.submit(cache_entry.id)
            logger.info(
                "Upload report attached to existing cache: report_id=%s cache_entry_id=%s status=%s",
                report.id,
                cache_entry.id,
                cache_entry.status,
            )
            return serialize_report(report)

        preview_payload = (
            build_small_file_payload(
                stored_upload=stored_upload,
                dataset_name=resolved_dataset_name,
                target_column=normalized_target,
            )
            if stored_upload.processing_mode == "small"
            else build_preview_payload(
                stored_upload=stored_upload,
                dataset_name=resolved_dataset_name,
                target_column=normalized_target,
            )
        )
        cache_entry = create_cache_entry(
            db,
            stored_upload=stored_upload,
            target_column=normalized_target,
            preview_payload=preview_payload,
        )
        report = create_upload_report(
            db,
            current_user=current_user,
            dataset_name=resolved_dataset_name,
            target_column=normalized_target,
            cache_entry=cache_entry,
        )
        db.commit()
        db.refresh(report)

        if stored_upload.processing_mode != "small":
            job_manager.submit(cache_entry.id)

        logger.info(
            "Upload report created: report_id=%s cache_entry_id=%s status=%s mode=%s",
            report.id,
            cache_entry.id,
            cache_entry.status,
            stored_upload.processing_mode,
        )
        return serialize_report(report)
    except ValueError as exc:
        delete_stored_upload(stored_upload)
        logger.warning("Upload validation failed: filename=%s error=%s", getattr(file, "filename", None), exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        delete_stored_upload(stored_upload)
        logger.exception("Upload analysis failed: filename=%s", getattr(file, "filename", None))
        raise HTTPException(status_code=500, detail=f"Analysis failed: {exc}") from exc


@router.post("/manual", response_model=AnalysisReportRead)
def manual_analysis(
    request: ManualEntryRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AnalysisReportRead:
    try:
        logger.info(
            "Manual analysis requested: user_id=%s dataset=%s rows=%s columns=%s",
            current_user.id,
            request.dataset_name,
            len(request.rows),
            len(request.columns),
        )
        dataframe = parse_manual_dataframe(request.columns, request.rows)
        payload = analyze_dataframe(
            dataframe=dataframe,
            dataset_name=request.dataset_name,
            source_type="manual",
            target_column=request.target_column,
            include_charts=True,
            metadata={"processing_mode": "manual", "file_type": "manual"},
        )
    except ValueError as exc:
        logger.warning("Manual analysis validation failed: dataset=%s error=%s", request.dataset_name, exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Manual analysis failed: dataset=%s", request.dataset_name)
        raise HTTPException(status_code=500, detail=f"Analysis failed: {exc}") from exc

    report = AnalysisReport(
        user_id=current_user.id,
        dataset_name=request.dataset_name,
        source_type="manual",
        target_column=payload.get("target_column"),
        status="completed",
        row_count=payload.get("overview", {}).get("row_count", 0),
        column_count=payload.get("overview", {}).get("column_count", 0),
        report_payload=payload,
    )
    db.add(report)
    db.commit()
    db.refresh(report)
    logger.info("Manual analysis completed: report_id=%s dataset=%s", report.id, request.dataset_name)
    return serialize_report(report)


@router.get("/history", response_model=list[AnalysisHistoryItem])
def list_history(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[AnalysisHistoryItem]:
    reports = db.scalars(
        select(AnalysisReport)
        .where(AnalysisReport.user_id == current_user.id)
        .options(selectinload(AnalysisReport.cache_link).selectinload(AnalysisReportCacheLink.cache_entry))
        .order_by(desc(AnalysisReport.created_at))
    ).all()
    return [serialize_history_item(report) for report in reports]


@router.get("/jobs/{job_id}", response_model=AnalysisJobStatusRead)
def get_job_status(
    job_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AnalysisJobStatusRead:
    report = db.scalar(
        select(AnalysisReport)
        .join(AnalysisReportCacheLink, AnalysisReportCacheLink.report_id == AnalysisReport.id)
        .join(AnalysisCacheEntry, AnalysisCacheEntry.id == AnalysisReportCacheLink.cache_entry_id)
        .where(AnalysisCacheEntry.id == job_id, AnalysisReport.user_id == current_user.id)
        .options(selectinload(AnalysisReport.cache_link).selectinload(AnalysisReportCacheLink.cache_entry))
        .order_by(desc(AnalysisReport.created_at))
    )
    if not report or not report.cache_link:
        raise HTTPException(status_code=404, detail="Job not found.")

    cache_entry = report.cache_link.cache_entry
    result_payload = materialize_report_payload(report, cache_entry) if (cache_entry.preview_payload or cache_entry.full_payload) else None
    return AnalysisJobStatusRead(
        job_id=cache_entry.id,
        report_id=report.id,
        dataset_name=report.dataset_name,
        status=cache_entry.status,
        progress=cache_entry.progress,
        message=cache_entry.progress_message,
        progress_message=cache_entry.progress_message,
        processing_mode=cache_entry.processing_mode,
        file_type=cache_entry.file_type,
        file_size_bytes=cache_entry.file_size_bytes,
        error_message=cache_entry.error_message,
        created_at=cache_entry.created_at,
        updated_at=cache_entry.updated_at,
        started_at=cache_entry.started_at,
        completed_at=cache_entry.completed_at,
        failed_at=cache_entry.failed_at,
        result=result_payload,
    )


@router.get("/reports/{report_id}", response_model=AnalysisReportRead)
def get_report(
    report_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AnalysisReportRead:
    report = get_owned_report(report_id, current_user.id, db)
    return serialize_report(report)


@router.get("/reports/{report_id}/sections/{section_name}", response_model=AnalysisSectionRead)
def get_report_section(
    report_id: str,
    section_name: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AnalysisSectionRead:
    report = get_owned_report(report_id, current_user.id, db)
    try:
        section_data = ensure_report_section(db, report, section_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return AnalysisSectionRead(section=section_name, data=section_data)


@router.get("/reports/{report_id}/rows", response_model=ReportRowsPageRead)
def get_report_rows(
    report_id: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=settings.default_table_page_size, ge=1, le=settings.max_table_page_size),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> ReportRowsPageRead:
    report = get_owned_report(report_id, current_user.id, db)
    return ReportRowsPageRead(**get_report_rows_page(db, report, page=page, page_size=page_size))


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
    report = get_shared_report_record(share_token, db)
    return serialize_report(report)


@router.get("/shared/{share_token}/sections/{section_name}", response_model=AnalysisSectionRead)
def get_shared_report_section(
    share_token: str,
    section_name: str,
    db: Session = Depends(get_db),
) -> AnalysisSectionRead:
    report = get_shared_report_record(share_token, db)
    try:
        section_data = ensure_report_section(db, report, section_name)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return AnalysisSectionRead(section=section_name, data=section_data)


@router.get("/shared/{share_token}/rows", response_model=ReportRowsPageRead)
def get_shared_report_rows(
    share_token: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=settings.default_table_page_size, ge=1, le=settings.max_table_page_size),
    db: Session = Depends(get_db),
) -> ReportRowsPageRead:
    report = get_shared_report_record(share_token, db)
    return ReportRowsPageRead(**get_report_rows_page(db, report, page=page, page_size=page_size))


@router.get("/reports/{report_id}/download-pdf")
def download_report_pdf(
    report_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    report = get_owned_report(report_id, current_user.id, db)
    payload = materialize_report_payload(report, report.cache_link.cache_entry if report.cache_link else None)
    report.report_payload = payload
    pdf_buffer = build_pdf_report(report)
    safe_name = report.dataset_name.lower().replace(" ", "-")
    return StreamingResponse(
        pdf_buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{safe_name}-report.pdf"'},
    )


def get_owned_report(report_id: str, user_id: str, db: Session) -> AnalysisReport:
    report = db.scalar(
        select(AnalysisReport)
        .where(AnalysisReport.id == report_id, AnalysisReport.user_id == user_id)
        .options(selectinload(AnalysisReport.cache_link).selectinload(AnalysisReportCacheLink.cache_entry))
    )
    if not report:
        raise HTTPException(status_code=404, detail="Report not found.")
    return report


def get_shared_report_record(share_token: str, db: Session) -> AnalysisReport:
    report = db.scalar(
        select(AnalysisReport)
        .where(AnalysisReport.share_token == share_token)
        .options(selectinload(AnalysisReport.cache_link).selectinload(AnalysisReportCacheLink.cache_entry))
    )
    if not report:
        raise HTTPException(status_code=404, detail="Shared report not found.")
    return report


def serialize_history_item(report: AnalysisReport) -> AnalysisHistoryItem:
    cache_entry = report.cache_link.cache_entry if report.cache_link else None
    payload = materialize_report_payload(report, cache_entry) if cache_entry else report.report_payload or {}
    overview = payload.get("overview", {})
    return AnalysisHistoryItem(
        id=report.id,
        job_id=cache_entry.id if cache_entry else None,
        job_status_url=f"{settings.api_v1_prefix}/analysis/jobs/{cache_entry.id}" if cache_entry else None,
        dataset_name=report.dataset_name,
        source_type=report.source_type,
        target_column=report.target_column,
        row_count=int(cache_entry.row_count if cache_entry else overview.get("row_count", report.row_count)),
        column_count=int(cache_entry.column_count if cache_entry else overview.get("column_count", report.column_count)),
        status=cache_entry.status if cache_entry else report.status,
        progress=cache_entry.progress if cache_entry else 100,
        progress_message=cache_entry.progress_message if cache_entry else report.notes,
        processing_mode=payload.get("metadata", {}).get("processing_mode"),
        file_type=payload.get("metadata", {}).get("file_type"),
        file_size_bytes=payload.get("metadata", {}).get("file_size_bytes"),
        error_message=cache_entry.error_message if cache_entry else None,
        share_token=report.share_token,
        share_url=build_share_url(report.share_token),
        created_at=report.created_at,
    )


def serialize_report(report: AnalysisReport) -> AnalysisReportRead:
    cache_entry = report.cache_link.cache_entry if report.cache_link else None
    payload = materialize_report_payload(report, cache_entry) if cache_entry else report.report_payload or {}
    history = serialize_history_item(report)
    return AnalysisReportRead(**history.model_dump(), report=payload)


def build_share_url(share_token: str) -> str:
    return f"{settings.report_base_url.rstrip('/')}/share/{share_token}"
