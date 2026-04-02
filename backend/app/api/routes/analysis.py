from __future__ import annotations
import hashlib
from datetime import datetime, timezone
import logging
from pathlib import Path
from uuid import uuid4
from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import desc, select
from sqlalchemy.orm import Session, selectinload
from app.api.deps import get_current_user
from app.core.config import settings
from app.db.models import (
    AnalysisCacheEntry,
    AnalysisReport,
    AnalysisReportCacheLink,
    AnalysisUploadSession,
    User,
)
from app.db.session import get_db
from app.schemas.analysis import (
    AnalysisHistoryItem,
    AnalysisJobStatusRead,
    AnalysisUploadCompleteRequest,
    AnalysisUploadSessionCreateRequest,
    AnalysisUploadSessionCreateResponse,
    AnalysisUploadSessionRead,
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
from app.services.processing import is_job_stale
from app.db.session import SessionLocal
from fastapi import HTTPException
import logging
from datetime import datetime, timezone
from app.services.reporting import build_pdf_report
from app.services.storage import (
    abort_multipart_storage_upload,
    build_materialized_storage_path,
    build_presigned_upload_session,
    build_upload_session_storage_key,
    classify_file_size,
    complete_multipart_storage_upload,
    create_stored_upload_from_existing_storage,
    delete_storage_artifacts,
    delete_stored_upload,
    get_storage_object_metadata,
    infer_file_type,
    save_upload_to_storage,
    storage_object_exists,
    uses_s3_storage,
    validate_upload_content_type,
)

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
    if uses_s3_storage():
        raise HTTPException(
            status_code=400,
            detail=(
                "Direct backend multipart uploads are disabled in production. "
                "Use the upload-session API with direct-to-storage uploads."
            ),
        )

    normalized_target = normalize_column_name(target_column) if target_column else None
    stored_upload = None

    try:
        stored_upload = await save_upload_to_storage(file)
        resolved_dataset_name = dataset_name or Path(stored_upload.original_filename).stem or "Dataset"
        report = create_or_attach_upload_report(
            db=db,
            current_user=current_user,
            stored_upload=stored_upload,
            dataset_name=resolved_dataset_name,
            target_column=normalized_target,
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


@router.post("/uploads/session", response_model=AnalysisUploadSessionCreateResponse)
def create_upload_session(
    request: AnalysisUploadSessionCreateRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AnalysisUploadSessionCreateResponse:
    if not uses_s3_storage():
        raise HTTPException(
            status_code=400,
            detail=(
                "Direct-to-storage upload sessions require S3-compatible storage. "
                "Use the direct backend upload endpoint for local development."
            ),
        )

    try:
        file_type, _ = infer_file_type(request.filename)
        validate_upload_content_type(file_type, request.content_type)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if request.file_size_bytes > settings.max_upload_size_bytes:
        raise HTTPException(
            status_code=413,
            detail=(
                f"Large file {settings.max_upload_size_mb}MB+ (limit 500MB). "
                "Upload accepted - poll session status."
            ),
            headers={"Retry-After": "60"},
        )

    resolved_dataset_name = request.dataset_name or Path(request.filename).stem or "Dataset"
    normalized_target = normalize_column_name(request.target_column) if request.target_column else None
    upload_id = str(uuid4())
    storage_key = build_upload_session_storage_key(upload_id, request.filename)

    try:
        upload_instructions = build_presigned_upload_session(
            storage_key=storage_key,
            content_type=request.content_type,
            file_size_bytes=request.file_size_bytes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Upload session creation failed: filename=%s", request.filename)
        raise HTTPException(status_code=500, detail=f"Could not prepare secure upload: {exc}") from exc

    upload_session = AnalysisUploadSession(
        id=upload_id,
        user_id=current_user.id,
        dataset_name=resolved_dataset_name,
        target_column=normalized_target,
        original_filename=request.filename,
        content_type=request.content_type,
        file_size_bytes=request.file_size_bytes,
        processing_mode=classify_file_size(request.file_size_bytes, file_type),
        storage_backend="s3",
        storage_key=storage_key,
        s3_upload_id=upload_instructions["multipart_upload_id"],
        upload_strategy=upload_instructions["upload_strategy"],
        status="created",
        expires_at=upload_instructions["expires_at"],
    )
    db.add(upload_session)
    db.commit()
    db.refresh(upload_session)

    logger.info(
        "Upload session created: upload_id=%s user_id=%s filename=%s size_bytes=%s strategy=%s",
        upload_session.id,
        current_user.id,
        upload_session.original_filename,
        upload_session.file_size_bytes,
        upload_session.upload_strategy,
    )

    return AnalysisUploadSessionCreateResponse(
        upload_id=upload_session.id,
        upload_strategy=upload_session.upload_strategy,
        storage_backend=upload_session.storage_backend,
        storage_key=upload_session.storage_key,
        processing_mode=upload_session.processing_mode,
        expires_at=upload_session.expires_at,
        chunk_size_bytes=upload_instructions["chunk_size_bytes"],
        single_part_url=upload_instructions["single_part_url"],
        single_part_headers=upload_instructions["single_part_headers"],
        multipart_upload_id=upload_instructions["multipart_upload_id"],
        multipart_parts=upload_instructions["multipart_parts"],
    )


@router.post("/uploads/{upload_id}/complete", response_model=AnalysisReportRead)
def complete_upload_session(
    upload_id: str,
    request: AnalysisUploadCompleteRequest | None = Body(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AnalysisReportRead:
    upload_session = get_owned_upload_session(upload_id, current_user.id, db)
    ensure_upload_session_not_expired(upload_session, db)

    existing_report = get_report_for_upload_session(upload_session, current_user.id, db)
    if existing_report:
        sync_upload_session_from_report(upload_session, existing_report)
        db.commit()
        return serialize_report(existing_report)

    try:
        if upload_session.upload_strategy == "multipart" and upload_session.s3_upload_id:
            if upload_session.status in {"created", "uploading"}:
                completed_parts = [
                    {"PartNumber": part.part_number, "ETag": part.etag}
                    for part in (request.parts if request else [])
                ]
                if not completed_parts:
                    raise HTTPException(
                        status_code=400,
                        detail="Multipart upload completion requires uploaded part ETags.",
                    )
                complete_multipart_storage_upload(
                    storage_key=upload_session.storage_key,
                    upload_id=upload_session.s3_upload_id,
                    parts=completed_parts,
                )
                upload_session.status = "uploaded"
                upload_session.error_message = None
                db.commit()

        if not storage_object_exists(upload_session.storage_key, expected_size=upload_session.file_size_bytes):
            raise HTTPException(
                status_code=400,
                detail="Upload not found in storage yet. Please finish the file upload and try again.",
            )

        upload_session.status = "finalizing"
        upload_session.error_message = None
        db.commit()

        if upload_session.storage_backend == "s3":
            report = create_or_attach_deferred_upload_report(
                db=db,
                current_user=current_user,
                upload_session=upload_session,
            )
        else:
            stored_upload = create_stored_upload_from_existing_storage(
                original_filename=upload_session.original_filename,
                content_type=upload_session.content_type,
                storage_backend=upload_session.storage_backend,
                storage_key=upload_session.storage_key,
                storage_path=build_materialized_storage_path(upload_session.storage_key, upload_session.original_filename),
                file_size_bytes=upload_session.file_size_bytes,
            )
            report = create_or_attach_upload_report(
                db=db,
                current_user=current_user,
                stored_upload=stored_upload,
                dataset_name=upload_session.dataset_name,
                target_column=upload_session.target_column,
            )
        report = get_owned_report(report.id, current_user.id, db)
        sync_upload_session_from_report(upload_session, report)
        db.commit()
        db.refresh(upload_session)
        logger.info(
            "Upload session finalized: upload_id=%s report_id=%s job_id=%s status=%s",
            upload_session.id,
            upload_session.report_id,
            upload_session.job_id,
            upload_session.status,
        )
        return serialize_report(report)
    except HTTPException:
        db.rollback()
        raise
    except ValueError as exc:
        db.rollback()
        upload_session = get_owned_upload_session(upload_id, current_user.id, db)
        upload_session.status = "failed"
        upload_session.error_message = str(exc)
        db.commit()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        upload_session = get_owned_upload_session(upload_id, current_user.id, db)
        upload_session.status = "failed"
        upload_session.error_message = str(exc)
        db.commit()
        logger.exception("Upload session finalize failed: upload_id=%s", upload_id)
        raise HTTPException(status_code=500, detail=f"Analysis finalization failed: {exc}") from exc


@router.get("/uploads/{upload_id}", response_model=AnalysisUploadSessionRead)
def get_upload_session(
    upload_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> AnalysisUploadSessionRead:
    upload_session = get_owned_upload_session(upload_id, current_user.id, db)
    ensure_upload_session_not_expired(upload_session, db)
    report = get_report_for_upload_session(upload_session, current_user.id, db)
    if report:
        sync_upload_session_from_report(upload_session, report)
        db.commit()
        db.refresh(upload_session)
    return serialize_upload_session(upload_session, report)


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
    if cache_entry.status == "processing" and is_job_stale(cache_entry):
        raise HTTPException(
            status_code=503,
            detail="Analytics service temporarily busy processing large dataset. Retry in 30s.",
            headers={"Retry-After": "30"}
        )

    result_payload = materialize_report_payload(report, cache_entry) if (cache_entry.preview_payload or cache_entry.full_payload) else None
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
def create_or_attach_upload_report(
    *,
    db: Session,
    current_user: User,
    stored_upload,
    dataset_name: str,
    target_column: str | None,
) -> AnalysisReport:
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
        target_column=target_column,
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
                dataset_name=dataset_name,
                target_column=target_column,
            )
            if stored_upload.processing_mode == "small"
            else build_preview_payload(
                stored_upload=stored_upload,
                dataset_name=dataset_name,
                target_column=target_column,
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
            dataset_name=dataset_name,
            target_column=target_column,
            cache_entry=cache_entry,
        )
        db.commit()
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
        return get_owned_report(report.id, current_user.id, db)

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
            dataset_name=dataset_name,
            target_column=target_column,
            cache_entry=cache_entry,
        )
        db.commit()
        if cache_entry.status in {"queued", "preview_ready", "processing"}:
            job_manager.submit(cache_entry.id)
        logger.info(
            "Upload report attached to existing cache: report_id=%s cache_entry_id=%s status=%s",
            report.id,
            cache_entry.id,
            cache_entry.status,
        )
        return get_owned_report(report.id, current_user.id, db)

    preview_payload = (
        build_small_file_payload(
            stored_upload=stored_upload,
            dataset_name=dataset_name,
            target_column=target_column,
        )
        if stored_upload.processing_mode == "small"
        else build_preview_payload(
            stored_upload=stored_upload,
            dataset_name=dataset_name,
            target_column=target_column,
        )
    )
    cache_entry = create_cache_entry(
        db,
        stored_upload=stored_upload,
        target_column=target_column,
        preview_payload=preview_payload,
    )
    report = create_upload_report(
        db,
        current_user=current_user,
        dataset_name=dataset_name,
        target_column=target_column,
        cache_entry=cache_entry,
    )
    db.commit()

    if stored_upload.processing_mode != "small":
        job_manager.submit(cache_entry.id)

    logger.info(
        "Upload report created: report_id=%s cache_entry_id=%s status=%s mode=%s",
        report.id,
        cache_entry.id,
        cache_entry.status,
        stored_upload.processing_mode,
    )
    return get_owned_report(report.id, current_user.id, db)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def build_upload_placeholder_payload(
    *,
    dataset_name: str,
    target_column: str | None,
    processing_mode: str,
    file_type: str,
    file_size_bytes: int,
    storage_backend: str,
) -> dict:
    return {
        "dataset_name": dataset_name,
        "source_type": "upload",
        "target_column": target_column,
        "overview": {
            "row_count": 0,
            "column_count": 0,
            "original_row_count": 0,
            "original_column_count": 0,
            "target_column": target_column,
            "preview_rows": [],
            "columns": [],
            "detected_data_types": {},
        },
        "cleaning": {
            "original_shape": {"rows": 0, "columns": 0},
            "cleaned_shape": {"rows": 0, "columns": 0},
            "column_mapping": {},
            "removed_all_null_columns": [],
            "empty_rows_dropped": 0,
            "duplicate_rows_removed": 0,
            "missing_values_before": 0,
            "missing_values_after": 0,
            "detected_data_types": {},
        },
        "summary_statistics": [],
        "correlations": {
            "available": False,
            "columns": [],
            "matrix": [],
            "strongest_pairs": [],
        },
        "outliers": [],
        "trends": [],
        "charts": [],
        "modeling": {
            "status": "pending",
            "mode": processing_mode,
            "target_column": target_column,
            "suggestions": [],
            "reason": "Secure upload stored. Generating preview analytics now.",
        },
        "insights": [],
        "recommendations": [],
        "metadata": {
            "is_preview": True,
            "processing_mode": processing_mode,
            "file_type": file_type,
            "file_size_bytes": file_size_bytes,
            "sample_row_count": 0,
            "optimized_mode": processing_mode == "large",
            "processing_strategy": (
                "direct" if processing_mode == "small" else "chunked" if processing_mode == "medium" else "optimized_background"
            ),
            "sample_strategy": "deferred",
            "max_upload_size_bytes": settings.max_upload_size_bytes,
            "storage_backend": storage_backend,
        },
        "sections": build_section_status(charts_ready=False),
    }


def build_upload_session_content_hash(upload_session: AnalysisUploadSession) -> str:
    object_metadata = (
        get_storage_object_metadata(upload_session.storage_key)
        if upload_session.storage_backend == "s3" and upload_session.storage_key
        else None
    )
    etag = str((object_metadata or {}).get("ETag") or "").strip('"')
    content_length = int((object_metadata or {}).get("ContentLength") or upload_session.file_size_bytes or 0)
    last_modified = (object_metadata or {}).get("LastModified")
    last_modified_value = ""
    if hasattr(last_modified, "timestamp"):
        last_modified_value = str(int(last_modified.timestamp()))
    extension = Path(upload_session.original_filename).suffix.lower()
    fingerprint_source = "|".join(
        [
            upload_session.storage_backend,
            etag,
            str(content_length),
            last_modified_value,
            extension,
            upload_session.storage_key if not etag else "",
        ]
    )
    return hashlib.sha256(fingerprint_source.encode("utf-8")).hexdigest()


def create_or_attach_deferred_upload_report(
    *,
    db: Session,
    current_user: User,
    upload_session: AnalysisUploadSession,
) -> AnalysisReport:
    content_hash = build_upload_session_content_hash(upload_session)
    existing_cache_entry = find_cache_entry(
        db,
        content_hash=content_hash,
        target_column=upload_session.target_column,
    )
    if existing_cache_entry:
        report = attach_report_to_existing_cache(
            db,
            current_user=current_user,
            dataset_name=upload_session.dataset_name,
            target_column=upload_session.target_column,
            cache_entry=existing_cache_entry,
        )
        db.flush()
        if (
            upload_session.storage_backend == "s3"
            and upload_session.storage_key
            and upload_session.storage_key != existing_cache_entry.storage_key
        ):
            delete_storage_artifacts(
                storage_path=None,
                storage_backend=upload_session.storage_backend,
                storage_key=upload_session.storage_key,
            )
        if existing_cache_entry.status in {"queued", "preview_ready", "processing"}:
            job_manager.submit(existing_cache_entry.id)
        return get_owned_report(report.id, current_user.id, db)

    file_type, _ = infer_file_type(upload_session.original_filename)
    cache_entry = AnalysisCacheEntry(
        content_hash=content_hash,
        original_filename=upload_session.original_filename,
        file_type=file_type,
        target_column=upload_session.target_column,
        processing_mode=upload_session.processing_mode,
        status="processing",
        progress=18 if upload_session.processing_mode == "small" else 24 if upload_session.processing_mode == "medium" else 20,
        progress_message="Secure upload stored. Generating preview analytics now.",
        file_size_bytes=upload_session.file_size_bytes,
        row_count=0,
        column_count=0,
        storage_backend=upload_session.storage_backend,
        storage_key=upload_session.storage_key,
        storage_path=str(
            build_materialized_storage_path(upload_session.storage_key, upload_session.original_filename)
        ),
        preview_payload=build_upload_placeholder_payload(
            dataset_name=upload_session.dataset_name,
            target_column=upload_session.target_column,
            processing_mode=upload_session.processing_mode,
            file_type=file_type,
            file_size_bytes=upload_session.file_size_bytes,
            storage_backend=upload_session.storage_backend,
        ),
        full_payload=None,
        sections_ready=build_section_status(charts_ready=False),
        error_message=None,
    )
    db.add(cache_entry)
    db.flush()
    report = create_upload_report(
        db,
        current_user=current_user,
        dataset_name=upload_session.dataset_name,
        target_column=upload_session.target_column,
        cache_entry=cache_entry,
    )
    db.flush()
    job_manager.submit(cache_entry.id)
    return get_owned_report(report.id, current_user.id, db)


def get_owned_upload_session(upload_id: str, user_id: str, db: Session) -> AnalysisUploadSession:
    upload_session = db.scalar(
        select(AnalysisUploadSession).where(
            AnalysisUploadSession.id == upload_id,
            AnalysisUploadSession.user_id == user_id,
        )
    )
    if not upload_session:
        raise HTTPException(status_code=404, detail="Upload session not found.")
    return upload_session


def get_report_for_upload_session(
    upload_session: AnalysisUploadSession,
    user_id: str,
    db: Session,
) -> AnalysisReport | None:
    if not upload_session.report_id:
        return None
    return db.scalar(
        select(AnalysisReport)
        .where(AnalysisReport.id == upload_session.report_id, AnalysisReport.user_id == user_id)
        .options(selectinload(AnalysisReport.cache_link).selectinload(AnalysisReportCacheLink.cache_entry))
    )
def ensure_upload_session_not_expired(upload_session: AnalysisUploadSession, db: Session) -> None:
    if upload_session.report_id or not upload_session.expires_at:
        return
    if upload_session.expires_at >= utcnow():
        return

    upload_session.status = "expired"
    upload_session.error_message = "Upload session expired. Please upload the file again."
    if upload_session.upload_strategy == "multipart" and upload_session.s3_upload_id:
        abort_multipart_storage_upload(
            storage_key=upload_session.storage_key,
            upload_id=upload_session.s3_upload_id,
        )
    delete_storage_artifacts(
        storage_path=None,
        storage_backend=upload_session.storage_backend,
        storage_key=upload_session.storage_key,
    )
    db.commit()
    raise HTTPException(status_code=410, detail=upload_session.error_message)
def derive_upload_session_status(cache_status: str) -> str:
    if cache_status == "completed":
        return "completed"
    if cache_status == "failed":
        return "failed"
    if cache_status == "preview_ready":
        return "preview_ready"
    return "processing"
def sync_upload_session_from_report(upload_session: AnalysisUploadSession, report: AnalysisReport) -> None:
    cache_entry = report.cache_link.cache_entry if report.cache_link else None
    upload_session.report_id = report.id
    upload_session.job_id = cache_entry.id if cache_entry else upload_session.job_id
    upload_session.error_message = cache_entry.error_message if cache_entry else upload_session.error_message
    if cache_entry:
        upload_session.status = derive_upload_session_status(cache_entry.status)
    elif report.status == "completed":
        upload_session.status = "completed"
def serialize_upload_session(
    upload_session: AnalysisUploadSession,
    report: AnalysisReport | None = None,
) -> AnalysisUploadSessionRead:
    cache_entry = report.cache_link.cache_entry if report and report.cache_link else None
    progress = cache_entry.progress if cache_entry else get_upload_session_progress(upload_session.status)
    message = cache_entry.progress_message if cache_entry else get_upload_session_message(upload_session.status)
    return AnalysisUploadSessionRead(
        upload_id=upload_session.id,
        dataset_name=upload_session.dataset_name,
        target_column=upload_session.target_column,
        original_filename=upload_session.original_filename,
        content_type=upload_session.content_type,
        file_size_bytes=upload_session.file_size_bytes,
        processing_mode=upload_session.processing_mode,
        upload_strategy=upload_session.upload_strategy,
        storage_backend=upload_session.storage_backend,
        storage_key=upload_session.storage_key,
        status=upload_session.status,
        progress=progress,
        message=message,
        progress_message=message,
        error_message=upload_session.error_message,
        report_id=upload_session.report_id,
        job_id=upload_session.job_id,
        created_at=upload_session.created_at,
        updated_at=upload_session.updated_at,
        expires_at=upload_session.expires_at,
        report=serialize_report(report) if report else None,
    )
def get_upload_session_progress(status: str) -> int:
    progress_map = {
        "created": 5,
        "uploading": 15,
        "uploaded": 70,
        "finalizing": 80,
        "preview_ready": 90,
        "processing": 92,
        "completed": 100,
        "failed": 100,
        "expired": 100,
    }
    return progress_map.get(status, 0)
def get_upload_session_message(status: str) -> str:
    message_map = {
        "created": "Preparing secure upload.",
        "uploading": "Uploading to secure storage.",
        "uploaded": "Upload complete. Finalizing analysis.",
        "finalizing": "Upload complete. Finalizing analysis.",
        "preview_ready": "Quick preview analytics are ready. Full analytics continue in the background.",
        "processing": "Large dataset detected. Processing in background.",
        "completed": "Analysis ready.",
        "failed": "Upload finalization failed.",
        "expired": "Upload session expired. Please upload the file again.",
    }
    return message_map.get(status, "Preparing secure upload.")
def build_share_url(share_token: str) -> str:
    return f"{settings.report_base_url.rstrip('/')}/share/{share_token}"
