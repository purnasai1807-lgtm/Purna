from __future__ import annotations
from datetime import datetime, timezone
from sqlalchemy import select
from app.db.models import AnalysisUploadSession
from app.db.session import SessionLocal
from app.services.storage import abort_multipart_storage_upload, delete_storage_artifacts
def utcnow() -> datetime:
    return datetime.now(timezone.utc)
def cleanup_expired_upload_sessions() -> int:
    expired_count = 0
    with SessionLocal() as db:
        sessions = db.scalars(
            select(AnalysisUploadSession).where(
                AnalysisUploadSession.report_id.is_(None),
                AnalysisUploadSession.expires_at.is_not(None),
                AnalysisUploadSession.expires_at < utcnow(),
                AnalysisUploadSession.status.notin_(("completed", "processing", "preview_ready", "expired")),
            )
        ).all()
        for upload_session in sessions:
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
            upload_session.status = "expired"
            upload_session.error_message = "Upload session expired. Please upload the file again."
            expired_count += 1
        db.commit()
    return expired_count