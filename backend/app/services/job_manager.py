from __future__ import annotations
import logging
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Lock
from sqlalchemy import select
from app.core.config import settings
from app.db.models import AnalysisCacheEntry
from app.db.session import SessionLocal
from app.services.celery_tasks import process_cache_entry_task
from app.services.processing import (
    get_preview_ready_state,
    is_job_stale,
    process_cache_entry,
)
logger = logging.getLogger(__name__)
class AnalyticsJobManager:
    def __init__(self) -> None:
        self._lock = Lock()
        self._executor = (
            None
            if settings.uses_celery_workers and process_cache_entry_task is not None
            else ThreadPoolExecutor(max_workers=settings.background_worker_count)
        )
        self._futures: dict[str, Future[None]] = {}
    def submit(self, cache_entry_id: str) -> str | None:
        if settings.uses_celery_workers and process_cache_entry_task is not None:
            return self._submit_celery(cache_entry_id)
        with self._lock:
            future = self._futures.get(cache_entry_id)
            if future and not future.done():
                logger.info(
                    "Background job already active: cache_entry_id=%s",
                    cache_entry_id,
                )
                return None
            next_future = self._executor.submit(process_cache_entry, cache_entry_id)
            self._futures[cache_entry_id] = next_future
            next_future.add_done_callback(lambda _: self._clear(cache_entry_id))
            logger.info("Background job submitted: cache_entry_id=%s", cache_entry_id)
            return None
    def resume_pending(self) -> int:
        with SessionLocal() as db:
            pending_entries = db.scalars(
                select(AnalysisCacheEntry).where(
                    AnalysisCacheEntry.status.in_(
                        ("queued", "preview_ready", "processing")
                    )
                )
            ).all()
            pending_ids: list[str] = []
            for cache_entry in pending_entries:
                if cache_entry.status == "processing" and not is_job_stale(cache_entry):
                    continue
                if cache_entry.status == "processing" and is_job_stale(cache_entry):
                    cache_entry.status = "preview_ready"
                    cache_entry.progress = max(
                        cache_entry.progress,
                        get_preview_ready_state(cache_entry.processing_mode)[0],
                    )
                    cache_entry.progress_message = (
                        "Resuming analytics after an interrupted background job."
                    )
                    cache_entry.celery_task_id = None
                pending_ids.append(cache_entry.id)
            db.commit()
        for cache_entry_id in pending_ids:
            self.submit(cache_entry_id)
        logger.info("Pending background jobs resumed: count=%s", len(pending_ids))
        return len(pending_ids)
    def shutdown(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=False)
    def _clear(self, cache_entry_id: str) -> None:
        with self._lock:
            self._futures.pop(cache_entry_id, None)
    def _submit_celery(self, cache_entry_id: str) -> str | None:
        with SessionLocal() as db:
            cache_entry = db.get(AnalysisCacheEntry, cache_entry_id)
            if not cache_entry:
                logger.warning(
                    "Cannot submit missing cache entry to Celery: cache_entry_id=%s",
                    cache_entry_id,
                )
                return None
            if cache_entry.status == "completed" and cache_entry.full_payload:
                return cache_entry.celery_task_id
            if (
                cache_entry.celery_task_id
                and cache_entry.status in {"preview_ready", "processing"}
                and not is_job_stale(cache_entry)
            ):
                logger.info(
                    "Reusing active Celery job: cache_entry_id=%s task_id=%s",
                    cache_entry_id,
                    cache_entry.celery_task_id,
                )
                return cache_entry.celery_task_id
            task_result = process_cache_entry_task.apply_async(
                args=[cache_entry_id],
                queue=settings.celery_queue_name,
            )
            cache_entry.celery_task_id = task_result.id
            db.commit()
            logger.info(
                "Celery job submitted: cache_entry_id=%s task_id=%s",
                cache_entry_id,
                task_result.id,
            )
            return task_result.id

job_manager = AnalyticsJobManager()