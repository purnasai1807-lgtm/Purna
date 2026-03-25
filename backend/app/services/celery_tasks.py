from __future__ import annotations

from app.services.celery_app import celery_app
from app.services.processing import process_cache_entry


if celery_app is None:  # pragma: no cover - exercised only when celery is unavailable
    process_cache_entry_task = None
else:

    @celery_app.task(name="analytics.process_cache_entry")
    def process_cache_entry_task(cache_entry_id: str) -> str:
        process_cache_entry(cache_entry_id)
        return cache_entry_id
