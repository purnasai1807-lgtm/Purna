from __future__ import annotations

from app.core.config import settings

try:
    from celery import Celery
except ImportError:  # pragma: no cover - optional until dependencies are installed
    Celery = None


if Celery is None:  # pragma: no cover - exercised only when celery is unavailable
    celery_app = None
else:
    celery_app = Celery(
        "auto_analytics_ai",
        broker=settings.celery_broker_url,
        backend=settings.celery_result_backend or settings.celery_broker_url,
        include=["app.services.celery_tasks"],
    )
    celery_app.conf.update(
        task_default_queue=settings.celery_queue_name,
        task_track_started=True,
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        broker_connection_retry_on_startup=True,
        worker_prefetch_multiplier=1,
        beat_schedule={
            "cleanup-expired-upload-sessions": {
                "task": "analytics.cleanup_upload_sessions",
                "schedule": settings.upload_cleanup_interval_minutes * 60,
            }
        },
    )
