import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded
    from slowapi.util import get_remote_address
except ImportError:
    Limiter = None
    RateLimitExceeded = None
    _rate_limit_exceeded_handler = None
    get_remote_address = None
from app.api.routes.analysis import router as analysis_router
from app.api.routes.auth import router as auth_router
from app.api.routes.health import router as health_router
from app.core.config import settings
from app.db.session import init_db
from app.services.job_manager import job_manager
from app.services.storage import ensure_storage_directories
logger = logging.getLogger(__name__)
@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    ensure_storage_directories()
    resumed_jobs = job_manager.resume_pending()
    if settings.background_job_backend.strip().lower() == "celery" and not settings.celery_broker_url:
        logger.warning("Celery background mode requested without CELERY_BROKER_URL. Falling back to in-process jobs.")
    if settings.uses_celery_workers and settings.app_env != "development" and not settings.uses_s3_storage:
        logger.warning(
            "Celery workers are enabled without S3-compatible storage. Configure STORAGE_BACKEND=s3 and S3_* env vars for distributed uploads."
        )
    logger.info(
        "API startup complete: env=%s storage_root=%s storage_backend=%s background_backend=%s report_base_url=%s resumed_jobs=%s workers=%s stale_min=%s",
        settings.app_env,
        settings.resolved_storage_root,
        settings.storage_backend,
        settings.background_job_backend,
        settings.report_base_url,
        resumed_jobs,
        settings.background_worker_count,
        settings.job_stale_after_minutes,
    )
    if resumed_jobs == 0:
        logger.info("No pending jobs found at startup. Background processing ready.")
    yield
    job_manager.shutdown()
    logger.info("API shutdown complete.")
app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    lifespan=lifespan,
    description="Production-ready analytics API for Auto Analytics AI.",
)
if Limiter is not None and get_remote_address is not None:
    limiter = Limiter(key_func=get_remote_address)
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
else:
    logger.warning("slowapi is not installed; rate limiting is disabled.")
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_origin_regex=settings.cors_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(health_router)
app.include_router(auth_router, prefix=settings.api_v1_prefix)
app.include_router(analysis_router, prefix=settings.api_v1_prefix)
@app.get("/")
def read_root() -> dict[str, str]:
    return {
        "message": "Auto Analytics AI API is running.",
        "docs": "/docs",
    }