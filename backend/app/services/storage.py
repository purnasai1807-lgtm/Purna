from __future__ import annotations

import hashlib
import logging
import secrets
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal

from fastapi import UploadFile

from app.core.config import settings

try:
    import boto3
    from boto3.s3.transfer import TransferConfig
    from botocore.config import Config as BotoConfig
except ImportError:  # pragma: no cover - optional until dependencies are installed
    boto3 = None
    TransferConfig = None
    BotoConfig = None

logger = logging.getLogger(__name__)

SupportedFileType = Literal["csv", "excel", "json"]
GENERIC_BINARY_CONTENT_TYPES = {"application/octet-stream", "binary/octet-stream"}
ALLOWED_CONTENT_TYPES: dict[SupportedFileType, set[str]] = {
    "csv": {
        "text/csv",
        "application/csv",
        "text/plain",
        *GENERIC_BINARY_CONTENT_TYPES,
    },
    "excel": {
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel.sheet.macroenabled.12",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.template",
        "application/vnd.ms-excel.template.macroenabled.12",
        *GENERIC_BINARY_CONTENT_TYPES,
    },
    "json": {
        "application/json",
        "text/json",
        "text/plain",
        *GENERIC_BINARY_CONTENT_TYPES,
    },
}


@dataclass(slots=True)
class StorageDirectories:
    root: Path
    uploads: Path
    parquet: Path


@dataclass(slots=True)
class StoredUpload:
    original_filename: str
    file_type: SupportedFileType
    extension: str
    storage_path: Path
    storage_backend: str
    storage_key: str | None
    content_hash: str
    file_size_bytes: int
    processing_mode: str


def ensure_storage_directories() -> StorageDirectories:
    root = settings.resolved_storage_root
    uploads = root / "uploads"
    parquet = root / "parquet"
    uploads.mkdir(parents=True, exist_ok=True)
    parquet.mkdir(parents=True, exist_ok=True)
    return StorageDirectories(root=root, uploads=uploads, parquet=parquet)


def infer_file_type(filename: str) -> tuple[SupportedFileType, str]:
    extension = Path(filename).suffix.lower()
    if extension == ".csv":
        return "csv", extension
    if extension in {".xlsx", ".xlsm", ".xltx", ".xltm", ".xls"}:
        return "excel", extension
    if extension == ".json":
        return "json", extension
    raise ValueError("Unsupported file type. Please upload a CSV, Excel, or JSON file.")


def validate_upload_content_type(file_type: SupportedFileType, content_type: str | None) -> None:
    if not content_type:
        return

    normalized = content_type.strip().lower()
    if not normalized:
        return

    allowed_types = ALLOWED_CONTENT_TYPES[file_type]
    if normalized in allowed_types:
        return

    raise ValueError(
        f"Unsupported content type '{content_type}' for this file. Please upload a valid CSV, Excel, or JSON file."
    )


def classify_file_size(file_size_bytes: int, file_type: SupportedFileType) -> str:
    megabytes = file_size_bytes / (1024 * 1024)
    if file_type == "excel":
        if megabytes <= settings.small_excel_threshold_mb:
            return "small"
        if megabytes <= settings.medium_excel_threshold_mb:
            return "medium"
        return "large"

    if megabytes <= settings.small_file_threshold_mb:
        return "small"
    if megabytes <= settings.medium_file_threshold_mb:
        return "medium"
    return "large"


def build_parquet_path(content_hash: str) -> Path:
    directories = ensure_storage_directories()
    return directories.parquet / f"{content_hash}.parquet"


def uses_s3_storage() -> bool:
    return settings.uses_s3_storage


@lru_cache(maxsize=1)
def get_s3_client():
    if not uses_s3_storage():
        return None
    if boto3 is None:
        raise RuntimeError("S3 storage is configured, but boto3 is not installed.")

    session = boto3.session.Session()
    config = None
    if BotoConfig is not None and settings.s3_force_path_style:
        config = BotoConfig(s3={"addressing_style": "path"})
    return session.client(
        "s3",
        region_name=settings.s3_region,
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        use_ssl=settings.s3_use_ssl,
        config=config,
    )


def build_storage_key(unique_name: str) -> str:
    prefix = settings.s3_prefix.strip("/")
    return f"{prefix}/{unique_name}" if prefix else unique_name


def upload_local_file_to_object_storage(
    local_path: Path,
    storage_key: str,
    *,
    content_type: str | None,
) -> None:
    client = get_s3_client()
    if client is None:
        return
    if TransferConfig is None:
        raise RuntimeError("S3 storage is configured, but boto3 transfer support is unavailable.")

    transfer_config = TransferConfig(
        multipart_threshold=max(5 * 1024 * 1024, settings.upload_chunk_size_bytes),
        multipart_chunksize=max(5 * 1024 * 1024, settings.upload_chunk_size_bytes),
    )
    extra_args = {"ContentType": content_type} if content_type else None
    client.upload_file(
        str(local_path),
        settings.s3_bucket_name,
        storage_key,
        ExtraArgs=extra_args,
        Config=transfer_config,
    )


def download_object_storage_file(storage_key: str, destination: Path) -> Path:
    client = get_s3_client()
    if client is None:
        raise RuntimeError("Object storage is not configured.")

    destination.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(settings.s3_bucket_name, storage_key, str(destination))
    return destination


def delete_object_storage_file(storage_key: str | None) -> None:
    if not storage_key or not uses_s3_storage():
        return

    client = get_s3_client()
    if client is None:
        return

    try:
        client.delete_object(Bucket=settings.s3_bucket_name, Key=storage_key)
    except Exception:
        logger.warning("Could not delete object storage file: key=%s", storage_key, exc_info=True)


def delete_storage_artifacts(
    *,
    storage_path: str | Path | None,
    storage_backend: str,
    storage_key: str | None,
) -> None:
    delete_file_if_exists(storage_path)
    if storage_backend == "s3":
        delete_object_storage_file(storage_key)


def delete_stored_upload(upload: StoredUpload | None) -> None:
    if upload is None:
        return

    delete_storage_artifacts(
        storage_path=upload.storage_path,
        storage_backend=upload.storage_backend,
        storage_key=upload.storage_key,
    )


def ensure_local_storage_copy(
    *,
    storage_path: str | Path,
    storage_backend: str,
    storage_key: str | None,
) -> Path:
    local_path = Path(storage_path)
    if local_path.exists():
        return local_path

    if storage_backend == "s3" and storage_key:
        return download_object_storage_file(storage_key, local_path)

    raise FileNotFoundError(f"Stored dataset source is missing: {local_path}")


async def save_upload_to_storage(upload: UploadFile) -> StoredUpload:
    filename = upload.filename or ""
    if not filename:
        raise ValueError("Please select a CSV, Excel, or JSON file.")

    file_type, extension = infer_file_type(filename)
    validate_upload_content_type(file_type, upload.content_type)
    directories = ensure_storage_directories()
    safe_stem = sanitize_file_stem(Path(filename).stem)
    unique_name = f"{safe_stem}-{secrets.token_hex(8)}{extension}"
    destination = directories.uploads / unique_name
    storage_key = build_storage_key(unique_name) if uses_s3_storage() else None

    digest = hashlib.sha256()
    file_size_bytes = 0

    try:
        with destination.open("wb") as output_stream:
            while True:
                chunk = await upload.read(settings.upload_chunk_size_bytes)
                if not chunk:
                    break
                file_size_bytes += len(chunk)
                if file_size_bytes > settings.max_upload_size_bytes:
                    delete_file_if_exists(destination)
                    raise ValueError(
                        f"Upload exceeds the {settings.max_upload_size_mb} MB limit. "
                        "Please upload a smaller file."
                    )
                digest.update(chunk)
                output_stream.write(chunk)

        if storage_key:
            try:
                upload_local_file_to_object_storage(destination, storage_key, content_type=upload.content_type)
            except Exception:
                delete_file_if_exists(destination)
                raise
    finally:
        await upload.close()

    if file_size_bytes == 0:
        delete_file_if_exists(destination)
        raise ValueError("The uploaded file is empty.")

    return StoredUpload(
        original_filename=filename,
        file_type=file_type,
        extension=extension,
        storage_path=destination.resolve(),
        storage_backend="s3" if storage_key else "local",
        storage_key=storage_key,
        content_hash=digest.hexdigest(),
        file_size_bytes=file_size_bytes,
        processing_mode=classify_file_size(file_size_bytes, file_type),
    )


def sanitize_file_stem(value: str) -> str:
    sanitized = "".join(character if character.isalnum() else "-" for character in value.strip())
    sanitized = "-".join(part for part in sanitized.split("-") if part)
    return sanitized.lower()[:80] or "dataset"


def delete_file_if_exists(path: str | Path | None) -> None:
    if not path:
        return
    try:
        Path(path).unlink(missing_ok=True)
    except OSError:
        return
