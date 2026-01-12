"""
FastAPI application and route handlers.
"""
from fastapi import FastAPI, UploadFile, File, HTTPException, status
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import uuid
from datetime import datetime
import logging

from app.config import get_settings
from app.models import (
    FileUploadResponse,
    JobStatusResponse,
    HealthCheckResponse,
    JobStatus
)
from app.database import db
from app.cache import cache
from app.storage import storage
from app.queue import queue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting API...")

    try:
        storage.ensure_base_path()
        await db.connect()
        await cache.connect()
        await queue.connect()
        logger.info("Services initialized")
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        raise

    yield

    logger.info("Shutting down...")
    try:
        await db.disconnect()
        await cache.disconnect()
        await queue.disconnect()
    except Exception as e:
        logger.error(f"Shutdown error: {e}")


# Create FastAPI application
app = FastAPI(
    title="File Scanner API",
    description="Microservices-based file scanning system for counting A-Z characters",
    version="1.0.0",
    lifespan=lifespan
)


@app.post(
    "/api/v1/files",
    response_model=FileUploadResponse,
    status_code=status.HTTP_200_OK,
    summary="Upload file for scanning",
    description="Upload a text file and get a job_id for tracking scan progress"
)
async def upload_file(file: UploadFile = File(...)):
    """Upload file for character counting with deduplication."""
    try:
        if not file.filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No file provided"
            )

        if not file.file:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File does not exist or is empty"
            )

        if not file.content_type or not file.content_type.startswith('text/'):
            logger.warning(f"Non-text file uploaded: {file.content_type}")

        can_upload, storage_msg = storage.should_accept_upload()
        if not can_upload:
            raise HTTPException(
                status_code=status.HTTP_507_INSUFFICIENT_STORAGE,
                detail=storage_msg
            )

        content_length = 0
        if hasattr(file, 'size') and file.size:
            content_length = file.size

        if content_length > settings.max_upload_size:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File too large. Maximum size: {settings.max_file_size_mb}MB"
            )

        file_hash, file_size, file_path = await storage.calculate_hash_and_save(
            file.file,
            file.filename
        )

        cached_results = await cache.get_file_results(file_hash)
        if cached_results and cached_results.get("results"):
            job_id = uuid.uuid4()
            await db.create_job(
                job_id=job_id,
                file_hash=file_hash,
                original_filename=file.filename,
                file_size=file_size,
                status=JobStatus.COMPLETED,
                results=cached_results["results"]
            )

            return FileUploadResponse(
                job_id=job_id,
                status=JobStatus.COMPLETED,
                message="File already scanned, instant results from cache",
                deduplication=True,
                results=cached_results["results"]
            )

        existing_job = await db.get_completed_job_by_hash(file_hash)
        if existing_job and existing_job.get("results"):
            await cache.set_file_results(
                file_hash=file_hash,
                results=existing_job["results"],
                file_size=file_size
            )

            job_id = uuid.uuid4()
            await db.create_job(
                job_id=job_id,
                file_hash=file_hash,
                original_filename=file.filename,
                file_size=file_size,
                status=JobStatus.COMPLETED,
                results=existing_job["results"]
            )

            return FileUploadResponse(
                job_id=job_id,
                status=JobStatus.COMPLETED,
                message="File already scanned, instant results from database",
                deduplication=True,
                results=existing_job["results"]
            )

        job_id = uuid.uuid4()
        job_data = await db.create_job(
            job_id=job_id,
            file_hash=file_hash,
            original_filename=file.filename,
            file_size=file_size,
            status=JobStatus.PENDING
        )

        await cache.set_job_status(job_id, job_data)
        await queue.publish_job(
            job_id=job_id,
            file_hash=file_hash,
            file_path=file_path,
            file_size=file_size
        )

        return FileUploadResponse(
            job_id=job_id,
            status=JobStatus.PENDING,
            message="File uploaded successfully, processing started",
            deduplication=False
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading file: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@app.get(
    "/api/v1/files/{job_id}",
    response_model=JobStatusResponse,
    status_code=status.HTTP_200_OK,
    summary="Get job status",
    description="Query the status and results of a file scan job"
)
async def get_job_status(job_id: uuid.UUID):
    """Get job status and results."""
    try:
        cached_data = await cache.get_job_status(job_id)
        if cached_data:
            return JobStatusResponse(
                job_id=uuid.UUID(cached_data["job_id"]),
                status=JobStatus(cached_data["status"]),
                original_filename=cached_data["original_filename"],
                file_size=cached_data["file_size"],
                results=cached_data.get("results"),
                error_message=cached_data.get("error_message"),
                created_at=datetime.fromisoformat(cached_data["created_at"]),
                updated_at=datetime.fromisoformat(cached_data["updated_at"]),
                completed_at=datetime.fromisoformat(cached_data["completed_at"]) if cached_data.get("completed_at") else None
            )

        job_data = await db.get_job(job_id)
        if not job_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job not found: {job_id}"
            )

        await cache.set_job_status(job_id, job_data)

        return JobStatusResponse(
            job_id=job_data["job_id"],
            status=JobStatus(job_data["status"]),
            original_filename=job_data["original_filename"],
            file_size=job_data["file_size"],
            results=job_data.get("results"),
            error_message=job_data.get("error_message"),
            created_at=job_data["created_at"],
            updated_at=job_data["updated_at"],
            completed_at=job_data.get("completed_at")
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@app.get(
    "/health",
    response_model=HealthCheckResponse,
    status_code=status.HTTP_200_OK,
    summary="Health check",
    description="Check the health of API service and dependencies"
)
async def health_check():
    """Health check for monitoring."""
    try:
        db_healthy = await db.health_check()
        db_status = "healthy" if db_healthy else "unhealthy"

        redis_healthy = await cache.health_check()
        redis_status = "healthy" if redis_healthy else "unhealthy"

        storage_stats = storage.get_storage_stats()
        overall_status = "healthy" if (db_healthy and redis_healthy) else "degraded"

        return HealthCheckResponse(
            status=overall_status,
            database=db_status,
            redis=redis_status,
            storage=storage_stats
        )

    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "error": str(e)
            }
        )


@app.get("/", summary="Root endpoint")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "File Scanner API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "upload": "POST /api/v1/files",
            "status": "GET /api/v1/files/{job_id}",
            "health": "GET /health"
        }
    }
