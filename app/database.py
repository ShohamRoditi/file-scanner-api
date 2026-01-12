"""
Database connection and operations using asyncpg.
"""
import asyncpg
import json
from typing import Optional, Dict, Any, List
from uuid import UUID
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import logging

from app.config import get_settings
from app.models import CREATE_JOBS_TABLE, JobStatus

logger = logging.getLogger(__name__)
settings = get_settings()


class Database:
    """Database connection pool and operations manager."""

    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Initialize database connection pool."""
        try:
            self.pool = await asyncpg.create_pool(
                settings.database_url,
                min_size=settings.database_pool_min_size,
                max_size=settings.database_pool_max_size,
                command_timeout=60
            )
            logger.info("Database connection pool created successfully")

            # Initialize schema
            await self.init_schema()
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    async def disconnect(self):
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    async def init_schema(self):
        """Initialize database schema."""
        async with self.pool.acquire() as conn:
            await conn.execute(CREATE_JOBS_TABLE)
        logger.info("Database schema initialized")

    @asynccontextmanager
    async def acquire(self):
        """Acquire a connection from the pool."""
        async with self.pool.acquire() as connection:
            yield connection

    async def create_job(
        self,
        job_id: UUID,
        file_hash: str,
        original_filename: str,
        file_size: int,
        status: JobStatus = JobStatus.PENDING,
        results: Optional[Dict[str, int]] = None
    ) -> Dict[str, Any]:
        """Create a new job record."""
        expires_at = datetime.utcnow() + timedelta(days=settings.file_retention_days)

        # Convert results dict to JSON string if provided
        results_json = json.dumps(results) if results else None

        query = """
        INSERT INTO jobs (
            job_id, file_hash, original_filename, file_size,
            status, results, expires_at
        ) VALUES ($1, $2, $3, $4, $5, $6::JSONB, $7)
        RETURNING
            job_id, file_hash, original_filename, file_size,
            status, results, created_at, updated_at, completed_at
        """

        async with self.acquire() as conn:
            row = await conn.fetchrow(
                query,
                job_id,
                file_hash,
                original_filename,
                file_size,
                status.value,
                results_json,
                expires_at
            )

        result = dict(row)
        # Parse JSON string back to dict if results exist
        # (asyncpg might return it as string depending on the driver)
        if result.get('results') and isinstance(result['results'], str):
            result['results'] = json.loads(result['results'])

        return result

    async def get_job(self, job_id: UUID) -> Optional[Dict[str, Any]]:
        """Get job by ID."""
        query = """
        SELECT
            job_id, file_hash, original_filename, file_size,
            status, results, error_message, created_at,
            updated_at, completed_at
        FROM jobs
        WHERE job_id = $1
        """

        async with self.acquire() as conn:
            row = await conn.fetchrow(query, job_id)

        if not row:
            return None

        result = dict(row)
        # Parse JSON string back to dict if results exist
        if result.get('results') and isinstance(result['results'], str):
            result['results'] = json.loads(result['results'])

        return result

    async def get_completed_job_by_hash(self, file_hash: str) -> Optional[Dict[str, Any]]:
        """Get a completed job by file hash for deduplication."""
        query = """
        SELECT
            job_id, file_hash, original_filename, file_size,
            status, results, created_at, updated_at, completed_at
        FROM jobs
        WHERE file_hash = $1 AND status = $2
        ORDER BY completed_at DESC
        LIMIT 1
        """

        async with self.acquire() as conn:
            row = await conn.fetchrow(query, file_hash, JobStatus.COMPLETED.value)

        if not row:
            return None

        result = dict(row)
        # Parse JSON string back to dict if results exist
        if result.get('results') and isinstance(result['results'], str):
            result['results'] = json.loads(result['results'])

        return result

    async def update_job_status(
        self,
        job_id: UUID,
        status: JobStatus,
        results: Optional[Dict[str, int]] = None,
        error_message: Optional[str] = None
    ) -> bool:
        """Update job status and results."""
        completed_at = datetime.utcnow() if status in [JobStatus.COMPLETED, JobStatus.FAILED] else None

        # Convert results dict to JSON string if provided
        results_json = json.dumps(results) if results else None

        query = """
        UPDATE jobs
        SET
            status = $2,
            results = COALESCE($3::JSONB, results),
            error_message = $4,
            completed_at = COALESCE($5, completed_at)
        WHERE job_id = $1
        """

        async with self.acquire() as conn:
            result = await conn.execute(
                query,
                job_id,
                status.value,
                results_json,
                error_message,
                completed_at
            )

        return result == "UPDATE 1"

    async def delete_expired_jobs(self) -> int:
        """Delete expired jobs."""
        query = """
        DELETE FROM jobs
        WHERE expires_at < NOW()
        RETURNING job_id, file_hash
        """

        async with self.acquire() as conn:
            rows = await conn.fetch(query)

        return len(rows)

    async def get_old_file_hashes(self, days: int) -> List[str]:
        """Get file hashes of files older than specified days."""
        query = """
        SELECT DISTINCT file_hash
        FROM jobs
        WHERE created_at < NOW() - INTERVAL '1 day' * $1
        """

        async with self.acquire() as conn:
            rows = await conn.fetch(query, days)

        return [row['file_hash'] for row in rows]

    async def health_check(self) -> bool:
        """Check database health."""
        try:
            async with self.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False


# Global database instance
db = Database()
