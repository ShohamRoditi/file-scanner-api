"""
Redis cache service for job status and file metadata.
"""
import json
import redis.asyncio as redis
from typing import Optional, Dict, Any
from uuid import UUID
import logging

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class CacheService:
    """Redis cache service for job and file metadata."""

    def __init__(self):
        self.redis: Optional[redis.Redis] = None

    async def connect(self):
        """Initialize Redis connection."""
        try:
            self.redis = await redis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True,
                max_connections=settings.redis_max_connections
            )
            await self.redis.ping()
            logger.info("Redis connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def disconnect(self):
        """Close Redis connection."""
        if self.redis:
            await self.redis.close()
            logger.info("Redis connection closed")

    async def set_job_status(
        self,
        job_id: UUID,
        job_data: Dict[str, Any],
        ttl: Optional[int] = None
    ):
        """Cache job status data."""
        key = f"job:{job_id}:status"
        ttl = ttl or settings.job_cache_ttl

        # Convert datetime objects to ISO format strings
        cache_data = {
            "job_id": str(job_data.get("job_id")),
            "status": job_data.get("status"),
            "original_filename": job_data.get("original_filename"),
            "file_size": job_data.get("file_size"),
            "results": job_data.get("results"),
            "error_message": job_data.get("error_message"),
            "created_at": job_data.get("created_at").isoformat() if job_data.get("created_at") else None,
            "updated_at": job_data.get("updated_at").isoformat() if job_data.get("updated_at") else None,
            "completed_at": job_data.get("completed_at").isoformat() if job_data.get("completed_at") else None,
        }

        try:
            await self.redis.setex(
                key,
                ttl,
                json.dumps(cache_data)
            )
            logger.debug(f"Cached job status: {job_id}")
        except Exception as e:
            logger.error(f"Failed to cache job status {job_id}: {e}")

    async def get_job_status(self, job_id: UUID) -> Optional[Dict[str, Any]]:
        """Get cached job status."""
        key = f"job:{job_id}:status"

        try:
            data = await self.redis.get(key)
            if data:
                logger.debug(f"Cache hit for job: {job_id}")
                return json.loads(data)
            logger.debug(f"Cache miss for job: {job_id}")
            return None
        except Exception as e:
            logger.error(f"Failed to get job status from cache {job_id}: {e}")
            return None

    async def set_file_results(
        self,
        file_hash: str,
        results: Dict[str, int],
        file_size: int,
        ttl: Optional[int] = None
    ):
        """Cache file scan results for deduplication."""
        key = f"file:{file_hash}:results"
        ttl = ttl or settings.file_cache_ttl

        cache_data = {
            "file_hash": file_hash,
            "results": results,
            "file_size": file_size,
            "cached_at": None  # Will be set by caller if needed
        }

        try:
            await self.redis.setex(
                key,
                ttl,
                json.dumps(cache_data)
            )
            logger.debug(f"Cached file results: {file_hash[:16]}...")
        except Exception as e:
            logger.error(f"Failed to cache file results {file_hash}: {e}")

    async def get_file_results(self, file_hash: str) -> Optional[Dict[str, Any]]:
        """Get cached file scan results."""
        key = f"file:{file_hash}:results"

        try:
            data = await self.redis.get(key)
            if data:
                logger.debug(f"Cache hit for file: {file_hash[:16]}...")
                return json.loads(data)
            logger.debug(f"Cache miss for file: {file_hash[:16]}...")
            return None
        except Exception as e:
            logger.error(f"Failed to get file results from cache {file_hash}: {e}")
            return None

    async def invalidate_job(self, job_id: UUID):
        """Invalidate job cache."""
        key = f"job:{job_id}:status"
        try:
            await self.redis.delete(key)
            logger.debug(f"Invalidated cache for job: {job_id}")
        except Exception as e:
            logger.error(f"Failed to invalidate job cache {job_id}: {e}")

    async def invalidate_file(self, file_hash: str):
        """Invalidate file cache."""
        key = f"file:{file_hash}:results"
        try:
            await self.redis.delete(key)
            logger.debug(f"Invalidated cache for file: {file_hash[:16]}...")
        except Exception as e:
            logger.error(f"Failed to invalidate file cache {file_hash}: {e}")

    async def health_check(self) -> bool:
        """Check Redis health."""
        try:
            await self.redis.ping()
            return True
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False


# Global cache instance
cache = CacheService()
