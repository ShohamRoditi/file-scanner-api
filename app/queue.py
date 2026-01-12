"""
Redis Stream message queue service for job distribution.
"""
import redis.asyncio as redis
import json
import time
from typing import Optional, Dict, Any
from uuid import UUID
import logging

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class QueueService:
    """Redis Stream message queue for distributing scan jobs to workers."""

    def __init__(self):
        self.redis: Optional[redis.Redis] = None
        self.stream_name = settings.redis_stream_name
        self.consumer_group = settings.redis_consumer_group

    async def connect(self):
        """Initialize Redis connection for queue."""
        try:
            self.redis = await redis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True,
                max_connections=settings.redis_max_connections
            )

            # Create consumer group (ignore error if already exists)
            try:
                await self.redis.xgroup_create(
                    name=self.stream_name,
                    groupname=self.consumer_group,
                    id='0',
                    mkstream=True
                )
                logger.info(f"Created consumer group: {self.consumer_group}")
            except redis.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    logger.info(f"Consumer group already exists: {self.consumer_group}")
                else:
                    raise

            logger.info("Queue service initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize queue service: {e}")
            raise

    async def disconnect(self):
        """Close Redis connection."""
        if self.redis:
            await self.redis.close()
            logger.info("Queue service connection closed")

    async def publish_job(
        self,
        job_id: UUID,
        file_hash: str,
        file_path: str,
        file_size: int
    ) -> str:
        """
        Publish a scan job to the Redis Stream.

        Returns:
            Message ID
        """
        message = {
            "job_id": str(job_id),
            "file_hash": file_hash,
            "file_path": file_path,
            "file_size": str(file_size),
            "timestamp": str(int(time.time()))
        }

        try:
            message_id = await self.redis.xadd(
                name=self.stream_name,
                fields=message
            )
            logger.info(f"Published job to queue: {job_id} (message_id: {message_id})")
            return message_id
        except Exception as e:
            logger.error(f"Failed to publish job {job_id}: {e}")
            raise

    async def get_stream_length(self) -> int:
        """Get the number of pending messages in the stream."""
        try:
            return await self.redis.xlen(self.stream_name)
        except Exception as e:
            logger.error(f"Failed to get stream length: {e}")
            return 0

    async def get_pending_count(self) -> int:
        """Get the number of pending messages in the consumer group."""
        try:
            info = await self.redis.xpending(self.stream_name, self.consumer_group)
            return info['pending'] if info else 0
        except Exception as e:
            logger.error(f"Failed to get pending count: {e}")
            return 0

    async def health_check(self) -> Dict[str, Any]:
        """Check queue health and get statistics."""
        try:
            stream_length = await self.get_stream_length()
            pending_count = await self.get_pending_count()

            return {
                "status": "healthy",
                "stream_length": stream_length,
                "pending_count": pending_count
            }
        except Exception as e:
            logger.error(f"Queue health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }


# Global queue instance
queue = QueueService()
