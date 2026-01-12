"""
Scanner worker for file processing.

Consumes jobs from Redis Stream, scans files, and updates results.
"""
import asyncio
import asyncpg
import redis.asyncio as redis
import logging
import time
from pathlib import Path
from typing import Dict
import sys
import os

# Add parent directory to path to import app modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()


class WorkerSimulator:
    def __init__(self):
        self.db_pool = None
        self.redis = None
        self.consumer_name = f"worker_{int(time.time())}"

    async def connect(self):
        self.db_pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=2,
            max_size=5
        )
        logger.info("Connected to database")

        self.redis = await redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True
        )

        try:
            await self.redis.xgroup_create(
                name=settings.redis_stream_name,
                groupname=settings.redis_consumer_group,
                id='0-0',
                mkstream=True
            )
            logger.info(f"Created consumer group: {settings.redis_consumer_group}")
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        logger.info("Connected to Redis")

    async def disconnect(self):
        if self.db_pool:
            await self.db_pool.close()
        if self.redis:
            await self.redis.close()

    async def scan_file(self, file_path: str) -> Dict[str, int]:
        """Scan file and count letter frequency (A-Z)."""
        counts = {chr(i): 0 for i in range(65, 91)}  # A-Z

        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        chunk_size = 1024 * 1024
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            while chunk := f.read(chunk_size):
                for char in chunk.upper():
                    if 'A' <= char <= 'Z':
                        counts[char] += 1

        logger.info(f"Scanned {file_path}: {sum(counts.values())} letters")
        return counts

    async def update_job_status(
        self,
        job_id: str,
        status: str,
        results: Dict[str, int] = None,
        error_message: str = None
    ):
        import json
        results_json = json.dumps(results) if results else None

        query = """
        UPDATE jobs
        SET
            status = $2::VARCHAR,
            results = COALESCE($3::JSONB, results),
            error_message = $4,
            completed_at = CASE WHEN $2::VARCHAR IN ('completed', 'failed') THEN NOW() ELSE completed_at END,
            updated_at = NOW()
        WHERE job_id = $1
        """

        async with self.db_pool.acquire() as conn:
            await conn.execute(query, job_id, status, results_json, error_message)

        # Invalidate cache so API reads fresh data
        cache_key = f"job:{job_id}:status"
        await self.redis.delete(cache_key)

    async def cache_results(self, file_hash: str, results: Dict[str, int], file_size: int):
        import json
        key = f"file:{file_hash}:results"
        value = {
            "file_hash": file_hash,
            "results": results,
            "file_size": file_size
        }
        await self.redis.setex(key, settings.file_cache_ttl, json.dumps(value))

    async def process_message(self, message_id: str, message_data: dict):
        job_id = message_data.get('job_id')
        file_hash = message_data.get('file_hash')
        file_path = message_data.get('file_path')
        file_size = int(message_data.get('file_size', 0))

        # Path translation for hybrid deployments (Docker API + local worker)
        docker_path = os.getenv('DOCKER_FILE_PATH', '/data/files')
        local_path = os.getenv('LOCAL_FILE_PATH', settings.file_storage_path)
        if file_path.startswith(docker_path) and docker_path != local_path:
            file_path = file_path.replace(docker_path, local_path)

        logger.info(f"Processing {job_id}")

        try:
            await self.update_job_status(job_id, 'processing')
            await asyncio.sleep(1)

            results = await self.scan_file(file_path)
            await self.update_job_status(job_id, 'completed', results=results)
            await self.cache_results(file_hash, results, file_size)

            logger.info(f"Completed {job_id}")
            await self.redis.xack(
                settings.redis_stream_name,
                settings.redis_consumer_group,
                message_id
            )

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}")
            await self.update_job_status(job_id, 'failed', error_message=str(e))

    async def run(self):
        logger.info(f"Worker started: {self.consumer_name}")
        logger.info(f"Polling stream: {settings.redis_stream_name}")

        try:
            while True:
                try:
                    messages = await self.redis.xreadgroup(
                        groupname=settings.redis_consumer_group,
                        consumername=self.consumer_name,
                        streams={settings.redis_stream_name: '>'},
                        count=1,
                        block=5000
                    )
                except Exception as e:
                    logger.error(f"Stream read error: {e}")
                    await asyncio.sleep(5)
                    continue

                if not messages:
                    await asyncio.sleep(1)
                    continue

                for stream_name, stream_messages in messages:
                    for message_id, message_data in stream_messages:
                        await self.process_message(message_id, message_data)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Worker error: {e}", exc_info=True)


async def main():
    worker = WorkerSimulator()
    try:
        await worker.connect()
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await worker.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
