"""
Storage cleanup service for managing disk space.
"""
import asyncio
import logging
from datetime import datetime, timedelta

from app.config import get_settings
from app.database import db
from app.storage import storage

logger = logging.getLogger(__name__)
settings = get_settings()


class CleanupService:
    """Service for cleaning up old files and managing storage."""

    async def cleanup_expired_files(self) -> dict:
        """
        Delete files that have expired based on retention policy.

        Returns:
            Dictionary with cleanup statistics
        """
        try:
            logger.info("Starting cleanup of expired files...")

            # Delete expired job records from database
            deleted_jobs = await db.delete_expired_jobs()

            # Get file hashes of old files
            old_hashes = await db.get_old_file_hashes(settings.file_retention_days)

            # Delete files from storage
            deleted_files = await storage.delete_files(old_hashes)

            logger.info(
                f"Cleanup completed: {deleted_jobs} jobs, {deleted_files} files deleted"
            )

            return {
                "deleted_jobs": deleted_jobs,
                "deleted_files": deleted_files,
                "timestamp": datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Cleanup failed: {e}", exc_info=True)
            return {
                "error": str(e),
                "deleted_jobs": 0,
                "deleted_files": 0
            }

    async def cleanup_by_storage_state(self) -> dict:
        """
        Perform cleanup based on current storage state.

        Implements tiered cleanup strategy:
        - Warning (85-90%): Delete files older than 3 days
        - Critical (90-95%): Delete files older than 1 day
        - Emergency (95%+): Delete files older than 12 hours

        Also performs orphaned temp file cleanup on every run.
        """
        # IMPROVEMENT 2: Always cleanup orphaned temp files first
        orphaned_result = await storage.cleanup_orphaned_temp_files()
        logger.info(f"Orphaned file cleanup: {orphaned_result}")

        stats = storage.get_storage_stats()

        if "error" in stats:
            logger.error(f"Cannot check storage stats: {stats['error']}")
            return {"error": stats["error"], "orphaned_cleanup": orphaned_result}

        status = stats["status"]
        percent_used = stats["percent_used"]

        logger.info(f"Storage status: {status} ({percent_used}% used)")

        if status == "emergency":
            # Emergency: Delete files older than 12 hours
            logger.warning("EMERGENCY: Storage at 95%+, aggressive cleanup")
            old_hashes = await db.get_old_file_hashes(days=0)  # Less than 1 day
            deleted = await storage.delete_files(old_hashes)
            return {
                "status": "emergency",
                "deleted_files": deleted,
                "percent_used": percent_used,
                "orphaned_cleanup": orphaned_result
            }

        elif status == "critical":
            # Critical: Delete files older than 1 day
            logger.warning("CRITICAL: Storage at 90%+, aggressive cleanup")
            old_hashes = await db.get_old_file_hashes(days=1)
            deleted = await storage.delete_files(old_hashes)
            return {
                "status": "critical",
                "deleted_files": deleted,
                "percent_used": percent_used,
                "orphaned_cleanup": orphaned_result
            }

        elif status == "warning":
            # Warning: Delete files older than 3 days
            logger.warning("WARNING: Storage at 85%+, gentle cleanup")
            old_hashes = await db.get_old_file_hashes(days=3)
            deleted = await storage.delete_files(old_hashes)
            return {
                "status": "warning",
                "deleted_files": deleted,
                "percent_used": percent_used,
                "orphaned_cleanup": orphaned_result
            }

        else:
            # Healthy: Standard cleanup
            return await self.cleanup_expired_files()

    async def run_periodic_cleanup(self, interval_minutes: int = 60):
        """
        Run cleanup periodically in the background.

        Args:
            interval_minutes: Minutes between cleanup runs
        """
        logger.info(f"Starting periodic cleanup (every {interval_minutes} minutes)")

        while True:
            try:
                await asyncio.sleep(interval_minutes * 60)
                result = await self.cleanup_by_storage_state()
                logger.info(f"Periodic cleanup completed: {result}")

            except asyncio.CancelledError:
                logger.info("Periodic cleanup cancelled")
                break
            except Exception as e:
                logger.error(f"Periodic cleanup error: {e}", exc_info=True)


# Global cleanup service instance
cleanup_service = CleanupService()
