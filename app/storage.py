"""
File storage service with content-addressed storage and streaming.
"""
import os
import hashlib
import aiofiles
import shutil
import asyncio
import time
import uuid
import glob
from pathlib import Path
from typing import Tuple, BinaryIO
from fastapi import HTTPException, status
import logging

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class StorageService:
    """File storage service with content-addressed storage."""

    def __init__(self):
        self.base_path = Path(settings.file_storage_path)
        self.chunk_size = 1024 * 1024  # 1MB chunks

    def ensure_base_path(self):
        """Ensure base storage directory exists."""
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Storage base path ensured: {self.base_path}")

    def _get_file_path(self, file_hash: str) -> Path:
        """
        Get content-addressed file path.
        Structure: /data/files/{hash[0:2]}/{hash[2:4]}/{hash}
        """
        return (
            self.base_path
            / file_hash[0:2]
            / file_hash[2:4]
            / file_hash
        )

    async def calculate_hash_and_save(
        self,
        file_data: BinaryIO,
        filename: str
    ) -> Tuple[str, int, str]:
        """
        Calculate SHA256 hash while streaming file to storage.
        Single-pass operation with timeout, cleanup, and verification.

        Improvements:
        - Upload timeout protection (5 minutes)
        - Unique temp file names to avoid collisions
        - Data corruption detection with hash verification

        Returns:
            Tuple of (file_hash, file_size, file_path)
        """
        sha256 = hashlib.sha256()
        file_size = 0

        # Create temporary file with unique name to avoid collisions
        temp_path = self.base_path / f"temp_{uuid.uuid4().hex}_{filename}"
        self.base_path.mkdir(parents=True, exist_ok=True)

        try:
            # IMPROVEMENT 1: Add timeout protection
            async with asyncio.timeout(settings.upload_timeout_seconds):
                # Stream file to temp location while calculating hash
                async with aiofiles.open(temp_path, 'wb') as f:
                    while True:
                        chunk = file_data.read(self.chunk_size)
                        if not chunk:
                            break

                        # Update hash
                        sha256.update(chunk)
                        file_size += len(chunk)

                        # Write chunk
                        await f.write(chunk)

                file_hash = sha256.hexdigest()
                final_path = self._get_file_path(file_hash)

                # Check if file already exists (deduplication at storage level)
                if final_path.exists():
                    logger.info(f"File already exists in storage: {file_hash[:16]}...")
                    # Remove temp file
                    temp_path.unlink()
                else:
                    # IMPROVEMENT 3: Verify file integrity before moving
                    await self._verify_file_integrity(temp_path, file_hash)

                    # Create directory structure and move file atomically
                    final_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(temp_path), str(final_path))
                    logger.info(f"File saved and verified: {file_hash[:16]}... ({file_size} bytes)")

                return file_hash, file_size, str(final_path)

        except asyncio.TimeoutError:
            # Timeout exceeded
            if temp_path.exists():
                temp_path.unlink()
            logger.error(f"Upload timeout exceeded for {filename}")
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail=f"Upload timeout: file upload took longer than {settings.upload_timeout_seconds} seconds"
            )
        except Exception as e:
            # Clean up temp file on error
            if temp_path.exists():
                temp_path.unlink()
            logger.error(f"Failed to save file: {e}")
            raise

    async def _verify_file_integrity(self, file_path: Path, expected_hash: str):
        """
        Verify file integrity by recalculating hash.
        IMPROVEMENT 3: Data corruption detection.

        Args:
            file_path: Path to file to verify
            expected_hash: Expected SHA256 hash

        Raises:
            IOError: If file hash doesn't match (corruption detected)
        """
        verify_hash = hashlib.sha256()

        async with aiofiles.open(file_path, 'rb') as f:
            while True:
                chunk = await f.read(self.chunk_size)
                if not chunk:
                    break
                verify_hash.update(chunk)

        calculated_hash = verify_hash.hexdigest()

        if calculated_hash != expected_hash:
            # File corrupted during write!
            file_path.unlink()
            logger.error(
                f"File corruption detected! Expected: {expected_hash[:16]}..., "
                f"Got: {calculated_hash[:16]}..."
            )
            raise IOError("File corruption detected during upload")

    async def file_exists(self, file_hash: str) -> bool:
        """Check if file exists in storage."""
        file_path = self._get_file_path(file_hash)
        return file_path.exists()

    async def get_file_path(self, file_hash: str) -> str:
        """Get file path for a given hash."""
        return str(self._get_file_path(file_hash))

    async def delete_file(self, file_hash: str) -> bool:
        """Delete file from storage."""
        file_path = self._get_file_path(file_hash)

        try:
            if file_path.exists():
                file_path.unlink()
                logger.info(f"Deleted file: {file_hash[:16]}...")

                # Clean up empty parent directories
                try:
                    file_path.parent.rmdir()  # Remove {hash[2:4]} dir if empty
                    file_path.parent.parent.rmdir()  # Remove {hash[0:2]} dir if empty
                except OSError:
                    pass  # Directory not empty, that's fine

                return True
            return False
        except Exception as e:
            logger.error(f"Failed to delete file {file_hash}: {e}")
            return False

    async def delete_files(self, file_hashes: list[str]) -> int:
        """Delete multiple files from storage."""
        deleted_count = 0
        for file_hash in file_hashes:
            if await self.delete_file(file_hash):
                deleted_count += 1
        return deleted_count

    async def cleanup_orphaned_temp_files(self) -> dict:
        """
        IMPROVEMENT 2: Clean up orphaned temporary files.

        Removes temp files that are older than the configured threshold.
        These files are left behind when:
        - Server crashes during upload
        - Upload is killed with SIGKILL
        - System loses power during operation

        Returns:
            Dictionary with cleanup statistics
        """
        try:
            cleanup_threshold = settings.cleanup_orphaned_files_hours * 3600  # Convert to seconds
            current_time = time.time()
            deleted_count = 0
            deleted_size = 0

            # Find all temp files using glob pattern
            temp_pattern = str(self.base_path / "temp_*")
            temp_files = glob.glob(temp_pattern)

            for temp_file in temp_files:
                try:
                    file_path = Path(temp_file)

                    # Check if file is older than threshold
                    file_age = current_time - os.path.getmtime(temp_file)

                    if file_age > cleanup_threshold:
                        file_size = file_path.stat().st_size
                        file_path.unlink()
                        deleted_count += 1
                        deleted_size += file_size
                        logger.info(
                            f"Removed orphaned temp file: {file_path.name} "
                            f"(age: {file_age/3600:.1f} hours, size: {file_size} bytes)"
                        )
                except Exception as e:
                    logger.error(f"Failed to remove temp file {temp_file}: {e}")
                    continue

            result = {
                "deleted_count": deleted_count,
                "deleted_size_bytes": deleted_size,
                "threshold_hours": settings.cleanup_orphaned_files_hours,
                "status": "success"
            }

            if deleted_count > 0:
                logger.info(
                    f"Orphaned file cleanup: removed {deleted_count} files, "
                    f"freed {deleted_size / 1024 / 1024:.2f} MB"
                )

            return result

        except Exception as e:
            logger.error(f"Orphaned file cleanup failed: {e}")
            return {
                "deleted_count": 0,
                "deleted_size_bytes": 0,
                "error": str(e),
                "status": "error"
            }

    def get_storage_stats(self) -> dict:
        """Get storage statistics."""
        try:
            stat = shutil.disk_usage(self.base_path)
            total = stat.total
            used = stat.used
            free = stat.free
            percent_used = (used / total) * 100

            return {
                "total_bytes": total,
                "used_bytes": used,
                "free_bytes": free,
                "percent_used": round(percent_used, 2),
                "status": self._get_storage_status(percent_used)
            }
        except Exception as e:
            logger.error(f"Failed to get storage stats: {e}")
            return {
                "error": str(e),
                "status": "unknown"
            }

    def _get_storage_status(self, percent_used: float) -> str:
        """Determine storage status based on usage percentage."""
        if percent_used >= settings.storage_emergency_threshold:
            return "emergency"
        elif percent_used >= settings.storage_critical_threshold:
            return "critical"
        elif percent_used >= settings.storage_warning_threshold:
            return "warning"
        else:
            return "healthy"

    def should_accept_upload(self) -> Tuple[bool, str]:
        """Check if system should accept new uploads based on storage."""
        stats = self.get_storage_stats()

        if "error" in stats:
            return True, "ok"  # Allow uploads if we can't check (fail open)

        status = stats["status"]
        percent_used = stats["percent_used"]

        if status == "emergency":
            return False, f"Storage critical: {percent_used}% used. Uploads temporarily disabled."
        elif status == "critical":
            # In critical state, limit file size
            return True, f"warning: storage at {percent_used}%"
        else:
            return True, "ok"


# Global storage instance
storage = StorageService()
