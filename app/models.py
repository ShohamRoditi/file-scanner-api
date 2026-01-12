"""
Database models and Pydantic schemas.
"""
from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from uuid import UUID


class JobStatus(str, Enum):
    """Job processing status enumeration."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


# Database schema (used for table creation)
CREATE_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id UUID PRIMARY KEY,
    file_hash VARCHAR(64) NOT NULL,
    original_filename VARCHAR(255) NOT NULL,
    file_size BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    results JSONB,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_jobs_file_hash ON jobs(file_hash);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_expires_at ON jobs(expires_at);

-- Trigger to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_jobs_updated_at ON jobs;
CREATE TRIGGER update_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
"""


# Pydantic Models (API schemas)

class FileUploadResponse(BaseModel):
    """Response model for file upload."""
    job_id: UUID
    status: JobStatus
    message: str
    deduplication: bool = False
    results: Optional[Dict[str, int]] = None


class JobStatusResponse(BaseModel):
    """Response model for job status query."""
    job_id: UUID
    status: JobStatus
    original_filename: str
    file_size: int
    results: Optional[Dict[str, int]] = None
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None


class HealthCheckResponse(BaseModel):
    """Response model for health check."""
    status: str
    database: str
    redis: str
    storage: Dict[str, Any]


class ScanJobMessage(BaseModel):
    """Message schema for Redis Stream."""
    job_id: str
    file_hash: str
    file_path: str
    file_size: int
    timestamp: int
