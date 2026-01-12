"""
Application configuration management using Pydantic settings.
"""
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    # Database Configuration
    database_url: str = "postgresql://scanner_user:scanner_pass@localhost:5432/file_scanner"
    database_pool_min_size: int = 10
    database_pool_max_size: int = 20

    # Redis Configuration
    redis_url: str = "redis://localhost:6379/0"
    redis_max_connections: int = 50

    # File Storage Configuration
    file_storage_path: str = "/tmp/file_scanner_data/files"
    max_file_size_mb: int = 100
    storage_warning_threshold: int = 85
    storage_critical_threshold: int = 90
    storage_emergency_threshold: int = 95

    # Cache Configuration
    job_cache_ttl: int = 3600  # 1 hour
    file_cache_ttl: int = 86400  # 24 hours

    # Queue Configuration
    redis_stream_name: str = "scan_jobs"
    redis_consumer_group: str = "scanner_workers"

    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 4
    max_upload_size: int = 104857600  # 100MB in bytes

    # Cleanup Configuration
    file_retention_days: int = 7
    cleanup_orphaned_files_hours: int = 1  # Remove temp files older than 1 hour

    # Upload Configuration
    upload_timeout_seconds: int = 300  # 5 minutes max upload time

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached application settings."""
    return Settings()
