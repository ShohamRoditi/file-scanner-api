"""
Main entry point for the File Scanner API service.
"""
import uvicorn
from app.api import app
from app.config import get_settings

settings = get_settings()

if __name__ == "__main__":
    uvicorn.run(
        "app.api:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True,  # Set to False in production
        log_level="info"
    )
