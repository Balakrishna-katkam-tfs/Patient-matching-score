"""Main entry point for Patient Matching API"""
import logging
import uvicorn
from src.api.handlers import app
from src.config.settings import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format=settings.LOG_FORMAT
)

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8080,
        reload=True,  # Enable reload for development
        log_level=settings.LOG_LEVEL.lower()
    )