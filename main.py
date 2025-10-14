"""Main entry point for async Patient Matching API"""
import logging
import uvicorn
from mangum import Mangum
from src.api.handlers import app
from src.config.settings import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format=settings.LOG_FORMAT
)

# Lambda handler
handler = Mangum(app)

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level=settings.LOG_LEVEL.lower()
    )