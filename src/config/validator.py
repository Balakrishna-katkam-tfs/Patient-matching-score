"""Configuration validation"""
from src.config.settings import settings
import logging

logger = logging.getLogger(__name__)

def validate_config():
    """Validate required configuration"""
    errors = []
    
    if settings.USE_DOCUDB:
        if not settings.DOCUDB_CONNECTION_STRING:
            errors.append("DOCUDB_CONNECTION_STRING is required when USE_DOCUDB=true")
        if not settings.AWS_ACCESS_KEY_ID:
            errors.append("AWS_ACCESS_KEY_ID is required")
        if not settings.AWS_SECRET_ACCESS_KEY:
            errors.append("AWS_SECRET_ACCESS_KEY is required")
    
    if errors:
        for error in errors:
            logger.error(f"❌ Config Error: {error}")
        raise ValueError(f"Configuration validation failed: {', '.join(errors)}")
    
    logger.info("✅ Configuration validation passed")