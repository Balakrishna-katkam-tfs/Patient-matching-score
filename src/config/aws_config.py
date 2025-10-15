"""AWS configuration and client setup"""
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from src.config.settings import settings
import logging

logger = logging.getLogger(__name__)

class AWSConfig:
    def __init__(self):
        self.session = None
        self.docudb_client = None
        
    def get_session(self):
        """Get AWS session with credentials"""
        if not self.session:
            try:
                self.session = boto3.Session(
                    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                    region_name=settings.AWS_DEFAULT_REGION
                )
                logger.info("✅ AWS session created successfully")
            except (NoCredentialsError, PartialCredentialsError) as e:
                logger.error(f"❌ AWS credentials error: {e}")
                raise
        return self.session
    
    def get_docudb_client(self):
        """Get DocuDB client"""
        if not self.docudb_client:
            session = self.get_session()
            self.docudb_client = session.client('docdb')
        return self.docudb_client

# Global AWS config instance
aws_config = AWSConfig()