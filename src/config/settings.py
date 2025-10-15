"""Configuration settings for Patient Matching API"""
import os
from typing import List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Settings:
    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    
    # DocuDB settings
    DOCUDB_CONNECTION_STRING = os.getenv("DOCUDB_CONNECTION_STRING")
    DOCUDB_DATABASE = os.getenv("DOCUDB_DATABASE", "patient_db")
    DOCUDB_COLLECTION = os.getenv("DOCUDB_COLLECTION", "patients")
    USE_DOCUDB = os.getenv("USE_DOCUDB", "false").lower() == "true"
    
    # Data paths (fallback for local testing)
    SCORED_DATASET_PATH = os.getenv("SCORED_DATASET_PATH", "patient_scores_final_v5555.csv")
    MERGED_DATASET_PATH = os.getenv("MERGED_DATASET_PATH", "merged_patient_dataset_raw_v2.csv")
    
    # API settings
    API_TITLE = "Patient Matching API"
    API_VERSION = "1.0.0"
    DEFAULT_TOP_K = 50
    
    # Distance calculation
    DISTANCE_COUNTRY = "us"
    DEFAULT_DISTANCE = 999
    MAX_DISTANCE_KM = int(os.getenv("MAX_DISTANCE_KM", "500"))
    
    # Scoring thresholds
    FUZZY_MATCH_THRESHOLD = 85
    FUZZY_MATCH_FALLBACK = 60
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

settings = Settings()