"""Configuration settings for Patient Matching API"""
import os
from typing import List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Settings:
    
    # Data paths (fallback for local testing)
    SCORED_DATASET_PATH = os.getenv("SCORED_DATASET_PATH", "patient_scores_final_v5555.csv")
    MERGED_DATASET_PATH = os.getenv("MERGED_DATASET_PATH", "merged_patient_dataset_raw_v2.csv")
    
    # API settings
    API_TITLE = "Patient Matching API"
    API_VERSION = "1.0.0"
    DEFAULT_TOP_K = 50
    
    # Distance calculation
    DEFAULT_DISTANCE = 999
    MAX_DISTANCE_KM = int(os.getenv("MAX_DISTANCE_KM", "500"))
    DISTANCE_CACHE_SIZE = int(os.getenv("DISTANCE_CACHE_SIZE", "10000"))
    
    # Scoring thresholds
    FUZZY_MATCH_THRESHOLD = 85
    FUZZY_MATCH_FALLBACK = 60
    
    # Global distance settings
    ENABLE_CROSS_COUNTRY_DISTANCE = os.getenv("ENABLE_CROSS_COUNTRY_DISTANCE", "true").lower() == "true"
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

settings = Settings()