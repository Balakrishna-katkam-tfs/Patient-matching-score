"""Configuration settings for Patient Matching API"""
import os
from typing import List

class Settings:
    # Data paths
    SCORED_DATASET_PATH = os.getenv("SCORED_DATASET_PATH", "patient_scores_final_v5555.csv")
    MERGED_DATASET_PATH = os.getenv("MERGED_DATASET_PATH", "merged_patient_dataset_raw_v2.csv")
    
    # API settings
    API_TITLE = "Patient Matching API"
    API_VERSION = "1.0.0"
    DEFAULT_TOP_K = 50
    
    # Distance calculation
    DISTANCE_COUNTRY = "us"
    DEFAULT_DISTANCE = 999
    
    # Scoring thresholds
    FUZZY_MATCH_THRESHOLD = 85
    FUZZY_MATCH_FALLBACK = 60
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

settings = Settings()