# Patient Matching API

Scalable FastAPI application for clinical trial patient matching with comprehensive scoring system.

## Features
- Real-time patient matching with distance calculations
- Async architecture for handling millions of patients
- Comprehensive scoring: recency, PPD screening, similar studies, distance
- Modular design for easy maintenance and scaling

## Quick Start

### Local Development
```bash
pip install -r requirements.txt
python main_async.py
```

### AWS Lambda Deployment
```bash
python deploy.py
```

## API Endpoints
- `POST /query` - Main patient matching endpoint
- `GET /health` - Health check
- `GET /conditions` - Available conditions list

## Configuration
Environment variables in `src/config/settings.py`:
- `LOG_LEVEL` - Logging level (default: INFO)
- `MAX_DISTANCE_KM` - Maximum distance for scoring (default: 500)
- Distance scoring tiers: 0-50km (20pts), 50-100km (15pts), 100-200km (10pts), 200-500km (5pts)

## Data Files Required
- `patient_scores_final_v5555.csv` - Primary patient dataset
- `merged_patient_dataset_raw_v2.csv` - Geographic data with POSTAL_CODE
- `EDW_DATA_4_ML.csv` - Raw EDW dataset for preprocessing