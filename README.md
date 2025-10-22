# Patient Matching API

Production-ready FastAPI application for clinical trial patient matching with comprehensive scoring system.

## Features
- Real-time patient matching with distance calculations
- Async architecture for handling large patient datasets
- Multi-factor scoring: recency, PPD screening, similar studies, distance, past qualification
- Global distance calculation support
- Fuzzy matching for medical conditions
- Modular design for easy maintenance and scaling

## Quick Start

### Local Development
```bash
pip install -r requirements.txt
python main.py
```

Server runs on `http://localhost:8080`



## API Endpoints
- `POST /query` - Main patient matching endpoint
- `GET /` - Health check
- `GET /conditions` - Available conditions list
- `POST /zip-query` - Zip code only query

## API Usage

### Query Patients
```json
{
    "query": "Target: ADHD Male age >= 18",
    "site_zip_codes": ["10001", "33142"],
    "top_k": 50
}
```

### Response Format
```json
{
    "patients": [
        {
            "patient_id": "12345",
            "age": 25,
            "sex": "M",
            "indication": "ADHD",
            "score_details": {
                "total_business_score": 85,
                "breakdown": [...]
            },
            "match_score_percent": 95.2
        }
    ],
    "total_matching_patients": 150,
    "returned_patients": 50
}
```

## Configuration
Environment variables (optional `.env` file):
- `LOG_LEVEL` - Logging level (default: INFO)
- `MAX_DISTANCE_KM` - Maximum distance for scoring (default: 500)

## Scoring System
1. **Recency** (0-50 pts) - Based on diagnosis or recent activity
2. **PPD Screening** (0-40 pts) - Released > Qualified > Respondents
3. **Similar Studies** (0-200+ pts) - Priority-based indication scoring
4. **Distance** (0-20 pts) - Geographic proximity to sites
5. **Past Qualification** (0-25 pts) - Previous randomization history

## Data Files Required
- `patient_scores_final_v5555.csv` - Primary patient dataset
- `merged_patient_dataset_raw_v2.csv` - Geographic data with POSTAL_CODE
- `EDW_DATA_4_ML.csv` - Raw EDW dataset (optional)

## Interactive Documentation
Once running, visit:
- Swagger UI: `http://localhost:8080/docs`
- ReDoc: `http://localhost:8080/redoc`