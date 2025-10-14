"""FastAPI handlers for patient matching"""
import asyncio
import logging
from fastapi import FastAPI, HTTPException
from ..core.models import QueryRequest, QueryResponse, PatientResult, ScoreDetails, ScoreBreakdown
from ..core.filtering import filter_patients_async
from ..core.scoring import compute_score_batch
from ..utils.query_parser import parse_query
from ..data.loader import data_loader
from ..core.distance import distance_calculator
from ..config.settings import settings

logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title=settings.API_TITLE, version=settings.API_VERSION)

@app.on_event("startup")
async def startup_event():
    """Initialize data and services on startup"""
    logger.info("🚀 Starting Patient Matching API...")
    
    # Load datasets
    await data_loader.load_datasets()
    
    # Initialize distance calculator
    await distance_calculator.initialize()
    
    logger.info("✅ API startup complete")

@app.post("/query", response_model=QueryResponse)
async def query_patients(request: QueryRequest):
    """Main patient matching endpoint"""
    try:
        # Parse query
        filters = parse_query(request.query)
        
        # Early return if no indication matches found
        if "indication" in filters and not filters["indication"]:
            return QueryResponse(
                patients=[],
                total_matching_patients=0,
                returned_patients=0
            )
        
        # Get total matching patients count before limiting
        all_matching_patients = await filter_patients_async(filters, top_k=None)
        total_matching_patients = len(all_matching_patients)
        
        # Get limited results
        patients = await filter_patients_async(filters, request.top_k)
        
        if not patients:
            return QueryResponse(
                patients=[],
                total_matching_patients=0,
                returned_patients=0
            )
        
        # Compute scores in batches
        scored_patients = await compute_score_batch(patients, request.site_zip_codes)
        
        # Build results
        results = []
        for i, p in enumerate(patients):
            score_details = scored_patients[i]
            results.append({
                "patient_id": str(p["PATIENT_ID"]),
                "age": p.get("age"),
                "sex": p.get("sex"),
                "study_id": p.get("study_id"),
                "indication": p.get("indication"),
                "latest_milestone": p.get("latest_milestone"),
                "score_details": score_details,
                "_sort_score": score_details["total_business_score"]
            })
        
        # Sort by newly calculated scores
        results.sort(key=lambda x: x["_sort_score"], reverse=True)
        
        # Calculate match_score_percent based on ranking
        if results:
            max_score = results[0]["_sort_score"]
            min_score = results[-1]["_sort_score"]
            
            for result in results:
                if max_score > min_score:
                    normalized = (result["_sort_score"] - min_score) / (max_score - min_score)
                    result["match_score_percent"] = round(normalized * 100, 2)
                    # Update score_details normalization
                    result["score_details"]["business_score_normalized"] = round(normalized, 4)
                    result["score_details"]["business_score_percent"] = round(normalized * 100, 2)
                else:
                    result["match_score_percent"] = 100.0
                    result["score_details"]["business_score_normalized"] = 1.0
                    result["score_details"]["business_score_percent"] = 100.0
        
        # Remove temporary sort field
        for result in results:
            del result["_sort_score"]
        
        return QueryResponse(
            patients=results,
            total_matching_patients=total_matching_patients,
            returned_patients=len(results)
        )
        
    except Exception as e:
        logger.error(f"Error processing query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "message": "Patient Matching API is running"}

@app.get("/conditions")
def get_available_conditions():
    """Get all available medical conditions"""
    try:
        df, _ = data_loader.get_datasets_sync()
        conditions = df.select("indication").unique().sort("indication").to_series().drop_nulls().to_list()
        return {"available_conditions": conditions, "total_count": len(conditions)}
    except Exception as e:
        logger.error(f"Error getting conditions: {e}")
        raise HTTPException(status_code=500, detail=str(e))