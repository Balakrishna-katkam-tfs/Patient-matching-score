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
        
        # Get ALL matching patients (no limit yet)
        all_matching_patients = await filter_patients_async(filters, top_k=None)
        total_matching_patients = len(all_matching_patients)
        
        if not all_matching_patients:
            return QueryResponse(
                patients=[],
                total_matching_patients=0,
                returned_patients=0
            )
        
        # Smart limiting: Score enough candidates to find best matches
        # Need to score more candidates to account for distance bonuses changing rankings
        max_score_candidates = 5000 if request.top_k is None else min(request.top_k * 50, 10000)
        
        # Use provided top_k or return all results
        effective_top_k = request.top_k
        
        # Pre-sort by base business_score and take top candidates
        all_matching_patients.sort(key=lambda x: x.get("business_score", 0), reverse=True)
        candidates_to_score = all_matching_patients[:max_score_candidates]
        
        logger.info(f"Scoring {len(candidates_to_score)} top candidates (from {total_matching_patients} total)")
        
        # Compute scores for selected candidates
        scored_patients = await compute_score_batch(candidates_to_score, request.site_zip_codes)
        
        # Combine candidates with their new scores
        for i, patient in enumerate(candidates_to_score):
            patient["_new_score"] = scored_patients[i]["total_business_score"]
            patient["_score_details"] = scored_patients[i]
        
        # Sort by NEW scores
        candidates_to_score.sort(key=lambda x: x["_new_score"], reverse=True)
        
        # Apply effective top_k limit if specified
        patients = candidates_to_score[:effective_top_k] if effective_top_k else candidates_to_score
        
        # Build results using pre-calculated scores
        results = []
        for p in patients:
            score_details = p["_score_details"]
            results.append({
                "patient_id": str(p["PATIENT_ID"]),
                "age": p.get("age"),
                "sex": p.get("sex"),
                "study_id": p.get("study_id"),
                "indication": p.get("indication"),
                "latest_milestone": p.get("latest_milestone"),
                "score_details": score_details,
                "_sort_score": p["_new_score"]
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

@app.post("/zip-query", response_model=QueryResponse)
async def query_by_zip_only(request: dict):
    """Query patients by zip code only - simplified endpoint"""
    site_zip_codes = request.get("site_zip_codes", [])
    top_k = request.get("top_k")  # None by default
    
    query_request = QueryRequest(
        query=None,
        site_zip_codes=site_zip_codes,
        top_k=top_k
    )
    
    return await query_patients(query_request)