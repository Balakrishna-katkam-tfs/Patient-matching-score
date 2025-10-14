import polars as pl
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import re
from rapidfuzz import fuzz
import logging
from datetime import datetime
import pgeocode

# ===============================================================
# Logging Setup
# ===============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

# ===============================================================
# Load Datasets
# ===============================================================
df = pl.read_csv(
    "patient_scores_final_v5555.csv",
    infer_schema_length=10000,
    ignore_errors=True,
    try_parse_dates=True
)

merged_df = pl.read_csv(
    "merged_patient_dataset_raw_v2.csv",
    infer_schema_length=10000,
    ignore_errors=True,
    try_parse_dates=True
)

logger.info(f"✅ Scored dataset: {df.shape}")
logger.info(f"✅ Merged dataset: {merged_df.shape}")

# Initialize distance calculator
try:
    distance_calc = pgeocode.GeoDistance('us')
    logger.info("✅ Distance calculator initialized")
except Exception as e:
    logger.warning(f"⚠️ Distance calculator failed to initialize: {e}")
    distance_calc = None

# ===============================================================
# FastAPI Setup
# ===============================================================
app = FastAPI(title="Patient Matching API", version="1.0.0")

class QueryRequest(BaseModel):
    query: str
    site_zip_codes: List[str] = []  # List of trial site zip codes
    top_k: int = 50  # Default limit to prevent performance issues

# ===============================================================
# Helper Functions
# ===============================================================
def safe_int(x):
    try:
        return int(float(x))
    except Exception:
        return 0

def safe_date(date_str):
    """Try parsing date strings safely"""
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(date_str).split(" ")[0], fmt)
        except Exception:
            continue
    return None

def calculate_closest_site_distance(patient_zip: str, site_zip_codes: List[str]) -> float:
    """Calculate distance to closest site"""
    if not patient_zip or not site_zip_codes or not distance_calc:
        logger.warning(f"Distance calc failed: patient_zip={patient_zip}, site_zips={site_zip_codes}, distance_calc={distance_calc}")
        return 999  # Unknown distance
    
    min_distance = float('inf')
    
    for site_zip in site_zip_codes:
        try:
            logger.info(f"Calculating distance: {patient_zip} to {site_zip}")
            distance = distance_calc.query_postal_code(patient_zip, site_zip)
            logger.info(f"Distance result: {distance}")
            if distance is not None and not (isinstance(distance, float) and distance != distance) and distance < min_distance:
                min_distance = distance
        except Exception as e:
            logger.warning(f"Distance calculation error for {patient_zip} to {site_zip}: {e}")
            continue
    
    final_distance = min_distance if min_distance != float('inf') else 999
    logger.info(f"Final distance from {patient_zip} to closest site: {final_distance}")
    return final_distance

def fuzzy_match_filter(query_term: str, column: str = "indication", threshold: int = 85):
    query_term = query_term.lower().strip()
    unique_values = df.select(column).unique().to_series().drop_nulls().to_list()
    matches = []
    
    # First try exact match
    for val in unique_values:
        if query_term == str(val).lower().strip():
            matches.append(val)
            logger.info(f"🎯 Exact match: '{query_term}' → '{val}'")
            return matches
    
    # Then fuzzy matching with higher threshold
    for val in unique_values:
        val_str = str(val).lower().strip()
        if fuzz.ratio(query_term, val_str) >= threshold:
            matches.append(val)
            logger.info(f"🧠 Fuzzy match: '{query_term}' → '{val}' (score: {fuzz.ratio(query_term, val_str)})")
    
    # If no matches found, try with lower threshold (60) for partial matches
    if not matches:
        logger.info(f"⚠️ No matches with threshold {threshold}, trying lower threshold (60)")
        for val in unique_values:
            val_str = str(val).lower().strip()
            if (
                fuzz.partial_ratio(query_term, val_str) >= 60 or
                fuzz.token_sort_ratio(query_term, val_str) >= 60
            ):
                matches.append(val)
                logger.info(f"🔍 Partial match: '{query_term}' → '{val}' (partial: {fuzz.partial_ratio(query_term, val_str)}, token: {fuzz.token_sort_ratio(query_term, val_str)})")
    
    logger.info(f"🧠 Final matches for '{query_term}' → {matches}")
    return matches

# ===============================================================
# Query Parser
# ===============================================================
def parse_query(query: str) -> Dict:
    filters = {}
    logger.info(f"Parsing query: {query}")

    sex_match = re.search(r"(Female|Male)", query, re.IGNORECASE)
    if sex_match:
        filters["sex"] = sex_match.group(1)[0].upper()

    age_match = re.search(r"age\s*>=\s*(\d+)", query, re.IGNORECASE)
    if age_match:
        filters["age_min"] = int(age_match.group(1))

    target_match = re.search(r"Target:\s*([^\n]+?)(?=\s*EXCLUSION:|$)", query, re.IGNORECASE)
    if target_match:
        target = target_match.group(1).strip()
        # Extract just the medical condition, ignore demographics
        medical_condition = re.sub(r'\b(Male|Female|age\s*>=?\s*\d+)\b', '', target, flags=re.IGNORECASE).strip()
        synonyms = fuzzy_match_filter(medical_condition, "indication")
        filters["indication"] = synonyms

    excl_match = re.search(r"EXCLUSION:\s*([^\n]+)", query, re.IGNORECASE)
    if excl_match:
        exclusion = excl_match.group(1).strip()
        synonyms = fuzzy_match_filter(exclusion, "indication")
        filters["exclude"] = synonyms

    return filters

# ===============================================================
# CORRECTED Score Computation
# ===============================================================
def compute_score_with_breakdown(row: Dict[str, Any], site_zip_codes: List[str] = None) -> Dict[str, Any]:
    """CORRECTED scoring logic to fix identical scores"""
    pid = row.get("PATIENT_ID")
    patient_raw = merged_df.filter(pl.col("PATIENT_ID") == pid)
    breakdown = []
    score = 0
    today = datetime.today()
    
    # === 1. DIAGNOSIS RECENCY ===
    pts = int(row.get("recency_points", 0) or 0)
    recency_reason = row.get("recency_reason", "Unknown")
    
    # Get actual activity dates for detailed reason
    qualified_dates = patient_raw.filter(pl.col("ACTIVITY_CATEGORY") == "QUALIFIED RESPONDENTS").select("ACTIVITY_DATE").to_series().drop_nulls().to_list()
    
    if pts > 0 and recency_reason == "Diagnosis-based":
        years_ago = (50 - pts) / 10
        reason = f"Diagnosis temporal info: {years_ago:.1f} years ago → {pts} points"
    elif pts > 0 and recency_reason == "Recent Activity-based":
        years_ago = (50 - pts) / 10
        last_activity = qualified_dates[-1] if qualified_dates else "unknown date"
        reason = f"Recent activity date = {last_activity} (~{years_ago:.1f} years ago) → {pts} points"
    elif qualified_dates:
        last_qualified = qualified_dates[-1] if qualified_dates else None
        if last_qualified:
            parsed_date = safe_date(last_qualified)
            if parsed_date:
                years_since = (today - parsed_date).days / 365.25
                reason = f"Missing diagnosis info; last 'Qualified Respondents' date = {last_qualified} (~{years_since:.1f} years ago) → 0 points"
            else:
                reason = f"Missing diagnosis info; last 'Qualified Respondents' date = {last_qualified} (unparseable) → 0 points"
        else:
            reason = "Missing diagnosis info; no qualified respondents activity → 0 points"
    else:
        reason = "Missing diagnosis info; no activity data available → 0 points"
    
    score += pts
    breakdown.append({"criterion": "Recency", "reason": reason, "points": pts})
    
    # === 2. PPD SCREENING ===
    activities = patient_raw.select("ACTIVITY_CATEGORY").to_series().drop_nulls().unique().to_list()
    
    if "QUALIFIED RESPONDENTS" in activities:
        qualified_date = qualified_dates[-1] if qualified_dates else "unknown date"
        pts = 30
        reason = f"Found 'QUALIFIED RESPONDENTS' activity ({qualified_date}) → 30 points"
    elif "RESPONDENTS" in activities:
        pts = 20
        reason = "Found 'RESPONDENTS' activity only → 20 points"
    else:
        pts = 0
        reason = "No PPD screening record found → 0 points"
    
    score += pts
    breakdown.append({"criterion": "PPD Screening", "reason": reason, "points": pts})
    
    # === 3. SIMILAR STUDIES ===
    indications = patient_raw.select("INDICATION_NAME").to_series().drop_nulls().unique().to_list()
    study_count = len(indications)
    pts = study_count * 20
    indication_list = str(indications[:3]) if len(indications) <= 3 else str(indications[:3])[:-1] + f", +{len(indications)-3} more]"
    reason = f"{study_count} unique indication(s): {indication_list} → {pts} points"
    
    score += pts
    breakdown.append({"criterion": "Similar Studies", "reason": reason, "points": pts})
    
    # === 4. DISTANCE ===
    # Get patient zip code from merged_df (POSTAL_CODE column)
    patient_zip_data = patient_raw.select("POSTAL_CODE").unique().drop_nulls().to_series().to_list()
    patient_zip = patient_zip_data[0] if patient_zip_data else None
    
    if site_zip_codes:
        if patient_zip:
            distance = calculate_closest_site_distance(str(patient_zip), site_zip_codes)
            if distance < 10:
                pts = 20
                reason = f"Patient ZIP {patient_zip}: Distance = {distance:.1f}km to closest site → 20 points (Very close)"
            elif distance <= 50:
                pts = 15
                reason = f"Patient ZIP {patient_zip}: Distance = {distance:.1f}km to closest site → 15 points (Moderate)"
            elif distance <= 100:
                pts = 10
                reason = f"Patient ZIP {patient_zip}: Distance = {distance:.1f}km to closest site → 10 points (Far)"
            elif distance == 999:
                pts = 0
                reason = f"Patient ZIP {patient_zip} → 0 points (Unable to calculate distance)"
            else:
                pts = 5
                reason = f"Patient ZIP {patient_zip}: Distance = {distance:.1f}km to closest site → 5 points (Very far)"
        else:
            pts = 0
            reason = f"Site zip codes provided but patient zip missing → 0 points (No patient location)"
    else:
        pts = 0
        reason = f"No site zip codes provided → 0 points (Distance calculation not possible)"
    
    score += pts
    breakdown.append({"criterion": "Distance to Site", "reason": reason, "points": pts})
    
    # === 5. SMS RESPONSE === (COMMENTED OUT - TO BE IMPLEMENTED LATER)
    # sms_response = "NO"
    # if "SMS" in patient_raw.columns:
    #     sms_values = patient_raw.select("SMS").to_series().drop_nulls().to_list()
    #     sms_response = sms_values[0].upper() if sms_values else "NO"
    # 
    # recency_pts = int(row.get("recency_points", 0) or 0)
    # if sms_response in ["YES", "Y"] and recency_pts > 30:
    #     pts = 25
    #     reason = f"SMS = '{sms_response}' with recent activity → 25 points"
    # elif sms_response in ["YES", "Y"]:
    #     pts = 0
    #     reason = f"SMS = '{sms_response}' but activity too old → 0 points"
    # else:
    #     pts = 0
    #     reason = f"SMS = '{sms_response}' → No recent response"
    # 
    # score += pts
    # breakdown.append({"criterion": "SMS Response", "reason": reason, "points": pts})
    
    # === 6. PAST QUALIFICATION ===
    randomization_dates = (
        patient_raw.filter(pl.col("ACTIVITY_CATEGORY") == "RANDOMIZATION")
        .select("ACTIVITY_DATE").to_series().drop_nulls().sort().to_list()
    )
    
    latest_milestone = row.get("latest_milestone", "Unknown")
    
    if randomization_dates:
        last_randomization_date = safe_date(randomization_dates[-1])
        if last_randomization_date:
            years_since_randomization = (today - last_randomization_date).days / 365.25
            if years_since_randomization >= 1:
                pts = 25
                reason = f"Randomized on {randomization_dates[-1]} ({years_since_randomization:.1f} years ago) → 25 points"
            else:
                pts = 0
                reason = f"Recently randomized on {randomization_dates[-1]} → excluded → 0 points"
        else:
            pts = 0
            reason = f"Unparseable randomization date: {randomization_dates[-1]} → 0 points"
    else:
        pts = 0
        reason = f"Latest milestone = {latest_milestone} → No randomization history"
    
    score += pts
    breakdown.append({"criterion": "Past Qualification", "reason": reason, "points": pts})
    
    # Normalize score (max possible = ~200 points)
    normalized_score = min(1.0, score / 200.0)
    
    return {
        "total_business_score": score,
        "business_score_normalized": round(normalized_score, 4),
        "business_score_percent": round(normalized_score * 100, 2),
        "breakdown": breakdown
    }

# ===============================================================
# Filter Logic
# ===============================================================
def filter_patients(filters: Dict, top_k: int = None):
    filtered = df.clone()
    logger.info(f"Initial count: {filtered.shape[0]}")
    
    # Quick check: Do the expected patients exist in the dataset at all?
    expected_ids = [69912664, 69939975, 69903374, 69947581]
    for expected_id in expected_ids:
        exists = df.filter(pl.col("PATIENT_ID") == expected_id).shape[0] > 0
        if exists:
            patient_info = df.filter(pl.col("PATIENT_ID") == expected_id).select(["PATIENT_ID", "age", "sex", "indication"]).to_dicts()[0]
            logger.info(f"🔍 Patient {expected_id} in dataset: age={patient_info['age']}, sex='{patient_info['sex']}', indication='{patient_info['indication']}'")
        else:
            logger.info(f"🚫 Patient {expected_id} NOT in dataset")
    
    if "sex" in filters:
        before_sex = filtered.shape[0]
        # Debug: Check unique sex values in dataset
        sex_values = filtered.select("sex").unique().to_series().to_list()
        logger.info(f"Available sex values in dataset: {sex_values}")
        logger.info(f"Looking for sex = '{filters['sex']}'")
        
        filtered = filtered.filter(pl.col("sex") == filters["sex"])
        logger.info(f"Sex filter '{filters['sex']}': {before_sex} → {filtered.shape[0]}")
        
    if "age_min" in filters:
        before_age = filtered.shape[0]
        # Debug: Show age distribution for remaining patients
        if filtered.shape[0] <= 20:
            ages = filtered.select(["PATIENT_ID", "age"]).to_dicts()
            for p in ages:
                logger.info(f"  Patient {p['PATIENT_ID']}: age = {p['age']}")
        
        filtered = filtered.filter(pl.col("age") >= filters["age_min"])
        logger.info(f"Age filter '>= {filters['age_min']}': {before_age} → {filtered.shape[0]}")
        
    if "indication" in filters:
        # Show patient count per condition
        for condition in filters["indication"]:
            condition_count = filtered.filter(pl.col("indication") == condition).shape[0]
            logger.info(f"  - '{condition}': {condition_count} patients")
        
        cond = None
        for syn in filters["indication"]:
            clause = pl.col("indication").cast(pl.Utf8) == syn
            cond = clause if cond is None else (cond | clause)
        if cond is not None:
            before_indication = filtered.shape[0]
            filtered = filtered.filter(cond)
            logger.info(f"Indication filter: {before_indication} → {filtered.shape[0]} patients total")
            
            # Debug: Show patient details for each matching patient
            if filtered.shape[0] <= 10:  # Only show details if small number
                patient_details = filtered.select(["PATIENT_ID", "age", "sex", "indication", "business_score"]).to_dicts()
                for p in patient_details:
                    logger.info(f"  Patient {p['PATIENT_ID']}: age={p['age']}, sex='{p['sex']}', indication='{p['indication']}', score={p['business_score']}")
            
            patient_ids = filtered.select("PATIENT_ID").to_series().to_list()
            unique_patients = filtered.select("PATIENT_ID").unique().shape[0]
            logger.info(f"Patient IDs found: {patient_ids}")
            logger.info(f"Unique patient IDs before deduplication: {unique_patients}")
            
            # Check if we have the expected patient IDs from Excel
            expected_ids = [69912664, 69939975, 69903374, 69947581]
            for expected_id in expected_ids:
                if expected_id in patient_ids:
                    logger.info(f"✅ Found expected patient {expected_id}")
                else:
                    # Check if patient exists anywhere in the dataset
                    exists_in_dataset = df.filter(pl.col("PATIENT_ID") == expected_id).shape[0] > 0
                    if exists_in_dataset:
                        patient_info = df.filter(pl.col("PATIENT_ID") == expected_id).select(["PATIENT_ID", "age", "sex", "indication"]).to_dicts()[0]
                        logger.info(f"⚠️ Patient {expected_id} exists but filtered out: age={patient_info['age']}, sex='{patient_info['sex']}', indication='{patient_info['indication']}'")
                    else:
                        logger.info(f"❌ Patient {expected_id} does not exist in dataset at all")

    if "exclude" in filters and filters["exclude"]:
        cond = None
        for syn in filters["exclude"]:
            clause = pl.col("indication").cast(pl.Utf8).str.contains(syn, literal=False)
            cond = clause if cond is None else (cond | clause)
        if cond is not None:
            filtered = filtered.filter(~cond)
            logger.info(f"After exclusion filter: {filtered.shape[0]}")

    # Remove duplicates by keeping the highest business_score per patient
    filtered = (
        filtered.sort("business_score", descending=True)
        .group_by("PATIENT_ID")
        .first()
    )
    final_patient_ids = filtered.select("PATIENT_ID").to_series().to_list()
    logger.info(f"Final patient IDs: {final_patient_ids}")
    logger.info(f"After deduplication: {filtered.shape[0]} unique patients")
    
    # Query-specific normalization: Calculate min/max from filtered results only
    if filtered.shape[0] > 0:
        min_score = filtered.select("business_score").min().item()
        max_score = filtered.select("business_score").max().item()
        
        # Recalculate normalized scores for this query
        if max_score > min_score:
            filtered = filtered.with_columns(
                ((pl.col("business_score") - min_score) / (max_score - min_score)).alias("query_normalized_score")
            )
        else:
            filtered = filtered.with_columns(pl.lit(1.0).alias("query_normalized_score"))
        
        logger.info(f"Query-specific normalization: min={min_score}, max={max_score}")
    
    filtered = filtered.sort("business_score", descending=True)
    
    # Debug: Show score distribution
    if filtered.shape[0] > 0:
        scores = filtered.select("business_score").to_series().to_list()
        logger.info(f"Score distribution: min={min(scores)}, max={max(scores)}, count={len(scores)}")
        logger.info(f"Bottom 5 scores: {sorted(scores)[:5]}")
    
    logger.info(f"Final count: {filtered.shape[0]}")
    
    if top_k is None:
        return filtered.to_dicts()  # Return all results
    else:
        return filtered.head(top_k).to_dicts()

# ===============================================================
# API Endpoint
# ===============================================================
@app.post("/query")
def query_patients(request: QueryRequest):
    filters = parse_query(request.query)
    
    # Debug: Log available columns and site zip codes
    logger.info(f"Available columns in df: {df.columns}")
    logger.info(f"Site zip codes provided: {request.site_zip_codes}")
    
    # Early return if no indication matches found
    if "indication" in filters and not filters["indication"]:
        return {"detail": "No matching medical conditions found. Please check your query.", "total_matching_patients": 0}
    
    # Get total matching patients count before limiting
    all_matching_patients = filter_patients(filters, top_k=None)
    total_matching_patients = len(all_matching_patients)
    
    patients = filter_patients(filters, request.top_k)

    if not patients:
        return {"detail": "No patients matched the query.", "total_matching_patients": 0}

    results = []
    for p in patients:
        score_details = compute_score_with_breakdown(p, request.site_zip_codes)
        results.append({
            "patient_id": str(p["PATIENT_ID"]),
            "age": p.get("age"),
            "sex": p.get("sex"),
            "study_id": p.get("study_id"),
            "indication": p.get("indication"),
            "latest_milestone": p.get("latest_milestone"),
            "score_details": score_details,
            "_sort_score": score_details["total_business_score"]  # For sorting
        })

    # Sort by the newly calculated total_business_score in descending order
    results.sort(key=lambda x: x["_sort_score"], reverse=True)
    
    # Calculate match_score_percent and update score_details normalization based on ranking after sorting
    if results:
        max_score = results[0]["_sort_score"]
        min_score = results[-1]["_sort_score"]
        
        for result in results:
            if max_score > min_score:
                # Normalize based on actual score range: highest gets 100%, lowest gets proportional %
                normalized = (result["_sort_score"] - min_score) / (max_score - min_score)
                result["match_score_percent"] = round(normalized * 100, 2)
                # Update score_details normalization to match
                result["score_details"]["business_score_normalized"] = round(normalized, 4)
                result["score_details"]["business_score_percent"] = round(normalized * 100, 2)
            else:
                # All patients have same score
                result["match_score_percent"] = 100.0
                result["score_details"]["business_score_normalized"] = 1.0
                result["score_details"]["business_score_percent"] = 100.0
    
    # Remove the temporary sort field
    for result in results:
        del result["_sort_score"]

    return {
        "patients": results,
        "total_matching_patients": total_matching_patients,
        "returned_patients": len(results)
    }

# ===============================================================
# Health Check Endpoint
# ===============================================================
@app.get("/")
def health_check():
    return {"status": "healthy", "message": "Patient Matching API is running"}

@app.get("/conditions")
def get_available_conditions():
    """Get all available medical conditions in the dataset"""
    conditions = df.select("indication").unique().sort("indication").to_series().drop_nulls().to_list()
    return {"available_conditions": conditions, "total_count": len(conditions)}

# ===============================================================
# Run Server
# ===============================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)