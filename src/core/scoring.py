"""Async scoring engine for patient matching"""
import asyncio
import polars as pl
from datetime import datetime
from typing import Dict, Any, List
from ..core.distance import distance_calculator
from ..data.loader import data_loader
from ..utils.helpers import safe_date

async def compute_score_batch(
    patients: List[Dict[str, Any]], 
    site_zip_codes: List[str] = None,
    batch_size: int = 500,
    max_concurrent: int = 100
) -> List[Dict[str, Any]]:
    """Process patients in optimized batches with concurrency control"""
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_with_semaphore(patient):
        async with semaphore:
            return await compute_score_with_breakdown_async(patient, site_zip_codes)
    
    # Process all patients concurrently with semaphore control
    tasks = [process_with_semaphore(patient) for patient in patients]
    return await asyncio.gather(*tasks, return_exceptions=True)

# Cache merged_df at module level to avoid repeated calls
_cached_merged_df = None

async def compute_score_with_breakdown_async(
    row: Dict[str, Any], 
    site_zip_codes: List[str] = None
) -> Dict[str, Any]:
    """Async version of score computation"""
    global _cached_merged_df
    if _cached_merged_df is None:
        _, _cached_merged_df = data_loader.get_datasets_sync()
    
    pid = row.get("PATIENT_ID")
    patient_raw = _cached_merged_df.filter(pl.col("PATIENT_ID") == pid)
    breakdown = []
    score = 0
    today = datetime.today()
    
    # === 1. DIAGNOSIS RECENCY ===
    pts = int(row.get("recency_points", 0) or 0)
    recency_reason = row.get("recency_reason", "Unknown")
    
    qualified_dates = patient_raw.filter(
        pl.col("ACTIVITY_CATEGORY") == "QUALIFIED RESPONDENTS"
    ).select("ACTIVITY_DATE").to_series().drop_nulls().to_list()
    
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
    
    # Get dates for each activity type
    released_dates = patient_raw.filter(
        pl.col("ACTIVITY_CATEGORY") == "RELEASED"
    ).select("ACTIVITY_DATE").to_series().drop_nulls().to_list()
    
    respondents_dates = patient_raw.filter(
        pl.col("ACTIVITY_CATEGORY") == "RESPONDENTS"
    ).select("ACTIVITY_DATE").to_series().drop_nulls().to_list()
    
    if "RELEASED" in activities:
        released_date = released_dates[-1] if released_dates else "unknown date"
        pts = 40
        reason = f"Found 'RELEASED' activity ({released_date}) → 40 points"
    elif "QUALIFIED RESPONDENTS" in activities:
        qualified_date = qualified_dates[-1] if qualified_dates else "unknown date"
        pts = 30
        reason = f"Found 'QUALIFIED RESPONDENTS' activity ({qualified_date}) → 30 points"
    elif "RESPONDENTS" in activities:
        respondents_date = respondents_dates[-1] if respondents_dates else "unknown date"
        pts = 20
        reason = f"Found 'RESPONDENTS' activity ({respondents_date}) → 20 points"
    else:
        pts = 0
        reason = "No PPD screening record found → 0 points"
    
    score += pts
    breakdown.append({"criterion": "PPD Screening", "reason": reason, "points": pts})
    
    # === 3. SIMILAR STUDIES ===
    # Priority-based scoring for indications based on activity type
    indication_scores = {}
    
    # Get indications for each activity type with priority scoring
    released_indications = patient_raw.filter(
        pl.col("ACTIVITY_CATEGORY") == "RELEASED"
    ).select("INDICATION_NAME").to_series().drop_nulls().unique().to_list()
    
    qualified_indications = patient_raw.filter(
        pl.col("ACTIVITY_CATEGORY") == "QUALIFIED RESPONDENTS"
    ).select("INDICATION_NAME").to_series().drop_nulls().unique().to_list()
    
    respondent_indications = patient_raw.filter(
        pl.col("ACTIVITY_CATEGORY") == "RESPONDENTS"
    ).select("INDICATION_NAME").to_series().drop_nulls().unique().to_list()
    
    # Score each indication based on highest priority activity
    for indication in released_indications:
        indication_scores[indication] = max(indication_scores.get(indication, 0), 40)
    
    for indication in qualified_indications:
        indication_scores[indication] = max(indication_scores.get(indication, 0), 30)
    
    for indication in respondent_indications:
        indication_scores[indication] = max(indication_scores.get(indication, 0), 20)
    
    # Calculate total points and build reason
    pts = sum(indication_scores.values())
    study_count = len(indication_scores)
    
    if study_count > 0:
        indication_list = list(indication_scores.keys())[:3]
        if len(indication_scores) <= 3:
            reason = f"{study_count} indication(s) with priority scoring: {indication_list} → {pts} points"
        else:
            reason = f"{study_count} indication(s) with priority scoring: {indication_list}, +{len(indication_scores)-3} more → {pts} points"
    else:
        reason = "0 indication(s) found → 0 points"
    
    score += pts
    breakdown.append({"criterion": "Similar Studies", "reason": reason, "points": pts})
    
    # === 4. DISTANCE (ASYNC) ===
    patient_zip_data = patient_raw.select("POSTAL_CODE").unique().drop_nulls().to_series().to_list()
    patient_zip = patient_zip_data[0] if patient_zip_data else None
    
    if site_zip_codes:
        if patient_zip:
            # Quick exact match check first
            if str(patient_zip) in site_zip_codes:
                distance = 0.0
            else:
                distance = await distance_calculator.calculate_closest_distance(
                    str(patient_zip), site_zip_codes
                )
            if distance == 0.0:
                pts = 20
                reason = f"Patient ZIP {patient_zip}: Distance = {distance:.1f}km to closest site → 20 points (Exact match)"
            elif distance < 10:
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
    
    # === 5. PAST QUALIFICATION ===
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
    
    # Return raw score - normalization handled in handlers.py
    return {
        "total_business_score": score,
        "business_score_normalized": 0,  # Will be calculated in handlers.py
        "business_score_percent": 0,     # Will be calculated in handlers.py
        "breakdown": breakdown
    }