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
    batch_size: int = 100
) -> List[Dict[str, Any]]:
    """Process patients in batches for better performance"""
    results = []
    
    for i in range(0, len(patients), batch_size):
        batch = patients[i:i + batch_size]
        batch_tasks = [
            compute_score_with_breakdown_async(patient, site_zip_codes)
            for patient in batch
        ]
        batch_results = await asyncio.gather(*batch_tasks)
        results.extend(batch_results)
    
    return results

async def compute_score_with_breakdown_async(
    row: Dict[str, Any], 
    site_zip_codes: List[str] = None
) -> Dict[str, Any]:
    """Async version of score computation"""
    _, merged_df = data_loader.get_datasets_sync()
    
    pid = row.get("PATIENT_ID")
    patient_raw = merged_df.filter(pl.col("PATIENT_ID") == pid)
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
    
    # === 4. DISTANCE (ASYNC) ===
    patient_zip_data = patient_raw.select("POSTAL_CODE").unique().drop_nulls().to_series().to_list()
    patient_zip = patient_zip_data[0] if patient_zip_data else None
    
    if site_zip_codes:
        if patient_zip:
            distance = await distance_calculator.calculate_closest_distance(
                str(patient_zip), site_zip_codes
            )
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
    
    # Normalize score
    normalized_score = min(1.0, score / 200.0)
    
    return {
        "total_business_score": score,
        "business_score_normalized": round(normalized_score, 4),
        "business_score_percent": round(normalized_score * 100, 2),
        "breakdown": breakdown
    }