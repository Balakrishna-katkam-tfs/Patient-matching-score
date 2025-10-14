"""Pydantic models for Patient Matching API"""
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

class QueryRequest(BaseModel):
    query: str
    site_zip_codes: List[str] = []
    top_k: int = 50

class ScoreBreakdown(BaseModel):
    criterion: str
    reason: str
    points: int

class ScoreDetails(BaseModel):
    total_business_score: int
    business_score_normalized: float
    business_score_percent: float
    breakdown: List[ScoreBreakdown]

class PatientResult(BaseModel):
    patient_id: str
    age: Optional[int]
    sex: Optional[str]
    study_id: Optional[int]
    indication: Optional[str]
    latest_milestone: Optional[str]
    score_details: ScoreDetails
    match_score_percent: float

class QueryResponse(BaseModel):
    patients: List[PatientResult]
    total_matching_patients: int
    returned_patients: int