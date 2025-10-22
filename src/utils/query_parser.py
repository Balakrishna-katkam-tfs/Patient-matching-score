"""Query parsing utilities"""
import re
import logging
from typing import Dict, List
from rapidfuzz import fuzz
from ..config.settings import settings
from ..data.loader import data_loader

logger = logging.getLogger(__name__)

def parse_query(query: str = None) -> Dict:
    """Parse natural language query into filters"""
    filters = {}
    
    # Handle empty/null query - return empty filters for zip-code-only queries
    if not query or query.strip() == "":
        logger.info("Empty query - returning all patients for zip code filtering")
        return filters
        
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
        medical_condition = re.sub(r'\b(Male|Female|age\s*>=?\s*\d+)\b', '', target, flags=re.IGNORECASE).strip()
        synonyms = fuzzy_match_filter(medical_condition, "indication")
        filters["indication"] = synonyms

    excl_match = re.search(r"EXCLUSION:\s*([^\n]+)", query, re.IGNORECASE)
    if excl_match:
        exclusion = excl_match.group(1).strip()
        synonyms = fuzzy_match_filter(exclusion, "indication")
        filters["exclude"] = synonyms

    return filters

def fuzzy_match_filter(query_term: str, column: str = "indication") -> List[str]:
    """Fuzzy match query terms against dataset values"""
    df, _ = data_loader.get_datasets_sync()
    
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
        if fuzz.ratio(query_term, val_str) >= settings.FUZZY_MATCH_THRESHOLD:
            matches.append(val)
            logger.info(f"🧠 Fuzzy match: '{query_term}' → '{val}' (score: {fuzz.ratio(query_term, val_str)})")
    
    # If no matches found, try with lower threshold for partial matches
    if not matches:
        logger.info(f"⚠️ No matches with threshold {settings.FUZZY_MATCH_THRESHOLD}, trying lower threshold ({settings.FUZZY_MATCH_FALLBACK})")
        for val in unique_values:
            val_str = str(val).lower().strip()
            if (
                fuzz.partial_ratio(query_term, val_str) >= settings.FUZZY_MATCH_FALLBACK or
                fuzz.token_sort_ratio(query_term, val_str) >= settings.FUZZY_MATCH_FALLBACK
            ):
                matches.append(val)
                logger.info(f"🔍 Partial match: '{query_term}' → '{val}' (partial: {fuzz.partial_ratio(query_term, val_str)}, token: {fuzz.token_sort_ratio(query_term, val_str)})")
    
    logger.info(f"🧠 Final matches for '{query_term}' → {matches}")
    return matches