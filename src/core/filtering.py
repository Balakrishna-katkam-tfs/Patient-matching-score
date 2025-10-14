"""Async patient filtering engine"""
import asyncio
import polars as pl
import logging
from typing import Dict, List, Optional
from ..data.loader import data_loader

logger = logging.getLogger(__name__)

async def filter_patients_async(filters: Dict, top_k: Optional[int] = None) -> List[Dict]:
    """Async patient filtering with batch processing"""
    df, _ = data_loader.get_datasets_sync()
    filtered = df.clone()
    
    logger.info(f"Initial count: {filtered.shape[0]}")
    
    # Apply filters in parallel where possible
    filter_tasks = []
    
    if "sex" in filters:
        filter_tasks.append(_apply_sex_filter(filtered, filters["sex"]))
    
    if "age_min" in filters:
        filter_tasks.append(_apply_age_filter(filtered, filters["age_min"]))
    
    if "indication" in filters:
        filter_tasks.append(_apply_indication_filter(filtered, filters["indication"]))
    
    if "exclude" in filters and filters["exclude"]:
        filter_tasks.append(_apply_exclusion_filter(filtered, filters["exclude"]))
    
    # Apply filters sequentially (since they depend on each other)
    for task in filter_tasks:
        filtered = await task if asyncio.iscoroutine(task) else task
    
    # Remove duplicates by keeping highest business_score per patient
    filtered = (
        filtered.sort("business_score", descending=True)
        .group_by("PATIENT_ID")
        .first()
    )
    
    logger.info(f"After deduplication: {filtered.shape[0]} unique patients")
    
    # Query-specific normalization
    if filtered.shape[0] > 0:
        min_score = filtered.select("business_score").min().item()
        max_score = filtered.select("business_score").max().item()
        
        if max_score > min_score:
            filtered = filtered.with_columns(
                ((pl.col("business_score") - min_score) / (max_score - min_score)).alias("query_normalized_score")
            )
        else:
            filtered = filtered.with_columns(pl.lit(1.0).alias("query_normalized_score"))
        
        logger.info(f"Query-specific normalization: min={min_score}, max={max_score}")
    
    filtered = filtered.sort("business_score", descending=True)
    
    logger.info(f"Final count: {filtered.shape[0]}")
    
    if top_k is None:
        return filtered.to_dicts()
    else:
        return filtered.head(top_k).to_dicts()

async def _apply_sex_filter(df: pl.DataFrame, sex: str) -> pl.DataFrame:
    """Apply sex filter"""
    before_count = df.shape[0]
    filtered = df.filter(pl.col("sex") == sex)
    logger.info(f"Sex filter '{sex}': {before_count} → {filtered.shape[0]}")
    return filtered

async def _apply_age_filter(df: pl.DataFrame, age_min: int) -> pl.DataFrame:
    """Apply age filter"""
    before_count = df.shape[0]
    filtered = df.filter(pl.col("age") >= age_min)
    logger.info(f"Age filter '>= {age_min}': {before_count} → {filtered.shape[0]}")
    return filtered

async def _apply_indication_filter(df: pl.DataFrame, indications: List[str]) -> pl.DataFrame:
    """Apply indication filter"""
    before_count = df.shape[0]
    
    cond = None
    for syn in indications:
        clause = pl.col("indication").cast(pl.Utf8) == syn
        cond = clause if cond is None else (cond | clause)
    
    if cond is not None:
        filtered = df.filter(cond)
        logger.info(f"Indication filter: {before_count} → {filtered.shape[0]} patients total")
        return filtered
    
    return df

async def _apply_exclusion_filter(df: pl.DataFrame, exclusions: List[str]) -> pl.DataFrame:
    """Apply exclusion filter"""
    before_count = df.shape[0]
    
    cond = None
    for syn in exclusions:
        clause = pl.col("indication").cast(pl.Utf8).str.contains(syn, literal=False)
        cond = clause if cond is None else (cond | clause)
    
    if cond is not None:
        filtered = df.filter(~cond)
        logger.info(f"After exclusion filter: {before_count} → {filtered.shape[0]}")
        return filtered
    
    return df