"""Async data loading and caching"""
import polars as pl
import asyncio
import logging
from typing import Optional
from ..config.settings import settings

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self):
        self._scored_df: Optional[pl.DataFrame] = None
        self._merged_df: Optional[pl.DataFrame] = None
        self._loading_lock = asyncio.Lock()
    
    async def load_datasets(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Load datasets with async support"""
        async with self._loading_lock:
            if self._scored_df is None or self._merged_df is None:
                await self._load_data()
            return self._scored_df, self._merged_df
    
    async def _load_data(self):
        """Internal data loading method"""
        logger.info("Loading datasets...")
        
        # Load in parallel using asyncio
        scored_task = asyncio.create_task(self._load_scored_dataset())
        merged_task = asyncio.create_task(self._load_merged_dataset())
        
        self._scored_df, self._merged_df = await asyncio.gather(scored_task, merged_task)
        
        logger.info(f"✅ Scored dataset: {self._scored_df.shape}")
        logger.info(f"✅ Merged dataset: {self._merged_df.shape}")
    
    async def _load_scored_dataset(self) -> pl.DataFrame:
        """Load scored dataset"""
        return await asyncio.to_thread(
            pl.read_csv,
            settings.SCORED_DATASET_PATH,
            infer_schema_length=10000,
            ignore_errors=True,
            try_parse_dates=True
        )
    
    async def _load_merged_dataset(self) -> pl.DataFrame:
        """Load merged dataset"""
        return await asyncio.to_thread(
            pl.read_csv,
            settings.MERGED_DATASET_PATH,
            infer_schema_length=10000,
            ignore_errors=True,
            try_parse_dates=True
        )
    
    def get_datasets_sync(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Synchronous access to datasets"""
        if self._scored_df is None or self._merged_df is None:
            raise RuntimeError("Datasets not loaded. Call load_datasets() first.")
        return self._scored_df, self._merged_df

# Global data loader instance
data_loader = DataLoader()