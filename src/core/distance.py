"""Distance calculation module"""
import pgeocode
import asyncio
import logging
from typing import List, Optional
from ..config.settings import settings

logger = logging.getLogger(__name__)

class DistanceCalculator:
    def __init__(self):
        self._distance_calc: Optional[pgeocode.GeoDistance] = None
        self._init_lock = asyncio.Lock()
    
    async def initialize(self):
        """Initialize distance calculator"""
        async with self._init_lock:
            if self._distance_calc is None:
                try:
                    self._distance_calc = await asyncio.to_thread(
                        pgeocode.GeoDistance, settings.DISTANCE_COUNTRY
                    )
                    logger.info("✅ Distance calculator initialized")
                except Exception as e:
                    logger.warning(f"⚠️ Distance calculator failed: {e}")
                    self._distance_calc = None
    
    async def calculate_closest_distance(
        self, patient_zip: str, site_zip_codes: List[str]
    ) -> float:
        """Calculate distance to closest site"""
        if not self._distance_calc:
            await self.initialize()
        
        if not patient_zip or not site_zip_codes or not self._distance_calc:
            return settings.DEFAULT_DISTANCE
        
        min_distance = float('inf')
        
        # Calculate distances in parallel
        tasks = [
            self._calculate_single_distance(patient_zip, site_zip)
            for site_zip in site_zip_codes
        ]
        
        distances = await asyncio.gather(*tasks, return_exceptions=True)
        
        for distance in distances:
            if isinstance(distance, (int, float)) and distance < min_distance:
                min_distance = distance
        
        return min_distance if min_distance != float('inf') else settings.DEFAULT_DISTANCE
    
    async def _calculate_single_distance(self, patient_zip: str, site_zip: str) -> float:
        """Calculate distance between two zip codes"""
        try:
            distance = await asyncio.to_thread(
                self._distance_calc.query_postal_code, patient_zip, site_zip
            )
            
            if distance is not None and not (isinstance(distance, float) and distance != distance):
                return distance
            return settings.DEFAULT_DISTANCE
        except Exception as e:
            logger.warning(f"Distance calculation error {patient_zip} to {site_zip}: {e}")
            return settings.DEFAULT_DISTANCE

# Global distance calculator instance
distance_calculator = DistanceCalculator()