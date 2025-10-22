"""Dynamic global distance calculation module"""
import pgeocode
import pandas as pd
import asyncio
import logging
from math import radians, sin, cos, sqrt, atan2
from functools import lru_cache
from typing import List, Optional, Tuple
from ..config.settings import settings

logger = logging.getLogger(__name__)

# Global country codes supported by pgeocode
COUNTRY_CODES = [
    "AD","AR","AS","AT","AU","AX","AZ","BD","BE","BG","BM","BR","BY","CA","CH","CL","CO","CR","CY","CZ","DE","DK",
    "DO","DZ","EE","ES","FI","FM","FO","FR","GB","GF","GG","GL","GP","GT","GU","HR","HT","HU","IE","IM","IN","IS",
    "IT","JE","JP","KR","LI","LK","LT","LU","LV","MC","MD","MH","MK","MP","MQ","MT","MW","MX","MY","NC","NL","NO",
    "NZ","PE","PH","PK","PL","PM","PR","PT","PW","RE","RO","RS","RU","SE","SG","SI","SJ","SK","SM","TH","TR","UA",
    "US","UY","VA","VI","WF","YT","ZA"
]

# Caches
_nominatim_cache = {}
_geo_distance_cache = {}

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance using haversine formula"""
    R = 6371.0  # Earth's radius in kilometers
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2.0)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2.0)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def get_nominatim(country_code: str):
    """Get cached Nominatim object"""
    if country_code not in _nominatim_cache:
        _nominatim_cache[country_code] = pgeocode.Nominatim(country_code)
    return _nominatim_cache[country_code]

def get_geodistance_obj(country_code: str):
    """Get cached GeoDistance object"""
    if country_code not in _geo_distance_cache:
        try:
            _geo_distance_cache[country_code] = pgeocode.GeoDistance(country_code)
        except Exception:
            _geo_distance_cache[country_code] = None
    return _geo_distance_cache[country_code]

@lru_cache(maxsize=10000)
def detect_country_and_coords(postal_code: str) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    """Dynamically detect country and coordinates for postal code"""
    pc = str(postal_code).strip()
    
    # Handle country prefix format (e.g., "GB:SW1A 1AA")
    if ":" in pc:
        maybe_country, maybe_postal = pc.split(":", 1)
        maybe_country = maybe_country.upper()
        if maybe_country in COUNTRY_CODES:
            try:
                nom = get_nominatim(maybe_country)
                rec = nom.query_postal_code(maybe_postal.strip())
                if rec is not None and pd.notna(rec.latitude) and pd.notna(rec.longitude):
                    return maybe_country, float(rec.latitude), float(rec.longitude)
            except Exception:
                pass
    
    # Priority countries for common patterns
    priority_countries = []
    if pc.isdigit():
        if len(pc) == 5:  # US ZIP codes
            priority_countries = ["US", "CA", "IN"]
        elif len(pc) == 6:  # Canadian postal codes (if all digits)
            priority_countries = ["CA", "US"]
    else:
        # Non-numeric codes - prioritize common formats
        if len(pc.replace(" ", "")) <= 7:  # UK, CA format
            priority_countries = ["GB", "CA", "US"]
    
    # Try priority countries first
    search_order = priority_countries + [cc for cc in COUNTRY_CODES if cc not in priority_countries]
    
    for cc in search_order:
        try:
            nom = get_nominatim(cc)
            rec = nom.query_postal_code(pc)
            if rec is None:
                continue
            lat, lon = rec.latitude, rec.longitude
            if pd.notna(lat) and pd.notna(lon):
                return cc, float(lat), float(lon)
        except Exception:
            continue
    return None, None, None

class DistanceCalculator:
    def __init__(self):
        self._init_lock = asyncio.Lock()
    
    async def calculate_closest_distance(
        self, patient_zip: str, site_zip_codes: List[str]
    ) -> float:
        """Calculate distance to closest site using dynamic global detection"""
        if not patient_zip or not site_zip_codes:
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
        """Calculate distance between two zip codes with global support"""
        try:
            # Handle identical zip codes
            if patient_zip == site_zip:
                return 0.0
            
            # Detect countries and coordinates (cached)
            c1, lat1, lon1 = await asyncio.to_thread(detect_country_and_coords, patient_zip)
            c2, lat2, lon2 = await asyncio.to_thread(detect_country_and_coords, site_zip)
            
            # Same country - use GeoDistance if available
            if c1 and c2 and (c1 == c2):
                gd = get_geodistance_obj(c1)
                if gd is not None:
                    try:
                        dist = await asyncio.to_thread(
                            gd.query_postal_code, patient_zip, site_zip
                        )
                        if pd.notna(dist):
                            return float(dist)
                    except Exception:
                        pass
            
            # Fallback to Haversine if we have coordinates
            if all(coord is not None for coord in [lat1, lon1, lat2, lon2]):
                dist = haversine_distance(lat1, lon1, lat2, lon2)
                return dist
            
            return settings.DEFAULT_DISTANCE
            
        except Exception as e:
            logger.warning(f"Distance calculation error {patient_zip} to {site_zip}: {e}")
            return settings.DEFAULT_DISTANCE

# Global distance calculator instance
distance_calculator = DistanceCalculator()