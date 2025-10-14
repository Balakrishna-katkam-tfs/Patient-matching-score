"""Utility helper functions"""
from datetime import datetime
from typing import Optional

def safe_int(x) -> int:
    """Safely convert to integer"""
    try:
        return int(float(x))
    except Exception:
        return 0

def safe_date(date_str) -> Optional[datetime]:
    """Try parsing date strings safely"""
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(date_str).split(" ")[0], fmt)
        except Exception:
            continue
    return None