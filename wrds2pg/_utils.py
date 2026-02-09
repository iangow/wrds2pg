# wrds2pg/_utils.py
from __future__ import annotations

from datetime import datetime, timezone

def get_now() -> str:
    """Return current UTC time as a compact string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
