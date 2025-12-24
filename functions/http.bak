#!/usr/bin/env python3
# ==============================================================================
# [FILE] functions/http.py
# [PROJECT] StreamLedger
# [ROLE] HTTP fetch with cache
# [VERSION] v1.0
# [UPDATED] 2025-12-24
# ==============================================================================

import requests
from pathlib import Path

def fetch(url: str, cache_file: Path, user_agent: str = None):
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    if cache_file.exists():
        return str(cache_file)
    headers = {"User-Agent": user_agent} if user_agent else {}
    response = requests.get(url, timeout=30, headers=headers)
    response.raise_for_status()
    cache_file.write_bytes(response.content)
    return str(cache_file)
