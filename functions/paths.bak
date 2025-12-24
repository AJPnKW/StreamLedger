#!/usr/bin/env python3
# ==============================================================================
# [FILE] functions/paths.py
# [PROJECT] StreamLedger
# [ROLE] Cache path helper
# [VERSION] v1.0
# [UPDATED] 2025-12-24
# ==============================================================================

from pathlib import Path

def cache_path(url: str) -> Path:
    return Path("cache") / url.split("/")[-1]
