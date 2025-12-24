#!/usr/bin/env python3
# ==============================================================================
# [FILE] functions/http.py
# [PROJECT] StreamLedger
# [ROLE] HTTP fetch with cache and HEAD check
# [VERSION] v1.0
# [UPDATED] 2025-12-24
# ==============================================================================

import requests
from pathlib import Path

def fetch(url: str, cache_file: Path, user_agent: str = None):
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    if cache_file.exists():
        return str(cache_file)
    headers = {"User-Agent": user_agent or "StreamLedger/1.0"}
    response = requests.get(url, timeout=30, headers=headers)
    response.raise_for_status()
    cache_file.write_bytes(response.content)
    return str(cache_file)

def head_ok(url: str, validation_cfg: dict) -> bool:
    timeout = validation_cfg.get("timeout_sec", 10)
    retries = validation_cfg.get("retries", 1)
    headers = {"User-Agent": "StreamLedger/1.0"}
    for _ in range(retries + 1):
        try:
            r = requests.head(url, timeout=timeout, headers=headers, allow_redirects=True)
            if r.status_code == 200:
                return True
        except Exception:
            pass
    return False
