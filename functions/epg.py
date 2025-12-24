#!/usr/bin/env python3
# ==============================================================================
# [FILE] functions/epg.py
# [PROJECT] StreamLedger
# [ROLE] EPG XML writing helper
# [VERSION] v1.0
# [UPDATED] 2025-12-24
# ==============================================================================

from pathlib import Path

def write_xml(epg_xml: str, file_path: str):
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(epg_xml)
