#!/usr/bin/env python3
# ==============================================================================
# [FILE] functions/m3u.py
# [PROJECT] StreamLedger
# [ROLE] M3U parsing and matching helpers
# [VERSION] v1.0
# [UPDATED] 2025-12-24
# ==============================================================================

import re

def read_entries(file_path: str):
    entries = []
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("#EXTINF:"):
            info = line
            i += 1
            if i < len(lines):
                url_line = lines[i].strip()
                if url_line.startswith("http"):
                    # Parse attributes
                    attrs = {}
                    parts = info[8:].split(" ", 1)
                    if len(parts) > 1:
                        attr_str = parts[0]
                        name = parts[1].split(",", 1)[-1] if "," in parts[1] else parts[1]
                        for pair in attr_str.split(" "):
                            if "=" in pair:
                                k, v = pair.split("=", 1)
                                attrs[k] = v.strip('"')
                    else:
                        name = info.split(",", 1)[-1] if "," in info else ""
                    entries.append({"name": name.strip(), "url": url_line, **attrs})
        i += 1
    return entries

def normalize_name(name: str) -> str:
    name = re.sub(r'\s+[\[\(].*?[\]\)]', '', name)
    return name.strip().lower()

def match_region(name: str, regions: dict) -> bool:
    lower = name.lower()
    for data in regions.values():
        locations = data.get("locations", []) + data.get("markets", []) + data.get("allow_suffix", [])
        if any(loc.lower() in lower for loc in locations):
            return True
    return False  # or True if no strict region
