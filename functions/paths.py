#!/usr/bin/env python3
# ==============================================================================
# [FILE] functions/paths.py
# [PROJECT] StreamLedger
# [ROLE] Path helpers for cache, outputs, archive
# [VERSION] v1.1
# [UPDATED] 2025-12-24
# ==============================================================================

from pathlib import Path
import shutil
from datetime import datetime

BASE_DIR = Path(__file__).parent.parent

def cache_path(url: str) -> Path:
    return BASE_DIR / "cache" / Path(url).name

def outputs_path(filename: str) -> str:
    return str(BASE_DIR / "outputs" / filename)

def archive_previous():
    outputs = BASE_DIR / "outputs"
    archive = BASE_DIR / "archive" / datetime.now().strftime("%Y%m%d_%H%M%S")
    if outputs.exists():
        archive.mkdir(parents=True, exist_ok=True)
        for file in outputs.iterdir():
            if file.is_file():
                shutil.copy(file, archive / file.name)
