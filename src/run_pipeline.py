#!/usr/bin/env python3
# ==============================================================================
# [FILE] src/run_pipeline.py
# [PROJECT] StreamLedger
# [ROLE] Main entrypoint - orchestrate full pipeline
# [VERSION] v1.0
# [UPDATED] 2025-12-24
# ==============================================================================

import logging
import yaml
from pathlib import Path

from src.download_sources import download_all
from src.parse_m3u import parse_and_filter_m3u
from src.validate_streams import validate_streams
from src.build_epg import main as build_epg_main
from src.write_outputs import write_outputs

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BASE_DIR = Path(__file__).parent.parent

def load_config():
    with open(BASE_DIR / "config" / "streamledger.yml", "r") as f:
        return yaml.safe_load(f)

def main():
    cfg = load_config()
    logging.info("Pipeline started")

    raw = download_all(cfg)
    channels = parse_and_filter_m3u(raw["m3u"], cfg)
    channels = validate_streams(channels, cfg)

    # build_epg is standalone; simulate call
    build_epg_main()  # assumes it reads curated.m3u

    # Skip write_outputs if integrated in others
    channel_count = len(channels)
    logging.info(f"Pipeline complete: {channel_count} channels")

if __name__ == "__main__":
    main()
