#!/usr/bin/env python3
# ==============================================================================
# [FILE] src/run_pipeline.py
# [PROJECT] StreamLedger
# [ROLE] Main entrypoint - download, filter, validate, build EPG, write outputs
# [VERSION] v1.0
# [UPDATED] 2025-12-24
# ==============================================================================

import logging
from pathlib import Path

# Assuming functions/ helpers exist; adjust imports if needed
from functions.config import load_config  # or direct yaml if no helper
from src.download_sources import download_all
from src.parse_m3u import parse_and_filter_m3u
from src.validate_streams import validate_streams
from src.build_epg import build_epg  # Updated to current name
from src.write_outputs import write_outputs

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    cfg_path = Path(__file__).parent.parent / "config" / "streamledger.yml"
    cfg = load_config(str(cfg_path))  # Adjust if load_config not exist

    logging.info("Starting pipeline...")
    raw = download_all(cfg)
    channels = parse_and_filter_m3u(raw["m3u"], cfg)
    channels = validate_streams(channels, cfg)

    epg_xml = build_epg(channels, raw["epg"], cfg)  # Use current build_epg

    write_outputs(channels, epg_xml, cfg)

    channel_count = len(channels)
    min_ch = cfg.get("pipeline", {}).get("min_channels", 400)
    max_ch = cfg.get("pipeline", {}).get("max_channels", 500)

    logging.info(f"Pipeline complete: {channel_count} channels")
    if not (min_ch <= channel_count <= max_ch):
        logging.error(f"Channel count {channel_count} outside range {min_ch}-{max_ch}")
        raise SystemExit(1)

if __name__ == "__main__":
    main()
