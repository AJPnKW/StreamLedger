#!/usr/bin/env python3

# ==============================================================================
# [FILE]    src/test_pipeline.py
# [PROJECT] StreamLedger
# [ROLE]    Test pipeline locally (simulate GitHub Actions)
# [VERSION] v1.0
# [UPDATED] 2025-12-21
# ==============================================================================

import gzip
import logging
import re
import subprocess
from pathlib import Path
from typing import Tuple

import xml.etree.ElementTree as ET


BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUTS_DIR = BASE_DIR / "outputs"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

CURATED_M3U = OUTPUTS_DIR / "curated.m3u"
CURATED_EPG_GZ = OUTPUTS_DIR / "curated_epg.xml.gz"

logging.basicConfig(
    filename=LOG_DIR / "test_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def run_cmd(args: list) -> int:
    logging.info("RUN: %s", " ".join(args))
    p = subprocess.run(args, cwd=str(BASE_DIR), capture_output=True, text=True)
    if p.stdout:
        logging.info("STDOUT: %s", p.stdout.strip())
    if p.stderr:
        logging.warning("STDERR: %s", p.stderr.strip())
    return p.returncode


def count_m3u_channels(path: Path) -> int:
    if not path.exists():
        return 0
    n = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if line.startswith("#EXTINF:"):
                n += 1
    return n


def epg_coverage(epg_gz: Path, m3u_path: Path) -> Tuple[float, int, int]:
    """
    Coverage = channels in curated.m3u with tvg-id that appear as <channel id="..."> in EPG.
    Returns: (coverage_ratio, matched, total_with_id)
    """
    tvg_ids = set()
    rx = re.compile(r'tvg-id="([^"]+)"')

    with m3u_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if not line.startswith("#EXTINF:"):
                continue
            m = rx.search(line)
            if m:
                tvg_ids.add(m.group(1).strip())

    total = len(tvg_ids)
    if total == 0:
        return 0.0, 0, 0

    found = set()
    try:
        with gzip.open(epg_gz, "rb") as f:
            for ev, el in ET.iterparse(f, events=("end",)):
                if el.tag == "channel":
                    cid = (el.attrib.get("id", "") or "").strip()
                    if cid in tvg_ids:
                        found.add(cid)
                    el.clear()
    except Exception as e:
        logging.error("EPG parse failed: %s", e)
        return 0.0, 0, total

    matched = len(found)
    return matched / total, matched, total


def validate_outputs() -> int:
    # channel count 400-500
    ch_count = count_m3u_channels(CURATED_M3U)
    logging.info("Channels: %d", ch_count)
    if not (400 <= ch_count <= 500):
        logging.error("Channel count out of range (expected 400-500): %d", ch_count)
        return 2

    # EPG exists
    if not CURATED_EPG_GZ.exists():
        logging.error("Missing outputs/curated_epg.xml.gz")
        return 2

    # coverage > 80%
    cov, matched, total = epg_coverage(CURATED_EPG_GZ, CURATED_M3U)
    logging.info("EPG coverage: %.2f (matched=%d total=%d)", cov, matched, total)
    if total > 0 and cov < 0.80:
        logging.error("EPG coverage below threshold 0.80: %.2f", cov)
        return 2

    return 0


def main() -> int:
    # simulate GitHub Actions: run the two pipeline steps
    rc = run_cmd(["python", "src/filter_playlist.py"])
    if rc != 0:
        logging.error("filter_playlist.py failed: rc=%d", rc)
        return 2

    rc = run_cmd(["python", "src/build_epg.py"])
    if rc != 0:
        logging.error("build_epg.py failed: rc=%d", rc)
        return 2

    rc = validate_outputs()
    if rc != 0:
        logging.error("Validation failed: rc=%d", rc)
        return rc

    logging.info("Pipeline test PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
