"""
StreamLedger — Build EPG (Production Step 2)

Purpose
- Build a minimized EPG file containing only channels (and programmes) relevant to outputs/curated.m3u.
- Merge multiple EPG sources using streaming XML parsing (iterparse) to reduce memory usage.
- Update outputs/report.json with EPG coverage metrics.
- Treat EPG coverage as a knob (warn below soft threshold; hard-fail only below catastrophic threshold).

Inputs
- config/streamledger.yml
- outputs/curated.m3u
- Remote EPG XML sources defined in config -> downloaded to cache/

Outputs
- outputs/curated_epg.xml.gz
- updates outputs/report.json (adds epg.coverage etc.)

Config Keys (streamledger.yml)
- sources.epg[]
- pipeline.epg_coverage_soft_min
- pipeline.epg_coverage_hard_fail_below
- validation.light.timeout_sec  (used for downloads)
- epg.match_priority (documented; current implementation uses tvg-id then normalized display-name)

Matching Strategy (Practical)
- Primary: channel @id matches curated tvg-id
- Fallback: channel/display-name normalized matches curated tvg-name or display name

Exit Behavior
- Warn-only if coverage < soft_min (stored in report.json warnings)
- Hard-fail only if coverage < hard_fail_below (keeps forward progress)

Change Log
- 1.1.0 (2025-12-24): Coverage metrics in report.json; soft/hard thresholds; two-pass iterparse (channels then programmes).
"""

__app__ = "StreamLedger"
__component__ = "build_epg"
__version__ = "1.1.0"

import gzip
import json
import re
import time
from pathlib import Path
from typing import Dict, Set, Tuple, Optional

import requests
import xml.etree.ElementTree as ET
import yaml


BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = BASE_DIR / "config"
CACHE_DIR = BASE_DIR / "cache"
OUTPUTS_DIR = BASE_DIR / "outputs"

CACHE_DIR.mkdir(exist_ok=True)
OUTPUTS_DIR.mkdir(exist_ok=True)


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def normalize(s: str) -> str:
    return re.sub(r"\W+", "", (s or "").lower())


def parse_curated_m3u(path: Path) -> Tuple[Set[str], Set[str]]:
    """
    Returns (wanted_ids, wanted_names_normalized)
    """
    wanted_ids: Set[str] = set()
    wanted_names: Set[str] = set()

    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.startswith("#EXTINF"):
            continue
        tvg_id = re.search(r'tvg-id="([^"]*)"', line)
        tvg_name = re.search(r'tvg-name="([^"]*)"', line)
        name = line.split(",", 1)[-1].strip()

        if tvg_id and tvg_id.group(1).strip():
            wanted_ids.add(tvg_id.group(1).strip())
        if tvg_name and tvg_name.group(1).strip():
            wanted_names.add(normalize(tvg_name.group(1).strip()))
        if name:
            wanted_names.add(normalize(name))

    return wanted_ids, wanted_names


def download_xml(url: str, ua: str, timeout_sec: int) -> Path:
    fn = CACHE_DIR / Path(url).name
    if fn.exists() and fn.stat().st_size > 0:
        return fn

    r = requests.get(url, timeout=timeout_sec, headers={"User-Agent": ua})
    r.raise_for_status()
    fn.write_bytes(r.content)
    return fn


def is_match_channel_elem(elem: ET.Element, wanted_ids: Set[str], wanted_names: Set[str]) -> bool:
    cid = (elem.get("id") or "").strip()
    if cid and cid in wanted_ids:
        return True

    # fallback: display-name matches wanted_names
    for dn in elem.findall("display-name"):
        txt = (dn.text or "").strip()
        if txt and normalize(txt) in wanted_names:
            return True

    return False


def build_epg(cfg: dict) -> Dict[str, object]:
    curated_m3u = OUTPUTS_DIR / "curated.m3u"
    if not curated_m3u.exists():
        raise FileNotFoundError(f"Missing {curated_m3u}")

    wanted_ids, wanted_names = parse_curated_m3u(curated_m3u)

    ua = cfg["pipeline"]["user_agent"]
    timeout = int(cfg["validation"]["light"]["timeout_sec"])

    out_gz = OUTPUTS_DIR / "curated_epg.xml.gz"

    kept_channel_ids: Set[str] = set()
    channels_written = 0
    programmes_written = 0
    sources_used = 0

    with gzip.open(out_gz, "wb") as gz:
        # write minimal header
        gz.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        gz.write(b'<tv generator-info-name="StreamLedger">\n')

        # PASS 1: channels
        for url in cfg["sources"]["epg"]:
            src = download_xml(url, ua=ua, timeout_sec=timeout)
            sources_used += 1

            # iterparse streaming
            for event, elem in ET.iterparse(str(src), events=("end",)):
                if elem.tag == "channel":
                    if is_match_channel_elem(elem, wanted_ids, wanted_names):
                        cid = (elem.get("id") or "").strip()
                        if cid and cid not in kept_channel_ids:
                            kept_channel_ids.add(cid)
                            gz.write(ET.tostring(elem, encoding="utf-8"))
                            gz.write(b"\n")
                            channels_written += 1
                    elem.clear()

        # PASS 2: programmes
        for url in cfg["sources"]["epg"]:
            src = CACHE_DIR / Path(url).name  # already downloaded in pass 1 (or will exist)
            if not src.exists():
                # fallback (should not happen): download
                src = download_xml(url, ua=ua, timeout_sec=timeout)

            for event, elem in ET.iterparse(str(src), events=("end",)):
                if elem.tag == "programme":
                    ch_id = (elem.get("channel") or "").strip()
                    if ch_id and ch_id in kept_channel_ids:
                        gz.write(ET.tostring(elem, encoding="utf-8"))
                        gz.write(b"\n")
                        programmes_written += 1
                    elem.clear()

        gz.write(b"</tv>\n")

    coverage = (channels_written / max(1, len(wanted_ids) + len(wanted_names)))  # heuristic signal only

    return {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "curated_channels_ids_count": len(wanted_ids),
        "curated_names_keys_count": len(wanted_names),
        "epg_channels_written": channels_written,
        "epg_programmes_written": programmes_written,
        "sources_used": sources_used,
        "coverage_signal": coverage,
        "output": str(out_gz),
    }


def main() -> None:
    cfg = load_yaml(CONFIG_DIR / "streamledger.yml")
    result = build_epg(cfg)

    # update report.json if present
    report_path = OUTPUTS_DIR / "report.json"
    if report_path.exists():
        report = json.loads(report_path.read_text(encoding="utf-8"))
    else:
        report = {"warnings": []}

    soft_min = float(cfg["pipeline"].get("epg_coverage_soft_min", 0.80))
    hard_fail = float(cfg["pipeline"].get("epg_coverage_hard_fail_below", 0.30))

    # Coverage metric we can actually trust for gating is: matched channels / curated count
    # Here we approximate curated count by ids count (best available without full mapping table).
    curated_count = max(1, int(report.get("counts", {}).get("final_written", 1)))
    matched_channels = int(result["epg_channels_written"])
    coverage = matched_channels / curated_count

    report.setdefault("epg", {})
    report["epg"].update(
        {
            "matched_channels": matched_channels,
            "curated_channels": curated_count,
            "coverage": round(coverage, 4),
            "sources_used": result["sources_used"],
            "programmes_written": result["epg_programmes_written"],
        }
    )

    if coverage < soft_min:
        report.setdefault("warnings", []).append(f"epg_coverage_below_soft_min: {coverage:.3f} < {soft_min:.3f}")

    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    # hard fail only if catastrophically low (keeps forward progress)
    if coverage < hard_fail:
        raise SystemExit(f"EPG coverage hard-fail: {coverage:.3f} < {hard_fail:.3f}")

    print(f"Wrote EPG → {OUTPUTS_DIR / 'curated_epg.xml.gz'}")
    print(f"Updated report → {report_path}")


if __name__ == "__main__":
    main()
