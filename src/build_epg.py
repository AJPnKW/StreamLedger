#!/usr/bin/env python3
# ==============================================================================
# [FILE]     src/build_epg.py
# [PROJECT]  StreamLedger
# [ROLE]     Build curated EPG from sources (resilient / knobs-not-blockers)
# [VERSION]  v1.2.2
# [UPDATED]  2025-12-24
# ==============================================================================

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



from __future__ import annotations

import gzip
import json
import logging
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
import xml.etree.ElementTree as ET
import yaml

# ----------------------------
# Paths
# ----------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config" / "streamledger.yml"
OUTPUTS_DIR = BASE_DIR / "outputs"
CACHE_DIR = BASE_DIR / "cache" / "epg"
TEMP_DIR = BASE_DIR / "temp"
LOG_DIR = BASE_DIR / "logs"

for d in (OUTPUTS_DIR, CACHE_DIR, TEMP_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

CURATED_M3U = OUTPUTS_DIR / "curated.m3u"
OUT_GZ = OUTPUTS_DIR / "curated_epg.xml.gz"
TMP_XML = TEMP_DIR / "curated_epg.xml"
REPORT_JSON = OUTPUTS_DIR / "report.json"

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    filename=LOG_DIR / "build_epg.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# ----------------------------
# Regex helpers
# ----------------------------
TVG_ID_RX = re.compile(r'tvg-id="([^"]*)"')
TVG_NAME_RX = re.compile(r'tvg-name="([^"]*)"')

# ----------------------------
# YAML / Report
# ----------------------------

def load_cfg() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_report() -> dict:
    if REPORT_JSON.exists():
        try:
            return json.loads(REPORT_JSON.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def write_report(rep: dict) -> None:
    REPORT_JSON.write_text(json.dumps(rep, indent=2, ensure_ascii=False), encoding="utf-8")


def add_warning(rep: dict, msg: str) -> None:
    rep.setdefault("warnings", [])
    rep["warnings"].append(msg)


# ----------------------------
# Normalization
# ----------------------------

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w ]+", "", s)
    return s


# ----------------------------
# Download (resilient)
# ----------------------------

def http_get_to_file(url: str, dest: Path, user_agent: str, timeout_sec: int) -> Tuple[bool, str]:
    """
    Returns (ok, reason). Never raises.
    """
    try:
        r = requests.get(url, timeout=timeout_sec, headers={"User-Agent": user_agent}, stream=True)
        if r.status_code >= 400:
            return False, f"http={r.status_code}"
        with dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
        if dest.exists() and dest.stat().st_size > 0:
            return True, "ok"
        return False, "empty"
    except Exception as e:
        return False, f"exc={type(e).__name__}"


def download_epg(url: str, user_agent: str, timeout_sec: int, retries: int) -> Tuple[Optional[Path], str]:
    """
    Download an EPG source into cache. Supports .xml and .xml.gz.
    Returns (path_or_none, reason). Never raises.
    """
    dest = CACHE_DIR / Path(url).name

    # Cache hit
    if dest.exists() and dest.stat().st_size > 0:
        return dest, "cache_hit"

    last_reason = "unknown"
    for _ in range(max(1, retries + 1)):
        ok, reason = http_get_to_file(url, dest, user_agent=user_agent, timeout_sec=timeout_sec)
        last_reason = reason
        if ok:
            return dest, reason
        time.sleep(0.25)

    return None, last_reason


def open_xml_stream(path: Path):
    if path.name.lower().endswith(".gz"):
        return gzip.open(path, "rb")
    return path.open("rb")


# ----------------------------
# Targets from curated.m3u
# ----------------------------

def parse_curated_targets(m3u_path: Path) -> Tuple[Set[str], Set[str]]:
    """
    Returns:
      wanted_ids: tvg-id values (exact)
      wanted_names: normalized tvg-name/display-name fallback values
    """
    wanted_ids: Set[str] = set()
    wanted_names: Set[str] = set()

    with m3u_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("#EXTINF:"):
                continue

            m_id = TVG_ID_RX.search(line)
            m_nm = TVG_NAME_RX.search(line)
            tvg_id = (m_id.group(1) if m_id else "").strip()
            tvg_name = (m_nm.group(1) if m_nm else "").strip()
            disp = line.split(",", 1)[-1].strip()

            if tvg_id:
                wanted_ids.add(tvg_id)

            wanted_names.add(norm(tvg_name or disp))

    wanted_names.discard("")
    return wanted_ids, wanted_names


# ----------------------------
# EPG indexing
# ----------------------------

def index_epg_channels(epg_path: Path) -> Tuple[Dict[str, List[str]], Dict[str, str]]:
    """
    Returns:
      id_to_display_norms: channel_id -> list of normalized display-name
      id_to_channel_xml: channel_id -> serialized <channel> xml (utf-8)
    """
    id_to_display_norms: Dict[str, List[str]] = {}
    id_to_channel_xml: Dict[str, str] = {}

    try:
        with open_xml_stream(epg_path) as f:
            for _ev, el in ET.iterparse(f, events=("end",)):
                if el.tag != "channel":
                    continue

                cid = (el.attrib.get("id", "") or "").strip()
                if not cid:
                    el.clear()
                    continue

                norms: List[str] = []
                for dn in el.findall("display-name"):
                    n = norm(dn.text or "")
                    if n:
                        norms.append(n)

                id_to_display_norms[cid] = norms
                id_to_channel_xml[cid] = ET.tostring(el, encoding="utf-8").decode("utf-8", errors="ignore")
                el.clear()

    except Exception as e:
        logging.error("Index channels failed: %s :: %s", epg_path.name, e)

    return id_to_display_norms, id_to_channel_xml


def build_name_to_ids(id_to_display_norms: Dict[str, List[str]]) -> Dict[str, List[str]]:
    name_to_ids: Dict[str, List[str]] = {}
    for cid, norms in id_to_display_norms.items():
        for n in norms:
            name_to_ids.setdefault(n, []).append(cid)
    return name_to_ids


# ----------------------------
# Output writer
# ----------------------------

def write_final_xml(
    kept_all_ids: Set[str],
    channel_xml_by_id: Dict[str, str],
    per_source_assignments: Dict[Path, Set[str]],
) -> None:
    if TMP_XML.exists():
        TMP_XML.unlink()

    with TMP_XML.open("w", encoding="utf-8", newline="\n") as out:
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        out.write("<tv>\n")

        # channels
        for cid in sorted(kept_all_ids):
            ch_xml = channel_xml_by_id.get(cid)
            if ch_xml:
                out.write(ch_xml)
                out.write("\n")

        # programmes
        for epg_path, ids in per_source_assignments.items():
            if not ids:
                continue
            try:
                with open_xml_stream(epg_path) as f:
                    for _ev, el in ET.iterparse(f, events=("end",)):
                        if el.tag == "programme":
                            cid = (el.attrib.get("channel", "") or "").strip()
                            if cid in ids:
                                out.write(ET.tostring(el, encoding="utf-8").decode("utf-8", errors="ignore"))
                                out.write("\n")
                            el.clear()
                        elif el.tag == "channel":
                            el.clear()
            except Exception as e:
                logging.error("Programme extract failed: %s :: %s", epg_path.name, e)

        out.write("</tv>\n")


def gzip_output() -> None:
    if OUT_GZ.exists():
        OUT_GZ.unlink()
    with TMP_XML.open("rb") as f_in, gzip.open(OUT_GZ, "wb") as f_out:
        while True:
            chunk = f_in.read(1024 * 256)
            if not chunk:
                break
            f_out.write(chunk)


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    if not CURATED_M3U.exists():
        logging.error("Missing outputs/curated.m3u (run filter first).")
        return 2

    cfg = load_cfg()
    rep = load_report()

    user_agent = (cfg.get("pipeline", {}) or {}).get("user_agent", "StreamLedger/1.0")

    # knobs
    soft_min = float((cfg.get("pipeline", {}) or {}).get("epg_coverage_soft_min", 0.80))
    hard_fail = float((cfg.get("pipeline", {}) or {}).get("epg_coverage_hard_fail_below", 0.30))

    vcfg = cfg.get("validation", {}) or {}
    timeout = int((vcfg.get("deep", {}) or {}).get("timeout_sec", 10))
    retries = int((vcfg.get("deep", {}) or {}).get("retries", 2))

    epg_urls = ((cfg.get("sources", {}) or {}).get("epg") or [])
    if not epg_urls:
        logging.error("No EPG sources configured.")
        add_warning(rep, "epg_no_sources_configured")
        write_report(rep)
        return 2

    wanted_ids, wanted_names = parse_curated_targets(CURATED_M3U)
    total_targets = max(1, len(wanted_ids) + len(wanted_names))
    logging.info("Targets: tvg-id=%d, name=%d", len(wanted_ids), len(wanted_names))

    # download sources (resilient)
    epg_paths: List[Path] = []
    per_url_status: List[dict] = []

    for url in epg_urls:
        p, reason = download_epg(url, user_agent=user_agent, timeout_sec=timeout, retries=retries)
        if p:
            epg_paths.append(p)
            per_url_status.append({"url": url, "ok": True, "reason": reason, "file": p.name})
            logging.info("EPG ready: %s", p.name)
        else:
            per_url_status.append({"url": url, "ok": False, "reason": reason})
            logging.warning("EPG download failed: %s :: %s", url, reason)

    rep.setdefault("epg", {})
    rep["epg"]["sources"] = per_url_status

    if not epg_paths:
        add_warning(rep, "epg_all_sources_failed_download")
        write_report(rep)
        return 2

    # assignment pass
    remaining_ids = set(wanted_ids)
    remaining_names = set(wanted_names)

    per_source_assignments: Dict[Path, Set[str]] = {}
    channel_xml_by_id_global: Dict[str, str] = {}
    kept_all_ids: Set[str] = set()

    for epg_path in epg_paths:
        if not remaining_ids and not remaining_names:
            break

        id_to_display_norms, id_to_channel_xml = index_epg_channels(epg_path)

        for cid, xml_str in id_to_channel_xml.items():
            if cid not in channel_xml_by_id_global:
                channel_xml_by_id_global[cid] = xml_str

        matched_ids: Set[str] = set()

        # tvg-id priority
        if remaining_ids:
            for cid in list(remaining_ids):
                if cid in id_to_display_norms:
                    matched_ids.add(cid)

        # name fallback
        name_to_ids = build_name_to_ids(id_to_display_norms)
        if remaining_names:
            for nm in list(remaining_names):
                ids = name_to_ids.get(nm)
                if not ids:
                    continue
                matched_ids.add(ids[0])

        if matched_ids:
            per_source_assignments[epg_path] = matched_ids
            kept_all_ids |= matched_ids

            # remove matched ids
            remaining_ids -= matched_ids

            # remove matched names that were satisfied by this source
            matched_names = {nm for nm in list(remaining_names) if nm in name_to_ids}
            remaining_names -= matched_names

        logging.info("Assigned %d channels -> %s", len(matched_ids), epg_path.name)

    if not kept_all_ids:
        add_warning(rep, "epg_no_matches_found")
        rep["epg"]["coverage"] = 0.0
        write_report(rep)
        return 2

    # coverage knobs
    coverage = len(kept_all_ids) / max(1, len(wanted_ids) if wanted_ids else len(wanted_names))
    rep["epg"]["matched_channels"] = len(kept_all_ids)
    rep["epg"]["target_channels"] = (len(wanted_ids) if wanted_ids else len(wanted_names))
    rep["epg"]["coverage"] = round(coverage, 4)

    if coverage < soft_min:
        add_warning(rep, f"epg_coverage_below_soft_min:{coverage:.4f}<{soft_min:.2f}")

    if coverage < hard_fail:
        add_warning(rep, f"epg_coverage_below_hard_fail:{coverage:.4f}<{hard_fail:.2f}")
        write_report(rep)
        return 2

    # write outputs
    write_final_xml(kept_all_ids, channel_xml_by_id_global, per_source_assignments)
    gzip_output()
    logging.info("Wrote outputs/curated_epg.xml.gz")

    write_report(rep)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
