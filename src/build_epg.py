#!/usr/bin/env python3

# ==============================================================================
# [FILE]    src/build_epg.py
# [PROJECT] StreamLedger
# [ROLE]    Build curated EPG from sources
# [VERSION] v1.0
# [UPDATED] 2025-12-21
# ==============================================================================

import gzip
import logging
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

import requests
import xml.etree.ElementTree as ET
import yaml

from functions.http import fetch  # existing helper (required)


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


# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    filename=LOG_DIR / "build_epg.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


# ----------------------------
# Constants
# ----------------------------
TIMEOUT = 12
TVG_ID_RX = re.compile(r'tvg-id="([^"]*)"')
TVG_NAME_RX = re.compile(r'tvg-name="([^"]*)"')


def load_cfg() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w ]+", "", s)
    return s


def download_epg(url: str, user_agent: str) -> Path:
    dest = CACHE_DIR / Path(url).name

    # Use existing helper; tolerate differing signatures
    try:
        p = fetch(url, dest, user_agent=user_agent)  # type: ignore[arg-type]
    except TypeError:
        try:
            p = fetch(url, dest)  # type: ignore[misc]
        except TypeError:
            p = fetch(url)  # type: ignore[misc]

    if isinstance(p, (str, Path)):
        return Path(p)
    return dest


def open_xml_stream(path: Path):
    if path.name.lower().endswith(".gz"):
        return gzip.open(path, "rb")
    return path.open("rb")


def parse_curated_targets(m3u_path: Path) -> Tuple[Set[str], Set[str]]:
    """
    Returns:
      wanted_ids: tvg-id values (exact)
      wanted_names: normalized tvg-name/display-name values for fallback matching
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

    # remove empty norms
    wanted_names.discard("")
    return wanted_ids, wanted_names


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
            for ev, el in ET.iterparse(f, events=("end",)):
                if el.tag != "channel":
                    continue

                cid = (el.attrib.get("id", "") or "").strip()
                if not cid:
                    el.clear()
                    continue

                norms: List[str] = []
                for dn in el.findall("display-name"):
                    norms.append(norm(dn.text or ""))

                id_to_display_norms[cid] = [n for n in norms if n]
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

        # channels unique
        for cid in sorted(kept_all_ids):
            ch_xml = channel_xml_by_id.get(cid)
            if ch_xml:
                out.write(ch_xml)
                out.write("\n")

        # programmes from assigned sources only
        for epg_path, ids in per_source_assignments.items():
            if not ids:
                continue

            try:
                with open_xml_stream(epg_path) as f:
                    for ev, el in ET.iterparse(f, events=("end",)):
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


def main() -> int:
    if not CURATED_M3U.exists():
        logging.error("Missing outputs/curated.m3u (run filter first).")
        return 2

    cfg = load_cfg()
    user_agent = cfg.get("pipeline", {}).get("user_agent", "StreamLedger/1.0")

    epg_urls = (cfg.get("sources", {}) or {}).get("epg", []) or []
    if not epg_urls:
        logging.error("No EPG sources configured.")
        return 2

    wanted_ids, wanted_names = parse_curated_targets(CURATED_M3U)
    if not wanted_ids and not wanted_names:
        logging.error("No targets parsed from curated.m3u")
        return 2

    logging.info("Targets: tvg-id=%d, name=%d", len(wanted_ids), len(wanted_names))

    # download sources
    epg_paths: List[Path] = []
    for url in epg_urls:
        try:
            p = download_epg(url, user_agent)
            epg_paths.append(p)
            logging.info("EPG ready: %s", p.name)
        except Exception as e:
            logging.error("EPG download failed: %s :: %s", url, e)

    if not epg_paths:
        logging.error("No EPG sources downloaded.")
        return 2

    # best-source assignment: first source that matches wins
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
        if remaining_names:
            name_to_ids = build_name_to_ids(id_to_display_norms)
            for nm in list(remaining_names):
                ids = name_to_ids.get(nm)
                if not ids:
                    continue
                chosen = ids[0]
                matched_ids.add(chosen)

        if matched_ids:
            per_source_assignments[epg_path] = matched_ids
            kept_all_ids |= matched_ids

            remaining_ids -= matched_ids
            remaining_names -= {n for n in remaining_names if n in build_name_to_ids(id_to_display_norms)}

            logging.info("Assigned %d channels -> %s", len(matched_ids), epg_path.name)

    if not kept_all_ids:
        logging.error("No EPG matches found for curated channels.")
        return 2

    logging.info("Kept channels with EPG: %d", len(kept_all_ids))

    write_final_xml(kept_all_ids, channel_xml_by_id_global, per_source_assignments)
    gzip_output()

    logging.info("Wrote outputs/curated_epg.xml.gz")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
