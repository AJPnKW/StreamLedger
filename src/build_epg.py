"""
StreamLedger v1.0 - Build Curated EPG
Purpose: Download EPG sources, parse XML/XML.GZ, select best source per channel (tvg-id priority),
         and output outputs/curated_epg.xml.gz filtered to curated.m3u channels.
"""

import gzip
import logging
import re
import shutil
from pathlib import Path
from typing import Dict, Set, Tuple, Iterable, Optional

import requests
import xml.etree.ElementTree as ET
import yaml


# ---- paths ----
BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = BASE_DIR / "config"
CACHE_DIR = BASE_DIR / "cache" / "epg"
TEMP_DIR = BASE_DIR / "temp"
OUTPUTS_DIR = BASE_DIR / "outputs"
LOG_DIR = BASE_DIR / "logs"

for d in (CACHE_DIR, TEMP_DIR, OUTPUTS_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)


# ---- logging ----
logging.basicConfig(
    filename=LOG_DIR / "build_epg.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


TIMEOUT = 12


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def http_get(url: str, dest: Path, user_agent: str) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        logging.info("Cache hit: %s", dest.name)
        return dest

    logging.info("Downloading: %s", url)
    headers = {"User-Agent": user_agent}
    r = requests.get(url, headers=headers, timeout=TIMEOUT, stream=True)
    r.raise_for_status()
    with dest.open("wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)
    return dest


def iter_lines_m3u(m3u_path: Path) -> Iterable[str]:
    with m3u_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            yield line.rstrip("\n")


def parse_curated_ids(m3u_path: Path) -> Tuple[Set[str], Dict[str, str]]:
    """
    Returns:
      - wanted_ids: set of tvg-id values (normalized as-is, not lowercased)
      - name_map:  fallback mapping (normalized key->display name) for rare no-id cases
    """
    wanted_ids: Set[str] = set()
    name_map: Dict[str, str] = {}

    tvg_id_rx = re.compile(r'tvg-id="([^"]*)"')
    tvg_name_rx = re.compile(r'tvg-name="([^"]*)"')

    for line in iter_lines_m3u(m3u_path):
        if not line.startswith("#EXTINF:"):
            continue

        tvg_id_m = tvg_id_rx.search(line)
        tvg_name_m = tvg_name_rx.search(line)

        tvg_id = (tvg_id_m.group(1) if tvg_id_m else "").strip()
        tvg_name = (tvg_name_m.group(1) if tvg_name_m else "").strip()

        # channel display name after comma
        disp = line.split(",", 1)[-1].strip()

        if tvg_id:
            wanted_ids.add(tvg_id)
            if tvg_name:
                name_map[tvg_id] = tvg_name
            else:
                name_map[tvg_id] = disp

    return wanted_ids, name_map


def open_epg_stream(path: Path):
    """
    Returns a binary file-like object for XML parsing.
    Supports .gz or plain .xml.
    """
    if path.name.lower().endswith(".gz"):
        return gzip.open(path, "rb")
    return path.open("rb")


def scan_channel_ids(epg_path: Path, wanted_ids: Set[str]) -> Set[str]:
    """
    Fast scan: returns channel ids present in this EPG that intersect wanted_ids.
    """
    found: Set[str] = set()
    try:
        with open_epg_stream(epg_path) as f:
            ctx = ET.iterparse(f, events=("start", "end"))
            for ev, el in ctx:
                if ev == "end" and el.tag == "channel":
                    cid = el.attrib.get("id", "")
                    if cid in wanted_ids:
                        found.add(cid)
                    el.clear()
    except Exception as e:
        logging.error("EPG scan failed (%s): %s", epg_path.name, e)
    return found


def write_filtered_from_source(
    epg_path: Path,
    assigned_ids: Set[str],
    channels_out: Path,
    programmes_out: Path
) -> None:
    """
    Writes matching <channel> elements to channels_out
    and matching <programme> elements to programmes_out.
    """
    if not assigned_ids:
        return

    try:
        with open_epg_stream(epg_path) as f, \
             channels_out.open("ab") as ch_out, \
             programmes_out.open("ab") as pr_out:

            ctx = ET.iterparse(f, events=("start", "end"))
            for ev, el in ctx:
                if ev != "end":
                    continue

                if el.tag == "channel":
                    cid = el.attrib.get("id", "")
                    if cid in assigned_ids:
                        ch_out.write(ET.tostring(el, encoding="utf-8"))
                        ch_out.write(b"\n")
                    el.clear()

                elif el.tag == "programme":
                    cid = el.attrib.get("channel", "")
                    if cid in assigned_ids:
                        pr_out.write(ET.tostring(el, encoding="utf-8"))
                        pr_out.write(b"\n")
                    el.clear()

    except Exception as e:
        logging.error("EPG filter/write failed (%s): %s", epg_path.name, e)


def gzip_file(src: Path, dest_gz: Path) -> None:
    with src.open("rb") as f_in, gzip.open(dest_gz, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


def main() -> int:
    cfg = load_yaml(CONFIG_DIR / "streamledger.yml")
    user_agent = cfg.get("pipeline", {}).get("user_agent", "StreamLedger/1.0")

    curated_m3u = OUTPUTS_DIR / "curated.m3u"
    if not curated_m3u.exists():
        logging.error("Missing outputs/curated.m3u. Run filter step first.")
        print("ERROR: outputs/curated.m3u not found (run filter first).")
        return 2

    wanted_ids, _name_map = parse_curated_ids(curated_m3u)
    if not wanted_ids:
        logging.error("No tvg-id values found in curated.m3u")
        print("ERROR: no tvg-id values found in outputs/curated.m3u")
        return 2

    epg_urls = cfg.get("sources", {}).get("epg", [])
    order = cfg.get("priorities", {}).get("epg", {}).get("source_order", [])
    # order is informational; selection is by listed URL order
    logging.info("Wanted channels: %d", len(wanted_ids))
    logging.info("EPG sources: %d", len(epg_urls))

    # ---- download sources ----
    local_epg_paths = []
    for url in epg_urls:
        name = Path(url).name
        dest = CACHE_DIR / name
        try:
            local_epg_paths.append(http_get(url, dest, user_agent))
        except Exception as e:
            logging.error("Download failed: %s :: %s", url, e)

    if not local_epg_paths:
        logging.error("No EPG sources downloaded successfully.")
        print("ERROR: no EPG sources downloaded successfully.")
        return 2

    # ---- assign best source per channel id (first source that contains it wins) ----
    remaining = set(wanted_ids)
    assignments: Dict[Path, Set[str]] = {}

    for epg_path in local_epg_paths:
        if not remaining:
            break
        present = scan_channel_ids(epg_path, remaining)
        if present:
            assignments[epg_path] = present
            remaining -= present
            logging.info("Assigned %d ids to %s", len(present), epg_path.name)

    if remaining:
        logging.warning("Unmatched ids (no EPG found): %d", len(remaining))

    # ---- write filtered xmltv ----
    tmp_channels = TEMP_DIR / "curated_epg.channels.xml.part"
    tmp_programmes = TEMP_DIR / "curated_epg.programmes.xml.part"
    tmp_full = TEMP_DIR / "curated_epg.xml"

    # reset temp parts
    for p in (tmp_channels, tmp_programmes, tmp_full):
        if p.exists():
            p.unlink()

    # per-source extract
    for epg_path, ids in assignments.items():
        write_filtered_from_source(epg_path, ids, tmp_channels, tmp_programmes)

    # build final xml
    with tmp_full.open("wb") as out:
        out.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        out.write(b"<tv>\n")
        if tmp_channels.exists():
            out.write(tmp_channels.read_bytes())
        if tmp_programmes.exists():
            out.write(tmp_programmes.read_bytes())
        out.write(b"</tv>\n")

    out_gz = OUTPUTS_DIR / "curated_epg.xml.gz"
    gzip_file(tmp_full, out_gz)

    logging.info("Wrote EPG: %s", out_gz)
    print(f"Written EPG → {out_gz}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
