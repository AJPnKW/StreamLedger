"""
StreamLedger — Filter Playlist (Production Step 1)

Purpose
- Build a curated M3U playlist from multiple public English/CA/US/UK/AU sources.
- Apply deterministic include/exclude rules, de-duplicate channels, optionally validate streams,
  and write outputs/curated.m3u plus outputs/report.json for QA/visibility.

Inputs
- config/streamledger.yml  (single source of truth)
- Remote M3U sources defined in config -> downloaded to cache/

Outputs
- outputs/curated.m3u
- outputs/report.json

Config Keys (streamledger.yml)
- pipeline.min_channels, pipeline.max_channels
- sources.m3u[]
- include.networks[], include.specialty[], include.news_allow_only[]
- exclude.group_title[], exclude.name_regex[]
- priorities.stream.protocol_order[], priorities.stream.feed_order[]
- validation.soft_alive_on_403_405
- validation.light.timeout_sec, validation.light.retries
- validation.deep.timeout_sec, validation.deep.retries, validation.deep.range_bytes

Environment Variables
- VALIDATION_MODE: light | deep | none  (default: light)

Determinism Guarantees
- Selection uses a stable score + stable tie-break sort.
- Capping to max_channels is deterministic (no upstream ordering dependency).

Exit Behavior
- Always writes curated.m3u and report.json.
- Does NOT hard-fail build on channel count; instead writes warnings to report.json.
  (Hard gates, if any, are handled by CI checks or subsequent steps.)

Change Log
- 1.1.0 (2025-12-24): Config becomes single-source-of-truth; deterministic scoring/cap; validation modes; report.json.
"""

__app__ = "StreamLedger"
__component__ = "filter_playlist"
__version__ = "1.1.0"

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import yaml


BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = BASE_DIR / "config"
CACHE_DIR = BASE_DIR / "cache"
OUTPUTS_DIR = BASE_DIR / "outputs"

CACHE_DIR.mkdir(exist_ok=True)
OUTPUTS_DIR.mkdir(exist_ok=True)


@dataclass
class Channel:
    extinf_raw: str
    url: str
    tvg_id: str
    tvg_name: str
    group_title: str
    name: str


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def write_json(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def _rx_attr(attr: str, s: str) -> str:
    m = re.search(rf'{attr}="([^"]*)"', s)
    return m.group(1).strip() if m else ""


def download_text(url: str, ua: str, timeout_sec: int) -> Path:
    fn = CACHE_DIR / Path(url).name
    if fn.exists() and fn.stat().st_size > 0:
        return fn

    r = requests.get(url, timeout=timeout_sec, headers={"User-Agent": ua})
    r.raise_for_status()
    fn.write_text(r.text, encoding="utf-8")
    return fn


def parse_m3u(path: Path) -> List[Channel]:
    channels: List[Channel] = []
    current_extinf: Optional[str] = None

    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue

        if line.startswith("#EXTINF"):
            current_extinf = line
            continue

        if current_extinf and (line.startswith("http://") or line.startswith("https://")):
            ext = current_extinf
            tvg_id = _rx_attr("tvg-id", ext)
            tvg_name = _rx_attr("tvg-name", ext)
            group_title = _rx_attr("group-title", ext)
            name = ext.split(",", 1)[-1].strip()

            channels.append(
                Channel(
                    extinf_raw=ext,
                    url=line,
                    tvg_id=tvg_id,
                    tvg_name=tvg_name,
                    group_title=group_title,
                    name=name,
                )
            )
            current_extinf = None

    return channels


def normalize(s: str) -> str:
    return re.sub(r"\W+", "", (s or "").lower())


def canonical_key(ch: Channel) -> str:
    base = ch.tvg_id or ch.tvg_name or ch.name
    return normalize(base)


def compile_regex_list(patterns: List[str]) -> List[re.Pattern]:
    out: List[re.Pattern] = []
    for p in patterns or []:
        out.append(re.compile(p, re.IGNORECASE))
    return out


def matches_any(rx_list: List[re.Pattern], text: str) -> bool:
    if not text:
        return False
    return any(rx.search(text) for rx in rx_list)


def score_channel(ch: Channel, cfg: dict) -> int:
    """Higher is better. Stable scoring -> deterministic capping."""
    s = 0

    # protocol preference
    proto_order = cfg["priorities"]["stream"]["protocol_order"]
    if ch.url.startswith("https://"):
        s += 200 if (proto_order and proto_order[0] == "https") else 150
    elif ch.url.startswith("http://"):
        s += 100

    # feed preference
    feed_order = [f.lower() for f in cfg["priorities"]["stream"]["feed_order"]]
    nm = (ch.tvg_name or ch.name).lower()
    for i, f in enumerate(feed_order):
        if f in nm:
            s += 50 - i  # earlier feed gets higher

    # prefer having tvg-id
    if ch.tvg_id:
        s += 25

    # prefer having tvg-name
    if ch.tvg_name:
        s += 10

    # group title presence
    if ch.group_title:
        s += 3

    return s


def stream_alive(url: str, mode: str, cfg: dict) -> Tuple[bool, str]:
    """
    Returns (alive, reason).
    - light: HEAD only (soft-alive on 403/405 if enabled)
    - deep: HEAD then GET Range fallback
    - none: always alive (no validation)
    """
    if mode == "none":
        return True, "validation=none"

    vcfg = cfg["validation"]
    ua = cfg["pipeline"]["user_agent"]

    if mode == "deep":
        timeout = int(vcfg["deep"]["timeout_sec"])
        retries = int(vcfg["deep"]["retries"])
        range_bytes = str(vcfg["deep"].get("range_bytes", "0-0"))
    else:
        timeout = int(vcfg["light"]["timeout_sec"])
        retries = int(vcfg["light"]["retries"])
        range_bytes = "0-0"

    soft_403_405 = bool(vcfg.get("soft_alive_on_403_405", True))

    last_exc = None
    for _ in range(max(1, retries + 1)):
        try:
            r = requests.head(url, timeout=timeout, allow_redirects=True, headers={"User-Agent": ua})
            if r.status_code < 400:
                return True, f"head={r.status_code}"
            if r.status_code in (403, 405) and soft_403_405 and mode == "light":
                return True, f"head_soft={r.status_code}"
            if mode == "deep" and r.status_code in (403, 405, 406):
                # try small GET with Range
                gr = requests.get(
                    url,
                    timeout=timeout,
                    stream=True,
                    headers={"User-Agent": ua, "Range": f"bytes={range_bytes}"},
                    allow_redirects=True,
                )
                if gr.status_code < 400:
                    return True, f"get_range={gr.status_code}"
                return False, f"get_range_fail={gr.status_code}"
            return False, f"head_fail={r.status_code}"
        except Exception as e:
            last_exc = e
            time.sleep(0.2)

    return False, f"exc={type(last_exc).__name__}" if last_exc else "exc=unknown"


def match_channel(ch: Channel, cfg: dict, rx_ex_name: List[re.Pattern], rx_in_net: List[re.Pattern], rx_in_spec: List[re.Pattern]) -> bool:
    name = ch.tvg_name or ch.name
    group = ch.group_title or ""

    # exclude group
    if group in (cfg["exclude"].get("group_title") or []):
        return False

    # exclude regex
    if matches_any(rx_ex_name, name):
        return False

    # allow-only news (explicit whitelist)
    allow_news = cfg["include"].get("news_allow_only") or []
    for token in allow_news:
        if token.lower() in name.lower():
            return True

    # match networks/specialty
    if matches_any(rx_in_net, name):
        return True
    if matches_any(rx_in_spec, name):
        return True

    return False


def main() -> None:
    cfg = load_yaml(CONFIG_DIR / "streamledger.yml")
    mode = os.getenv("VALIDATION_MODE", "light").strip().lower()
    if mode not in ("light", "deep", "none"):
        mode = "light"

    rx_ex_name = compile_regex_list(cfg["exclude"].get("name_regex", []))
    rx_in_net = compile_regex_list(cfg["include"].get("networks", []))
    rx_in_spec = compile_regex_list(cfg["include"].get("specialty", []))

    # download + parse all sources
    ua = cfg["pipeline"]["user_agent"]
    timeout_dl = int(cfg["validation"]["light"]["timeout_sec"])

    all_channels: List[Channel] = []
    for url in cfg["sources"]["m3u"]:
        src_path = download_text(url, ua=ua, timeout_sec=timeout_dl)
        all_channels.extend(parse_m3u(src_path))

    # filter + best-of dedupe
    selected: Dict[str, Channel] = {}
    selected_score: Dict[str, int] = {}
    drop_reason_counts: Dict[str, int] = {}

    pre_count = 0
    for ch in all_channels:
        if not match_channel(ch, cfg, rx_ex_name, rx_in_net, rx_in_spec):
            drop_reason_counts["filtered_out"] = drop_reason_counts.get("filtered_out", 0) + 1
            continue

        pre_count += 1
        key = canonical_key(ch)
        sc = score_channel(ch, cfg)

        # keep best scored candidate for same key (deterministic)
        if key not in selected or sc > selected_score[key]:
            selected[key] = ch
            selected_score[key] = sc

    # validate (optional)
    alive: Dict[str, Channel] = {}
    validation_stats = {"alive": 0, "dead": 0, "soft": 0, "reasons": {}}

    for key, ch in selected.items():
        ok, reason = stream_alive(ch.url, mode=mode, cfg=cfg)
        validation_stats["reasons"][reason] = validation_stats["reasons"].get(reason, 0) + 1
        if ok:
            alive[key] = ch
            validation_stats["alive"] += 1
            if "soft" in reason:
                validation_stats["soft"] += 1
        else:
            validation_stats["dead"] += 1

    # deterministic ordering + cap
    max_channels = int(cfg["pipeline"]["max_channels"])
    min_channels = int(cfg["pipeline"]["min_channels"])

    ordered = sorted(
        alive.values(),
        key=lambda c: (
            -score_channel(c, cfg),
            (c.tvg_name or c.name).lower(),
            (c.group_title or "").lower(),
            c.url.lower(),
        ),
    )

    final_list = ordered[:max_channels]
    out_m3u = OUTPUTS_DIR / "curated.m3u"
    with out_m3u.open("w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for ch in final_list:
            f.write(f"{ch.extinf_raw}\n{ch.url}\n")

    # report
    report = {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "validation_mode": mode,
        "sources": {"m3u_count": len(cfg["sources"]["m3u"]), "epg_count": len(cfg["sources"]["epg"])},
        "counts": {
            "parsed_total": len(all_channels),
            "matched_pre_dedupe": pre_count,
            "deduped_pre_validation": len(selected),
            "validated_alive": len(alive),
            "final_written": len(final_list),
            "min_channels": min_channels,
            "max_channels": max_channels,
        },
        "validation": validation_stats,
        "drops": drop_reason_counts,
        "warnings": [],
    }

    if len(final_list) < min_channels:
        report["warnings"].append(f"final_written_below_min: {len(final_list)} < {min_channels}")

    write_json(OUTPUTS_DIR / "report.json", report)

    print(f"Written {len(final_list)} channels → {out_m3u}")
    print(f"Wrote report → {OUTPUTS_DIR / 'report.json'}")


if __name__ == "__main__":
    main()
