#!/usr/bin/env python3
# ==============================================================================
# [FILE]     src/filter_playlist.py
# [PROJECT]  StreamLedger
# [PURPOSE]  End-to-end playlist build step:
#            - Download M3U sources
#            - Parse channels
#            - De-duplicate
#            - Validate streams (light/deep/none)
#            - Apply include/exclude rules + manual overrides
#            - Write outputs/curated.m3u + outputs/report.json
#            - Write logs/filter_playlist.debug.json for RCA
#
# [CI]       Never hard-fails on low channel count (warn only). Hard-fails only
#            on true fatal errors (missing config, download total failure).
#
# [VERSION]  1.2.3
# [UPDATED]  2025-12-24
# ==============================================================================

from __future__ import annotations

import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import yaml


BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config" / "streamledger.yml"
MANUAL_OVERRIDES_PATH = BASE_DIR / "config" / "manual_overrides.yaml"

CACHE_DIR = BASE_DIR / "cache"
OUTPUTS_DIR = BASE_DIR / "outputs"
LOGS_DIR = BASE_DIR / "logs"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

PARSED_JSON = CACHE_DIR / "parsed_channels.json"
CURATED_M3U = OUTPUTS_DIR / "curated.m3u"
REPORT_JSON = OUTPUTS_DIR / "report.json"
DEBUG_JSON = LOGS_DIR / "filter_playlist.debug.json"


@dataclass
class Channel:
    tvg_id: str = ""
    tvg_name: str = ""
    group_title: str = ""
    name: str = ""
    url: str = ""
    source: str = ""          # which M3U URL it came from
    validated: Optional[bool] = None
    validate_code: Optional[int] = None
    validate_reason: str = ""


# ----------------------------
# Utility
# ----------------------------

def _read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def _norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _compile_rx_list(patterns: List[str]) -> List[re.Pattern]:
    out = []
    for p in patterns or []:
        out.append(re.compile(p, flags=re.IGNORECASE))
    return out


def _matches_any(rx_list: List[re.Pattern], text: str) -> bool:
    t = text or ""
    return any(rx.search(t) for rx in rx_list)


def _get_user_agent(cfg: dict) -> str:
    return (cfg.get("pipeline", {}) or {}).get("user_agent", "StreamLedger/1.x")


def _validation_mode() -> str:
    # VALIDATION_MODE env var: light | deep | none
    return (os.getenv("VALIDATION_MODE") or "light").strip().lower()


# ----------------------------
# Download + Parse M3U
# ----------------------------

def download_text(url: str, ua: str, timeout_sec: int = 20) -> Tuple[bool, str, str]:
    """Returns (ok, text, error)"""
    try:
        r = requests.get(url, headers={"User-Agent": ua}, timeout=timeout_sec)
        if r.status_code != 200:
            return False, "", f"HTTP {r.status_code}"
        return True, r.text, ""
    except Exception as e:
        return False, "", f"{type(e).__name__}: {e}"


_ATTR_RX = re.compile(r'(\w[\w-]*)="([^"]*)"')


def parse_m3u_text(text: str, source_url: str) -> List[Channel]:
    channels: List[Channel] = []
    if not text:
        return channels

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    current: Optional[Channel] = None

    for ln in lines:
        if ln.startswith("#EXTINF:"):
            # Example:
            # #EXTINF:-1 tvg-id="X" tvg-name="Y" group-title="Z",Display Name
            attrs = dict(_ATTR_RX.findall(ln))
            name = ""
            if "," in ln:
                name = ln.split(",", 1)[1].strip()

            current = Channel(
                tvg_id=_norm(attrs.get("tvg-id", "")),
                tvg_name=_norm(attrs.get("tvg-name", "")),
                group_title=_norm(attrs.get("group-title", "")),
                name=_norm(name),
                url="",
                source=source_url,
            )
        elif ln.startswith("#"):
            continue
        else:
            # URL line
            if current is not None:
                current.url = ln
                # backfill display name
                if not current.tvg_name:
                    current.tvg_name = current.name
                if not current.name:
                    current.name = current.tvg_name
                channels.append(current)
                current = None

    return channels


def load_all_sources(cfg: dict, dbg: dict) -> List[Channel]:
    ua = _get_user_agent(cfg)
    srcs = ((cfg.get("sources", {}) or {}).get("m3u", []) or [])
    if not srcs:
        raise ValueError("No sources.m3u configured in config/streamledger.yml")

    downloaded_ok = 0
    all_channels: List[Channel] = []
    failures = []

    for url in srcs:
        ok, txt, err = download_text(url, ua=ua, timeout_sec=25)
        if not ok:
            failures.append({"url": url, "error": err})
            continue
        downloaded_ok += 1
        parsed = parse_m3u_text(txt, url)
        all_channels.extend(parsed)

    dbg["download"] = {
        "sources": srcs,
        "downloaded_ok": downloaded_ok,
        "download_failures": failures,
        "parsed_total": len(all_channels),
    }

    if downloaded_ok == 0:
        # true fatal: nothing to work with
        raise RuntimeError(f"All M3U source downloads failed: {failures[:3]}")

    return all_channels


# ----------------------------
# De-dup + Validate
# ----------------------------

def dedupe_channels(channels: List[Channel], dbg: dict) -> List[Channel]:
    seen = set()
    out = []
    dupes = 0

    for ch in channels:
        key = (ch.tvg_id.lower() if ch.tvg_id else ch.name.lower(), ch.url)
        if key in seen:
            dupes += 1
            continue
        seen.add(key)
        out.append(ch)

    dbg["dedupe"] = {"input": len(channels), "output": len(out), "dupes_removed": dupes}
    return out


def head_check(url: str, ua: str, timeout_sec: int, range_bytes: Optional[str] = None) -> Tuple[bool, int, str]:
    headers = {"User-Agent": ua}
    if range_bytes:
        headers["Range"] = range_bytes

    try:
        r = requests.head(url, headers=headers, timeout=timeout_sec, allow_redirects=True)
        code = r.status_code

        # Many IPTV sources behave weirdly; treat 200/301/302/303/307/308 as OK
        if code in (200, 301, 302, 303, 307, 308):
            return True, code, "ok"

        # Some servers block HEAD; optionally accept 403/405 as "alive" in light mode
        return False, code, "head_not_ok"

    except Exception as e:
        return False, 0, f"{type(e).__name__}: {e}"


def validate_streams(channels: List[Channel], cfg: dict, dbg: dict) -> None:
    mode = _validation_mode()
    ua = _get_user_agent(cfg)

    vcfg = cfg.get("validation", {}) or {}
    soft_alive = bool(vcfg.get("soft_alive_on_403_405", True))

    profile = (vcfg.get(mode, {}) or {}) if mode in ("light", "deep") else {}
    timeout_sec = int(profile.get("timeout_sec", 6)) if profile else 0
    retries = int(profile.get("retries", 1)) if profile else 0
    range_bytes = profile.get("range_bytes") if profile else None

    dbg["validation"] = {
        "mode": mode,
        "timeout_sec": timeout_sec,
        "retries": retries,
        "range_bytes": range_bytes,
        "soft_alive_on_403_405": soft_alive,
    }

    if mode == "none":
        for ch in channels:
            ch.validated = None
        return

    alive = 0
    dead = 0
    codes: Dict[str, int] = {}
    sample_failures = []

    for ch in channels:
        ok_final = False
        code_final = 0
        reason_final = ""

        for attempt in range(retries + 1):
            ok, code, reason = head_check(ch.url, ua=ua, timeout_sec=timeout_sec, range_bytes=range_bytes)
            # treat 403/405 as alive if configured
            if not ok and soft_alive and code in (403, 405):
                ok = True
                reason = "soft_alive_403_405"

            if ok:
                ok_final = True
                code_final = code
                reason_final = reason
                break

            # retry delay (small)
            time.sleep(0.15)

            ok_final = False
            code_final = code
            reason_final = reason

        ch.validated = ok_final
        ch.validate_code = code_final
        ch.validate_reason = reason_final

        k = str(code_final)
        codes[k] = codes.get(k, 0) + 1

        if ok_final:
            alive += 1
        else:
            dead += 1
            if len(sample_failures) < 15:
                sample_failures.append({"name": ch.name, "url": ch.url, "code": code_final, "reason": reason_final})

    dbg["validation"]["alive"] = alive
    dbg["validation"]["dead"] = dead
    dbg["validation"]["codes"] = codes
    dbg["validation"]["sample_failures"] = sample_failures


# ----------------------------
# Include / Exclude + Manual Overrides
# ----------------------------

def load_manual_overrides(cfg: dict, dbg: dict) -> dict:
    mcfg = cfg.get("manual_overrides", {}) or {}
    enabled = bool(mcfg.get("enabled", False))
    dbg["manual_overrides"] = {"enabled": enabled, "path": str(MANUAL_OVERRIDES_PATH)}

    if not enabled:
        return {}

    if not MANUAL_OVERRIDES_PATH.exists():
        dbg["manual_overrides"]["status"] = "missing_file"
        return {}

    data = _read_yaml(MANUAL_OVERRIDES_PATH) or {}
    dbg["manual_overrides"]["status"] = "loaded"
    dbg["manual_overrides"]["counts"] = {
        "include_tvg_id": len((data.get("include", {}) or {}).get("tvg_id", []) or []),
        "include_name": len((data.get("include", {}) or {}).get("name", []) or []),
        "include_name_regex": len((data.get("include", {}) or {}).get("name_regex", []) or []),
        "exclude_tvg_id": len((data.get("exclude", {}) or {}).get("tvg_id", []) or []),
        "exclude_name": len((data.get("exclude", {}) or {}).get("name", []) or []),
        "exclude_name_regex": len((data.get("exclude", {}) or {}).get("name_regex", []) or []),
    }
    return data


def apply_rules(channels: List[Channel], cfg: dict, overrides: dict, dbg: dict) -> List[Channel]:
    include_cfg = cfg.get("include", {}) or {}
    exclude_cfg = cfg.get("exclude", {}) or {}

    network_rx = _compile_rx_list(include_cfg.get("networks", []) or [])
    specialty_rx = _compile_rx_list(include_cfg.get("specialty", []) or [])
    news_allow = [x for x in (include_cfg.get("news_allow_only", []) or []) if str(x).strip()]

    excl_groups = set(exclude_cfg.get("group_title", []) or [])
    excl_name_rx = _compile_rx_list(exclude_cfg.get("name_regex", []) or [])

    inc = overrides.get("include", {}) or {}
    exc = overrides.get("exclude", {}) or {}

    inc_ids = set((inc.get("tvg_id", []) or []))
    inc_names = set(_norm(x).lower() for x in (inc.get("name", []) or []))
    inc_name_rx = _compile_rx_list(inc.get("name_regex", []) or [])

    exc_ids = set((exc.get("tvg_id", []) or []))
    exc_names = set(_norm(x).lower() for x in (exc.get("name", []) or []))
    exc_name_rx = _compile_rx_list(exc.get("name_regex", []) or [])

    reasons = {"forced_out": 0, "excluded_group": 0, "excluded_name_regex": 0, "not_included": 0, "not_alive": 0}
    kept: List[Channel] = []
    dropped = 0

    for ch in channels:
        name = _norm(ch.tvg_name or ch.name)
        name_l = name.lower()
        tvg_id = (ch.tvg_id or "").strip()
        group = _norm(ch.group_title)

        # validation gate: if validated is False, drop
        if ch.validated is False:
            dropped += 1
            reasons["not_alive"] += 1
            continue

        # forced exclude
        if (tvg_id and tvg_id in exc_ids) or (name_l and name_l in exc_names) or _matches_any(exc_name_rx, name):
            dropped += 1
            reasons["forced_out"] += 1
            continue

        # exclude rules
        if group and group in excl_groups:
            dropped += 1
            reasons["excluded_group"] += 1
            continue
        if name and _matches_any(excl_name_rx, name):
            dropped += 1
            reasons["excluded_name_regex"] += 1
            continue

        # forced include
        if (tvg_id and tvg_id in inc_ids) or (name_l and name_l in inc_names) or _matches_any(inc_name_rx, name):
            kept.append(ch)
            continue

        # include rules
        is_news = (group.lower() == "news")
        if is_news and news_allow:
            if any(tok.lower() in name_l for tok in news_allow):
                kept.append(ch)
            else:
                dropped += 1
                reasons["not_included"] += 1
            continue

        if _matches_any(network_rx, name) or _matches_any(specialty_rx, name):
            kept.append(ch)
        else:
            dropped += 1
            reasons["not_included"] += 1

    dbg["filter"] = {"kept": len(kept), "dropped": dropped, "reasons": reasons}
    return kept


# ----------------------------
# Outputs
# ----------------------------

def write_curated_m3u(channels: List[Channel]) -> None:
    lines = ["#EXTM3U"]
    for ch in channels:
        tvg_id = ch.tvg_id or ""
        tvg_name = ch.tvg_name or ch.name or ""
        group = ch.group_title or ""
        display = ch.name or tvg_name
        lines.append(f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{tvg_name}" group-title="{group}",{display}')
        lines.append(ch.url)
    CURATED_M3U.write_text("\n".join(lines) + "\n", encoding="utf-8")


def update_report(cfg: dict, dbg: dict, curated_count: int) -> None:
    rep: Dict[str, Any] = {}
    if REPORT_JSON.exists():
        try:
            rep = json.loads(REPORT_JSON.read_text(encoding="utf-8"))
        except Exception:
            rep = {}

    rep.setdefault("playlist", {})
    rep["playlist"]["written_channels"] = curated_count
    rep["playlist"]["validation_mode"] = _validation_mode()

    # min/max info (warn only)
    pipe = cfg.get("pipeline", {}) or {}
    rep["playlist"]["min_channels"] = int(pipe.get("min_channels", 0) or 0)
    rep["playlist"]["max_channels"] = int(pipe.get("max_channels", 0) or 0)

    rep.setdefault("debug", {})
    rep["debug"]["filter"] = dbg.get("filter", {})
    rep["debug"]["download"] = dbg.get("download", {})
    rep["debug"]["dedupe"] = dbg.get("dedupe", {})
    rep["debug"]["validation"] = dbg.get("validation", {})
    rep["debug"]["manual_overrides"] = dbg.get("manual_overrides", {})

    _write_json(REPORT_JSON, rep)


# ----------------------------
# Main
# ----------------------------

def main
