"""
StreamLedger — Filter Playlist (Production Step 1)

Purpose
- Download public M3U sources, parse channels, apply include/exclude rules,
  validate streams, apply manual overrides, and write outputs/curated.m3u plus outputs/report.json.

Inputs
- config/streamledger.yml (authoritative)
- config/manual_overrides.yaml (optional; additive)

Outputs
- outputs/curated.m3u
- outputs/report.json
- logs/filter_playlist.error.json (only on error)

Environment
- VALIDATION_MODE: light | deep | none (default: light)

Change Log
- 1.2.2 (2025-12-24): Added deterministic error logging + explicit exit codes (no scope removed)
- 1.2.1 (2025-12-24): Single-config selection + manual overrides + light/deep validation knobs + report warnings.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import yaml

__app__ = "StreamLedger"
__component__ = "filter_playlist"
__version__ = "1.2.2"

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = BASE_DIR / "config"
CACHE_DIR = BASE_DIR / "cache"
OUTPUTS_DIR = BASE_DIR / "outputs"
LOGS_DIR = BASE_DIR / "logs"

CACHE_DIR.mkdir(exist_ok=True)
OUTPUTS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)


@dataclass
class Channel:
    extinf_raw: str
    url: str
    tvg_id: str
    tvg_name: str
    group_title: str
    name: str


# ----------------------------
# Helpers: IO / parsing
# ----------------------------

def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def write_json(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def _attr(extinf: str, attr: str) -> str:
    m = re.search(rf'{attr}="([^"]*)"', extinf)
    return m.group(1).strip() if m else ""


def download_text(url: str, ua: str, timeout_sec: int) -> Path:
    """Cache-by-filename download. Keeps runs fast and deterministic."""
    fn = CACHE_DIR / Path(url).name
    if fn.exists() and fn.stat().st_size > 0:
        return fn
    r = requests.get(url, timeout=timeout_sec, headers={"User-Agent": ua})
    r.raise_for_status()
    fn.write_text(r.text, encoding="utf-8")
    return fn


def parse_m3u(path: Path) -> List[Channel]:
    """Parse #EXTINF + URL pairs."""
    out: List[Channel] = []
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
            out.append(
                Channel(
                    extinf_raw=ext,
                    url=line,
                    tvg_id=_attr(ext, "tvg-id"),
                    tvg_name=_attr(ext, "tvg-name"),
                    group_title=_attr(ext, "group-title"),
                    name=ext.split(",", 1)[-1].strip(),
                )
            )
            current_extinf = None
    return out


def normalize(s: str) -> str:
    return re.sub(r"\W+", "", (s or "").lower())


def canonical_key(ch: Channel) -> str:
    """Stable key for dedupe: prefer tvg-id, else tvg-name, else display name."""
    return normalize(ch.tvg_id or ch.tvg_name or ch.name)


# ----------------------------
# Matching rules
# ----------------------------

def compile_regex_list(patterns: List[str]) -> List[re.Pattern]:
    return [re.compile(p, re.IGNORECASE) for p in (patterns or [])]


def matches_any(rx_list: List[re.Pattern], text: str) -> bool:
    return bool(text) and any(rx.search(text) for rx in rx_list)


def match_channel(
    ch: Channel,
    cfg: dict,
    rx_ex_name: List[re.Pattern],
    rx_in_net: List[re.Pattern],
    rx_in_spec: List[re.Pattern],
) -> bool:
    """
    Decide if a channel qualifies *before* manual overrides.
    Manual overrides are applied later as the final authority.
    """
    name = ch.tvg_name or ch.name
    group = ch.group_title or ""

    # Exclude: group-title (exact)
    if group in (cfg.get("exclude", {}).get("group_title") or []):
        return False

    # Exclude: regex against name
    if matches_any(rx_ex_name, name):
        return False

    # News allow-only whitelist
    for token in (cfg.get("include", {}).get("news_allow_only") or []):
        if token.lower() in name.lower():
            return True

    # Network/specialty match
    if matches_any(rx_in_net, name):
        return True
    if matches_any(rx_in_spec, name):
        return True

    return False


# ----------------------------
# Validation (light/deep/none)
# ----------------------------

def stream_alive(url: str, mode: str, cfg: dict) -> Tuple[bool, str]:
    """
    Returns (alive, reason)
      light: HEAD only, treat 403/405 as "soft alive" if enabled
      deep:  HEAD then GET Range fallback for common blocked HEAD cases
      none:  always alive (no validation)
    """
    if mode == "none":
        return True, "validation=none"

    vcfg = cfg.get("validation", {})
    ua = cfg.get("pipeline", {}).get("user_agent", "StreamLedger")
    soft_403_405 = bool(vcfg.get("soft_alive_on_403_405", True))

    profile = vcfg.get("deep" if mode == "deep" else "light", {})
    timeout = int(profile.get("timeout_sec", 6))
    retries = int(profile.get("retries", 1))
    range_bytes = str(vcfg.get("deep", {}).get("range_bytes", "0-0"))

    last_exc: Optional[Exception] = None

    for _ in range(max(1, retries + 1)):
        try:
            r = requests.head(url, timeout=timeout, allow_redirects=True, headers={"User-Agent": ua})
            if r.status_code < 400:
                return True, f"head={r.status_code}"

            if mode == "light" and r.status_code in (403, 405) and soft_403_405:
                return True, f"head_soft={r.status_code}"

            if mode == "deep" and r.status_code in (403, 405, 406):
                gr = requests.get(
                    url,
                    timeout=timeout,
                    stream=True,
                    allow_redirects=True,
                    headers={"User-Agent": ua, "Range": f"bytes={range_bytes}"},
                )
                if gr.status_code < 400:
                    return True, f"get_range={gr.status_code}"
                return False, f"get_range_fail={gr.status_code}"

            return False, f"head_fail={r.status_code}"

        except Exception as e:
            last_exc = e
            time.sleep(0.2)

    return False, f"exc={type(last_exc).__name__}" if last_exc else "exc=unknown"


# ----------------------------
# Manual overrides (final authority)
# ----------------------------

def load_overrides(cfg: dict) -> dict:
    mo = cfg.get("manual_overrides", {}) or {}
    if not bool(mo.get("enabled", True)):
        return {}

    rel = str(mo.get("file", "config/manual_overrides.yaml"))
    path = (BASE_DIR / rel).resolve()
    if not path.exists():
        return {}

    raw = load_yaml(path)
    return raw.get("overrides", {}) or {}


def apply_overrides(
    channels_by_key: Dict[str, Channel],
    all_channels: List[Channel],
    cfg: dict,
    report: dict,
) -> Dict[str, Channel]:
    """
    Applies overrides as final authority:
    - Exclusions always remove from selected.
    - Inclusions attempt to add from all_channels (best candidate).
    - Adds warnings into report if override target not found.
    """
    ov = load_overrides(cfg)
    if not ov:
        return channels_by_key

    ex_ids = set((ov.get("exclude_tvg_ids") or []))
    in_ids = set((ov.get("include_tvg_ids") or []))
    ex_names = set(normalize(x) for x in (ov.get("exclude_names") or []))
    in_names = set(normalize(x) for x in (ov.get("include_names") or []))
    ex_rx = compile_regex_list(ov.get("exclude_name_regex") or [])
    in_rx = compile_regex_list(ov.get("include_name_regex") or [])

    # 1) exclusions
    removed = 0
    for key in list(channels_by_key.keys()):
        ch = channels_by_key[key]
        nm = normalize(ch.tvg_name or ch.name)
        if (ch.tvg_id and ch.tvg_id in ex_ids) or (nm in ex_names) or matches_any(ex_rx, ch.tvg_name or ch.name):
            del channels_by_key[key]
            removed += 1

    report.setdefault("overrides", {})
    report["overrides"]["excluded_removed"] = removed

    # 2) inclusions
    added = 0
    not_found: List[str] = []

    def candidates_for_include() -> List[Channel]:
        return all_channels

    for target_id in in_ids:
        found = None
        for ch in candidates_for_include():
            if ch.tvg_id == target_id:
                found = ch
                break
        if not found:
            not_found.append(f"include_tvg_id:{target_id}")
            continue
        channels_by_key[canonical_key(found)] = found
        added += 1

    for target_name in in_names:
        found = None
        for ch in candidates_for_include():
            nm = normalize(ch.tvg_name or ch.name)
            if nm == target_name:
                found = ch
                break
        if not found:
            not_found.append(f"include_name:{target_name}")
            continue
        channels_by_key[canonical_key(found)] = found
        added += 1

    for rx in in_rx:
        found_any = False
        for ch in candidates_for_include():
            nm_raw = ch.tvg_name or ch.name
            if rx.search(nm_raw or ""):
                channels_by_key[canonical_key(ch)] = ch
                found_any = True
        if not found_any:
            not_found.append(f"include_name_regex:{rx.pattern}")

    report["overrides"]["included_added"] = added
    if not_found:
        report.setdefault("warnings", []).append("manual_overrides_not_found: " + ", ".join(not_found))

    return channels_by_key


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    cfg = load_yaml(CONFIG_DIR / "streamledger.yml")

    mode = os.getenv("VALIDATION_MODE", "light").strip().lower()
    if mode not in ("light", "deep", "none"):
        mode = "light"

    ua = cfg.get("pipeline", {}).get("user_agent", "StreamLedger")
    timeout_dl = int(cfg.get("validation", {}).get("light", {}).get("timeout_sec", 6))

    rx_ex_name = compile_regex_list(cfg.get("exclude", {}).get("name_regex", []))
    rx_in_net = compile_regex_list(cfg.get("include", {}).get("networks", []))
    rx_in_spec = compile_regex_list(cfg.get("include", {}).get("specialty", []))

    # Download + parse
    all_channels: List[Channel] = []
    for url in (cfg.get("sources", {}).get("m3u") or []):
        src_path = download_text(url, ua=ua, timeout_sec=timeout_dl)
        all_channels.extend(parse_m3u(src_path))

    # Candidate filter
    candidates: List[Channel] = [ch for ch in all_channels if match_channel(ch, cfg, rx_ex_name, rx_in_net, rx_in_spec)]

    # Dedupe scoring
    def score(ch: Channel) -> int:
        s = 0
        if ch.url.startswith("https://"):
            s += 200
        elif ch.url.startswith("http://"):
            s += 100
        if ch.tvg_id:
            s += 25
        if ch.tvg_name:
            s += 10
        return s

    selected: Dict[str, Channel] = {}
    selected_score: Dict[str, int] = {}
    for ch in candidates:
        k = canonical_key(ch)
        sc = score(ch)
        if k not in selected or sc > selected_score[k]:
            selected[k] = ch
            selected_score[k] = sc

    # Validation
    alive: Dict[str, Channel] = {}
    validation_stats = {"alive": 0, "dead": 0, "soft": 0, "reasons": {}}

    for k, ch in selected.items():
        ok, reason = stream_alive(ch.url, mode=mode, cfg=cfg)
        validation_stats["reasons"][reason] = validation_stats["reasons"].get(reason, 0) + 1
        if ok:
            alive[k] = ch
            validation_stats["alive"] += 1
            if "soft" in reason:
                validation_stats["soft"] += 1
        else:
            validation_stats["dead"] += 1

    # Report
    report = {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "app": __app__,
        "component": __component__,
        "version": __version__,
        "validation_mode": mode,
        "counts": {
            "parsed_total": len(all_channels),
            "matched_pre_dedupe": len(candidates),
            "deduped_pre_validation": len(selected),
            "validated_alive": len(alive),
        },
        "validation": validation_stats,
        "warnings": [],
    }

    # Overrides final authority
    alive = apply_overrides(alive, all_channels, cfg, report)

    # Order + cap
    max_channels = int(cfg.get("pipeline", {}).get("max_channels", 750))
    min_channels = int(cfg.get("pipeline", {}).get("min_channels", 400))

    ordered = sorted(
        alive.values(),
        key=lambda c: (
            -score(c),
            (c.tvg_name or c.name).lower(),
            (c.group_title or "").lower(),
            c.url.lower(),
        ),
    )
    final_list = ordered[:max_channels]

    # Write M3U
    out = OUTPUTS_DIR / "curated.m3u"
    with out.open("w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for ch in final_list:
            f.write(f"{ch.extinf_raw}\n{ch.url}\n")

    # Warnings only (no failure)
    report["counts"]["final_written"] = len(final_list)
    report["counts"]["min_channels"] = min_channels
    report["counts"]["max_channels"] = max_channels
    if len(final_list) < min_channels:
        report["warnings"].append(f"final_written_below_min: {len(final_list)} < {min_channels}")

    write_json(OUTPUTS_DIR / "report.json", report)

    print(f"Written {len(final_list)} channels → {out}")
    print(f"Wrote report → {OUTPUTS_DIR / 'report.json'}")


if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except SystemExit:
        raise
    except Exception as e:
        err = {
            "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "component": __component__,
            "version": __version__,
            "error_type": type(e).__name__,
            "error": str(e),
        }
        write_json(LOGS_DIR / "filter_playlist.error.json", err)
        print(f"FATAL: {type(e).__name__}: {e}", file=os.sys.stderr)
        raise SystemExit(2)
