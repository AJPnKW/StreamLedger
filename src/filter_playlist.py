import re
import yaml
import requests
from pathlib import Path

TIMEOUT = 6

BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUTS = BASE_DIR / "outputs"
CACHE = BASE_DIR / "cache"
CONFIG = BASE_DIR / "config"

OUTPUTS.mkdir(exist_ok=True)
CACHE.mkdir(exist_ok=True)

def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def download(url):
    fn = CACHE / Path(url).name
    if fn.exists():
        return fn
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    fn.write_text(r.text, encoding="utf-8")
    return fn

def parse_m3u(path):
    channels = []
    current = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("#EXTINF"):
            current = {
                "raw": line,
                "tvg-id": re.search(r'tvg-id="([^"]*)"', line),
                "tvg-name": re.search(r'tvg-name="([^"]*)"', line),
                "group": re.search(r'group-title="([^"]*)"', line),
                "name": line.split(",", 1)[-1].strip()
            }
            current["tvg-id"] = current["tvg-id"].group(1) if current["tvg-id"] else ""
            current["tvg-name"] = current["tvg-name"].group(1) if current["tvg-name"] else ""
            current["group"] = current["group"].group(1) if current["group"] else ""
        elif line.startswith("http"):
            current["url"] = line.strip()
            channels.append(current)
    return channels

def canonical_key(ch):
    base = ch["tvg-id"] or ch["tvg-name"] or ch["name"]
    return re.sub(r"\W+", "", base).lower()

def stream_alive(url):
    try:
        r = requests.head(url, timeout=TIMEOUT, allow_redirects=True)
        return r.status_code < 400
    except Exception:
        return False

def match_channel(ch, cfg):
    name = ch["tvg-name"] or ch["name"]
    group = ch["group"]

    for rx in cfg["exclude"]["name_regex"]:
        if re.search(rx, name):
            return False

    if group in cfg["exclude"]["group_title"]:
        return False

    if any(n.lower() in name.lower() for n in cfg["include"]["news_only"]):
        return True

    for block in ("majors", "specialty"):
        for rule in cfg["include"][block]:
            if re.search(rule["pattern"], name, re.I):
                return True

    return False

def main():
    cfg = load_yaml(CONFIG / "include_channels.yaml")
    srcs = load_yaml(CONFIG / "streamledger.yml")["sources"]["m3u"]

    all_channels = []
    for url in srcs:
        all_channels.extend(parse_m3u(download(url)))

    selected = {}
    for ch in all_channels:
        if not match_channel(ch, cfg):
            continue

        key = canonical_key(ch)
        if key in selected:
            continue

        if ch["url"].startswith("https") and stream_alive(ch["url"]):
            selected[key] = ch
        elif key not in selected and stream_alive(ch["url"]):
            selected[key] = ch

    out = OUTPUTS / "curated.m3u"
    with out.open("w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for ch in selected.values():
            f.write(f"{ch['raw']}\n{ch['url']}\n")

    print(f"Written {len(selected)} channels → {out}")

if __name__ == "__main__":
    main()
