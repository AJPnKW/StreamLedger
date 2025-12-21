import re
from functions.m3u import read_entries, normalize_name, match_region

def parse_and_filter_m3u(m3u_files, cfg):
    allowed = []
    inc_net = set(cfg["include"]["networks"] + cfg["include"]["specialty"])
    exc_rx = [re.compile(x) for x in cfg["exclude"]["channels_regex"]]

    for file in m3u_files:
        for ch in read_entries(file):
            name = normalize_name(ch["name"])

            if any(r.search(name) for r in exc_rx):
                continue

            if not match_region(name, cfg["regions"]):
                continue

            if not any(n.lower() in name.lower() for n in inc_net):
                continue

            allowed.append(ch)

    return allowed
