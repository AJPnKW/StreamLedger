from functions.http import fetch
from functions.paths import cache_path

def download_all(cfg):
    m3u_files, epg_files = [], []

    for url in cfg["sources"]["m3u"]:
        m3u_files.append(fetch(url, cache_path(url)))

    for url in cfg["sources"]["epg"]:
        epg_files.append(fetch(url, cache_path(url)))

    return {"m3u": m3u_files, "epg": epg_files}
