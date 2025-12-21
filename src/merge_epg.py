from functions.epg import load_epg, pick_best_source

def merge_epg(channels, epg_files, cfg):
    epgs = [load_epg(f) for f in epg_files]
    return pick_best_source(channels, epgs, cfg["priorities"]["epg"])
