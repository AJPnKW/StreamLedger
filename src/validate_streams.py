from functions.http import head_ok

def validate_streams(channels, cfg):
    out = []
    for ch in channels:
        if head_ok(ch["url"], cfg["validation"]):
            out.append(ch)
    return out
