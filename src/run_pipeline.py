from functions.config import load_config
from src.download_sources import download_all
from src.parse_m3u import parse_and_filter_m3u
from src.validate_streams import validate_streams
from src.merge_epg import merge_epg
from src.write_outputs import write_outputs

def main():
    cfg = load_config("config/streamledger.yml")

    raw = download_all(cfg)
    channels = parse_and_filter_m3u(raw["m3u"], cfg)
    channels = validate_streams(channels, cfg)

    epg_xml = merge_epg(channels, raw["epg"], cfg)
    write_outputs(channels, epg_xml, cfg)

if __name__ == "__main__":
    main()
