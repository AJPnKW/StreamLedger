from functions.m3u import write_m3u
from functions.epg import write_xml
from functions.paths import outputs_path, archive_previous

def write_outputs(channels, epg_xml, cfg):
    archive_previous()
    write_m3u(channels, outputs_path("streamledger.m3u"))
    write_xml(epg_xml, outputs_path("streamledger_epg.xml"))
