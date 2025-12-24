# StreamLedger – TiviMate Usage

## GitHub Raw URLs
Replace `OWNER` and `REPO` with your GitHub org/user and repo name.

- **M3U**
  - `https://raw.githubusercontent.com/OWNER/REPO/main/outputs/curated.m3u`

- **EPG (XML.GZ)**
  - `https://raw.githubusercontent.com/OWNER/REPO/main/outputs/curated_epg.xml.gz`

## TiviMate Setup
1. Open **TiviMate**
2. Go to **Settings → Playlists → Add playlist**
3. Choose **M3U playlist**
4. Paste the **M3U raw URL** above
5. Save

## Add EPG
1. Go to **Settings → EPG → EPG sources → Add source**
2. Paste the **EPG raw URL** above (xml.gz is supported)
3. Save
4. Go to **Settings → EPG** and run **Update EPG**

## Notes
- Outputs update every **6 hours** via GitHub Actions.
- If EPG shows missing data, force an EPG refresh in TiviMate after the next scheduled update.
