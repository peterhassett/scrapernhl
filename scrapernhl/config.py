"""Config.py : Constants, headers, session configuration for NHL data scraping"""
# DEFAULTS
DEFAULT_TEAM = "MTL" # Montreal Canadiens
DEFAULT_SEASON = "20252026" # 2025-2026 NHL Season
DEFAULT_DATE = "2025-11-11" # RANDOM DATE IN SEASON




# API ENDPOINTS
NHL_API_BASE_URL = "https://api-web.nhle.com"
NHL_API_BASE_URL_V1 = f"{NHL_API_BASE_URL}/v1"

STANDINGS_ENDPOINT = f"{NHL_API_BASE_URL_V1}/standings/{{date}}" # date in YYYY-MM-DD format
TEAM_SCHEDULE_ENDPOINT = f"{NHL_API_BASE_URL_V1}/club-schedule-season/{{team}}/{{season}}" # team_abbreviation in XXX format, season in YYYYYYYY format
FRANCHISES_ENDPOINT = f"{NHL_API_BASE_URL}/stats/rest/en/franchise?sort=fullName&include=lastSeason.id&include=firstSeason.id"



# Constants
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36",
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

DEFAULT_TIMEOUT = 10  # seconds