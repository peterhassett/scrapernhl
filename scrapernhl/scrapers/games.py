"""NHL game and play-by-play data scrapers."""

import logging
import requests
from datetime import datetime
from functools import lru_cache
from typing import Dict, List, Union

import pandas as pd
import polars as pl

from scrapernhl.core.http import fetch_json
from scrapernhl.core.utils import json_normalize
from scrapernhl.config import DEFAULT_HEADERS, DEFAULT_TIMEOUT

LOG = logging.getLogger(__name__)

# Session for special requests (goal replay data)
SESSION = requests.Session()


def convert_json_to_goal_url(json_url: str) -> str:
    """Convert a JSON URL to the NHL goal replay URL."""
    parts = json_url.split('/')
    game_id = parts[-2]
    event_id = parts[-1].replace('ev', '').replace('.json', '')
    return f"https://www.nhl.com/ppt-replay/goal/{game_id}/{event_id}"


def getGoalReplayData(json_url: str) -> List[Dict]:
    """
    Fetch NHL goal replay data.
    
    Args:
        json_url (str): The URL of the JSON file containing goal data.
        
    Returns:
        list[dict]: A list of dictionaries containing goal replay data.
    """
    goal_url = convert_json_to_goal_url(json_url)

    # Custom headers to simulate a browser request
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": goal_url,
        "Origin": "https://www.nhl.com",
    }

    # Make the request
    response = SESSION.get(json_url, headers={**DEFAULT_HEADERS, **headers}, timeout=DEFAULT_TIMEOUT)
    data = response.json() if response.status_code == 200 else []
    
    return data


def getGameData(game: Union[str, int], addGoalReplayData: bool = False) -> Dict:
    """
    Scrape NHL play-by-play data and enrich with metadata.
    
    Parameters:
    - game (str or int): Game ID
    - addGoalReplayData (bool): Whether to fetch goal replay data for goals
    
    Returns:
    - Dict: Complete game data with enriched plays
    """
    game = str(game)
    url = f"https://api-web.nhle.com/v1/gamecenter/{game}/play-by-play"
    now = datetime.utcnow().isoformat()
    data = {}

    try:
        response = fetch_json(url)
        if not isinstance(response, dict) or not response:
            raise ValueError(f"Unexpected response format: {response}")
        
        data = response
        extra_keys = ['gameDate', 'gameType', 'startTimeUTC', 'easternUTCOffset', 'venueUTCOffset']

        enriched_plays = []
        for play in data.get('plays', []):
            ppt_data = None
            if addGoalReplayData and play.get('pptReplayUrl'):
                ppt_data = getGoalReplayData(play['pptReplayUrl'])

            enriched_play = {
                **play,
                'pptReplayData': ppt_data,
                'gameId': data.get('id'),
                'venue': data.get('venue', {}).get('default'),
                'venueLocation': data.get('venueLocation', {}).get('default'),
                'scrapedOn': now,
                'source': 'NHL Play-by-Play API',
                **{key: data.get(key) for key in extra_keys}
            }
            enriched_plays.append(enriched_play)

        data['plays'] = enriched_plays

    except Exception as e:
        raise RuntimeError(f"Error fetching play-by-play data: {e}")

    data['scrapedOn'] = now
    data['source'] = 'NHL Play-by-Play API'
    return data


@lru_cache(maxsize=1000)
def scrapePlays(game: Union[str, int], addGoalReplayData: bool = False, output_format: str = "pandas") -> pd.DataFrame | pl.DataFrame:
    """
    Scrapes NHL game data from API for a given game ID.

    Parameters:
    - game (str or int): Game ID
    - addGoalReplayData (bool): Whether to fetch goal replay data
    - output_format (str): One of ["pandas", "polars"]

    Returns:
    - pd.DataFrame or pl.DataFrame: Play-by-play data including enriched play records with metadata in the specified format.
    """
    raw_data = getGameData(game, addGoalReplayData)
    plays = raw_data.get('plays', [])
    return json_normalize(plays, output_format)
