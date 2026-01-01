"""NHL schedule data scrapers."""

from datetime import datetime
from typing import Dict, List, Union

import pandas as pd
import polars as pl

from scrapernhl.core.http import fetch_json
from scrapernhl.core.utils import json_normalize


def getScheduleData(team: str = "MTL", season: Union[str, int] = "20252026") -> List[Dict]:
    """
    Scrapes raw NHL schedule data for a given team and season.

    Parameters:
    - team (str): Team abbreviation (e.g., "MTL")
    - season (str or int): Season ID (e.g., "20242025")

    Returns:
    - List[Dict]: Raw schedule records with metadata
    """
    season = str(season)
    url = f"https://api-web.nhle.com/v1/club-schedule-season/{team}/{season}"

    try:
        response = fetch_json(url)

        # Normalize nested keys
        if isinstance(response, dict) and "games" in response:
            data = response["games"]
        elif isinstance(response, list):
            data = response
        else:
            data = [response]

    except Exception as e:
        raise RuntimeError(f"Error fetching schedule data: {e}")

    now = datetime.utcnow().isoformat()
    return [
        {**record, "scrapedOn": now, "source": "NHL Schedule API"}
        for record in data
        if isinstance(record, dict)
    ]


def scrapeSchedule(team: str = "MTL", season: Union[str, int] = "20252026", output_format: str = "pandas") -> pd.DataFrame | pl.DataFrame:
    """
    Scrapes NHL schedule data for a given team and season.

    Parameters:
    - team (str): Team abbreviation (e.g., "MTL")
    - season (str or int): Season ID (e.g., "20242025")
    - output_format (str): One of ["pandas", "polars"]

    Returns:
    - pd.DataFrame or pl.DataFrame: Schedule data with metadata in the specified format.
    """
    raw_data = getScheduleData(team, season)
    return json_normalize(raw_data, output_format)
