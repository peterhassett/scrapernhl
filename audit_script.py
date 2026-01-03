import pandas as pd
import numpy as np
import json
import logging
from scrapernhl.scrapers.games import scrapePlays

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

def deep_inspect_game(game_id):
    LOG.info(f"--- DEEP INSPECTION: Game {game_id} ---")
    try:
        # 1. Fetch raw data BEFORE any processing or xG calculation
        df = scrapePlays(game_id)
        if df.empty:
            print("No data returned.")
            return

        print(f"Scanned {len(df)} rows. Searching for NAType...")

        # 2. Targeted Search for the specific NAType object
        found_problem = False
        for col in df.columns:
            # We must check the TYPE of the values, because NAType isn't a standard float
            is_natype = df[col].apply(lambda x: type(x).__name__ == 'NAType')
            if is_natype.any():
                found_problem = True
                print(f"!!! CULPRIT FOUND: Column '{col}' contains NAType objects.")
                print(f"    Index of first occurrence: {df.index[is_natype][0]}")
                print(f"    Sample value: {df[col][is_natype].iloc[0]}")
                
                # Check the context of this event
                problem_row = df[is_natype].iloc[0].to_dict()
                print(f"    Event Context: {problem_row.get('event', 'Unknown')} at {problem_row.get('time', 'Unknown')}")

        if not found_problem:
            print("No NAType found in raw data. The error likely occurs during xG engineering/prediction.")
            print("Checking columns that typically cause issues: xcoord, ycoord, duration, player1id")
            for col in ['xcoord', 'ycoord', 'duration', 'player1id']:
                if col in df.columns:
                    print(f"  Column '{col}' dtype: {df[col].dtype}")

    except Exception as e:
        print(f"Audit failed: {e}")

if __name__ == "__main__":
    for gid in [2025020004, 2025020009, 2025020010]:
        deep_inspect_game(gid)