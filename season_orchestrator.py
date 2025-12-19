import os
import time
import requests
import pandas as pd
from main_runner import run_pipeline, engine

SEASON_ID = "20242025" 
SLEEP_TIME = 5

def fetch_finished_games(season):
    url = f"https://api-web.nhle.com/v1/schedule-season/{season}/now"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    # Filter for Regular Season (02) and Final (OFF)
    return [g['id'] for g in data.get('games', []) 
            if str(g['id'])[4:6] == '02' and g.get('gameState') == 'OFF']

def get_scraped_ids():
    try:
        with engine.connect() as conn:
            return set(pd.read_sql("SELECT game_id FROM games", conn)['game_id'].tolist())
    except:
        return set()

def start_sync():
    print(f"ORCHESTRATOR: Starting Season Sync for {SEASON_ID}")
    
    finished = fetch_finished_games(SEASON_ID)
    existing = get_scraped_ids()
    to_do = [g for g in finished if g not in existing]
    
    print(f"ORCHESTRATOR: Found {len(to_do)} new games to process.")

    for i, gid in enumerate(to_do):
        print(f"[{i+1}/{len(to_do)}] Syncing {gid}...")
        try:
            run_pipeline(gid)
            time.sleep(SLEEP_TIME)
        except Exception as e:
            print(f"Skipping {gid} due to error: {e}")
            continue

if __name__ == "__main__":
    start_sync()