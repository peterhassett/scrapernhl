import os
import time
import requests
import pandas as pd
from main_runner import run_pipeline, engine

# --- Global Variables ---
SEASON_ID = "20252026" 
RUN_JOB = True          
REQUEST_SLEEP = 5       

def get_all_completed_game_ids(season):
    url = f"https://api-web.nhle.com/v1/schedule-season/{season}/now"
    response = requests.get(url)
    data = response.json()
    game_ids = []
    for game in data.get('games', []):
        gid = str(game['id'])
        # Only regular season (02) and finished games (OFF)
        if gid[4:6] == '02' and game.get('gameState') == 'OFF':
            game_ids.append(game['id'])
    return game_ids

def get_already_scraped_ids():
    try:
        with engine.connect() as conn:
            df = pd.read_sql("SELECT DISTINCT game_id FROM games", conn)
            return set(df['game_id'].tolist())
    except:
        return set()

def sync_job():
    if not RUN_JOB:
        print("JOB: Switch is OFF. Exiting.")
        return

    all_finished = get_all_completed_game_ids(SEASON_ID)
    scraped = get_already_scraped_ids()
    targets = [g for g in all_finished if g not in scraped]
    
    print(f"JOB: Found {len(targets)} games to sync for season {SEASON_ID}")

    for i, gid in enumerate(targets):
        print(f"[{i+1}/{len(targets)}] Processing Game {gid}...")
        try:
            run_pipeline(gid)
            time.sleep(REQUEST_SLEEP)
        except Exception as e:
            print(f"JOB ERROR: {e}")
            continue

if __name__ == "__main__":
    sync_job()