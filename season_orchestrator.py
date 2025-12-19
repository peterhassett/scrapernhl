import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from main_runner import run_pipeline, engine

# --- Global Variables  ---
SEASON_ID = "20252026"
RUN_JOB = True  
REQUEST_SLEEP = 5  # Cautious of rate-limiting

def sync_job():
    if not RUN_JOB:
        print("JOB: Switch is OFF. Exiting.")
        return

    # Fetch all games for the defined SEASON_ID
    all_finished = get_all_completed_game_ids(SEASON_ID)
    scraped = get_already_scraped_ids()
    
    # Identify games we don't have yet
    targets = [g for g in all_finished if g not in scraped]
    
    print(f"JOB: Found {len(targets)} games to sync for season {SEASON_ID}")

    for gid in targets:
        run_pipeline(gid)  # Pass the ID to the Worker
        time.sleep(REQUEST_SLEEP)

def get_all_completed_game_ids(season):
    """Fetches all games from the API that have already finished."""
    url = f"https://api-web.nhle.com/v1/schedule-season/{season}/now"
    response = requests.get(url)
    data = response.json()
    
    # Filter for Regular Season (02) games that are 'OFF'
    game_ids = []
    for game in data.get('games', []):
        gid = str(game['id'])
        if gid[4:6] == '02' and game.get('gameState') == 'OFF':
            game_ids.append(game['id'])
    return game_ids

def get_already_scraped_ids():
    """Queries your DB to see which games you already have."""
    try:
        with engine.connect() as conn:
            query = "SELECT DISTINCT game_id FROM games"
            df = pd.read_sql(query, conn)
            return set(df['game_id'].tolist())
    except:
        return set()

def sync_job(mode="daily"):
    if not RUN_JOB:
        print("JOB: Switch is OFF. Exiting.")
        return

    print(f"JOB: Starting {mode} sync for season {SEASON_ID}")
    
    # 1. Get targets
    all_finished = get_all_completed_game_ids(SEASON_ID)
    scraped = get_already_scraped_ids()
    
    # 2. Determine which games to run
    if mode == "catchup":
        # All finished games that aren't in our DB
        targets = [g for g in all_finished if g not in scraped]
    else:
        # Daily mode: Just look for games from yesterday
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        # Could also just use 'catchup' logic daily to be safe
        targets = [g for g in all_finished if g not in scraped]

    print(f"JOB: Found {len(targets)} new games to process.")

    # 3. Execution Loop
    for i, gid in enumerate(targets):
        print(f"[{i+1}/{len(targets)}] Processing Game {gid}...")
        try:
            run_pipeline(gid)
            print(f"JOB: Sleeping {REQUEST_SLEEP}s...")
            time.sleep(REQUEST_SLEEP)
        except Exception as e:
            print(f"JOB ERROR on {gid}: {e}")
            continue

if __name__ == "__main__":
    # To do the one-time catchup: sync_job(mode="catchup")
    # For daily runs: sync_job(mode="daily")
    sync_job(mode="catchup")