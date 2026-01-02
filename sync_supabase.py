import os
import sys
import logging
import pandas as pd
import numpy as np
from supabase import create_client, Client

# Scraper Imports
from scrapernhl import (
    scrape_game, 
    engineer_xg_features, 
    predict_xg_for_pbp, 
    on_ice_stats_by_player_strength
)
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# THE GROUND TRUTH WHITELIST - Flattened for Supabase
WHITELISTS = {
    "teams": ["id", "fullname", "teamabbrev", "teamcommonname", "teamplacename", "active_status", "conference_name", "division_name"],
    "players": ["id", "firstname_default", "lastname_default", "headshot", "positioncode", "heightininches", "weightinpounds", "birthdate", "birthcountry"],
    "rosters": ["id", "season", "teamabbrev", "sweaternumber", "positioncode"],
    "player_stats": ["id", "playerid", "season", "team", "strength", "gamesplayed", "goals", "assists", "points", "shots", "cf", "ca", "ff", "fa", "sf", "sa", "gf", "ga", "xg", "xga", "seconds", "minutes"],
    "schedule": ["id", "season", "gamedate", "gametype", "gamestate", "hometeam_id", "hometeam_abbrev", "hometeam_score", "hometeam_commonname_default", "awayteam_id", "awayteam_abbrev", "awayteam_score", "awayteam_commonname_default", "venue_default", "starttimeutc", "gamecenterlink"]
}

def clean_and_validate(df: pd.DataFrame, table_name: str, p_keys: list) -> pd.DataFrame:
    if df.empty: return df
    
    # 1. FLATTEN DOTTED COLUMNS (e.g., homeTeam.abbrev -> hometeam_abbrev)
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    # 2. SPECIFIC MAPPINGS BASED ON YOUR SAMPLES
    if table_name == "player_stats":
        if 'player1id' in df.columns: df['playerid'] = df['player1id']
        if 'eventteam' in df.columns: df['team'] = df['eventteam']
        # xG/xGA might be lowercase after step 1, ensuring they are numeric
        for col in ['xg', 'xga', 'cf', 'ca']:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')

    elif table_name == "schedule":
        # Ensure we have the snake_case keys for whitelisting
        if 'hometeam_id' not in df.columns and 'hometeam_id' in df.columns: pass # already flattened

    # 3. WHITELIST FILTER
    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()

    # 4. NUMERIC CASTING (BIGINT SAFETY)
    num_pats = ['id', 'season', 'goals', 'score', 'played']
    for col in df.columns:
        if any(p in col for p in num_pats) and not col.endswith('link'):
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round().astype(np.int64)

    # 5. DEDUPLICATE
    existing_pks = [k for k in p_keys if k in df.columns]
    if existing_pks:
        df = df.dropna(subset=existing_pks).drop_duplicates(subset=existing_pks, keep='first')
    
    return df.replace({np.nan: None})

def sync_table(table_name: str, df: pd.DataFrame, p_key_str: str):
    ready = clean_and_validate(df, table_name, p_key_str.split(','))
    if ready.empty: return
    try:
        supabase.table(table_name).upsert(ready.to_dict(orient="records"), on_conflict=p_key_str).execute()
        LOG.info(f"Synced {len(ready)} to {table_name}")
    except Exception as e:
        LOG.error(f"Sync failed for {table_name}: {e}")

def run_sync(mode="daily"):
    s_str, s_int = "20242025", 20242025
    teams_df = scrapeTeams(source="records")
    sync_table("teams", teams_df, "id")
    
    active_teams = ['MTL', 'BUF'] if mode == "debug" else teams_df['teamAbbrev'].unique().tolist()

    for team in active_teams:
        LOG.info(f"--- Processing {team} ---")
        
        # SCHEDULE & ADVANCED STATS
        schedule = scrapeSchedule(team, s_str)
        # Check for both FINAL and OFF states
        completed = schedule[schedule['gameState'].isin(['FINAL', 'OFF'])]
        
        # Sync Schedule metadata first
        sync_table("schedule", schedule, "id")

        # Process Advanced Stats from PBP
        game_ids = completed['id'].tolist()
        if mode == "debug": game_ids = game_ids[:2]

        all_game_stats = []
        for gid in game_ids:
            try:
                LOG.info(f"Calculating Advanced Stats: Game {gid}")
                pbp = scrape_game(gid)
                pbp = engineer_xg_features(pbp)
                pbp = predict_xg_for_pbp(pbp)
                stats = on_ice_stats_by_player_strength(pbp, include_goalies=False)
                all_game_stats.append(stats)
            except Exception as e:
                LOG.error(f"Advanced stats failed for {gid}: {e}")

        if