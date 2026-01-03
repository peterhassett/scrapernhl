import os
import logging
import pandas as pd
import numpy as np
import json
from supabase import create_client, Client

# Scrapers
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.games import scrapePlays 
# Importing these but wrapping them in try/except to prevent total sync failure
try:
    from scrapernhl import engineer_xg_features, predict_xg_for_pbp, on_ice_stats_by_player_strength
    ANALYTICS_ENABLED = True
except ImportError:
    ANALYTICS_ENABLED = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Whitelist cache based on your final_schema.sql
DB_COLS = {}

def get_valid_cols(table_name):
    if table_name in DB_COLS: return DB_COLS[table_name]
    try:
        res = supabase.table(table_name).select("*").limit(1).execute()
        DB_COLS[table_name] = list(res.data[0].keys()) if res.data else []
        return DB_COLS[table_name]
    except: return []

def sanitize_to_native(df):
    """
    TOTAL ISOLATION: Converts a DataFrame to a list of standard Python dicts.
    Strips all pandas-specific types (NAType, Timestamp, etc.) immediately.
    """
    # Force conversion to standard Python types via json-roundtrip or manual map
    # This is the 'Nuclear Option' to kill NAType
    return json.loads(df.to_json(orient='records', date_format='iso'))

def literal_sync(table_name, df, p_key):
    """Syncs data using your exact SQL column naming."""
    if df.empty: return
    
    # 1. Format columns to match SQL
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    # 2. Filter by whitelist
    valid = get_valid_cols(table_name)
    if valid:
        df = df[[c for c in df.columns if c in valid]]
    
    # 3. Clean and Sync
    records = sanitize_to_native(df)
    
    try:
        supabase.table(table_name).upsert(records, on_conflict=p_key).execute()
        LOG.info(f"Sync Success: {len(records)} records to '{table_name}'")
    except Exception as e:
        LOG.error(f"DB Error for '{table_name}': {e}")

def run_sync(mode="debug"):
    S_STR, S_INT = "20242025", 20242025
    LOG.info(f"--- STARTING ISOLATION SYNC | Mode: {mode} ---")

    # 1. Teams & Standings
    literal_sync("teams", scrapeTeams(source="calendar"), "id")
    
    std = scrapeStandings()
    if not std.empty:
        std.columns = [str(c).replace('.', '_').lower() for c in std.columns]
        std['id'] = std['date'].astype(str) + "_" + std['teamabbrev_default'].astype(str)
        literal_sync("standings", std, "id")

    # 2. Roster/Game Discovery
    active_teams = ['MTL', 'BUF'] if mode == "debug" else ['MTL', 'BUF', 'TOR', 'EDM']
    global_games = set()
    
    for team in active_teams:
        ros = scrapeRoster(team, S_STR)
        if not ros.empty:
            ros['season'] = S_INT
            ros['teamabbrev'] = team
            literal_sync("players", ros.copy(), "id")
            literal_sync("rosters", ros, "id,season")

        sched = scrapeSchedule(team, S_STR)
        sched.columns = [str(c).replace('.', '_').lower() for c in sched.columns]
        # REGULAR SEASON ONLY
        sched_f = sched[(sched['gametype'] == 2) & (sched['gamestate'].isin(['FINAL', 'OFF']))]
        global_games.update(sched_f['id'].tolist())

    # 3. Isolated Game Sync
    game_list = sorted(list(global_games))
    if mode == "debug": game_list = game_list[:3]
    
    for gid in game_list:
        try:
            LOG.info(f"Syncing Game: {gid}")
            pbp = scrapePlays(gid)
            if pbp.empty: continue

            # Attempt Analytics only if isolated from the primary sync payload
            if ANALYTICS_ENABLED:
                try:
                    # We run this on a COPY to ensure the primary pbp isn't corrupted by NATypes
                    enriched = predict_xg_for_pbp(engineer_xg_features(pbp.copy()))
                    p_df = enriched
                except Exception as e:
                    LOG.warning(f"Analytics failed for {gid}, syncing raw data only: {e}")
                    p_df = pbp
            else:
                p_df = pbp

            # Play Sync
            p_df.columns = [str(c).replace('.', '_').lower() for c in p_df.columns]
            if '#' in p_df.columns: p_df = p_df.rename(columns={'#': 'sortorder'})
            p_df['id'] = p_df.apply(lambda r: f"{gid}_{r.get('sortorder', 0)}", axis=1)
            
            # This ensures even if xG fails, the 'raw_data' column stores the API response
            p_df['raw_data'] = p_df.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
            
            literal_sync("plays", p_df, "id")
        except Exception as e:
            LOG.error(f"Critical Failure for Game {gid}: {e}")

if __name__ == "__main__":
    run_sync("debug")