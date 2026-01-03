import os
import sys
import logging
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from supabase import create_client, Client

# Scrapers - Using modern scrapePlays to avoid legacy report 404s
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.games import scrapePlays 
from scrapernhl import engineer_xg_features, predict_xg_for_pbp, on_ice_stats_by_player_strength

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Local cache to strictly enforce the final_schema.sql columns
DB_COLS = {}

def get_valid_cols(table_name):
    """Dynamically identifies SQL columns to prevent batch failures (PGRST204)."""
    if table_name in DB_COLS: return DB_COLS[table_name]
    try:
        res = supabase.table(table_name).select("*").limit(1).execute()
        DB_COLS[table_name] = list(res.data[0].keys()) if res.data else []
        return DB_COLS[table_name]
    except: return []

def literal_sync(table_name, df, p_key):
    """
    Standardizes names, handles 'NAType' errors, and filters by final_schema.sql.
    """
    if df.empty: return
    
    # 1. Column Alignment (Mirror CSV)
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    # 2. Schema Discovery Filter
    valid = get_valid_cols(table_name)
    if valid:
        df = df[[c for c in df.columns if c in valid]]
    
    # 3. CRITICAL: Fix NAType/float conversion error
    # Standard .replace isn't enough for pandas 2.0+ NAType
    for col in df.columns:
        df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
    
    # 4. Upsert preparation
    pk_list = [k.strip() for k in p_key.split(',')]
    temp_df = df.drop_duplicates(subset=pk_list, keep='last')
    records = temp_df.to_dict('records')
    
    # 5. Nested JSON handling
    for r in records:
        for k, v in r.items():
            if isinstance(v, (list, dict)):
                r[k] = json.dumps(v, default=str)
            
    try:
        supabase.table(table_name).upsert(records, on_conflict=p_key).execute()
        LOG.info(f"Sync Successful: {len(records)} records to '{table_name}'")
    except Exception as e:
        LOG.error(f"Sync Rejected for '{table_name}': {e}")

def run_sync(mode="debug"):
    S_STR, S_INT = "20252026", 20252026
    LOG.info(f"--- STARTING PRODUCTION SYNC | Mode: {mode} ---")

    # 1. STATIC DATA
    literal_sync("teams", scrapeTeams(source="calendar"), "id")
    
    std = scrapeStandings()
    if not std.empty:
        std.columns = [str(c).replace('.', '_').lower() for c in std.columns]
        std['id'] = std['date'].astype(str) + "_" + std['teamabbrev_default'].astype(str)
        literal_sync("standings", std, "id")

    # 2. SEASONAL SCAN (Discovery Phase)
    teams_df = scrapeTeams(source="calendar")
    active_teams = ['MTL', 'BUF'] if mode == "debug" else teams_df['abbrev'].unique().tolist()
    global_games = set()
    
    for team in active_teams:
        LOG.info(f"Scanning context for team: {team}")
        # Capture baseline biographies
        ros = scrapeRoster(team, S_STR)
        if not ros.empty:
            ros['season'] = S_INT
            ros['teamabbrev'] = team
            literal_sync("players", ros.copy(), "id")
            literal_sync("rosters", ros, "id,season")

        # Capture regular season game IDs (GameType 2)
        sched = scrapeSchedule(team, S_STR)
        sched.columns = [str(c).replace('.', '_').lower() for c in sched.columns]
        sched_f = sched[(sched['gametype'] == 2) & (sched['gamestate'].isin(['FINAL', 'OFF']))]
        global_games.update(sched_f['id'].tolist())

    # 3. ANALYTICS PROCESSING
    game_list = sorted(list(global_games))
    if mode == "debug": game_list = game_list[:3]
    
    all_game_stats = []
    for gid in game_list:
        try:
            LOG.info(f"Ingesting Game: {gid}")
            pbp = predict_xg_for_pbp(engineer_xg_features(scrapePlays(gid)))
            
            # Plays Table Sync
            plays_df = pbp.copy()
            if '#' in plays_df.columns: plays_df = plays_df.rename(columns={'#': 'sortorder'})
            plays_df['id'] = f"{gid}_{plays_df['sortorder']}"
            # raw_data JSONB catch-all defined in final_schema.sql
            plays_df['raw_data'] = plays_df.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
            literal_sync("plays", plays_df, "id")

            # Statistics Aggregation
            all_game_stats.append(on_ice_stats_by_player_strength(pbp, include_goalies=False))
        except Exception as e:
            LOG.error(f"Processing failed for Game {gid}: {e}")

    # 4. PLAYER IDENTITY ENRICHMENT (Captures all 56+ players)
    if all_game_stats:
        LOG.info("Syncing player registry from game evidence...")
        combined = pd.concat(all_game_stats)
        combined.columns = [str(c).replace('.', '_').lower() for c in combined.columns]
        
        # Identity registration for players found skated but not in active rosters
        u_pids = combined[['player1id', 'player1name']].dropna().drop_duplicates()
        u_pids = u_pids.rename(columns={'player1name': 'firstname_default', 'player1id': 'id'})
        literal_sync("players", u_pids, "id")

        # Seasonal Rollup
        agg = combined.groupby(['player1id', 'player1name', 'eventteam', 'strength']).sum(numeric_only=True).reset_index()
        agg['season'] = S_INT
        agg['id'] = agg.apply(lambda r: f"{int(r.player1id)}_{S_INT}_{r.strength}", axis=1)
        literal_sync("player_stats", agg, "id")

if __name__ == "__main__":
    run_sync("debug")