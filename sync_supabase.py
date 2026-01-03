import os
import sys
import logging
import pandas as pd
import numpy as np
import json
from datetime import datetime
from supabase import create_client, Client

# Scrapers
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.games import scrapePlays 
from scrapernhl import engineer_xg_features, predict_xg_for_pbp, on_ice_stats_by_player_strength

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

DB_COLS = {}

def get_valid_cols(table_name):
    if table_name in DB_COLS: return DB_COLS[table_name]
    try:
        res = supabase.table(table_name).select("*").limit(1).execute()
        DB_COLS[table_name] = list(res.data[0].keys()) if res.data else []
        return DB_COLS[table_name]
    except: return []

def literal_sync(table_name, df, p_key):
    """Mirror sync that ensures no NATypes reach the database client."""
    if df.empty: return
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    valid = get_valid_cols(table_name)
    if valid:
        df = df[[c for c in df.columns if c in valid]]
    
    # Manual type enforcement to kill pandas NAType sentinels
    def clean_cell(val):
        if pd.isna(val): return None
        if isinstance(val, (np.integer, int)): return int(val)
        if isinstance(val, (np.floating, float)): return float(val)
        return val

    records = []
    for _, row in df.iterrows():
        record = {k: clean_cell(v) for k, v in row.to_dict().items()}
        for k, v in record.items():
            if isinstance(v, (list, dict)):
                record[k] = json.dumps(v, default=str)
        records.append(record)

    try:
        supabase.table(table_name).upsert(records, on_conflict=p_key).execute()
        LOG.info(f"Sync Success: {len(records)} records to '{table_name}'")
    except Exception as e:
        LOG.error(f"DB Error for '{table_name}': {e}")

def safe_process_game(gid):
    """
    PRE-SANITIZER: Cleans raw data BEFORE it hits the internal scrapernhl logic
    to prevent the 'NAType' float conversion crash.
    """
    raw_df = scrapePlays(gid)
    if raw_df.empty: return pd.DataFrame()

    # Kill potential NATypes in columns used by engineer_xg_features
    # This forces them to standard floats/NaNs which the package can handle
    for col in raw_df.columns:
        if raw_df[col].dtype.name in ['Int64', 'Float64', 'boolean']:
            raw_df[col] = raw_df[col].astype(float)

    # Now it is safe to run internal analytics
    enriched_df = engineer_xg_features(raw_df)
    final_df = predict_xg_for_pbp(enriched_df)
    return final_df

def run_sync(mode="daily"):
    S_STR, S_INT = "20252026", 20252026
    LOG.info(f"--- STARTING GLOBAL SYNC | Mode: {mode} ---")

    # 1. Teams & Standings
    literal_sync("teams", scrapeTeams(source="calendar"), "id")
    
    std = scrapeStandings()
    if not std.empty:
        std.columns = [str(c).replace('.', '_').lower() for c in std.columns]
        std['id'] = std['date'].astype(str) + "_" + std['teamabbrev_default'].astype(str)
        literal_sync("standings", std, "id")

    # 2. Roster Discovery
    teams_df = scrapeTeams(source="calendar")
    active_teams = ['MTL', 'BUF'] if mode == "debug" else teams_df['abbrev'].unique().tolist()
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
        # STICK TO REGULAR SEASON (GameType 2)
        sched_f = sched[(sched['gametype'] == 2) & (sched['gamestate'].isin(['FINAL', 'OFF']))]
        global_games.update(sched_f['id'].tolist())

    # 3. Game-by-Game Processing
    game_list = sorted(list(global_games))
    if mode == "debug": game_list = game_list[:3]
    
    all_game_stats = []
    for gid in game_list:
        try:
            LOG.info(f"Ingesting Game: {gid}")
            # Use the Pre-Sanitizer logic
            pbp = safe_process_game(gid)
            if pbp.empty: continue
            
            # Sync individual plays (using raw_data JSONB from schema)
            p_df = pbp.copy()
            if '#' in p_df.columns: p_df = p_df.rename(columns={'#': 'sortorder'})
            p_df['id'] = f"{gid}_{p_df['sortorder']}"
            p_df['raw_data'] = p_df.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
            literal_sync("plays", p_df, "id")

            # Aggregate statistics
            all_game_stats.append(on_ice_stats_by_player_strength(pbp, include_goalies=False))
        except Exception as e:
            LOG.error(f"Processing error for Game {gid}: {e}")

    # 4. Final Aggregation
    if all_game_stats:
        combined = pd.concat(all_game_stats)
        combined.columns = [str(c).replace('.', '_').lower() for c in combined.columns]
        
        # Identity registration for call-ups
        u_pids = combined[['player1id', 'player1name']].dropna().drop_duplicates()
        u_pids = u_pids.rename(columns={'player1name': 'firstname_default', 'player1id': 'id'})
        literal_sync("players", u_pids, "id")

        agg = combined.groupby(['player1id', 'player1name', 'eventteam', 'strength']).sum(numeric_only=True).reset_index()
        agg['season'] = S_INT
        agg['id'] = agg.apply(lambda r: f"{int(r.player1id)}_{S_INT}_{r.strength}", axis=1)
        literal_sync("player_stats", agg, "id")

if __name__ == "__main__":
    run_sync("debug")