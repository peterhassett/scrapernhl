import os
import sys
import logging
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
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

# Cache of the worst-case schema columns
DB_COLS = {}

def get_valid_cols(table_name):
    if table_name in DB_COLS: return DB_COLS[table_name]
    try:
        res = supabase.table(table_name).select("*").limit(1).execute()
        DB_COLS[table_name] = list(res.data[0].keys()) if res.data else []
        return DB_COLS[table_name]
    except: return []

def literal_sync(table_name, df, p_key):
    if df.empty: return
    
    # 1. Column Formatting
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    # 2. Strict Filter by SQL Schema Discovery
    valid = get_valid_cols(table_name)
    if valid:
        df = df[[c for c in df.columns if c in valid]]
    
    # 3. CRITICAL: SANITIZE EVERY CELL FOR NAType
    # This manually maps every pandas-specific null to Python's None
    def sanitize(val):
        if pd.isna(val): return None
        if isinstance(val, (np.integer, np.floating)): return float(val)
        return val

    # Apply to every cell in the dataframe
    records = []
    for _, row in df.iterrows():
        record = {k: sanitize(v) for k, v in row.to_dict().items()}
        # 4. JSON Serialization for remaining nested objects
        for k, v in record.items():
            if isinstance(v, (list, dict)):
                record[k] = json.dumps(v, default=str)
        records.append(record)

    # Deduplicate records by primary key
    unique_records = {}
    p_keys = [k.strip() for k in p_key.split(',')]
    for r in records:
        key_val = tuple(r.get(k) for k in p_keys)
        unique_records[key_val] = r

    final_payload = list(unique_records.values())

    try:
        supabase.table(table_name).upsert(final_payload, on_conflict=p_key).execute()
        LOG.info(f"SUCCESS: {len(final_payload)} records to '{table_name}'")
    except Exception as e:
        LOG.error(f"FAILURE for '{table_name}': {e}")

def run_sync(mode="debug"):
    S_STR, S_INT = "20252026", 20252026
    LOG.info(f"--- PRODUCTION SYNC START | Mode: {mode} ---")

    # BASE DATA
    literal_sync("teams", scrapeTeams(source="calendar"), "id")
    
    std = scrapeStandings()
    if not std.empty:
        std.columns = [str(c).replace('.', '_').lower() for c in std.columns]
        std['id'] = std['date'].astype(str) + "_" + std['teamabbrev_default'].astype(str)
        literal_sync("standings", std, "id")

    # CONTEXT SCAN
    active_teams = ['MTL', 'BUF'] if mode == "debug" else ['MTL', 'BUF', 'TOR', 'EDM', 'FLA']
    global_games = set()
    
    for team in active_teams:
        LOG.info(f"Scanning Team Context: {team}")
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

    # GAME ANALYTICS
    game_list = sorted(list(global_games))
    if mode == "debug": game_list = game_list[:3]
    
    all_game_stats = []
    for gid in game_list:
        try:
            LOG.info(f"Ingesting Game Analytics: {gid}")
            pbp = predict_xg_for_pbp(engineer_xg_features(scrapePlays(gid)))
            
            # Individual Play Sync
            p_df = pbp.copy()
            if '#' in p_df.columns: p_df = p_df.rename(columns={'#': 'sortorder'})
            p_df['id'] = f"{gid}_{p_df['sortorder']}"
            p_df['raw_data'] = p_df.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
            literal_sync("plays", p_df, "id")

            # Stat Aggregation
            all_game_stats.append(on_ice_stats_by_player_strength(pbp, include_goalies=False))
        except Exception as e:
            LOG.error(f"Processing error for Game {gid}: {e}")

    # GLOBAL IDENTITY & STAT ROLLUP
    if all_game_stats:
        LOG.info("Syncing Global Player Registry...")
        combined = pd.concat(all_game_stats)
        combined.columns = [str(c).replace('.', '_').lower() for c in combined.columns]
        
        # Identity registration for players found skated but not in rosters
        u_pids = combined[['player1id', 'player1name']].dropna().drop_duplicates()
        u_pids = u_pids.rename(columns={'player1name': 'firstname_default', 'player1id': 'id'})
        literal_sync("players", u_pids, "id")

        LOG.info("Pushing Final Player Stats...")
        agg = combined.groupby(['player1id', 'player1name', 'eventteam', 'strength']).sum(numeric_only=True).reset_index()
        agg['season'] = S_INT
        agg['id'] = agg.apply(lambda r: f"{int(r.player1id)}_{S_INT}_{r.strength}", axis=1)
        literal_sync("player_stats", agg, "id")

if __name__ == "__main__":
    run_sync("debug")