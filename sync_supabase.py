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

# Cache of valid columns in DB
DB_COLS = {}

def get_valid_cols(table_name):
    if table_name in DB_COLS: return DB_COLS[table_name]
    try:
        res = supabase.table(table_name).select("*").limit(1).execute()
        DB_COLS[table_name] = list(res.data[0].keys()) if res.data else []
        return DB_COLS[table_name]
    except: return []

def literal_sync(table_name, df, p_key):
    """
    Consolidated sync: handles NAType, filters columns, and serializes JSON.
    """
    if df.empty: return
    
    # 1. Format Columns
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    # 2. Strict Whitelist Filter
    valid = get_valid_cols(table_name)
    if valid:
        df = df[[c for c in df.columns if c in valid]]
    
    # 3. CRITICAL: Manual NAType and Float Sanitation
    # pd.isna() catches NaN, None, and the problematic NAType
    def clean_cell(val):
        if pd.isna(val): return None
        if isinstance(val, (np.integer, int)): return int(val)
        if isinstance(val, (np.floating, float)): return float(val)
        return val

    records = []
    for _, row in df.iterrows():
        # Clean every cell manually to ensure no Sentinels remain
        record = {k: clean_cell(v) for k, v in row.to_dict().items()}
        
        # 4. JSONB Serialization
        for k, v in record.items():
            if isinstance(v, (list, dict)):
                record[k] = json.dumps(v, default=str)
        records.append(record)

    # Deduplicate payload
    pk_list = [k.strip() for k in p_key.split(',')]
    unique_map = {tuple(r.get(k) for k in pk_list): r for r in records}
    payload = list(unique_map.values())

    try:
        supabase.table(table_name).upsert(payload, on_conflict=p_key).execute()
        LOG.info(f"SUCCESS: {len(payload)} records to '{table_name}'")
    except Exception as e:
        LOG.error(f"FAILURE for '{table_name}': {e}")

def run_sync(mode="debug"):
    S_STR, S_INT = "20252026", 20252026
    LOG.info(f"--- GLOBAL PRODUCTION SYNC START | Mode: {mode} ---")

    # 1. Static Tables
    literal_sync("teams", scrapeTeams(source="calendar"), "id")
    
    std = scrapeStandings()
    if not std.empty:
        std.columns = [str(c).replace('.', '_').lower() for c in std.columns]
        std['id'] = std['date'].astype(str) + "_" + std['teamabbrev_default'].astype(str)
        literal_sync("standings", std, "id")

    # 2. Context Discovery (Discovery Phase)
    active_teams = ['MTL', 'BUF'] if mode == "debug" else ['MTL', 'BUF', 'TOR', 'EDM', 'FLA']
    global_games = set()
    
    for team in active_teams:
        LOG.info(f"Scanning context for team: {team}")
        ros = scrapeRoster(team, S_STR)
        if not ros.empty:
            ros['season'] = S_INT
            ros['teamabbrev'] = team
            literal_sync("players", ros.copy(), "id")
            literal_sync("rosters", ros, "id,season")

        sched = scrapeSchedule(team, S_STR)
        sched.columns = [str(c).replace('.', '_').lower() for c in sched.columns]
        # REGULAR SEASON ONLY (GameType 2)
        sched_f = sched[(sched['gametype'] == 2) & (sched['gamestate'].isin(['FINAL', 'OFF']))]
        global_games.update(sched_f['id'].tolist())

    # 3. Analytics Processing (Game Phase)
    game_list = sorted(list(global_games))
    if mode == "debug": game_list = game_list[:3]
    
    all_game_stats = []
    for gid in game_list:
        try:
            LOG.info(f"Processing Game Analytics: {gid}")
            pbp = predict_xg_for_pbp(engineer_xg_features(scrapePlays(gid)))
            
            # Sync Plays (Literal Map)
            p_df = pbp.copy()
            if '#' in p_df.columns: p_df = p_df.rename(columns={'#': 'sortorder'})
            p_df['id'] = f"{gid}_{p_df['sortorder']}"
            p_df['raw_data'] = p_df.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
            literal_sync("plays", p_df, "id")

            # Stat Rollup
            all_game_stats.append(on_ice_stats_by_player_strength(pbp, include_goalies=False))
        except Exception as e:
            LOG.error(f"Processing error for Game {gid}: {e}")

    # 4. Identity Enrichment & Final Push
    if all_game_stats:
        LOG.info("Syncing Registry from Evidence...")
        combined = pd.concat(all_game_stats)
        combined.columns = [str(c).replace('.', '_').lower() for c in combined.columns]
        
        # Discover and register players skated but missing from active roster
        u_pids = combined[['player1id', 'player1name']].dropna().drop_duplicates()
        u_pids = u_pids.rename(columns={'player1name': 'firstname_default', 'player1id': 'id'})
        literal_sync("players", u_pids, "id")

        # Global Aggregate
        agg = combined.groupby(['player1id', 'player1name', 'eventteam', 'strength']).sum(numeric_only=True).reset_index()
        agg['season'] = S_INT
        agg['id'] = agg.apply(lambda r: f"{int(r.player1id)}_{S_INT}_{r.strength}", axis=1)
        literal_sync("player_stats", agg, "id")

if __name__ == "__main__":
    run_sync("debug")