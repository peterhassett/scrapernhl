import os
import sys
import logging
import pandas as pd
import numpy as np
import json
from datetime import datetime
from supabase import create_client, Client

# Scrapers - Using modern scrapePlays to avoid legacy report 404s
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.games import scrapePlays 
from scrapernhl import engineer_xg_features, predict_xg_for_pbp, on_ice_stats_by_player_strength

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

# Supabase Configuration
supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Local cache for schema-compliant columns
DB_COLS = {}

def get_valid_cols(table_name):
    """Dynamically fetches column names from the database to ensure sync compliance."""
    if table_name in DB_COLS: return DB_COLS[table_name]
    try:
        res = supabase.table(table_name).select("*").limit(1).execute()
        DB_COLS[table_name] = list(res.data[0].keys()) if res.data else []
        return DB_COLS[table_name]
    except Exception as e:
        LOG.warning(f"Metadata fetch failed for {table_name}: {e}")
        return []

def literal_sync(table_name, df, p_key):
    """
    Synchronizes DataFrame to Supabase with strict column alignment and null-safety.
    Targeted fix for 'NAType' float conversion errors.
    """
    if df.empty: return
    
    # 1. Column Alignment (dots to underscores, lowercase)
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    # 2. Strict Whitelist Filtering based on SQL Schema
    valid = get_valid_cols(table_name)
    if valid:
        df = df[[c for c in df.columns if c in valid]]
    
    # 3. CRITICAL: Manual NAType and Float Sanitation
    # pd.isna() captures standard NaN, None, and the problematic pandas 2.0 NAType sentinel
    def clean_cell(val):
        if pd.isna(val): return None
        if isinstance(val, (np.integer, int)): return int(val)
        if isinstance(val, (np.floating, float)): return float(val)
        return val

    records = []
    for _, row in df.iterrows():
        # Clean every cell to ensure no Sentinels reach the float constructor
        record = {k: clean_cell(v) for k, v in row.to_dict().items()}
        
        # 4. JSONB Serialization for nested lists/dicts
        for k, v in record.items():
            if isinstance(v, (list, dict)):
                record[k] = json.dumps(v, default=str)
        records.append(record)

    # 5. Deduplication
    pk_list = [k.strip() for k in p_key.split(',')]
    unique_map = {tuple(r.get(k) for k in pk_list): r for r in records}
    payload = list(unique_map.values())

    try:
        supabase.table(table_name).upsert(payload, on_conflict=p_key).execute()
        LOG.info(f"Sync Success: {len(payload)} records to '{table_name}'")
    except Exception as e:
        LOG.error(f"Sync Failure for '{table_name}': {e}")

def run_sync(mode="daily"):
    S_STR, S_INT = "20252026", 20252026
    LOG.info(f"--- STARTING GLOBAL SYNC | Mode: {mode} ---")

    # 1. Base Tables
    literal_sync("teams", scrapeTeams(source="calendar"), "id")
    
    std = scrapeStandings()
    if not std.empty:
        std.columns = [str(c).replace('.', '_').lower() for c in std.columns]
        std['id'] = std['date'].astype(str) + "_" + std['teamabbrev_default'].astype(str)
        literal_sync("standings", std, "id")

    # 2. Discovery Phase
    teams_df = scrapeTeams(source="calendar")
    active_teams = ['MTL', 'BUF'] if mode == "debug" else teams_df['abbrev'].unique().tolist()
    global_games = set()
    
    for team in active_teams:
        LOG.info(f"Processing context for team: {team}")
        # Capture Baseline Biographies
        ros = scrapeRoster(team, S_STR)
        if not ros.empty:
            ros['season'] = S_INT
            ros['teamabbrev'] = team
            literal_sync("players", ros.copy(), "id")
            literal_sync("rosters", ros, "id,season")

        # Capture Regular Season Game IDs (GameType 2)
        sched = scrapeSchedule(team, S_STR)
        sched.columns = [str(c).replace('.', '_').lower() for c in sched.columns]
        sched_f = sched[(sched['gametype'] == 2) & (sched['gamestate'].isin(['FINAL', 'OFF']))]
        global_games.update(sched_f['id'].tolist())

    # 3. Analytics Processing
    game_list = sorted(list(global_games))
    if mode == "debug": game_list = game_list[:3]
    
    all_game_stats = []
    for gid in game_list:
        try:
            LOG.info(f"Ingesting Analytics for Game: {gid}")
            # Use modern scrapePlays to bypass legacy 404s
            pbp = predict_xg_for_pbp(engineer_xg_features(scrapePlays(gid)))
            
            # Sync Individual Plays
            p_df = pbp.copy()
            if '#' in p_df.columns: p_df = p_df.rename(columns={'#': 'sortorder'})
            p_df['id'] = f"{gid}_{p_df['sortorder']}"
            # raw_data JSONB catch-all for any metadata outside the schema
            p_df['raw_data'] = p_df.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
            literal_sync("plays", p_df, "id")

            # Aggregate counting and on-ice statistics
            all_game_stats.append(on_ice_stats_by_player_strength(pbp, include_goalies=False))
        except Exception as e:
            LOG.error(f"Processing error for Game {gid}: {e}")

    # 4. Global Identity Enrichment (Ensure 56+ Player Count)
    if all_game_stats:
        LOG.info("Finalizing Player Registry from game evidence...")
        combined = pd.concat(all_game_stats)
        combined.columns = [str(c).replace('.', '_').lower() for c in combined.columns]
        
        # Identity Registration for call-ups/trades missing from active roster
        u_pids = combined[['player1id', 'player1name']].dropna().drop_duplicates()
        u_pids = u_pids.rename(columns={'player1name': 'firstname_default', 'player1id': 'id'})
        literal_sync("players", u_pids, "id")

        # Seasonal Rollup
        agg = combined.groupby(['player1id', 'player1name', 'eventteam', 'strength']).sum(numeric_only=True).reset_index()
        agg['season'] = S_INT
        agg['id'] = agg.apply(lambda r: f"{int(r.player1id)}_{S_INT}_{r.strength}", axis=1)
        literal_sync("player_stats", agg, "id")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["daily", "catchup", "debug"], default="daily", nargs="?")
    args = parser.parse_args()
    run_sync(args.mode)