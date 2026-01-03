import os
import logging
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from supabase import create_client, Client

# Scrapers from your package
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.games import scrapePlays 
from scrapernhl import engineer_xg_features, predict_xg_for_pbp, on_ice_stats_by_player_strength

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Global column whitelist cache based on your newly provided SQL schema
DB_COLS = {}

def get_valid_cols(table_name):
    """Fetches valid columns from Supabase to prevent PGRST204 errors."""
    if table_name in DB_COLS: return DB_COLS[table_name]
    try:
        res = supabase.table(table_name).select("*").limit(1).execute()
        DB_COLS[table_name] = list(res.data[0].keys()) if res.data else []
        return DB_COLS[table_name]
    except: return []

def literal_sync(table_name, df, p_key):
    """Syncs DataFrame to DB using your final_schema.sql columns."""
    if df.empty: return
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    # Filter using the discovered "Worst-Case" columns
    valid = get_valid_cols(table_name)
    if valid:
        df = df[[c for c in df.columns if c in valid]]
    
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
    pk_list = [k.strip() for k in p_key.split(',')]
    records = df.drop_duplicates(subset=pk_list, keep='last').to_dict('records')
    
    for r in records:
        for k, v in r.items():
            if isinstance(v, (list, dict)): r[k] = json.dumps(v)
            
    try:
        supabase.table(table_name).upsert(records, on_conflict=p_key).execute()
        LOG.info(f"Sync Successful: {len(records)} records to '{table_name}'")
    except Exception as e:
        LOG.error(f"Sync Failed for '{table_name}': {e}")

def run_sync(mode="debug"):
    S_STR, S_INT = "20252026", 20252026
    LOG.info(f"--- STARTING DISCOVERY-FIRST SYNC | Mode: {mode} ---")

    # 1. Base Tables (Teams, Standings)
    teams_df = scrapeTeams(source="calendar")
    literal_sync("teams", teams_df, "id")
    
    std = scrapeStandings()
    if not std.empty:
        std.columns = [str(c).replace('.', '_').lower() for c in std.columns]
        std['id'] = std['date'].astype(str) + "_" + std['teamabbrev_default'].astype(str)
        literal_sync("standings", std, "id")

    # 2. Team-Based Discovery
    active_teams = ['MTL', 'BUF'] if mode == "debug" else teams_df['abbrev'].unique().tolist()
    global_games = set()
    
    for team in active_teams:
        # Pre-sync roster to capture known bio data
        ros = scrapeRoster(team, S_STR)
        if not ros.empty:
            ros['season'] = S_INT
            ros['teamabbrev'] = team
            literal_sync("players", ros.copy(), "id")
            literal_sync("rosters", ros, "id,season")

        # Capture list of finished regular season games
        sched = scrapeSchedule(team, S_STR)
        sched.columns = [str(c).replace('.', '_').lower() for c in sched.columns]
        sched_f = sched[(sched['gametype'] == 2) & (sched['gamestate'].isin(['FINAL', 'OFF']))]
        global_games.update(sched_f['id'].tolist())

    # 3. Game-Based Discovery (Final Identity Check)
    game_list = sorted(list(global_games))
    if mode == "debug": game_list = game_list[:3]
    
    all_game_stats = []
    for gid in game_list:
        try:
            LOG.info(f"Processing Game ID: {gid}")
            pbp = predict_xg_for_pbp(engineer_xg_features(scrapePlays(gid)))
            
            # Sync Plays (Literal Map)
            plays_df = pbp.copy()
            if '#' in plays_df.columns: plays_df = plays_df.rename(columns={'#': 'sortorder'})
            plays_df['id'] = f"{gid}_{plays_df['sortorder']}"
            literal_sync("plays", plays_df, "id")

            # Aggregate counting and on-ice stats
            stats = on_ice_stats_by_player_strength(pbp, include_goalies=False)
            all_game_stats.append(stats)
        except Exception as e: LOG.error(f"Error processing Game {gid}: {e}")

    # 4. Final Bio Check: Sync missing players from games (The 56+ Player Fix)
    if all_game_stats:
        combined = pd.concat(all_game_stats)
        combined.columns = [str(c).replace('.', '_').lower() for c in combined.columns]
        
        # Identity Scan: Capture IDs not found in official team rosters
        u_pids = combined[['player1id', 'player1name']].dropna().drop_duplicates()
        u_pids = u_pids.rename(columns={'player1name': 'firstname_default', 'player1id': 'id'})
        literal_sync("players", u_pids, "id")

        # Global Seasonal Aggregation
        agg = combined.groupby(['player1id', 'player1name', 'eventteam', 'strength']).sum(numeric_only=True).reset_index()
        agg['season'] = S_INT
        agg['id'] = agg.apply(lambda r: f"{int(r.player1id)}_{S_INT}_{r.strength}", axis=1)
        literal_sync("player_stats", agg, "id")

if __name__ == "__main__":
    run_sync("debug")