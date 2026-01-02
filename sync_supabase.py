import os
import sys
import logging
import pandas as pd
import numpy as np
import argparse
import json
import time
from datetime import datetime, timedelta
from supabase import create_client, Client

# Scrapers - Verified Names from package inspection
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.draft import scrapeDraftData
from scrapernhl import (
    scrape_game, 
    engineer_xg_features, 
    predict_xg_for_pbp, 
    on_ice_stats_by_player_strength
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

# Supabase Initialization
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
if not url or not key:
    LOG.error("Missing SUPABASE_URL or SUPABASE_KEY environment variables.")
    sys.exit(1)

supabase: Client = create_client(url, key)

# Column cache to handle PostgREST schema cache delay
VALID_COLS_CACHE = {}

def get_db_columns(table_name):
    """Dynamically fetches columns from Supabase to avoid PGRST204 errors."""
    if table_name in VALID_COLS_CACHE:
        return VALID_COLS_CACHE[table_name]
    try:
        # Fetch metadata by requesting one row
        res = supabase.table(table_name).select("*").limit(1).execute()
        if res.data:
            VALID_COLS_CACHE[table_name] = list(res.data[0].keys())
        else:
            # Table is empty, cannot easily infer columns via PostgREST without data
            VALID_COLS_CACHE[table_name] = []
        return VALID_COLS_CACHE[table_name]
    except Exception as e:
        LOG.warning(f"Metadata fetch failed for {table_name}: {e}")
        return []

def literal_sync(table_name, df, p_key):
    """Universal sync mapping scraper DataFrames to literal SQL columns."""
    if df.empty: return
    
    # 1. Normalize column names (dots to underscores, lowercase)
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    # 2. Safety filter: only send columns that exist in the DB
    db_cols = get_db_columns(table_name)
    if db_cols:
        df = df[[c for c in df.columns if c in db_cols]]
    
    # 3. Handle JSON compliance and quantitative types
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
    
    # 4. Batch Deduplication within current payload
    pk_list = [k.strip() for k in p_key.split(',')]
    temp_df = df.drop_duplicates(subset=pk_list, keep='last')
    records = temp_df.to_dict('records')
    
    # 5. Serialization for JSONB columns
    for r in records:
        for k, v in r.items():
            if isinstance(v, (list, dict)):
                r[k] = json.dumps(v)

    try:
        supabase.table(table_name).upsert(records, on_conflict=p_key).execute()
        LOG.info(f"Literal Sync: {len(records)} records to '{table_name}'")
    except Exception as e:
        LOG.error(f"Sync failed for '{table_name}': {e}")

def run_sync(mode="daily"):
    S_STR, S_INT = "20252026", 20252026
    LOG.info(f"--- LITERAL SYNC START | Mode: {mode} ---")

    # PRE-FLIGHT: Teams first (defines constraints for teamabbrev)
    teams_df = scrapeTeams(source="calendar")
    literal_sync("teams", teams_df, "id")

    # STANDINGS
    std = scrapeStandings()
    if not std.empty:
        std.columns = [str(c).replace('.', '_').lower() for c in std.columns]
        std['id'] = std['date'].astype(str) + "_" + std['teamabbrev_default'].astype(str)
        literal_sync("standings", std, "id")

    # DRAFT (Catchup)
    if mode == "catchup":
        for year in range(2020, 2026):
            drft = scrapeDraftData(year)
            if not drft.empty:
                drft['id'] = str(year) + "_" + drft['overallPick'].astype(str)
                literal_sync("draft", drft, "id")

    active_teams = ['MTL', 'BUF'] if mode == "debug" else teams_df['abbrev'].unique().tolist()
    
    global_games = set()
    for team in active_teams:
        LOG.info(f"Scanning Team: {team}")
        
        # PLAYERS/ROSTERS FIRST (defines constraints for player_stats/plays)
        ros = scrapeRoster(team, S_STR)
        if not ros.empty:
            ros['season'] = S_INT
            ros['teamabbrev'] = team
            literal_sync("players", ros.copy(), "id")
            literal_sync("rosters", ros, "id,season")

        # SCHEDULE
        sched = scrapeSchedule(team, S_STR)
        sched.columns = [str(c).replace('.', '_').lower() for c in sched.columns]
        sched_f = sched[sched['gametype'] == 2]
        if mode == "daily":
            # Sync only games from the last 5 days
            cutoff = (datetime.now() - timedelta(days=5))
            sched_f = sched_f[pd.to_datetime(sched_f['gamedate']) >= cutoff]
        literal_sync("schedule", sched_f, "id")
        global_games.update(sched_f[sched_f['gamestate'].isin(['FINAL', 'OFF'])]['id'].tolist())

    # STATS & PLAYS PROCESSING
    game_list = sorted(list(global_games))
    if mode == "debug": game_list = game_list[:3]
    
    all_game_stats = []
    for gid in game_list:
        try:
            LOG.info(f"Processing Play-by-Play: {gid}")
            pbp = predict_xg_for_pbp(engineer_xg_features(scrape_game(gid)))
            
            # Sync individual plays
            pbp_c = pbp.copy()
            if '#' in pbp_c.columns: pbp_c = pbp_c.rename(columns={'#': 'sortorder'})
            pbp_c['id'] = str(gid) + "_" + pbp_c['sortorder'].astype(str)
            literal_sync("plays", pbp_c, "id")

            # Aggregate counting and on-ice stats
            stats = on_ice_stats_by_player_strength(pbp, include_goalies=False)
            pbp['Event'] = pbp['Event'].str.upper()
            for ev, lbl in [('GOAL', 'goals'), ('SHOT', 'shots')]:
                cnt = pbp[pbp['Event'] == ev].groupby(['player1Id', 'eventTeam', 'strength']).size().reset_index(name=lbl)
                stats = stats.merge(cnt, on=['player1Id', 'eventTeam', 'strength'], how='left')
            all_game_stats.append(stats)
        except Exception as e:
            LOG.error(f"Failed to process stats for Game {gid}: {e}")

    if all_game_stats:
        LOG.info("Finalizing global player stats aggregation...")
        combined = pd.concat(all_game_stats)
        combined.columns = [str(c).replace('.', '_').lower() for c in combined.columns]
        
        # DYNAMIC PLAYER REGISTRY: Add any IDs found in PBP but missing from official rosters
        u_pids = combined[['player1id', 'player1name']].dropna().drop_duplicates()
        u_pids = u_pids.rename(columns={'player1name': 'firstname_default', 'player1id': 'id'})
        literal_sync("players", u_pids, "id")

        # Seasonal Aggregation
        agg = combined.groupby(['player1id', 'player1name', 'eventteam', 'strength']).sum(numeric_only=True).reset_index()
        agg['season'] = S_INT
        agg['id'] = agg.apply(lambda r: f"{int(r.player1id)}_{S_INT}_{r.strength}", axis=1)
        
        # Calculate games played
        gp = combined.groupby(['player1id', 'eventteam', 'strength']).size().reset_index(name='gamesplayed')
        agg = agg.merge(gp, on=['player1id', 'eventteam', 'strength'])
        literal_sync("player_stats", agg, "id")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["daily", "catchup", "debug"], default="daily", nargs="?")
    args = parser.parse_args()
    run_sync(args.mode)