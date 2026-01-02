import os
import sys
import logging
import pandas as pd
import numpy as np
import argparse
import json
from datetime import datetime, timedelta
from supabase import create_client, Client

# Scrapers - Names verified from source files
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

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def literal_sync(table_name, df, p_key):
    """Syncs scraper DataFrames to Supabase matching CSV headers exactly."""
    if df.empty: return
    
    # 1. Dot-to-Underscore and Lowercase (Matching SQL)
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    # 2. Filtering valid columns from SQL schema
    try:
        res = supabase.table(table_name).select("*").limit(1).execute()
        if res.data:
            valid_cols = list(res.data[0].keys())
            df = df[[c for c in df.columns if c in valid_cols]]
    except:
        pass 

    # 3. Handle JSON compliance (NaN -> None)
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
    
    # 4. Final Deduplication
    pk_list = [k.strip() for k in p_key.split(',')]
    temp_df = df.drop_duplicates(subset=pk_list, keep='last')
    records = temp_df.to_dict('records')
    
    # 5. Handle nested fields for JSONB
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
    LOG.info(f"--- STARTING LITERAL SYNC | Mode: {mode} ---")

    # 1. TEAMS
    literal_sync("teams", scrapeTeams(source="calendar"), "id")

    # 2. STANDINGS
    std = scrapeStandings()
    if not std.empty:
        std.columns = [str(c).replace('.', '_').lower() for c in std.columns]
        std['id'] = std['date'].astype(str) + "_" + std['teamabbrev_default'].astype(str)
        literal_sync("standings", std, "id")

    # 3. DRAFT (Catchup)
    if mode == "catchup":
        for year in range(2020, 2026):
            drft = scrapeDraftData(year)
            if not drft.empty:
                drft['id'] = str(year) + "_" + drft['overallPick'].astype(str)
                literal_sync("draft", drft, "id")

    teams_df = scrapeTeams(source="calendar")
    active_teams = ['MTL', 'BUF'] if mode == "debug" else teams_df['abbrev'].unique().tolist()
    
    global_games = set()
    for team in active_teams:
        # 4. ROSTER / PLAYERS
        ros = scrapeRoster(team, S_STR)
        if not ros.empty:
            ros['season'] = S_INT
            ros['teamabbrev'] = team
            literal_sync("players", ros.copy(), "id")
            literal_sync("rosters", ros, "id,season")

        # 5. SCHEDULE
        sched = scrapeSchedule(team, S_STR)
        sched.columns = [str(c).replace('.', '_').lower() for c in sched.columns]
        sched_filtered = sched[sched['gametype'] == 2]
        if mode == "daily":
            sched_filtered = sched_filtered[pd.to_datetime(sched_filtered['gamedate']) >= (datetime.now() - timedelta(days=5))]
        
        literal_sync("schedule", sched_filtered, "id")
        global_games.update(sched_filtered[sched_filtered['gamestate'].isin(['FINAL', 'OFF'])]['id'].tolist())

    # 6. GLOBAL PLAYER STATS & PLAYS
    game_list = sorted(list(global_games))
    if mode == "debug": game_list = game_list[:3]
    
    all_game_stats = []
    for gid in game_list:
        try:
            pbp = predict_xg_for_pbp(engineer_xg_features(scrape_game(gid)))
            
            # Sync Plays (Literal from game.csv)
            plays_df = pbp.copy()
            # Rename '#' to sortorder if needed to match SQL
            if '#' in plays_df.columns: plays_df = plays_df.rename(columns={'#': 'sortorder'})
            plays_df['id'] = str(gid) + "_" + plays_df['sortorder'].astype(str)
            literal_sync("plays", plays_df, "id")

            stats = on_ice_stats_by_player_strength(pbp, include_goalies=False)
            pbp['Event'] = pbp['Event'].str.upper()
            for ev, lbl in [('GOAL', 'goals'), ('SHOT', 'shots')]:
                cnt = pbp[pbp['Event'] == ev].groupby(['player1Id', 'eventTeam', 'strength']).size().reset_index(name=lbl)
                stats = stats.merge(cnt, on=['player1Id', 'eventTeam', 'strength'], how='left')
            all_game_stats.append(stats)
        except Exception as e: LOG.error(f"Game {gid} failed: {e}")

    if all_game_stats:
        combined = pd.concat(all_game_stats)
        combined.columns = [str(c).replace('.', '_').lower() for c in combined.columns]
        agg = combined.groupby(['player1id', 'player1name', 'eventteam', 'strength']).sum(numeric_only=True).reset_index()
        agg['season'] = S_INT
        agg['id'] = agg.apply(lambda r: f"{int(r.player1id)}_{S_INT}_{r.strength}", axis=1)
        gp = combined.groupby(['player1id', 'eventteam', 'strength']).size().reset_index(name='gamesplayed')
        agg = agg.merge(gp, on=['player1id', 'eventteam', 'strength'])
        literal_sync("player_stats", agg, "id")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["daily", "catchup", "debug"], default="daily", nargs="?")
    args = parser.parse_args()
    run_sync(args.mode)