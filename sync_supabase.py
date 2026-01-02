import os
import sys
import logging
import pandas as pd
import numpy as np
import argparse
import json
from datetime import datetime, timedelta
from supabase import create_client, Client

# Scrapers - Verified Names
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

def clean_for_upsert(df):
    """Aligns columns with SQL schema and handles JSON compliance."""
    # Mirror CSV Export: dots to underscores
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    # Replace NaN/Inf with None
    return df.replace({np.nan: None, np.inf: None, -np.inf: None})

def sync_table(table_name, df, p_key):
    if df.empty: return
    df = clean_for_upsert(df)
    
    # Batch Deduplication
    pk_list = [k.strip() for k in p_key.split(',')]
    temp_df = df.drop_duplicates(subset=pk_list, keep='last')
    
    records = temp_df.to_dict('records')
    # Handle list-to-string for JSONB columns if necessary
    for r in records:
        for k, v in r.items():
            if isinstance(v, (list, dict)):
                r[k] = json.dumps(v)

    try:
        supabase.table(table_name).upsert(records, on_conflict=p_key).execute()
        LOG.info(f"Synced {len(records)} records to {table_name}")
    except Exception as e:
        LOG.error(f"Sync failed for {table_name}: {e}")

def run_sync(mode="daily"):
    S_STR, S_INT = "20252026", 20252026
    LOG.info(f"STARTING REBUILD SYNC: Mode={mode}")

    # 1. TEAMS
    sync_table("teams", scrapeTeams(source="records"), "id")

    # 2. STANDINGS
    std = scrapeStandings(date=datetime.now().strftime('%Y-%m-%d'))
    if not std.empty:
        std['id'] = std['date'].astype(str) + "_" + std['teamAbbrev.default']
        sync_table("standings", std, "id")

    # 3. DRAFT
    if mode == "catchup":
        for year in range(2020, 2026):
            drft = scrapeDraftData(year)
            if not drft.empty:
                drft['id'] = str(year) + "_" + drft['overallPick'].astype(str)
                sync_table("draft", drft, "id")

    # Determine teams
    active_teams = ['MTL', 'BUF'] if mode == "debug" else ['MTL', 'BUF', 'TOR', 'BOS', 'NYR'] # Add full list for catchup
    
    global_games = set()
    for team in active_teams:
        LOG.info(f"--- Processing {team} ---")
        # 4. ROSTER & PLAYERS
        ros = scrapeRoster(team, S_STR)
        if not ros.empty:
            ros['season'] = S_INT
            ros['teamabbrev'] = team
            sync_table("players", ros.copy(), "id")
            sync_table("rosters", ros, "id,season")

        # 5. SCHEDULE
        sched = scrapeSchedule(team, S_STR)
        sched = sched[sched['gameType'] == 2]
        if mode == "daily":
            sched = sched[pd.to_datetime(sched['gameDate']) >= (datetime.now() - timedelta(days=5))]
        sync_table("schedule", sched, "id")
        global_games.update(sched[sched['gameState'].isin(['FINAL', 'OFF'])]['id'].tolist())

    # 6. GLOBAL PLAYER STATS & PLAYS
    game_list = sorted(list(global_games))
    if mode == "debug": game_list = game_list[:3]
    
    all_game_stats = []
    for gid in game_list:
        try:
            LOG.info(f"Processing Game {gid}")
            pbp = predict_xg_for_pbp(engineer_xg_features(scrape_game(gid)))
            
            # Sync Plays (Full Row as JSONB backup)
            plays_df = pbp.copy()
            plays_df['id'] = str(gid) + "_" + plays_df['sortOrder'].astype(str)
            plays_df['raw_play_data'] = plays_df.apply(lambda r: json.dumps(r.to_dict()), axis=1)
            sync_table("plays", plays_df, "id")

            # Aggregate
            stats = on_ice_stats_by_player_strength(pbp, include_goalies=False)
            # Add counting stats manually as before to ensure accuracy
            pbp['Event'] = pbp['Event'].str.upper()
            for event, label in [('GOAL', 'goals'), ('SHOT', 'shots')]:
                cnt = pbp[pbp['Event'] == event].groupby(['player1Id', 'eventTeam', 'strength']).size().reset_index(name=label)
                stats = stats.merge(cnt, on=['player1Id', 'eventTeam', 'strength'], how='left')
            all_game_stats.append(stats)
        except Exception as e:
            LOG.error(f"Game {gid} failed: {e}")

    if all_game_stats:
        combined = pd.concat(all_game_stats)
        # Final seasonal aggregation
        agg = combined.groupby(['player1Id', 'player1Name', 'eventTeam', 'strength']).sum(numeric_only=True).reset_index()
        agg['season'] = S_INT
        agg['id'] = agg.apply(lambda r: f"{int(r.player1Id)}_{S_INT}_{r.strength}", axis=1)
        gp = combined.groupby(['player1Id', 'eventTeam', 'strength']).size().reset_index(name='gamesplayed')
        agg = agg.merge(gp, on=['player1Id', 'eventTeam', 'strength'])
        sync_table("player_stats", agg, "id")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["daily", "catchup", "debug"], default="daily", nargs="?")
    args = parser.parse_args()
    run_sync(args.mode)