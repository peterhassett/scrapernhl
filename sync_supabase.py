import os
import sys
import logging
import pandas as pd
import numpy as np
import argparse
from datetime import datetime, timedelta
from supabase import create_client, Client

# Scrapers
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.draft import scrapeDraft
from scrapernhl import scrape_game, engineer_xg_features, predict_xg_for_pbp, on_ice_stats_by_player_strength

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def clean_json(val):
    """Converts NaN/Inf to None for JSON compliance."""
    if pd.isna(val) or val is None: return None
    if isinstance(val, (float, np.floating)):
        return None if np.isnan(val) or np.isinf(val) else float(val)
    if isinstance(val, (int, np.integer)): return int(val)
    return str(val)

def sync_table(table_name, df, p_key):
    """Standardized sync with deduplication and JSON cleaning."""
    if df.empty: return
    # Rename columns to match SQL (replace . with _)
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    records = [{k: clean_json(v) for k, v in row.items()} for row in df.to_dict('records')]
    
    # Batch Deduplication on Primary Key
    pk_list = [p.strip() for p in p_key.split(',')]
    temp_df = pd.DataFrame(records).drop_duplicates(subset=pk_list, keep='last')
    
    try:
        supabase.table(table_name).upsert(temp_df.to_dict('records'), on_conflict=p_key).execute()
        LOG.info(f"Synced {len(temp_df)} records to {table_name}")
    except Exception as e:
        LOG.error(f"Sync failed for {table_name}: {e}")

def run_sync(mode="daily"):
    S_STR, S_INT = "20252026", 20252026
    LOG.info(f"STARTING FULL SYNC: {mode}")

    # 1. TEAMS (Flattened based on nhl_truth.txt)
    raw_teams = scrapeTeams(source="records")
    teams_list = []
    for _, row in raw_teams.iterrows():
        base = row.to_dict()
        nested = base.get('teams', [])
        if nested and isinstance(nested, list):
            base['activestatus'] = (nested[0].get('active') == 'Y')
            base['conferencename'] = nested[0].get('conference', {}).get('name')
            base['divisionname'] = nested[0].get('division', {}).get('name')
            base['logos'] = str(nested[0].get('logos', []))
        teams_list.append(base)
    sync_table("teams", pd.DataFrame(teams_list), "id")

    # 2. STANDINGS
    std = scrapeStandings(date=datetime.now().strftime('%Y-%m-%d'))
    if not std.empty:
        std['id'] = std.apply(lambda r: f"{r['date']}_{r['teamAbbrev.default']}", axis=1)
        sync_table("standings", std, "id")

    # 3. DRAFT (Historical/Catchup only)
    if mode == "catchup":
        for year in range(2020, 2026):
            drft = scrapeDraft(year)
            if not drft.empty:
                drft['id'] = drft.apply(lambda r: f"{year}_{r['overallPick']}", axis=1)
                sync_table("draft", drft, "id")

    active_teams = ['MTL', 'BUF'] if mode == "debug" else pd.DataFrame(teams_list).teamAbbrev.unique().tolist()
    
    global_games = set()
    for team in active_teams:
        LOG.info(f"Processing Team: {team}")
        
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
        global_games.update(sched[sched['gameState'] == 'FINAL']['id'].tolist())

    # 6. GLOBAL PLAYER STATS & PLAYS
    game_list = sorted(list(global_games))
    if mode == "debug": game_list = game_list[:3]
    
    all_stats = []
    for gid in game_list:
        try:
            LOG.info(f"Processing Game ID: {gid}")
            pbp = scrape_game(gid)
            pbp['Event'] = pbp['Event'].str.upper()
            pbp = predict_xg_for_pbp(engineer_xg_features(pbp))
            
            # Sync individual plays for this game
            pbp['id'] = pbp.apply(lambda r: f"{gid}_{r['#']}", axis=1)
            sync_table("plays", pbp, "id")
            
            # Aggregated Stats
            stats = on_ice_stats_by_player_strength(pbp, include_goalies=False)
            
            # Extract counting goals and shots 
            for event, name in [('GOAL', 'goals'), ('SHOT', 'shots')]:
                count = pbp[pbp['Event'] == event].groupby(['player1Id', 'eventTeam', 'strength']).size().reset_index(name=name)
                stats = stats.merge(count, on=['player1Id', 'eventTeam', 'strength'], how='left')
            
            all_stats.append(stats)
        except Exception as e:
            LOG.error(f"Game {gid} failed: {e}")

    if all_stats:
        combined = pd.concat(all_stats)
        agg = combined.groupby(['player1Id', 'eventTeam', 'strength']).sum(numeric_only=True).reset_index()
        agg['season'] = S_INT
        agg['id'] = agg.apply(lambda r: f"{int(r.player1Id)}_{S_INT}_{r.strength}", axis=1)
        
        # Games Played calculation
        gp = combined.groupby(['player1Id', 'eventTeam', 'strength']).size().reset_index(name='gamesplayed')
        agg = agg.merge(gp, on=['player1Id', 'eventTeam', 'strength'])
        
        sync_table("player_stats", agg, "id")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["daily", "catchup", "debug"], default="daily", nargs="?")
    args = parser.parse_args()
    run_sync(args.mode)