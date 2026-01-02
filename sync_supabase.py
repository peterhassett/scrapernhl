import os
import sys
import logging
import pandas as pd
import numpy as np
from supabase import create_client, Client

# Modular Scrapers
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule

# Advanced Analytics
from scrapernhl import (
    scrape_game, 
    engineer_xg_features, 
    predict_xg_for_pbp, 
    on_ice_stats_by_player_strength
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

WHITELISTS = {
    "teams": ["id", "fullname", "teamabbrev", "teamcommonname", "teamplacename", "active_status", "conference_name", "division_name"],
    "players": ["id", "firstname_default", "lastname_default", "headshot", "positioncode", "heightininches", "weightinpounds", "birthdate", "birthcountry"],
    "rosters": ["id", "season", "teamabbrev", "sweaternumber", "positioncode"],
    "player_stats": ["id", "playerid", "season", "team", "strength", "gamesplayed", "goals", "assists", "points", "shots", "cf", "ca", "ff", "fa", "sf", "sa", "gf", "ga", "xg", "xga", "seconds", "minutes"],
    "schedule": ["id", "season", "gamedate", "gametype", "gamestate", "hometeam_id", "hometeam_abbrev", "hometeam_score", "hometeam_commonname_default", "awayteam_id", "awayteam_abbrev", "awayteam_score", "awayteam_commonname_default", "venue_default", "starttimeutc", "gamecenterlink"]
}

def force_to_native(val, target_type='int'):
    """Ensures clean types for Postgres BigInt and Numeric compatibility."""
    if pd.isna(val) or val is None:
        return 0 if target_type == 'int' else 0.0
    try:
        if target_type == 'int':
            return int(round(float(val)))
        return float(val)
    except:
        return 0 if target_type == 'int' else 0.0

def clean_and_validate(df: pd.DataFrame, table_name: str, p_keys: list) -> list:
    if df.empty: return []
    
    # Flatten names
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    if table_name == "player_stats":
        if 'player1id' in df.columns: df['playerid'] = df['player1id']
        if 'eventteam' in df.columns: df['team'] = df['eventteam']

    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()

    # Field-Specific Type Mapping
    int_fields = ['id', 'season', 'goals', 'assists', 'points', 'shots', 'score', 'gamesplayed', 'sweaternumber', 'playerid', 'hometeam_id', 'awayteam_id']
    float_fields = ['xg', 'xga', 'cf', 'ca', 'ff', 'fa', 'sf', 'sa', 'gf', 'ga', 'seconds', 'minutes', 'weightinpounds', 'heightininches']

    records = []
    for _, row in df.iterrows():
        clean_row = {}
        for col, val in row.items():
            # 'id' in player_stats is a string PK (e.g. 847..._2024_5v5)
            if table_name == "player_stats" and col == "id":
                clean_row[col] = str(val)
            elif any(p in col for p in int_fields) and not col.endswith('link'):
                clean_row[col] = force_to_native(val, 'int')
            elif any(p in col for p in float_fields):
                clean_row[col] = force_to_native(val, 'float')
            else:
                clean_row[col] = val if not pd.isna(val) else None
        records.append(clean_row)
    
    return records

def sync_table(table_name: str, df: pd.DataFrame, p_key_str: str):
    records = clean_and_validate(df, table_name, p_key_str.split(','))
    if not records: return
    try:
        supabase.table(table_name).upsert(records, on_conflict=p_key_str).execute()
        LOG.info(f"Synced {len(records)} to {table_name}")
    except Exception as e:
        LOG.error(f"Sync failed for {table_name}: {e}")

def run_sync(mode="daily"):
    s_str, s_int = "20242025", 20242025
    teams_df = scrapeTeams(source="records")
    sync_table("teams", teams_df, "id")
    
    active_teams = ['MTL', 'BUF'] if mode == "debug" else teams_df['teamAbbrev'].unique().tolist()

    for team in active_teams:
        LOG.info(f"--- {team} ---")
        
        # 1. Schedule
        schedule = scrapeSchedule(team, s_str)
        completed = schedule[schedule['gameState'].isin(['FINAL', 'OFF'])]
        sync_table("schedule", schedule, "id")

        # 2. Player Stats (Advanced)
        game_ids = completed['id'].tolist()
        if mode == "debug": game_ids = game_ids[:2]

        all_game_stats = []
        for gid in game_ids:
            try:
                pbp = scrape_game(gid)
                pbp = engineer_xg_features(pbp)
                pbp = predict_xg_for_pbp(pbp)
                stats = on_ice_stats_by_player_strength(pbp, include_goalies=False)
                
                # Merge individual counts
                goals = pbp[pbp['Event'] == 'GOAL'].groupby(['player1Id', 'eventTeam', 'strength']).size().reset_index(name='goals')
                shots = pbp[pbp['Event'].isin(['SHOT', 'GOAL'])].groupby(['player1Id', 'eventTeam', 'strength']).size().reset_index(name='shots')
                a1 = pbp[pbp['Event'] == 'GOAL'].groupby(['player2Id', 'eventTeam', 'strength']).size().reset_index(name='a1')
                a2 = pbp[pbp['Event'] == 'GOAL'].groupby(['player3Id', 'eventTeam', 'strength']).size().reset_index(name='a2')
                
                stats = stats.merge(goals, on=['player1Id', 'eventTeam', 'strength'], how='left')
                stats = stats.merge(shots, on=['player1Id', 'eventTeam', 'strength'], how='left')
                stats = stats.merge(a1.rename(columns={'player2Id': 'player1Id'}), on=['player1Id', 'eventTeam', 'strength'], how='left')
                stats = stats.merge(a2.rename(columns={'player3Id': 'player1Id'}), on=['player1Id', 'eventTeam', 'strength'], how='left')
                all_game_stats.append(stats)
            except Exception as e:
                LOG.error(f"Game {gid} failed: {e}")

        if all_game_stats:
            combined = pd.concat(all_game_stats)
            metrics = ['seconds', 'minutes', 'CF', 'CA', 'xG', 'xGA', 'goals', 'shots', 'a1', 'a2']
            sum_map = {m: 'sum' for m in metrics if m in combined.columns}
            agg = combined.groupby(['player1Id', 'eventTeam', 'strength']).agg(sum_map).reset_index()
            
            agg['assists'] = agg.get('a1', 0) + agg.get('a2', 0)
            agg['points'] = agg.get('goals', 0) + agg['assists']
            agg['season'] = s_int
            agg['gamesplayed'] = len(game_ids)
            agg['id'] = agg.apply(lambda r: f"{r['player1Id']}_{s_int}_{r['strength']}", axis=1)
            sync_table("player_stats", agg, "id")

        # 3. Roster
        ros = scrapeRoster(team, s_str)
        if not ros.empty:
            ros['season'] = s_int
            ros['teamabbrev'] = team 
            sync_table("players", ros.copy(), "id")
            sync_table("rosters", ros, "id,season")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")