import os
import sys
import logging
import pandas as pd
import numpy as np
from supabase import create_client, Client

# Scraper Imports
from scrapernhl import (
    scrape_game, 
    engineer_xg_features, 
    predict_xg_for_pbp, 
    on_ice_stats_by_player_strength
)
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule

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

def clean_and_validate(df: pd.DataFrame, table_name: str, p_keys: list) -> pd.DataFrame:
    if df.empty: return df
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    if table_name == "player_stats":
        if 'player1id' in df.columns: df['playerid'] = df['player1id']
        if 'eventteam' in df.columns: df['team'] = df['eventteam']
        for col in ['xg', 'xga', 'cf', 'ca', 'ff', 'fa', 'sf', 'sa', 'gf', 'ga']:
            if col in df.columns: 
                df[col] = pd.to_numeric(df[col], errors='coerce')

    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()

    num_pats = ['id', 'season', 'goals', 'assists', 'points', 'shots', 'score', 'played']
    for col in df.columns:
        if any(p in col for p in num_pats) and not col.endswith('link'):
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round().astype(np.int64)

    existing_pks = [k for k in p_keys if k in df.columns]
    if existing_pks:
        df = df.dropna(subset=existing_pks).drop_duplicates(subset=existing_pks, keep='first')
    
    return df.replace({np.nan: None})

def sync_table(table_name: str, df: pd.DataFrame, p_key_str: str):
    ready = clean_and_validate(df, table_name, p_key_str.split(','))
    if ready.empty: return
    try:
        supabase.table(table_name).upsert(ready.to_dict(orient="records"), on_conflict=p_key_str).execute()
        LOG.info(f"Synced {len(ready)} to {table_name}")
    except Exception as e:
        LOG.error(f"Sync failed for {table_name}: {e}")

def run_sync(mode="daily"):
    s_str, s_int = "20242025", 20242025
    teams_df = scrapeTeams(source="records")
    sync_table("teams", teams_df, "id")
    
    active_teams = ['MTL', 'BUF'] if mode == "debug" else teams_df['teamAbbrev'].unique().tolist()

    for team in active_teams:
        LOG.info(f"--- Processing {team} ---")
        schedule = scrapeSchedule(team, s_str)
        completed = schedule[schedule['gameState'].isin(['FINAL', 'OFF'])]
        sync_table("schedule", schedule, "id")

        game_ids = completed['id'].tolist()
        if mode == "debug": game_ids = game_ids[:2]

        all_game_stats = []
        for gid in game_ids:
            try:
                LOG.info(f"Processing Game {gid}")
                pbp = scrape_game(gid)
                pbp = engineer_xg_features(pbp)
                pbp = predict_xg_for_pbp(pbp)
                
                # 1. Advanced Metrics (On-Ice)
                adv_stats = on_ice_stats_by_player_strength(pbp, include_goalies=False)
                
                # 2. Manual Box Score Aggregation from PBP
                # Goals
                goals = pbp[pbp['Event'] == 'GOAL'].groupby(['player1Id', 'eventTeam', 'strength']).size().reset_index(name='goals')
                # Assists (player2 and player3 are usually the assists)
                a1 = pbp[pbp['Event'] == 'GOAL'].groupby(['player2Id', 'eventTeam', 'strength']).size().reset_index(name='a1')
                a2 = pbp[pbp['Event'] == 'GOAL'].groupby(['player3Id', 'eventTeam', 'strength']).size().reset_index(name='a2')
                # Shots
                shots = pbp[pbp['Event'].isin(['SHOT', 'GOAL'])].groupby(['player1Id', 'eventTeam', 'strength']).size().reset_index(name='shots')

                # Merge box score stats into advanced stats
                adv_stats = adv_stats.merge(goals.rename(columns={'player1Id': 'player1Id'}), on=['player1Id', 'eventTeam', 'strength'], how='left')
                adv_stats = adv_stats.merge(shots.rename(columns={'player1Id': 'player1Id'}), on=['player1Id', 'eventTeam', 'strength'], how='left')
                # Simplifying assists for the agg
                adv_stats['assists'] = 0 # Initial
                
                all_game_stats.append(adv_stats)
            except Exception as e:
                LOG.error(f"Failed game {gid}: {e}")

        if all_game_stats:
            season_stats = pd.concat(all_game_stats)
            # Use columns that definitely exist
            agg_cols = {
                'seconds': 'sum', 'minutes': 'sum',
                'CF': 'sum', 'CA': 'sum', 'xG': 'sum', 'xGA': 'sum'
            }
            # Add box score cols only if they exist in the combined df
            for c in ['goals', 'shots', 'assists']:
                if c in season_stats.columns:
                    agg_cols[c] = 'sum'

            agg = season_stats.groupby(['player1Id', 'eventTeam', 'strength']).agg(agg_cols).reset_index()
            agg['points'] = agg.get('goals', 0) + agg.get('assists', 0)
            agg['season'] = s_int
            agg['gamesplayed'] = len(game_ids)
            agg['id'] = agg.apply(lambda r: f"{r['player1Id']}_{s_int}_{r['strength']}", axis=1)
            sync_table("player_stats", agg, "id")

        ros = scrapeRoster(team, s_str)
        if not ros.empty:
            ros['season'] = s_int
            ros['teamabbrev'] = team 
            sync_table("players", ros.copy(), "id")
            sync_table("rosters", ros, "id,season")

if __name__ == "__main__":
    sync_mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    run_sync(sync_mode)