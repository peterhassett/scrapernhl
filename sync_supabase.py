import os
import sys
import logging
import pandas as pd
import numpy as np
import argparse
from datetime import datetime, timedelta
from supabase import create_client, Client

# Modular Scrapers
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl import (
    scrape_game, 
    engineer_xg_features, 
    predict_xg_for_pbp, 
    on_ice_stats_by_player_strength
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

# Supabase Connection
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
if not url or not key:
    LOG.error("Missing SUPABASE_URL or SUPABASE_KEY environment variables.")
    sys.exit(1)

supabase: Client = create_client(url, key)

# --- MASTER WHITELISTS FOR ALL 8 TABLES ---
WHITELISTS = {
    "teams": [
        "id", "fullname", "teamabbrev", "teamcommonname", "teamplacename", 
        "firstseasonid", "lastseasonid", "mostrecentteamid", "active_status", 
        "conference_name", "division_name", "franchiseid", "logos"
    ],
    "players": [
        "id", "firstname_default", "lastname_default", "headshot", "positioncode", 
        "shootscatches", "heightininches", "heightincentimeters", "weightinpounds", 
        "weightinkilograms", "birthdate", "birthcountry", "birthcity_default", "birthstateprovince_default"
    ],
    "rosters": [
        "id", "season", "teamabbrev", "sweaternumber", "positioncode"
    ],
    "player_stats": [
        "id", "playerid", "season", "is_goalie", "team", "opp", "strength", 
        "gamesplayed", "gamesstarted", "goals", "assists", "points", "plusminus", 
        "penaltyminutes", "powerplaygoals", "shorthandedgoals", "gamewinninggoals", 
        "overtimegoals", "shots", "shotsagainst", "saves", "goalsagainst", "shutouts", 
        "shootingpctg", "savepercentage", "goalsagainstaverage", "cf", "ca", "cf_pct", 
        "ff", "fa", "ff_pct", "sf", "sa", "sf_pct", "gf", "ga", "gf_pct", "xg", "xga", 
        "xgf_pct", "pf", "pa", "give_for", "give_against", "take_for", "take_against", 
        "seconds", "minutes", "avgtimeonicepergame", "avgshiftspergame", "faceoffwinpctg"
    ],
    "schedule": [
        "id", "season", "gamedate", "gametype", "gamestate", "hometeam_id", 
        "hometeam_abbrev", "hometeam_score", "hometeam_commonname_default", 
        "hometeam_placename_default", "hometeam_logo", "awayteam_id", "awayteam_abbrev", 
        "awayteam_score", "awayteam_commonname_default", "awayteam_placename_default", 
        "awayteam_logo", "venue_default", "venue_location_default", "starttimeutc", 
        "easternutcoffset", "venueutcoffset", "gamecenterlink"
    ],
    "plays": [
        "id", "game_id", "event_id", "period", "period_type", "time_in_period", 
        "time_remaining", "situation_code", "home_team_defending_side", "event_type", 
        "type_desc_key", "x_coord", "y_coord", "zone_code", "ppt_replay_url"
    ],
    "draft": [
        "year", "overall_pick", "round_number", "pick_in_round", "team_tricode", 
        "player_id", "player_firstname", "player_lastname", "player_position", 
        "player_birthcountry", "player_birthstateprovince", "player_years_pro", 
        "amateurclubname", "amateurleague", "countrycode", "displayabbrev_default"
    ],
    "standings": [
        "date", "teamabbrev_default", "teamname_default", "teamcommonname_default", 
        "conference_name", "division_name", "gamesplayed", "wins", "losses", 
        "otlosses", "points", "pointpctg", "regulationwins", "row", "goalsfor", 
        "goalsagainst", "goaldifferential", "streak_code", "streak_count"
    ]
}

# BIGINT guard list - these columns must be pure integers in the JSON payload
STRICT_BIGINTS = [
    "id", "playerid", "season", "game_id", "event_id", "hometeam_id", "awayteam_id", 
    "franchiseid", "player_id", "firstseasonid", "lastseasonid", "mostrecentteamid",
    "year", "overall_pick", "round_number", "pick_in_round"
]

def terminal_cast(val, col, table):
    """Atomic cleaning of Python types for SQL BIGINT vs NUMERIC compatibility."""
    if pd.isna(val) or val is None:
        return None
    
    # Force pure Python int for BIGINT columns to satisfy PostgREST
    if col in STRICT_BIGINTS:
        # Exceptions for text-based IDs in player_stats and plays
        if not (table in ["player_stats", "plays"] and col == "id"):
            try:
                return int(round(float(val)))
            except:
                return 0
    
    # Force native Python float for all others (NUMERIC/Analytics columns)
    if isinstance(val, (float, np.floating, int, np.integer)):
        return float(val)
        
    return str(val)

def clean_and_validate(df: pd.DataFrame, table_name: str) -> list:
    if df.empty: return []
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    # Handle column naming variations for the stats table
    if table_name == "player_stats":
        if 'player1id' in df.columns: df['playerid'] = df['player1id']
        if 'eventteam' in df.columns: df['team'] = df['eventteam']

    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()

    records = []
    for row in df.to_dict(orient="records"):
        clean_row = {k: terminal_cast(v, k, table_name) for k, v in row.items()}
        # Date/Time cleanup for Pandas '0.0' or 'NaN' artifacts
        for d_col in ['gamedate', 'birthdate', 'starttimeutc', 'date']:
            if d_col in clean_row and str(clean_row[d_col]) in ["0.0", "0", "nan"]:
                clean_row[d_col] = None
        records.append(clean_row)
    return records

def sync_table(table_name: str, df: pd.DataFrame, p_key_str: str):
    records = clean_and_validate(df, table_name)
    if not records: return
    try:
        supabase.table(table_name).upsert(records, on_conflict=p_key_str).execute()
        LOG.info(f"Synced {len(records)} to {table_name}")
    except Exception as e:
        LOG.error(f"Sync failed for {table_name}: {e}")

def run_sync(mode="daily"):
    # --- PRODUCTION CONFIG: 2025-2026 Regular Season ---
    S_STR = "20252026"
    S_INT = 20252026
    
    LOG.info(f"SYNC START: Mode={mode} | Season={S_STR}")
    
    # 1. Teams Sync
    teams_df = scrapeTeams(source="records")
    sync_table("teams", teams_df, "id")
    
    # Determine which teams to process based on mode
    if mode == "debug":
        all_teams = ['MTL', 'BUF']
    else:
        all_teams = teams_df[teams_df['active_status'] == True]['teamAbbrev'].unique().tolist()
    
    for team in all_teams:
        LOG.info(f"--- Processing {team} ---")
        
        # 2. Roster/Players (Fills Parent Tables)
        ros = scrapeRoster(team, S_STR)
        if not ros.empty:
            ros['season'] = S_INT
            ros['teamabbrev'] = team 
            sync_table("players", ros.copy(), "id")
            sync_table("rosters", ros, "id,season")

        # 3. Schedule Sync
        schedule_raw = scrapeSchedule(team, S_STR)
        schedule_raw = schedule_raw[schedule_raw['gameType'] == 2] # Regular Season only
        
        if mode == "daily":
            # Only look at games in the last 5 days for daily updates
            schedule_raw['gamedate_dt'] = pd.to_datetime(schedule_raw['gameDate'])
            cutoff = datetime.now() - timedelta(days=5)
            schedule_raw = schedule_raw[schedule_raw['gamedate_dt'] >= cutoff]
        
        sync_table("schedule", schedule_raw, "id")

        # 4. Detailed Game Stats
        completed = schedule_raw[schedule_raw['gameState'].isin(['FINAL', 'OFF'])]
        game_ids = completed['id'].tolist()
        
        if mode == "debug":
            game_ids = game_ids[:2] # Limit to 2 games in debug mode

        all_game_stats = []
        for gid in game_ids:
            try:
                LOG.info(f"Processing Game {gid}...")
                pbp = scrape_game(gid)
                pbp = engineer_xg_features(pbp)
                pbp = predict_xg_for_pbp(pbp)
                
                # Dynamic Player Registry (Catches players not on current official roster)
                u_p = pbp[['player1Id', 'player1Name']].dropna().drop_duplicates()
                mini_p = pd.DataFrame({'id': u_p['player1Id'], 'firstname_default': u_p['player1Name']})
                sync_table("players", mini_p, "id")

                # Generate Analytics
                stats = on_ice_stats_by_player_strength(pbp, include_goalies=False)
                
                # Merge counting stats (Goals, Shots, Assists)
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
                LOG.error(f"Failed Game {gid}: {e}")

        if all_game_stats:
            combined = pd.concat(all_game_stats)
            metrics = ['seconds', 'minutes', 'CF', 'CA', 'FF', 'FA', 'SF', 'SA', 'GF', 'GA', 'xG', 'xGA', 'goals', 'shots', 'a1', 'a2']
            sum_map = {m: 'sum' for m in metrics if m in combined.columns}
            
            agg = combined.groupby(['player1Id', 'eventTeam', 'strength']).agg(sum_map).reset_index()
            agg['assists'] = agg.get('a1', 0) + agg.get('a2', 0)
            agg['points'] = agg.get('goals', 0) + agg['assists']
            agg['season'] = S_INT
            agg['gamesplayed'] = len(game_ids)
            
            # Create composite ID string while ensuring player ID is cast correctly
            agg['id'] = agg.apply(lambda r: f"{int(float(r['player1Id']))}_{S_INT}_{r['strength']}", axis=1)
            
            sync_table("player_stats", agg, "id")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["daily", "catchup", "debug"], default="daily", nargs="?")
    args = parser.parse_args()
    run_sync(args.mode)