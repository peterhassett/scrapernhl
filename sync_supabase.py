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

# Configuration for Logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
LOG = logging.getLogger(__name__)

# Supabase Connection
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
if not url or not key:
    LOG.error("Environment variables SUPABASE_URL or SUPABASE_KEY are missing.")
    sys.exit(1)

supabase: Client = create_client(url, key)

# --- MASTER WHITELISTS ---
WHITELISTS = {
    "teams": ["id", "fullname", "teamabbrev", "teamcommonname", "teamplacename", "activestatus", "firstseasonid", "lastseasonid", "mostrecentteamid", "conferencename", "divisionname", "franchiseid", "logos"],
    "players": ["id", "firstname_default", "lastname_default", "headshot", "positioncode", "shootscatches", "heightininches", "heightincentimeters", "weightinpounds", "weightinkilograms", "birthdate", "birthcountry", "birthcity_default", "birthstateprovince_default"],
    "rosters": ["id", "season", "teamabbrev", "sweaternumber", "positioncode"],
    "player_stats": ["id", "playerid", "season", "is_goalie", "team", "opp", "strength", "gamesplayed", "gamesstarted", "goals", "assists", "points", "plusminus", "penaltyminutes", "powerplaygoals", "shorthandedgoals", "gamewinninggoals", "overtimegoals", "shots", "shotsagainst", "saves", "goalsagainst", "shutouts", "shootingpctg", "savepercentage", "goalsagainstaverage", "cf", "ca", "cf_pct", "ff", "fa", "ff_pct", "sf", "sa", "sf_pct", "gf", "ga", "gf_pct", "xg", "xga", "xgf_pct", "pf", "pa", "give_for", "give_against", "take_for", "take_against", "seconds", "minutes", "avgtimeonicepergame", "avgshiftspergame", "faceoffwinpctg"],
    "schedule": ["id", "season", "gamedate", "gametype", "gamestate", "hometeam_id", "hometeam_abbrev", "hometeam_score", "hometeam_commonname_default", "hometeam_placename_default", "hometeam_logo", "awayteam_id", "awayteam_abbrev", "awayteam_score", "awayteam_commonname_default", "awayteam_placename_default", "awayteam_logo", "venue_default", "venue_location_default", "starttimeutc", "easternutcoffset", "venueutcoffset", "gamecenterlink"],
    "plays": ["id", "game_id", "event_id", "period", "period_type", "time_in_period", "time_remaining", "situation_code", "home_team_defending_side", "event_type", "type_desc_key", "x_coord", "y_coord", "zone_code", "ppt_replay_url"],
    "draft": ["year", "overallpick", "roundnumber", "pickinround", "team_tricode", "player_id", "player_firstname", "player_lastname", "player_position", "player_birthcountry", "player_birthstateprovince", "player_years_pro", "amateurclubname", "amateurleague", "countrycode", "displayabbrev_default"],
    "standings": ["date", "teamabbrev_default", "teamname_default", "teamcommonname_default", "conferencename", "divisionname", "gamesplayed", "wins", "losses", "otlosses", "points", "pointpctg", "regulationwins", "row", "goalsfor", "goalsagainst", "goaldifferential", "streakcode", "streakcount"]
}

STRICT_INTS = ["id", "playerid", "season", "game_id", "event_id", "hometeam_id", "awayteam_id", "franchiseid", "player_id", "firstseasonid", "lastseasonid", "mostrecentteamid", "year", "overallpick", "roundnumber", "pickinround", "gametype", "sweaternumber"]

def terminal_cast(val, col, table):
    """Rigid type enforcement for PostgreSQL compatibility and JSON compliance."""
    # Catch NaN, Infinity, and None immediately
    if pd.isna(val) or val is None or val == float('inf') or val == float('-inf'):
        return None
    
    if col == "activestatus":
        if isinstance(val, bool): return val
        return str(val).lower() in ['true', '1', 'y', '1.0']

    if col in STRICT_INTS:
        if not (table in ["player_stats", "plays"] and col == "id"):
            try: return int(round(float(val)))
            except: return 0
            
    if isinstance(val, (float, np.floating, int, np.integer)):
        # Final JSON safety check for numeric types
        f_val = float(val)
        return None if np.isnan(f_val) or np.isinf(f_val) else f_val
        
    return str(val)

def clean_and_validate(df: pd.DataFrame, table_name: str) -> list:
    if df.empty: return []
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    if table_name == "player_stats":
        if 'player1id' in df.columns: df['playerid'] = df['player1id']
        if 'eventteam' in df.columns: df['team'] = df['eventteam']
    allowed = WHITELISTS.get(table_name, [])
    actual_cols = [c for c in df.columns if c in allowed]
    df = df[actual_cols].copy()
    records = []
    for row in df.to_dict(orient="records"):
        clean_row = {k: terminal_cast(v, k, table_name) for k, v in row.items()}
        for d_col in ['gamedate', 'birthdate', 'starttimeutc', 'date']:
            if d_col in clean_row and str(clean_row[d_col]) in ["0.0", "0", "nan"]:
                clean_row[d_col] = None
        records.append(clean_row)
    return records

def sync_table(table_name: str, df: pd.DataFrame, p_key_str: str):
    records = clean_and_validate(df, table_name)
    if not records: return
    keys_to_check = [k.strip() for k in p_key_str.split(',')]
    temp_df = pd.DataFrame(records)
    if not temp_df.empty:
        temp_df = temp_df.drop_duplicates(subset=keys_to_check, keep='last')
        records = temp_df.to_dict(orient="records")
    try:
        supabase.table(table_name).upsert(records, on_conflict=p_key_str).execute()
        LOG.info(f"Successfully synced {len(records)} records to '{table_name}'.")
    except Exception as e:
        LOG.error(f"Sync failed for table '{table_name}': {e}")

def run_sync(mode="daily"):
    S_STR, S_INT = "20252026", 20252026
    LOG.info(f"--- NHL SUPABASE SYNC START | Mode: {mode} ---")
    
    raw_teams = scrapeTeams(source="records")
    flattened_data = []
    for _, row in raw_teams.iterrows():
        base = row.to_dict()
        nested_list = base.get('teams', [])
        base['activestatus'], base['conferencename'], base['divisionname'], base['logos'] = False, None, None, None
        if isinstance(nested_list, list) and len(nested_list) > 0:
            team_info = nested_list[0]
            if isinstance(team_info, dict):
                base['activestatus'] = True if team_info.get('active') == 'Y' else False
                conf, div = team_info.get('conference'), team_info.get('division')
                if isinstance(conf, dict): base['conferencename'] = conf.get('name')
                if isinstance(div, dict): base['divisionname'] = div.get('name')
                base['logos'] = str(team_info.get('logos', []))
        flattened_data.append(base)
    
    teams_df = pd.DataFrame(flattened_data)
    teams_df.columns = [str(c).replace('.', '_').lower() for c in teams_df.columns]
    sync_table("teams", teams_df, "id")
    
    all_teams = ['MTL', 'BUF'] if mode == "debug" else teams_df[teams_df['activestatus'] == True]['teamabbrev'].unique().tolist()
    
    global_game_ids = set()
    all_processed_stats = []

    for team in all_teams:
        LOG.info(f"*** Scanning Schedule: {team} ***")
        ros = scrapeRoster(team, S_STR)
        if not ros.empty:
            ros.columns = [str(c).replace('.', '_').lower() for c in ros.columns]
            ros['season'] = S_INT
            ros['teamabbrev'] = team 
            sync_table("players", ros.copy(), "id")
            sync_table("rosters", ros, "id,season")

        schedule_raw = scrapeSchedule(team, S_STR)
        schedule_raw.columns = [str(c).replace('.', '_').lower() for c in schedule_raw.columns]
        schedule_raw = schedule_raw[schedule_raw['gametype'] == 2]
        
        if mode == "daily":
            cutoff = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
            schedule_raw = schedule_raw[schedule_raw['gamedate'] >= cutoff]
        
        sync_table("schedule", schedule_raw, "id")
        completed = schedule_raw[schedule_raw['gamestate'].isin(['FINAL', 'OFF'])]
        global_game_ids.update(completed['id'].tolist())

    game_list = sorted(list(global_game_ids))
    if mode == "debug": game_list = game_list[:5]
    
    for gid in game_list:
        try:
            LOG.info(f"Processing Game ID: {gid}")
            pbp = scrape_game(gid)
            pbp = engineer_xg_features(pbp)
            pbp = predict_xg_for_pbp(pbp)
            u_p = pbp[['player1Id', 'player1Name']].dropna().drop_duplicates()
            mini_p = pd.DataFrame({'id': u_p['player1Id'], 'firstname_default': u_p['player1Name']})
            sync_table("players", mini_p, "id")
            stats = on_ice_stats_by_player_strength(pbp, include_goalies=False)
            goals = pbp[pbp['Event'] == 'GOAL'].groupby(['player1Id', 'eventTeam', 'strength']).size().reset_index(name='goals')
            shots = pbp[pbp['Event'].isin(['SHOT', 'GOAL'])].groupby(['player1Id', 'eventTeam', 'strength']).size().reset_index(name='shots')
            a1 = pbp[pbp['Event'] == 'GOAL'].groupby(['player2Id', 'eventTeam', 'strength']).size().reset_index(name='a1')
            a2 = pbp[pbp['Event'] == 'GOAL'].groupby(['player3Id', 'eventTeam', 'strength']).size().reset_index(name='a2')
            stats = stats.merge(goals, on=['player1Id', 'eventTeam', 'strength'], how='left')
            stats = stats.merge(shots, on=['player1Id', 'eventTeam', 'strength'], how='left')
            stats = stats.merge(a1.rename(columns={'player2Id': 'player1Id'}), on=['player1Id', 'eventTeam', 'strength'], how='left')
            stats = stats.merge(a2.rename(columns={'player3Id': 'player1Id'}), on=['player1Id', 'eventTeam', 'strength'], how='left')
            all_processed_stats.append(stats)
        except Exception as e:
            LOG.error(f"Failed Game {gid}: {e}")

    if all_processed_stats:
        combined = pd.concat(all_processed_stats)
        metrics = ['seconds', 'minutes', 'CF', 'CA', 'FF', 'FA', 'SF', 'SA', 'GF', 'GA', 'xG', 'xGA', 'goals', 'shots', 'a1', 'a2']
        sum_map = {m: 'sum' for m in metrics if m in combined.columns}
        agg = combined.groupby(['player1Id', 'eventTeam', 'strength']).agg(sum_map).reset_index()
        agg['assists'] = agg.get('a1', 0) + agg.get('a2', 0)
        agg['points'] = agg.get('goals', 0) + agg['assists']
        agg['season'] = S_INT
        gp_count = combined.groupby(['player1Id', 'eventTeam', 'strength']).size().reset_index(name='gamesplayed')
        agg = agg.drop(columns=['gamesplayed'], errors='ignore').merge(gp_count, on=['player1Id', 'eventTeam', 'strength'])
        agg['id'] = agg.apply(lambda r: f"{int(float(r['player1Id']))}_{S_INT}_{r['strength']}", axis=1)
        sync_table("player_stats", agg, "id")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["daily", "catchup", "debug"], default="daily", nargs="?")
    args = parser.parse_args()
    run_sync(args.mode)