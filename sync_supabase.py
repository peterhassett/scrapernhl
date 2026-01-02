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

# Ground Truth Whitelist
WHITELISTS = {
    "teams": ["id", "fullname", "teamabbrev", "teamcommonname", "teamplacename", "active_status", "conference_name", "division_name"],
    "players": ["id", "firstname_default", "lastname_default", "headshot", "positioncode", "heightininches", "weightinpounds", "birthdate", "birthcountry"],
    "rosters": ["id", "season", "teamabbrev", "sweaternumber", "positioncode"],
    "player_stats": ["id", "playerid", "season", "team", "strength", "gamesplayed", "goals", "assists", "points", "shots", "cf", "ca", "ff", "fa", "sf", "sa", "gf", "ga", "xg", "xga", "seconds", "minutes"],
    "schedule": ["id", "season", "gamedate", "gametype", "gamestate", "hometeam_id", "hometeam_abbrev", "hometeam_score", "hometeam_commonname_default", "awayteam_id", "awayteam_abbrev", "awayteam_score", "awayteam_commonname_default", "venue_default", "starttimeutc", "gamecenterlink"]
}

# The definitive list of fields that MUST be integers (BIGINT in Postgres)
# If it's on this list, we strip the decimal.
INT_FIELDS = [
    "playerid", "season", "gamesplayed", "goals", "assists", "points", "shots",
    "cf", "ca", "ff", "fa", "sf", "sa", "gf", "ga", "hometeam_id", "awayteam_id",
    "hometeam_score", "awayteam_score", "gametype", "sweaternumber"
]

def strict_cast(val, col, table):
    """Atomic casting to ensure no '1.0' values hit BIGINT columns."""
    if pd.isna(val) or val is None:
        return None
    
    # Force pure Python int for BIGINT columns
    if col in INT_FIELDS or (col == "id" and table != "player_stats"):
        try:
            return int(round(float(val)))
        except:
            return 0
            
    # Force native Python float for analytics (xG, seconds, minutes)
    if isinstance(val, (float, np.floating, int, np.integer)):
        return float(val)
        
    return str(val)

def clean_and_validate(df: pd.DataFrame, table_name: str) -> list:
    if df.empty: return []
    
    # Standardize column names
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    if table_name == "player_stats":
        if 'player1id' in df.columns: df['playerid'] = df['player1id']
        if 'eventteam' in df.columns: df['team'] = df['eventteam']

    # Filter to whitelisted columns
    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()

    # Move from Pandas to list of dicts and perform the Terminal Cast
    records = []
    for row in df.to_dict(orient="records"):
        clean_row = {k: strict_cast(v, k, table_name) for k, v in row.items()}
        
        # Guard against '0.0' dates
        for d_col in ['gamedate', 'birthdate', 'starttimeutc']:
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
    s_str, s_int = "20242025", 20242025
    
    LOG.info("Syncing Teams...")
    teams_df = scrapeTeams(source="records")
    sync_table("teams", teams_df, "id")
    
    active_teams = ['MTL', 'BUF'] if mode == "debug" else teams_df['teamAbbrev'].unique().tolist()

    for team in active_teams:
        LOG.info(f"--- {team} ---")
        schedule_raw = scrapeSchedule(team, s_str)
        sync_table("schedule", schedule_raw, "id")

        # Process games that are completed
        sched_norm = schedule_raw.copy()
        sched_norm.columns = [str(c).replace('.', '_').lower() for c in sched_norm.columns]
        # SKIP PRESEASON (Type 1) if requested, or just process FINAL games
        completed = sched_norm[sched_norm['gamestate'].isin(['FINAL', 'OFF'])]
        
        game_ids = completed['id'].tolist()
        if mode == "debug": game_ids = game_ids[:2]

        all_game_stats = []
        for gid in game_ids:
            try:
                LOG.info(f"Processing Game {gid}...")
                pbp = scrape_game(gid)
                pbp = engineer_xg_features(pbp)
                pbp = predict_xg_for_pbp(pbp)
                stats = on_ice_stats_by_player_strength(pbp, include_goalies=False)
                
                # Manual merge for goals/shots/assists
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
            metrics = ['seconds', 'minutes', 'CF', 'CA', 'xG', 'xGA', 'goals', 'shots', 'a1', 'a2']
            sum_map = {m: 'sum' for m in metrics if m in combined.columns}
            
            agg = combined.groupby(['player1Id', 'eventTeam', 'strength']).agg(sum_map).reset_index()
            agg['assists'] = agg.get('a1', 0) + agg.get('a2', 0)
            agg['points'] = agg.get('goals', 0) + agg['assists']
            agg['season'] = s_int
            agg['gamesplayed'] = len(game_ids)
            
            # Clean composite ID: '847..._2024_5v5'
            agg['id'] = agg.apply(lambda r: f"{int(float(r['player1Id']))}_{s_int}_{r['strength']}", axis=1)
            
            sync_table("player_stats", agg, "id")

        # Roster
        ros = scrapeRoster(team, s_str)
        if not ros.empty:
            ros['season'] = s_int
            ros['teamabbrev'] = team 
            sync_table("players", ros.copy(), "id")
            sync_table("rosters", ros, "id,season")

if __name__ == "__main__":
    sync_mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    run_sync(sync_mode)