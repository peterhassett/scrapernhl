import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from supabase import create_client, Client
from scrapernhl import scraper_legacy

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# Schema cache to avoid redundant API calls
SCHEMA_CACHE = {}

def get_actual_db_columns(table_name: str) -> list:
    """Fetches valid column names from the DB and caches them."""
    if table_name in SCHEMA_CACHE:
        return SCHEMA_CACHE[table_name]
    
    try:
        # Query the table header only
        res = supabase.table(table_name).select("*").limit(1).execute()
        if res.data:
            cols = list(res.data[0].keys())
            SCHEMA_CACHE[table_name] = cols
            return cols
    except Exception as e:
        print(f"Warning: Could not fetch schema for {table_name}: {e}")
    
    # FALLBACK: If DB query fails, use the "Completist" core list
    # This prevents the script from pruning everything and sending nothing
    core_cols = {
        "teams": ["id", "teamabbrev", "fullname", "teamcommonname", "teamplacename", "firstseasonid", "lastseasonid", "mostrecentteamid", "scrapedon", "source"],
        "players": ["id", "firstname_default", "lastname_default", "positioncode", "shootscatches", "birthdate", "birthcountry", "heightininches", "weightinpounds", "headshot", "sweaternumber", "is_active", "is_rookie", "scrapedon", "source"],
        "player_stats": ["id", "playerid", "season", "team_tri_code", "is_goalie", "gamesplayed", "goals", "assists", "points", "plusminus", "penaltyminutes", "xg_for", "xg_against", "xgf_pct", "corsi_for", "corsi_against", "corsi_pct", "scrapedon", "source"]
    }
    return core_cols.get(table_name, [])

def robust_prepare(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Flatten, type-fix, and prune data while ensuring at least core columns remain."""
    if df.empty: return df
    
    # 1. Standardize Names (Flatten dots)
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. Fix 22P02: Force IDs and counts to BIGINT (Int64)
    int_patterns = ['id', 'season', 'number', 'played', 'goals', 'assists', 'points', 'year', 'wins']
    for col in df.columns:
        if any(pat in col for pat in int_patterns):
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    # 3. JSON Compliance
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

    # 4. SELECTIVE PRUNING
    valid_db_cols = get_actual_db_columns(table_name)
    if valid_db_cols:
        matching_cols = [c for c in df.columns if c in valid_db_cols]
        if not matching_cols:
            print(f"CRITICAL: No matching columns for {table_name}. Check DB schema.")
            return pd.DataFrame()
        return df[matching_cols]
    
    return df

def sync_table(table_name: str, df: pd.DataFrame, p_key: str):
    """Execution of the fault-tolerant sync."""
    df_ready = robust_prepare(df, table_name)
    if df_ready.empty:
        print(f"Skipping {table_name}: No valid data found.")
        return
        
    data = df_ready.to_dict(orient="records")
    try:
        supabase.table(table_name).upsert(data, on_conflict=p_key).execute()
        print(f"Successfully synced {len(data)} rows to {table_name}")
    except Exception as e:
        print(f"Error syncing {table_name}: {e}")

def run_sync(mode="daily"):
    print(f"Starting COMPLETIST sync: {mode}")
    current_season = "20242025"
    
    # 1. Sync Teams (Foundation)
    from scrapernhl.scrapers.teams import scrapeTeams
    teams_df = scrapeTeams(source="records")
    sync_table("teams", teams_df, "id")
    
    # Get active teams
    t_ready = robust_prepare(teams_df.copy(), "teams")
    if not t_ready.empty and 'teamabbrev' in t_ready.columns:
        # Filter for teams without a 'lastseasonid' (active franchises)
        active_teams = t_ready[t_ready['lastseasonid'].isna()]['teamabbrev'].dropna().tolist()
    else:
        # Hardcoded fallback to ensure job doesn't exit if teams sync fails
        active_teams = ['MTL', 'VAN', 'CGY', 'NYI', 'NJD', 'WSH', 'EDM', 'CAR', 'COL', 'SJS', 'OTT', 'TBL']

    if mode == "catchup":
        for team in active_teams:
            print(f"Processing Team: {team}")
            from scrapernhl.scrapers.roster import scrapeRoster
            from scrapernhl.scrapers.schedule import scrapeSchedule
            
            # 2. Sync Rosters/Players
            roster_raw = scrapeRoster(team, current_season)
            sync_table("players", roster_raw, "id")
            
            # 3. Sync Schedule
            sync_table("schedule", scrapeSchedule(team, current_season), "id")

            # 4. Sync Advanced Stats
            for is_goalie in [False, True]:
                stats_raw = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                if not stats_raw.empty:
                    # Generate seasonal composite ID
                    p_id_col = 'playerid' if 'playerid' in robust_prepare(stats_raw.copy(), "player_stats").columns else 'playerId'
                    stats_raw['id'] = stats_raw.apply(lambda r: f"{r[p_id_col]}_{current_season}_{is_goalie}", axis=1)
                    stats_raw['team_tri_code'] = team
                    stats_raw['is_goalie'] = is_goalie
                    sync_table("player_stats", stats_raw, "id")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")