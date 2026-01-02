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

# CACHE for database columns to prevent redundant API calls
SCHEMA_CACHE = {}

def get_actual_db_columns(table_name: str) -> list:
    """Queries the database to find which columns currently exist in the table."""
    if table_name in SCHEMA_CACHE:
        return SCHEMA_CACHE[table_name]
    
    try:
        # Fetch the table definition (limit 0 is enough to get the header)
        res = supabase.table(table_name).select("*").limit(0).execute()
        cols = list(res.data[0].keys()) if res.data else []
        SCHEMA_CACHE[table_name] = cols
        return cols
    except Exception as e:
        print(f"Warning: Could not fetch schema for {table_name}: {e}")
        return []

def completist_prepare(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Prunes unexpected columns and fixes types before upload."""
    if df.empty: return df
    
    # 1. Standardize Names (dots to underscores)
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. Fix 22P02: Force IDs and counts to BIGINT (Int64)
    int_patterns = ['id', 'season', 'number', 'played', 'goals', 'assists', 'points', 'year']
    for col in df.columns:
        if any(pat in col for pat in int_patterns):
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    # 3. JSON Compliance
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

    # 4. SELECTIVE PRUNING: Only send columns that exist in your SQL schema
    valid_db_cols = get_actual_db_columns(table_name)
    if valid_db_cols:
        # Keep only the columns that are in both the DataFrame and the Database
        matching_cols = [c for c in df.columns if c in valid_db_cols]
        return df[matching_cols]
    
    return df

def sync_table(table_name: str, df: pd.DataFrame, p_key: str):
    """Execution of the fault-tolerant sync."""
    df_ready = completist_prepare(df, table_name)
    if df_ready.empty:
        print(f"No valid columns for {table_name}, skipping.")
        return
        
    try:
        supabase.table(table_name).upsert(df_ready.to_dict(orient="records"), on_conflict=p_key).execute()
        print(f"Synced {len(df_ready)} rows to {table_name}")
    except Exception as e:
        print(f"Error syncing {table_name}: {e}")

def run_sync(mode="daily"):
    print(f"Starting COMPLETIST sync: {mode}")
    current_season = "20242025"
    
    from scrapernhl.scrapers.teams import scrapeTeams
    teams_df = scrapeTeams(source="records")
    sync_table("teams", teams_df, "id")
    
    # Team list from active franchises
    active_teams = ['MTL', 'VAN', 'CGY', 'NYI', 'NJD', 'WSH', 'EDM', 'CAR', 'COL', 'SJS', 'OTT', 'TBL']

    if mode == "catchup":
        for team in active_teams:
            print(f"Deep Sync: {team}")
            from scrapernhl.scrapers.roster import scrapeRoster
            from scrapernhl.scrapers.schedule import scrapeSchedule
            
            # Sync schedule and players (pruning happens automatically inside sync_table)
            sync_table("schedule", scrapeSchedule(team, current_season), "id")
            sync_table("players", scrapeRoster(team, current_season), "id")

            for is_goalie in [False, True]:
                stats = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                if not stats.empty:
                    # Specific ID generation
                    p_id_col = 'playerid' if 'playerid' in stats.columns else 'playerId'
                    stats['id'] = stats.apply(lambda r: f"{r[p_id_col]}_{current_season}_{is_goalie}", axis=1)
                    stats['team_tri_code'] = team
                    stats['is_goalie'] = is_goalie
                    sync_table("player_stats", stats, "id")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")