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

def get_sql_type(dtype):
    """Maps Pandas dtypes to PostgreSQL types."""
    if pd.api.types.is_integer_dtype(dtype): return "BIGINT"
    if pd.api.types.is_float_dtype(dtype): return "NUMERIC"
    if pd.api.types.is_bool_dtype(dtype): return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dtype): return "TIMESTAMPTZ"
    return "TEXT"

def sync_schema_and_upsert(table_name: str, df: pd.DataFrame, p_key: str):
    """Detects new columns, adds them to SQL, then upserts the data."""
    if df.empty: return
    
    # 1. Standardize Names
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. Fix dtypes for Postgres (Int64 for integers to handle NaNs)
    for col in df.columns:
        if "id" in col or "year" in col or "played" in col or "goals" in col:
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')
    
    # 3. Dynamic Column Sync: Add missing columns to Supabase
    for col in df.columns:
        if col == "id" or col in p_key: continue
        sql_type = get_sql_type(df[col].dtype)
        # Call the helper function we created in SQL
        supabase.rpc('add_column_if_missing', {
            't_name': table_name, 
            'c_name': col, 
            'c_type': sql_type
        }).execute()

    # 4. Refresh PostgREST cache so it sees the new columns
    supabase.rpc('reload_schema_cache').execute() # Requires a simple reload RPC

    # 5. Clean JSON compliance and Upsert
    df_clean = df.replace({np.nan: None, np.inf: None, -np.inf: None})
    data = df_clean.to_dict(orient="records")
    
    try:
        supabase.table(table_name).upsert(data, on_conflict=p_key).execute()
        print(f"Successfully synced {len(data)} rows to {table_name}")
    except Exception as e:
        print(f"Error in {table_name}: {e}")

def run_sync(mode="daily"):
    print(f"Starting DYNAMIC COMPLETIST sync: {mode}")
    current_season = "20242025"
    
    # Example: Teams
    from scrapernhl.scrapers.teams import scrapeTeams
    teams_df = scrapeTeams(source="records")
    sync_schema_and_upsert("teams", teams_df, "id")
    
    # For catchup mode, process all active teams
    active_teams = ['MTL', 'VAN', 'CGY', 'NYI', 'NJD', 'WSH', 'EDM', 'CAR', 'COL', 'SJS', 'OTT', 'TBL']

    if mode == "catchup":
        for team in active_teams:
            print(f"Deep Syncing: {team}")
            from scrapernhl.scrapers.roster import scrapeRoster
            from scrapernhl.scrapers.schedule import scrapeSchedule
            
            # These will now automatically add columns like 'firstname_fi' if found
            sync_schema_and_upsert("players", scrapeRoster(team, current_season), "id")
            sync_schema_and_upsert("schedule", scrapeSchedule(team, current_season), "id")

            for is_goalie in [False, True]:
                stats = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                if not stats.empty:
                    p_id = 'playerid' if 'playerid' in stats.columns else 'playerId'
                    stats['id'] = stats.apply(lambda r: f"{r[p_id]}_{current_season}_{is_goalie}", axis=1)
                    sync_schema_and_upsert("player_stats", stats, "id")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")