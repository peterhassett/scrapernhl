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

def get_actual_db_columns(table_name: str) -> list:
    """Fetches valid column names from the DB to ensure no PGRST204 errors."""
    try:
        res = supabase.table(table_name).select("*").limit(0).execute()
        return list(res.data[0].keys()) if res.data else []
    except:
        return []

def completist_prepare(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Flatten, type-fix, and SELECTIVELY PRUNE to match physical DB columns."""
    if df.empty: return df
    
    # 1. Standardize Names
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. Kill 22P02: Force IDs and counts to BIGINT
    int_patterns = ['id', 'season', 'number', 'played', 'goals', 'assists', 'points', 'year']
    for col in df.columns:
        if any(pat in col for pat in int_patterns):
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    # 3. JSON Compliance
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

    # 4. SELECTIVE PRUNING (The "I Don't Care About Others" Fix)
    valid_db_cols = get_actual_db_columns(table_name)
    if valid_db_cols:
        matching_cols = [c for c in df.columns if c in valid_db_cols]
        return df[matching_cols]
    
    return df

def sync_table(table_name: str, df: pd.DataFrame, p_key: str):
    """Fault-tolerant sync execution."""
    df_ready = completist_prepare(df, table_name)
    if df_ready.empty: return
    try:
        supabase.table(table_name).upsert(df_ready.to_dict(orient="records"), on_conflict=p_key).execute()
        print(f"Synced {table_name}")
    except Exception as e:
        print(f"Failed {table_name}: {e}")

def run_sync(mode="daily"):
    print(f"Starting FINAL COMPLETIST sync: {mode}")
    current_season = "20242025"
    
    from scrapernhl.scrapers.teams import scrapeTeams
    sync_table("teams", scrapeTeams(source="records"), "id")
    
    active_teams = ['MTL', 'VAN', 'CGY', 'NYI', 'NJD', 'WSH', 'EDM', 'CAR', 'COL', 'SJS', 'OTT', 'TBL']

    if mode == "catchup":
        for team in active_teams:
            print(f"Deep Sync: {team}")
            from scrapernhl.scrapers.roster import scrapeRoster
            from scrapernhl.scrapers.schedule import scrapeSchedule
            
            sync_table("schedule", scrapeSchedule(team, current_season), "id")
            sync_table("players", scrapeRoster(team, current_season), "id")

            for is_goalie in [False, True]:
                stats = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                if not stats.empty:
                    # Specific ID generation
                    p_id_col = 'playerid' if 'playerid' in stats.columns else 'playerId'
                    stats['id'] = stats.apply(lambda r: f"{r[p_id_col]}_{current_season}_{is_goalie}", axis=1)
                    stats['playerid'] = stats[p_id_col]
                    sync_table("player_stats", stats, "id")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")