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

# CACHE for valid column names to prevent PGRST204 errors
PHYSICAL_SCHEMA_CACHE = {}

def get_actual_columns(table_name: str) -> list:
    """Fetches valid column names from the DB and caches them."""
    if table_name in PHYSICAL_SCHEMA_CACHE:
        return PHYSICAL_SCHEMA_CACHE[table_name]
    try:
        # Get the table structure header
        res = supabase.table(table_name).select("*").limit(0).execute()
        cols = list(res.data[0].keys()) if res.data else []
        PHYSICAL_SCHEMA_CACHE[table_name] = cols
        return cols
    except Exception as e:
        print(f"Warning: Fetching schema for {table_name} failed: {e}")
        return []

def completist_prepare(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Standardizes names, fixes types, and FILTERS noise columns."""
    if df.empty: return df
    
    # 1. Flatten dots to underscores (firstName.default -> firstname_default)
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. Targeted Numeric Casting (Prevents TypeError on strings like 'headshot')
    num_patterns = ['id', 'season', 'number', 'played', 'goals', 'assists', 'points', 'wins', 'pick']
    for col in df.columns:
        if any(pat in col for pat in num_patterns):
            # Safe numeric conversion only for target columns
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    # 3. JSON Compliance
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

    # 4. CRITICAL: Physical Intersection Check
    # Automatically ignores unwanted 'firstname_fi', 'airlinedesc', etc.
    db_cols = get_actual_columns(table_name)
    if db_cols:
        matching = [c for c in df.columns if c in db_cols]
        return df[matching]
    
    return df

def sync_table(table_name: str, df: pd.DataFrame, p_key: str):
    """Reliable tabular upsert."""
    df_ready = completist_prepare(df, table_name)
    if df_ready.empty: return
    try:
        supabase.table(table_name).upsert(df_ready.to_dict(orient="records"), on_conflict=p_key).execute()
        print(f"Successfully synced {len(df_ready)} rows to {table_name}")
    except Exception as e:
        print(f"Failed {table_name}: {e}")

def run_sync(mode="daily"):
    print(f"Starting FINAL COMPLETIST sync: {mode}")
    current_season = "20242025"
    
    # 1. Sync Teams (Foundation)
    from scrapernhl.scrapers.teams import scrapeTeams
    sync_table("teams", scrapeTeams(source="records"), "id")
    
    active_teams = ['MTL', 'VAN', 'CGY', 'NYI', 'NJD', 'WSH', 'EDM', 'CAR', 'COL', 'SJS', 'OTT', 'TBL']

    if mode == "catchup":
        # 2. DRAFT (Last 5 Years)
        from scrapernhl.scrapers.draft import scrapeDraftData
        for year in range(2020, 2026):
            sync_table("draft", scrapeDraftData(str(year), 1), "year,overall_pick")

        for team in active_teams:
            print(f"Deep Sync: {team}")
            from scrapernhl.scrapers.roster import scrapeRoster
            from scrapernhl.scrapers.schedule import scrapeSchedule
            
            # 3. SCHEDULE
            sync_table("schedule", scrapeSchedule(team, current_season), "id")

            # 4. PLAYERS & 5. ROSTERS
            roster_raw = scrapeRoster(team, current_season)
            sync_table("players", roster_raw.copy(), "id")
            sync_table("rosters", roster_raw, "id,season")

            # 6. PLAYER_STATS (XGBoost Metrics)
            for is_goalie in [False, True]:
                stats = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                if not stats.empty:
                    # Specific ID generation for seasonal stats
                    p_id_col = 'playerid' if 'playerid' in stats.columns else 'playerId'
                    stats['id'] = stats.apply(lambda r: f"{r[p_id_col]}_{current_season}_{is_goalie}", axis=1)
                    stats['playerid'] = stats[p_id_col]
                    stats['season'] = int(current_season)
                    sync_table("player_stats", stats, "id")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")