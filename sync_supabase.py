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

# SCHEMA CACHE: Stores valid columns for each table to prevent PGRST204 errors
VALID_COLS_CACHE = {}

def get_db_columns(table_name: str) -> list:
    """Queries Supabase for the actual columns in a table."""
    if table_name in VALID_COLS_CACHE:
        return VALID_COLS_CACHE[table_name]
    try:
        # Get table definition header
        res = supabase.table(table_name).select("*").limit(0).execute()
        cols = list(res.data[0].keys()) if res.data else []
        VALID_COLS_CACHE[table_name] = cols
        return cols
    except Exception as e:
        print(f"Warning: Could not fetch schema for {table_name}: {e}")
        return []

def completist_prepare(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Standardizes names, fixes types, and PRUNES unwanted columns."""
    if df.empty: return df
    
    # 1. Flatten Names
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. Kill 22P02 & TypeError: Targeted Numeric Casting
    num_targets = ['id', 'season', 'number', 'played', 'goals', 'assists', 'points', 'wins', 'losses', 'pick']
    for col in df.columns:
        if any(target in col for target in num_targets) and not isinstance(df[col].iloc[0], str):
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    # 3. JSON Compliance
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

    # 4. CRITICAL: Prune to physical DB schema (Ignores unwanted 'firstname_fi', etc.)
    db_cols = get_db_columns(table_name)
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
        print(f"Synced {len(df_ready)} rows to {table_name}")
    except Exception as e:
        print(f"Failed {table_name}: {e}")

def run_sync(mode="daily"):
    print(f"Starting FINAL COMPLETIST sync: {mode}")
    current_season = "20242025"
    
    # 1. TEAMS
    from scrapernhl.scrapers.teams import scrapeTeams
    sync_table("teams", scrapeTeams(source="records"), "id")
    
    active_teams = ['MTL', 'VAN', 'CGY', 'NYI', 'NJD', 'WSH', 'EDM', 'CAR', 'COL', 'SJS', 'OTT', 'TBL']

    if mode == "catchup":
        # 2. DRAFT
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

            # 6. PLAYER_STATS (Advanced Metrics)
            for is_goalie in [False, True]:
                stats = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                if not stats.empty:
                    p_id = 'playerid' if 'playerid' in stats.columns else 'playerId'
                    stats['id'] = stats.apply(lambda r: f"{r[p_id]}_{current_season}_{is_goalie}", axis=1)
                    stats['playerid'] = stats[p_id]
                    stats['season'] = int(current_season)
                    sync_table("player_stats", stats, "id")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")