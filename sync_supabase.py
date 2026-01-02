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

PHYSICAL_SCHEMA_CACHE = {}

def get_actual_columns(table_name: str) -> list:
    if table_name in PHYSICAL_SCHEMA_CACHE:
        return PHYSICAL_SCHEMA_CACHE[table_name]
    try:
        res = supabase.table(table_name).select("*").limit(0).execute()
        cols = list(res.data[0].keys()) if res.data else []
        PHYSICAL_SCHEMA_CACHE[table_name] = cols
        return cols
    except: return []

def toi_to_decimal(toi_str):
    """Converts MM:SS string to decimal minutes (Numeric)."""
    if pd.isna(toi_str) or not isinstance(toi_str, str) or ':' not in toi_str:
        return None
    try:
        minutes, seconds = map(int, toi_str.split(':'))
        return round(minutes + (seconds / 60.0), 2)
    except:
        return None

def completist_prepare(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    if df.empty: return df
    
    # 1. Flatten Names
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. CONVERT TOI TO NUMERIC
    if 'avgtimeonicepergame' in df.columns:
        df['avgtimeonicepergame'] = df['avgtimeonicepergame'].apply(toi_to_decimal)

    # 3. DROP ARRAYS/DICTS (Bypasses TypeError)
    cols_to_drop = []
    for col in df.columns:
        non_null = df[col].dropna()
        if not non_null.empty and isinstance(non_null.iloc[0], (list, dict)):
            cols_to_drop.append(col)
    df = df.drop(columns=cols_to_drop)

    # 4. Targeted Numeric Casting
    num_patterns = ['id', 'season', 'number', 'played', 'goals', 'assists', 'points', 'wins', 'pick']
    for col in df.columns:
        if any(pat in col for pat in num_patterns):
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    # 5. JSON Compliance & Intersection Check
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
    db_cols = get_actual_columns(table_name)
    if db_cols:
        matching = [c for c in df.columns if c in db_cols]
        return df[matching]
    return df

def sync_table(table_name: str, df: pd.DataFrame, p_key: str):
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
    
    from scrapernhl.scrapers.teams import scrapeTeams
    teams_raw = scrapeTeams(source="records")
    sync_table("teams", teams_raw, "id")
    
    teams_clean = completist_prepare(teams_raw.copy(), "teams")
    active_teams = teams_clean[teams_clean['lastseasonid'].isna()]['teamabbrev'].dropna().unique().tolist()
    print(f"Processing {len(active_teams)} active franchises.")

    if mode == "catchup":
        from scrapernhl.scrapers.draft import scrapeDraftData
        for year in range(2020, 2026):
            sync_table("draft", scrapeDraftData(str(year), 1), "year,overall_pick")

        for team in active_teams:
            print(f"Deep Sync: {team}")
            from scrapernhl.scrapers.roster import scrapeRoster
            from scrapernhl.scrapers.schedule import scrapeSchedule
            
            sync_table("schedule", scrapeSchedule(team, current_season), "id")
            roster_raw = scrapeRoster(team, current_season)
            sync_table("players", roster_raw.copy(), "id")
            sync_table("rosters", roster_raw, "id,season")

            for is_goalie in [False, True]:
                stats = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                if not stats.empty:
                    p_id_col = 'playerid' if 'playerid' in stats.columns else 'playerId'
                    stats['id'] = stats.apply(lambda r: f"{r[p_id_col]}_{current_season}_{is_goalie}", axis=1)
                    stats['playerid'] = stats[p_id_col]
                    stats['season'] = int(current_season)
                    sync_table("player_stats", stats, "id")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")