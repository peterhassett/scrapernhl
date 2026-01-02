import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from supabase import create_client, Client

# Modular imports based on your README
from scrapernhl import scraper_legacy
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.schedule import scrapeSchedule

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def completist_prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Prepares DF for Postgres by flattening, type-fixing, and JSON cleaning."""
    if df.empty: return df
    
    # 1. Flatten nested columns (firstName.default -> firstname_default)
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. Fix 22P02: Force all ID and count columns to nullable Int64 to avoid .0 float errors
    int_patterns = ['id', 'season', 'number', 'played', 'goals', 'assists', 'points', 'year', 'wins', 'losses']
    for col in df.columns:
        if any(pat in col for pat in int_patterns):
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    # 3. JSON Compliance: Convert NaNs to None
    return df.replace({np.nan: None, np.inf: None, -np.inf: None})

def sync_table(table_name: str, df: pd.DataFrame, p_key: str):
    """Generic upsert that adapts to the provided DataFrame."""
    df = completist_prepare(df)
    if df.empty: return
    
    # Note: If the table doesn't exist, Supabase requires manual creation 
    # for security. But now we have the EXACT field names from the préparé DF.
    try:
        data = df.to_dict(orient="records")
        supabase.table(table_name).upsert(data, on_conflict=p_key).execute()
        print(f"Synced {len(data)} rows to {table_name}")
    except Exception as e:
        print(f"Error syncing {table_name}: {e}")

def run_sync(mode="daily"):
    print(f"Starting DYNAMIC COMPLETIST sync: {mode}")
    current_season = "20242025"
    
    # TEAMS (The foundation)
    teams_df = scrapeTeams(source="records")
    sync_table("teams", teams_df, "id")
    
    # Use the cleaned version for the loop
    t_clean = completist_prepare(teams_df.copy())
    active_teams = t_clean[t_clean['lastseasonid'].isna()]['teamabbrev'].tolist()

    if mode == "catchup":
        # 1. DRAFT (Historical Registry)
        for year in range(2020, 2026):
            draft_df = scraper_legacy.scrapeDraftRecords(year)
            sync_table("draft", draft_df, "year,overall_pick")

        for team in active_teams:
            print(f"Deep Sync: {team}")
            # 2. SCHEDULE & PLAYERS
            sync_table("schedule", scrapeSchedule(team, current_season), "id")
            sync_table("players", scrapeRoster(team, current_season), "id")

            # 3. ADVANCED STATS (XGBoost Metrics)
            for is_goalie in [False, True]:
                stats_raw = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                if not stats_raw.empty:
                    stats_df = completist_prepare(stats_raw)
                    p_id_col = 'playerid' if 'playerid' in stats_df.columns else 'player_id'
                    stats_df['id'] = stats_df.apply(lambda r: f"{r[p_id_col]}_{current_season}_{is_goalie}", axis=1)
                    stats_df['team_tri_code'] = team
                    stats_df['is_goalie'] = is_goalie
                    sync_table("player_stats", stats_df, "id")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")