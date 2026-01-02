import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from supabase import create_client, Client

# Import core package to access the calculated legacy features
from scrapernhl import scraper_legacy
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.standings import scrapeStandings

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def clean_df_for_supabase(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes names for SQL and ensures JSON compliance (NaN to None)."""
    if df.empty:
        return df
    
    # 1. Standardize column names (dots and % to underscores)
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. Convert NaN and Inf to None (JSON 'null') to prevent JSON compliance errors
    # We use .replace(np.nan, None) or .where(pd.notnull(df), None)
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
    return df

def upsert(table: str, df: pd.DataFrame, p_key: str):
    """General helper to clean and upsert dataframes to Supabase."""
    df = clean_df_for_supabase(df)
    if df.empty:
        print(f"Skipping {table}: No data.")
        return
    
    # Convert to list of dicts for bulk upsert
    data = df.to_dict(orient="records")
    try:
        # default_to_null=False ensures we use DB column defaults when fields are missing
        supabase.table(table).upsert(data, on_conflict=p_key).execute()
        print(f"Successfully synced {len(data)} rows to {table}.")
    except Exception as e:
        print(f"Error syncing {table}: {e}")

def run_sync(mode="daily"):
    print(f"Starting COMPLETIST sync: {mode} mode")
    current_season = "20242025"
    
    # Always pull teams first to ensure foreign key integrity
    teams_df = scrapeTeams(source="records")
    upsert("teams", teams_df, "id")
    
    # Get active team abbreviations from the scraped data
    active_teams = teams_df[teams_df['lastseasonid'].isna()]['teamabbrev'].tolist() if 'teamabbrev' in teams_df.columns else []

    if mode == "catchup":
        for team in active_teams:
            print(f"Catchup for Team: {team}")
            
            # Sync Rosters
            upsert("rosters", scrapeRoster(team, current_season), "id,season")
            
            # Sync Advanced Stats (via legacy wrapper for xGF%, etc.)
            for is_goalie in [False, True]:
                stats_df = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                if not stats_df.empty:
                    stats_df['id'] = stats_df.apply(lambda r: f"{r['playerid']}_{current_season}_{is_goalie}", axis=1)
                    stats_df['is_goalie'] = is_goalie
                    stats_df['season'] = int(current_season)
                    stats_df['team_tri_code'] = team
                    upsert("player_stats", stats_df, "id")

    elif mode == "daily":
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        # Sync Standings
        upsert("standings", scrapeStandings(date=yesterday), "teamabbrev_default,date")
        print(f"Daily sync for {yesterday} complete.")

if __name__ == "__main__":
    sync_mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    run_sync(sync_mode)