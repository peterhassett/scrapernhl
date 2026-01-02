import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from supabase import create_client, Client

# Importing all scrapers from your package
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
    """Standardizes names and ensures JSON compliance for every column."""
    if df.empty: return df
    # Map dots and % to underscores for Postgres compatibility
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    # Convert NaNs to None to avoid JSON compliance errors
    return df.replace({np.nan: None, np.inf: None, -np.inf: None})

def run_sync(mode="daily"):
    print(f"Starting COMPLETIST sync: {mode} mode")
    current_season = "20242025"
    
    # 1. Teams (The base for all joins)
    teams_raw = scrapeTeams(source="records")
    teams_df = clean_df_for_supabase(teams_raw)
    supabase.table("teams").upsert(teams_df.to_dict(orient="records")).execute()
    
    # Identify active teams for iterative loops
    active_teams = teams_df[teams_df['lastseasonid'].isna()]['teamabbrev'].tolist()

    if mode == "catchup":
        for team in active_teams:
            print(f"Deep Sync for Team: {team}")
            
            # 2. Master Players Identity
            roster_df = clean_df_for_supabase(scrapeRoster(team, current_season))
            if not roster_df.empty:
                supabase.table("players").upsert(roster_df.to_dict(orient="records")).execute()
            
            # 3. Advanced Player Stats (captures xGF%, xG, etc.)
            for is_goalie in [False, True]:
                try:
                    stats_raw = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                    if not stats_raw.empty:
                        stats_df = clean_df_for_supabase(stats_raw)
                        # Identify ID column (playerid or player_id)
                        p_id_col = 'playerid' if 'playerid' in stats_df.columns else 'player_id'
                        stats_df['id'] = stats_df.apply(lambda r: f"{r[p_id_col]}_{current_season}_{is_goalie}", axis=1)
                        stats_df['is_goalie'] = is_goalie
                        stats_df['team_tri_code'] = team
                        supabase.table("player_stats").upsert(stats_df.to_dict(orient="records")).execute()
                except Exception as e:
                    print(f"Stats failed for {team}: {e}")

    elif mode == "daily":
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        # 4. Standings
        standings_df = clean_df_for_supabase(scrapeStandings(date=yesterday))
        if not standings_df.empty:
            supabase.table("standings").upsert(standings_df.to_dict(orient="records")).execute()

if __name__ == "__main__":
    sync_mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    run_sync(sync_mode)