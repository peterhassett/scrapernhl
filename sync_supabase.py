import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from supabase import create_client, Client

# Importing necessary modules from your package
from scrapernhl import scraper_legacy
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.standings import scrapeStandings

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def clean_df_for_supabase(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes names and ensures JSON compliance (NaN to None)."""
    if df.empty: return df
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    return df.replace({np.nan: None, np.inf: None, -np.inf: None})

def run_sync(mode="daily"):
    print(f"Starting COMPLETIST sync: {mode}")
    current_season = "20242025"
    
    # 1. Teams foundation
    teams_df = clean_df_for_supabase(scrapeTeams(source="records"))
    supabase.table("teams").upsert(teams_df.to_dict(orient="records")).execute()
    active_teams = teams_df[teams_df['lastseasonid'].isna()]['teamabbrev'].tolist()

    if mode == "catchup":
        for team in active_teams:
            print(f"Deep Syncing {team}...")
            # 2. Sync Master Player metadata
            roster_df = clean_df_for_supabase(scrapeRoster(team, current_season))
            if not roster_df.empty:
                supabase.table("players").upsert(roster_df.to_dict(orient="records")).execute()
            
            # 3. Sync Player Stats (Legacy xG, xGF%, etc.)
            for is_goalie in [False, True]:
                try:
                    stats_raw = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                    if not stats_raw.empty:
                        stats_df = clean_df_for_supabase(stats_raw)
                        # Identify ID col dynamically
                        p_id = 'playerid' if 'playerid' in stats_df.columns else 'player_id'
                        stats_df['id'] = stats_df.apply(lambda r: f"{r[p_id]}_{current_season}_{is_goalie}", axis=1)
                        stats_df['is_goalie'] = is_goalie
                        supabase.table("player_stats").upsert(stats_df.to_dict(orient="records")).execute()
                except Exception as e:
                    print(f"Stats failed for {team}: {e}")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")