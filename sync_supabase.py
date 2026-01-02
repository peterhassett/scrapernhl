import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from supabase import create_client, Client

from scrapernhl import scraper_legacy
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.schedule import scrapeSchedule

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def completist_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes names, fixes float-to-int JSON issues, and ensures null compliance."""
    if df.empty: return df
    
    # 1. Flatten dots (firstName.default -> firstname_default)
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. Fix 22P02 Error: Ensure IDs are proper integers or None, not floats (1917.0)
    # We identify columns that should be INTs and force them to nullable Int64
    int_cols = ['id', 'firstseasonid', 'lastseasonid', 'mostrecentteamid', 'playerid', 'season', 'sweaternumber']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    # 3. JSON Compliance: Replace all NaNs with Python None (null in JSON)
    return df.replace({np.nan: None, np.inf: None, -np.inf: None})

def run_sync(mode="daily"):
    print(f"Starting COMPLETIST sync: {mode}")
    current_season = "20242025"
    
    # FOUNDATIONAL: Sync Teams
    teams_df = completist_clean(scrapeTeams(source="records"))
    supabase.table("teams").upsert(teams_df.to_dict(orient="records")).execute()
    
    # Get active teams using the cleaned 'lastseasonid'
    active_teams = teams_df[teams_df['lastseasonid'].isna()]['teamabbrev'].tolist()

    if mode == "catchup":
        # 1. Sync Draft Data (Last 5 Years)
        for year in range(2020, 2026):
            draft_df = completist_clean(scraper_legacy.scrapeDraftRecords(year))
            if not draft_df.empty:
                supabase.table("draft").upsert(draft_df.to_dict(orient="records")).execute()

        for team in active_teams:
            print(f"Deep Sync: {team}")
            
            # 2. Sync Schedule
            sched_df = completist_clean(scrapeSchedule(team, current_season))
            if not sched_df.empty:
                # Store full row in raw_data JSONB to ensure no data loss
                sched_df['raw_data'] = sched_df.apply(lambda r: r.to_dict(), axis=1)
                supabase.table("schedule").upsert(sched_df.to_dict(orient="records")).execute()

            # 3. Sync Players
            roster_df = completist_clean(scrapeRoster(team, current_season))
            if not roster_df.empty:
                supabase.table("players").upsert(roster_df.to_dict(orient="records")).execute()

            # 4. Sync Advanced Stats (via Legacy XGBoost)
            for is_goalie in [False, True]:
                try:
                    stats_raw = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                    if not stats_raw.empty:
                        stats_df = completist_clean(stats_raw)
                        p_id_col = 'playerid' if 'playerid' in stats_df.columns else 'player_id'
                        stats_df['id'] = stats_df.apply(lambda r: f"{r[p_id_col]}_{current_season}_{is_goalie}", axis=1)
                        stats_df['team_tri_code'] = team
                        stats_df['is_goalie'] = is_goalie
                        supabase.table("player_stats").upsert(stats_df.to_dict(orient="records")).execute()
                except Exception as e:
                    print(f"Stats failed for {team}: {e}")

    elif mode == "daily":
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        # 5. Standings
        standings_df = completist_clean(scrapeStandings(date=yesterday))
        if not standings_df.empty:
            standings_df['raw_data'] = standings_df.apply(lambda r: r.to_dict(), axis=1)
            supabase.table("standings").upsert(standings_df.to_dict(orient="records")).execute()

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")