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

def bulletproof_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Distinct columns, forced integer types, and JSON compliance."""
    if df.empty: return df
    
    # 1. Flatten dots (firstName.default -> firstname_default)
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. Fix 22P02: Force numeric columns to Integers, not floats (Int64 handles NaNs)
    # Identify any column that is meant to be an INT in the database
    int_patterns = ['id', 'season', 'number', 'played', 'goals', 'assists', 'points', 'year', 'wins', 'losses']
    for col in df.columns:
        if any(pat in col for pat in int_patterns):
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    # 3. JSON Compliance: Convert NaNs to None
    return df.replace({np.nan: None, np.inf: None, -np.inf: None})

def run_sync(mode="daily"):
    print(f"Starting COMPLETIST sync: {mode}")
    current_season = "20242025"
    
    # TEAMS
    teams_df = bulletproof_clean(scrapeTeams(source="records"))
    supabase.table("teams").upsert(teams_df.to_dict(orient="records")).execute()
    active_teams = teams_df[teams_df['lastseasonid'].isna()]['teamabbrev'].tolist()

    if mode == "catchup":
        # 1. DRAFT (Historical)
        for year in range(2020, 2026):
            draft_df = bulletproof_clean(scraper_legacy.scrapeDraftRecords(year))
            if not draft_df.empty:
                supabase.table("draft").upsert(draft_df.to_dict(orient="records")).execute()

        for team in active_teams:
            print(f"Processing {team}...")
            # 2. SCHEDULE
            sched_df = bulletproof_clean(scrapeSchedule(team, current_season))
            if not sched_df.empty:
                supabase.table("schedule").upsert(sched_df.to_dict(orient="records")).execute()

            # 3. PLAYERS
            roster_df = bulletproof_clean(scrapeRoster(team, current_season))
            if not roster_df.empty:
                supabase.table("players").upsert(roster_df.to_dict(orient="records")).execute()

            # 4. STATS (Advanced Metrics)
            for is_goalie in [False, True]:
                try:
                    stats_raw = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                    if not stats_raw.empty:
                        stats_df = bulletproof_clean(stats_raw)
                        p_id = 'playerid' if 'playerid' in stats_df.columns else 'player_id'
                        stats_df['id'] = stats_df.apply(lambda r: f"{r[p_id]}_{current_season}_{is_goalie}", axis=1)
                        stats_df['team_tri_code'] = team
                        stats_df['is_goalie'] = is_goalie
                        supabase.table("player_stats").upsert(stats_df.to_dict(orient="records")).execute()
                except Exception as e:
                    print(f"Stats failed for {team}: {e}")

    elif mode == "daily":
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        standings_df = bulletproof_clean(scrapeStandings(date=yesterday))
        if not standings_df.empty:
            supabase.table("standings").upsert(standings_df.to_dict(orient="records")).execute()

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")