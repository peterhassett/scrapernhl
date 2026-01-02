import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client, Client

# MUST import the legacy wrapper to get calculated fields (xGF%, etc.)
from scrapernhl import scraper_legacy
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.standings import scrapeStandings

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes names for SQL: dots/percent signs to underscores."""
    if df.empty:
        return df
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    return df

def run_sync(mode="daily"):
    print(f"Starting COMPLETIST sync: {mode}")
    current_season = "20242025"
    
    # 1. Fetch Teams to drive the loop
    teams_df = scrapeTeams(source="records")
    active_teams = teams_df[teams_df['lastSeasonId'].isna()]['teamAbbrev'].tolist()
    
    # Ensure Teams table is populated first
    teams_clean = clean_column_names(teams_df.copy())
    supabase.table("teams").upsert(teams_clean.to_dict(orient="records")).execute()

    if mode == "catchup":
        for team in active_teams:
            print(f"Processing Team: {team}")
            
            # 2. Advanced Player Stats (captures xGF%, xG, etc.)
            for is_goalie in [False, True]:
                try:
                    # UPDATED: Using correct function name from scraper_legacy
                    stats_df = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                    if not stats_df.empty:
                        stats_df = clean_column_names(stats_df)
                        # Create unique ID
                        stats_df['id'] = stats_df.apply(lambda r: f"{r['playerid']}_{current_season}_{is_goalie}", axis=1)
                        stats_df['is_goalie'] = is_goalie
                        stats_df['season'] = int(current_season)
                        stats_df['team_tri_code'] = team
                        
                        data = stats_df.to_dict(orient="records")
                        supabase.table("player_stats").upsert(data, on_conflict="id").execute()
                except Exception as e:
                    print(f"Stats failed for {team}: {e}")

            # 3. Rosters
            roster_df = clean_column_names(scrapeRoster(team, current_season))
            if not roster_df.empty:
                supabase.table("rosters").upsert(roster_df.to_dict(orient="records")).execute()

    elif mode == "daily":
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        # 4. Standings
        standings_df = clean_column_names(scrapeStandings(date=yesterday))
        if not standings_df.empty:
            data = standings_df.to_dict(orient="records")
            supabase.table("standings").upsert(data).execute()
            print(f"Synced standings for {yesterday}")

if __name__ == "__main__":
    sync_mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    run_sync(sync_mode)