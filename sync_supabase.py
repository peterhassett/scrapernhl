import os
import sys
import pandas as pd
from datetime import datetime
from supabase import create_client, Client

# MUST import the legacy wrapper to get calculated fields (xGF%, etc.)
from scrapernhl import scraper_legacy
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

def get_complete_data(team, season, is_goalie):
    """Fetches stats through the legacy layer to include advanced metrics."""
    # This triggers the XGBoost models to calculate xG, xGF%, etc.
    df = scraper_legacy.scrape_team_stats(team, season, goalies=is_goalie)
    if df.empty:
        return df
    
    # Cleaning: Replace characters that are invalid in SQL column names
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # Add Identity Fields
    df['id'] = df.apply(lambda r: f"{r['playerid']}_{season}_{is_goalie}", axis=1)
    df['is_goalie'] = is_goalie
    df['season'] = int(season)
    df['team_tri_code'] = team
    df['scraped_on'] = datetime.utcnow().isoformat()
    return df

def run_sync(mode="daily"):
    print(f"Starting COMPLETIST sync: {mode}")
    current_season = "20242025"
    
    # Fetch Teams to drive the loop
    teams_df = scrapeTeams(source="records")
    active_teams = teams_df[teams_df['lastSeasonId'].isna()]['teamAbbrev'].tolist()
    
    if mode == "catchup":
        for team in active_teams:
            print(f"Syncing all fields for {team}...")
            for is_goalie in [False, True]:
                df = get_complete_data(team, current_season, is_goalie)
                if not df.empty:
                    # The .upsert() call here includes EVERY column in the DataFrame
                    data = df.to_dict(orient="records")
                    supabase.table("player_stats").upsert(data, on_conflict="id").execute()
                    print(f"Synced {len(data)} rows (including advanced metrics) for {team}.")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    run_sync(mode)