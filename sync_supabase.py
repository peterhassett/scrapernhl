import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from supabase import create_client, Client

# MUST import the legacy wrapper to get calculated fields (xGF%, etc.)
from scrapernhl import scraper_legacy
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.standings import scrapeStandings

# Initialize Supabase Client
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def clean_df_for_supabase(df: pd.DataFrame) -> pd.DataFrame:
    """Standardizes names for SQL and ensures JSON compliance (NaN to None)."""
    if df.empty:
        return df
    
    # 1. Standardize column names (dots and % to underscores)
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. Critical Fix: Convert NaN/Inf to None (JSON 'null')
    # This prevents the 'ValueError: Out of range float values' crash
    return df.replace({np.nan: None, np.inf: None, -np.inf: None})

def get_id_col(df: pd.DataFrame) -> str:
    """Dynamically finds the correct player ID column after cleaning."""
    possible_ids = ['playerid', 'player_id', 'playerid_default', 'id']
    for col in possible_ids:
        if col in df.columns:
            return col
    raise KeyError(f"Could not find a player ID column in: {df.columns.tolist()}")

def upsert(table: str, df: pd.DataFrame, p_key: str):
    """General helper to clean and upsert dataframes to Supabase."""
    df = clean_df_for_supabase(df)
    if df.empty:
        print(f"Skipping {table}: No data.")
        return
    
    data = df.to_dict(orient="records")
    try:
        supabase.table(table).upsert(data, on_conflict=p_key).execute()
        print(f"Successfully synced {len(data)} rows to {table}.")
    except Exception as e:
        print(f"Error syncing {table}: {e}")

def run_sync(mode="daily"):
    print(f"Starting COMPLETIST sync: {mode} mode")
    current_season = "20242025"
    
    # 1. Sync Teams (Foundation for all data)
    teams_df = scrapeTeams(source="records")
    upsert("teams", teams_df, "id")
    
    # Extract abbreviations for iterative loops
    t_clean = clean_df_for_supabase(teams_df.copy())
    active_teams = t_clean[t_clean['lastseasonid'].isna()]['teamabbrev'].tolist() if 'teamabbrev' in t_clean.columns else []

    if mode == "catchup":
        for team in active_teams:
            print(f"Syncing Team: {team}")
            
            # 2. Master Players Identity Table (from Roster API)
            roster_df = scrapeRoster(team, current_season)
            upsert("players", roster_df, "id") # Bio metadata
            
            # 3. Advanced Player Stats (including xGF%, CF%, etc.)
            for is_goalie in [False, True]:
                try:
                    # Calls the correct function from your legacy module
                    stats_raw = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                    if not stats_raw.empty:
                        stats_df = clean_df_for_supabase(stats_raw)
                        p_id_col = get_id_col(stats_df)
                        
                        # Generate seasonal composite ID: playerid_season_isgoalie
                        stats_df['id'] = stats_df.apply(
                            lambda r: f"{r[p_id_col]}_{current_season}_{is_goalie}", axis=1
                        )
                        stats_df['is_goalie'] = is_goalie
                        stats_df['season'] = int(current_season)
                        stats_df['team_tri_code'] = team
                        upsert("player_stats", stats_df, "id")
                except Exception as e:
                    print(f"Stats failed for {team}: {e}")

    elif mode == "daily":
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        # 4. Standings Update
        upsert("standings", scrapeStandings(date=yesterday), "teamabbrev_default,date")

if __name__ == "__main__":
    sync_mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    run_sync(sync_mode)