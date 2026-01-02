import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from supabase import create_client, Client
from scrapernhl import scraper_legacy

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

def get_valid_columns(table_name: str) -> list:
    """Fetches valid column names from the DB to prevent PGRST204."""
    try:
        res = supabase.table(table_name).select("*").limit(0).execute()
        return list(res.data[0].keys()) if res.data else []
    except:
        return []

def completist_prepare(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Flatten, type-fix, and filter columns to match SQL schema exactly."""
    if df.empty: return df
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # Kill 22P02: Force BIGINTs
    int_patterns = ['id', 'season', 'number', 'played', 'goals', 'assists', 'points', 'year', 'wins', 'losses']
    for col in df.columns:
        if any(pat in col for pat in int_patterns):
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    # Handle Broadcasts/Lists as strings
    for col in df.columns:
        if df[col].dtype == 'object' and not df[col].empty:
            if isinstance(df[col].iloc[0], (list, dict)):
                df[col] = df[col].astype(str)

    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
    valid_cols = get_valid_columns(table_name)
    if valid_cols:
        return df[[c for c in df.columns if c in valid_cols]]
    return df

def run_sync(mode="daily"):
    print(f"Starting COMPLETIST sync: {mode}")
    current_season = "20242025"
    
    # 1. TEAMS
    from scrapernhl.scrapers.teams import scrapeTeams
    teams_df = completist_prepare(scrapeTeams(source="records"), "teams")
    supabase.table("teams").upsert(teams_df.to_dict(orient="records")).execute()
    
    active_teams = ['MTL', 'VAN', 'CGY', 'NYI', 'NJD', 'WSH', 'EDM', 'CAR', 'COL', 'SJS', 'OTT', 'TBL']

    if mode == "catchup":
        # 2. DRAFT (Historical)
        for year in range(2020, 2026):
            draft_df = completist_prepare(scraper_legacy.scrapeDraftRecords(year), "draft")
            if not draft_df.empty:
                supabase.table("draft").upsert(draft_df.to_dict(orient="records")).execute()

        for team in active_teams:
            print(f"Deep Sync: {team}")
            from scrapernhl.scrapers.schedule import scrapeSchedule
            from scrapernhl.scrapers.roster import scrapeRoster
            
            # 3. SCHEDULE
            sched_df = completist_prepare(scrapeSchedule(team, current_season), "schedule")
            supabase.table("schedule").upsert(sched_df.to_dict(orient="records")).execute()

            # 4. PLAYERS & 5. ROSTERS
            roster_raw = scrapeRoster(team, current_season)
            players_df = completist_prepare(roster_raw.copy(), "players")
            supabase.table("players").upsert(players_df.to_dict(orient="records")).execute()
            
            rosters_df = completist_prepare(roster_raw, "rosters")
            supabase.table("rosters").upsert(rosters_df.to_dict(orient="records")).execute()

            # 6. PLAYER_STATS (Advanced Metrics)
            for is_goalie in [False, True]:
                stats_raw = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                if not stats_raw.empty:
                    p_id_col = 'playerid' if 'playerid' in stats_raw.columns else 'playerId'
                    stats_raw['id'] = stats_raw.apply(lambda r: f"{r[p_id_col]}_{current_season}_{is_goalie}", axis=1)
                    stats_raw['team_tri_code'] = team
                    stats_raw['is_goalie'] = is_goalie
                    
                    stats_final = completist_prepare(stats_raw, "player_stats")
                    supabase.table("player_stats").upsert(stats_final.to_dict(orient="records")).execute()

    elif mode == "daily":
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        # 7. STANDINGS
        from scrapernhl.scrapers.standings import scrapeStandings
        stand_df = completist_prepare(scrapeStandings(date=yesterday), "standings")
        supabase.table("standings").upsert(stand_df.to_dict(orient="records")).execute()

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")