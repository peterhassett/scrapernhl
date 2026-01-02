import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from supabase import create_client, Client

# Modular Imports from the repository
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.stats import scrapeTeamStats
from scrapernhl.scrapers.draft import scrapeDraftRecords
from scrapernhl.scrapers.standings import scrapeStandings

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# RIGOROUS WHITELISTS (Matches the SQL schema exactly)
WHITELISTS = {
    "teams": ["id", "fullname", "teamabbrev", "teamcommonname", "teamplacename", "firstseasonid", "lastseasonid", "mostrecentteamid"],
    "players": ["id", "firstname_default", "lastname_default", "headshot", "positioncode", "shootscatches", "heightininches", "weightinpounds", "birthdate", "birthcountry", "birthcity_default", "birthstateprovince_default"],
    "rosters": ["id", "season", "teamabbrev", "sweaternumber", "positioncode"],
    "player_stats": ["id", "playerid", "season", "is_goalie", "gamesplayed", "goals", "assists", "points", "plusminus", "penaltyminutes", "powerplaygoals", "shorthandedgoals", "gamewinninggoals", "overtimegoals", "shots", "shootingpctg", "avgtimeonicepergame", "avgshiftspergame", "faceoffwinpctg", "saves", "shutouts", "savepercentage", "goalsagainstaverage"],
    "schedule": ["id", "season", "gamedate", "gametype", "gamestate", "hometeam_abbrev", "awayteam_abbrev", "hometeam_score", "awayteam_score", "venue_default", "starttimeutc"],
    "draft": ["year", "overall_pick", "round_number", "pick_in_round", "team_tricode", "player_id", "player_firstname", "player_lastname", "amateurclubname", "amateurleague", "player_birthcountry"],
    "standings": ["date", "teamabbrev_default", "teamname_default", "gamesplayed", "wins", "losses", "otlosses", "points", "pointpctg"]
}

def toi_to_decimal(toi_str):
    if pd.isna(toi_str) or not isinstance(toi_str, str) or ':' not in toi_str:
        return None
    try:
        m, s = map(int, toi_str.split(':'))
        return round(m + (s / 60.0), 2)
    except:
        return None

def rigorous_prepare(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    if df.empty: return df
    
    # 1. Repo Normalization (Dots to Underscores)
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 2. TOI Conversion
    if 'avgtimeonicepergame' in df.columns:
        df['avgtimeonicepergame'] = df['avgtimeonicepergame'].apply(toi_to_decimal)

    # 3. STRICT WHITELIST (Skips junk like 'awayteam_airlinedesc')
    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()

    # 4. FIX BIGINT ERROR: Cast to Int64 (removes the ".0" decimals)
    num_cols = ['id', 'season', 'played', 'goals', 'points', 'pick', 'wins', 'number']
    for col in df.columns:
        if any(pat in col for pat in num_cols):
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    return df.replace({np.nan: None})

def sync_table(table_name: str, df: pd.DataFrame, p_key: str):
    ready_df = rigorous_prepare(df, table_name)
    if ready_df.empty: return
    try:
        data = ready_df.to_dict(orient="records")
        supabase.table(table_name).upsert(data, on_conflict=p_key).execute()
        print(f"Synced {len(ready_df)} rows to {table_name}")
    except Exception as e:
        print(f"Sync failed for {table_name}: {e}")

def run_sync(mode="daily"):
    print(f"Starting SYNC: {mode}")
    current_season = "20242025"
    
    # 1. Teams Sync
    teams_raw = scrapeTeams(source="records")
    sync_table("teams", teams_raw, "id")
    
    # Identify Active Teams (No lastSeasonId)
    teams_clean = rigorous_prepare(teams_raw.copy(), "teams")
    active_teams = teams_clean[teams_clean['lastseasonid'].isna()]['teamabbrev'].dropna().unique().tolist()
    
    # 2. Standings Sync
    sync_table("standings", scrapeStandings(), "date,teamabbrev_default")

    if mode == "catchup":
        # 3. Draft Sync (2020-2025)
        for year in range(2020, 2026):
            sync_table("draft", scrapeDraftRecords(str(year)), "year,overall_pick")

        # 4. Iterative Sync for Active Teams
        for team in active_teams:
            print(f"Processing {team}...")
            sync_table("schedule", scrapeSchedule(team, current_season), "id")
            
            roster = scrapeRoster(team, current_season)
            sync_table("players", roster.copy(), "id")
            sync_table("rosters", roster, "id,season")

            for is_goalie in [False, True]:
                stats = scrapeTeamStats(team, current_season, goalies=is_goalie)
                if not stats.empty:
                    # Specific primary key for stats (player_season_type)
                    stats['id'] = stats['playerId'].astype(str) + "_" + current_season + "_" + str(is_goalie)
                    stats['playerid'] = stats['playerId']
                    stats['season'] = int(current_season)
                    stats['is_goalie'] = is_goalie
                    sync_table("player_stats", stats, "id")

if __name__ == "__main__":
    sync_mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    run_sync(sync_mode)