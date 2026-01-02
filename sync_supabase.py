import os
import sys
import logging
import pandas as pd
import numpy as np
from supabase import create_client, Client

# Scraper Imports
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.stats import scrapeTeamStats
from scrapernhl.scrapers.draft import scrapeDraftRecords
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.games import scrapePlays
from scrapernhl import scraper_legacy

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
LOG = logging.getLogger(__name__)

# Initialize Supabase Client
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
if not url or not key:
    LOG.error("SUPABASE_URL or SUPABASE_KEY environment variables not set.")
    sys.exit(1)
supabase: Client = create_client(url, key)

# THE GROUND TRUTH WHITELIST - MATCHES SQL SCHEMA
WHITELISTS = {
    "teams": ["id", "fullname", "teamabbrev", "teamcommonname", "teamplacename", "firstseasonid", "lastseasonid", "mostrecentteamid", "active_status", "conference_name", "division_name", "franchiseid", "logos"],
    "players": ["id", "firstname_default", "lastname_default", "headshot", "positioncode", "shootscatches", "heightininches", "heightincentimeters", "weightinpounds", "weightinkilograms", "birthdate", "birthcountry", "birthcity_default", "birthstateprovince_default"],
    "rosters": ["id", "season", "teamabbrev", "sweaternumber", "positioncode"],
    "player_stats": ["id", "playerid", "season", "is_goalie", "team", "opp", "strength", "gamesplayed", "gamesstarted", "goals", "assists", "points", "plusminus", "penaltyminutes", "powerplaygoals", "shorthandedgoals", "gamewinninggoals", "overtimegoals", "shots", "shotsagainst", "saves", "goalsagainst", "shutouts", "shootingpctg", "savepercentage", "goalsagainstaverage", "cf", "ca", "cf_pct", "ff", "fa", "ff_pct", "sf", "sa", "sf_pct", "gf", "ga", "gf_pct", "xg", "xga", "xgf_pct", "pf", "pa", "give_for", "give_against", "take_for", "take_against", "seconds", "minutes", "avgtimeonicepergame", "avgshiftspergame", "faceoffwinpctg"],
    "schedule": ["id", "season", "gamedate", "gametype", "gamestate", "hometeam_id", "hometeam_abbrev", "hometeam_score", "hometeam_commonname_default", "hometeam_placename_default", "hometeam_logo", "awayteam_id", "awayteam_abbrev", "awayteam_score", "awayteam_commonname_default", "awayteam_placename_default", "awayteam_logo", "venue_default", "venue_location_default", "starttimeutc", "easternutcoffset", "venueutcoffset", "gamecenterlink"],
    "standings": ["date", "teamabbrev_default", "teamname_default", "teamcommonname_default", "conference_name", "division_name", "gamesplayed", "wins", "losses", "otlosses", "points", "pointpctg", "regulationwins", "row", "goalsfor", "goalsagainst", "goaldifferential", "streak_code", "streak_count"],
    "draft": ["year", "overall_pick", "round_number", "pick_in_round", "team_tricode", "player_id", "player_firstname", "player_lastname", "player_position", "player_birthcountry", "player_birthstateprovince", "player_years_pro", "amateurclubname", "amateurleague", "countrycode", "displayabbrev_default"],
    "plays": ["id", "game_id", "event_id", "period", "period_type", "time_in_period", "time_remaining", "situation_code", "home_team_defending_side", "event_type", "type_desc_key", "x_coord", "y_coord", "zone_code", "ppt_replay_url"]
}

def toi_to_decimal(toi_str):
    if pd.isna(toi_str) or not isinstance(toi_str, str) or ':' not in toi_str:
        return None
    try:
        m, s = map(int, toi_str.split(':'))
        return round(m + (s / 60.0), 2)
    except Exception:
        return None

def clean_and_validate(df: pd.DataFrame, table_name: str, p_keys: list) -> pd.DataFrame:
    if df.empty:
        return df
    
    # 1. Normalize Column Names to Lowercase immediately
    df.columns = [str(c).replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. Draft Key Recovery (Mapping variant names to the PK 'overall_pick')
    if table_name == "draft":
        variants = ["overallpick", "pickoverall", "draft_overall", "overall_pick_number", "pick_overall", "pickinround"]
        for v in variants:
            if v in df.columns and ("overall_pick" not in df.columns or df["overall_pick"].isnull().all()):
                df["overall_pick"] = df[v]

    # 3. Filter international language fields (as per instructions)
    intl_suffixes = ('_cs', '_fi', '_sk', '_sv', '_de', '_fr', '_es')
    df = df.drop(columns=[c for c in df.columns if c.endswith(intl_suffixes)], errors='ignore')

    # 4. Whitelist Filter
    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()

    # 5. Type Casting (Ensures no decimals like "2.0" in BIGINT columns)
    num_patterns = ['id', 'season', 'pick', 'goals', 'played', 'number', 'wins', 'points', 'shots', 'saves', 'started']
    for col in df.columns:
        if any(p in col for p in num_patterns):
            # Only cast columns that are NOT the custom string IDs for player_stats or plays
            if not (table_name in ["player_stats", "plays"] and col == "id"):
                df[col] = pd.to_numeric(pd.Series(df[col]), errors='coerce').fillna(0).round().astype(np.int64)
        
        if 'timeonice' in col:
            df[col] = df[col].apply(toi_to_decimal)

    # 6. Primary Key Safety (Fixes KeyError and Batch Integrity)
    existing_pks = [k for k in p_keys if k in df.columns]
    if existing_pks:
        df = df.dropna(subset=existing_pks)
        df = df.drop_duplicates(subset=existing_pks, keep='first')
    
    return df.replace({np.nan: None})

def sync_table(table_name: str, df: pd.DataFrame, p_key_str: str):
    ready = clean_and_validate(df, table_name, p_key_str.split(','))
    if ready.empty:
        LOG.warning(f"No valid data to sync for table: {table_name}")
        return
    try:
        data = ready.to_dict(orient="records")
        supabase.table(table_name).upsert(data, on_conflict=p_key_str).execute()
        LOG.info(f"Successfully synced {len(ready)} rows to {table_name}")
    except Exception as e:
        LOG.error(f"Sync failed for {table_name}: {e}")

def run_sync(mode="daily"):
    season_str = "20242025"
    season_int = 20242025
    
    # 1. Initial Discovery (Teams and Standings)
    teams_df = scrapeTeams(source="records")
    # Force lowercase for the active_teams filter logic
    teams_df.columns = teams_df.columns.str.lower()
    
    sync_table("teams", teams_df, "id")
    sync_table("standings", scrapeStandings(), "date,teamabbrev_default")
    
    # Safely identify active teams (where lastseasonid is NaN)
    if 'lastseasonid' in teams_df.columns and 'teamabbrev' in teams_df.columns:
        active_teams = teams_df[teams_df['lastseasonid'].isna()]['teamabbrev'].dropna().unique().tolist()
    else:
        active_teams = teams_df['teamabbrev'].dropna().unique().tolist() if 'teamabbrev' in teams_df.columns else []
    
    LOG.info(f"Discovered {len(active_teams)} active teams for syncing.")

    if mode == "catchup":
        # 2. Historical Draft Data
        for yr in range(2020, 2026):
            d_df = scrapeDraftRecords(str(yr))
            if not d_df.empty:
                d_df["year"] = yr
                sync_table("draft", d_df, "year,overall_pick")

    # 3. Main Sync Loop (Iterate through Franchises)
    for team in active_teams:
        LOG.info(f"--- Processing Franchise: {team} ---")
        
        # A. Players & Rosters
        ros = scrapeRoster(team, season_str)
        if not ros.empty:
            ros['season'] = season_int
            # Sync parent profile first, then roster
            sync_table("players", ros.copy(), "id")
            sync_table("rosters", ros, "id,season")

        # B. Analytical Player Stats (Legacy Scraper)
        for goalie_flag in [False, True]:
            st = scraper_legacy.scrapeTeamStats(team, season_str, goalies=goalie_flag)
            if not st.empty:
                st = st.rename(columns={'playerId': 'playerid'})
                st['season'] = season_int
                st['is_goalie'] = goalie_flag
                # Create granular ID: playerid_season_isgoalie_strength
                st['id'] = st.apply(lambda r: f"{r['playerid']}_{season_int}_{goalie_flag}_{r.get('strength','all')}", axis=1)
                
                # Safety Upsert for player profiles
                p_safety = st[['playerid']].rename(columns={'playerid': 'id'}).drop_duplicates()
                sync_table("players", p_safety, "id")
                sync_table("player_stats", st, "id")

        # C. Schedule & Plays (Schedule MUST sync before Plays)
        sc = scrapeSchedule(team, season_str)
        if not sc.empty:
            sync_table("schedule", sc, "id")
            
            # Use schedule IDs to pull play-by-play data
            # Normalizing schedule columns for local loop
            sc.columns = sc.columns.str.lower()
            if 'id' in sc.columns:
                for gid in sc['id'].dropna().unique().tolist():
                    pl = scrapePlays(gid)
                    if not pl.empty:
                        # PK: game_id + eventid + period
                        pl['id'] = pl.apply(lambda r: f"{gid}_{r.get('eventid', '0')}_{r.get('period', '1')}", axis=1)
                        pl['game_id'] = gid
                        sync_table("plays", pl, "id")

if __name__ == "__main__":
    sync_mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    LOG.info(f"Starting NHL Supabase Sync in '{sync_mode}' mode.")
    run_sync(sync_mode)