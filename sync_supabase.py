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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# THE GROUND TRUTH WHITELIST
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

def clean_and_validate(df: pd.DataFrame, table_name: str, p_keys: list) -> pd.DataFrame:
    if df.empty: return df
    df.columns = [str(c).replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # MAPPING SHIMS: Fixes the NULL columns
    if table_name == "player_stats":
        if 'situation' in df.columns: df['strength'] = df['situation']
        if 'teamabbrev' in df.columns and 'team' not in df.columns: df['team'] = df['teamabbrev']
        if 'opponentabbrev' in df.columns and 'opp' not in df.columns: df['opp'] = df['opponentabbrev']
        # Convert MM:SS to seconds if needed
        if 'timeonice' in df.columns and ('seconds' not in df.columns or df['seconds'].isnull().all()):
            df['seconds'] = df['timeonice'].apply(lambda x: int(x.split(':')[0])*60 + int(x.split(':')[1]) if isinstance(x,str) and ':' in x else None)
        # Advanced Stats
        for m in ['cf', 'ca', 'ff', 'fa', 'xg', 'xga']:
            if f'raw_{m}' in df.columns: df[m] = df[f'raw_{m}']

    elif table_name == "draft":
        if 'roundnumber' in df.columns: df['round_number'] = df['roundnumber']
        if 'pickinround' in df.columns: df['pick_in_round'] = df['pickinround']
        for v in ["overallpick", "pickoverall", "draft_overall"]:
            if v in df.columns and ("overall_pick" not in df.columns or df["overall_pick"].isnull().all()):
                df["overall_pick"] = df[v]

    elif table_name == "teams":
        if 'conferencename' in df.columns: df['conference_name'] = df['conferencename']
        if 'divisionname' in df.columns: df['division_name'] = df['divisionname']
        if 'isactive' in df.columns: df['active_status'] = df['isactive']

    elif table_name == "schedule":
        if 'venuelocation' in df.columns: df['venue_location_default'] = df['venuelocation']

    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()

    # Cast integers to stop "2.0" bigint errors
    num_pats = ['id', 'season', 'pick', 'goals', 'played', 'number', 'wins', 'points', 'shots', 'saves', 'started']
    for col in df.columns:
        if any(p in col for p in num_pats):
            if not (table_name in ["player_stats", "plays"] and col == "id"):
                df[col] = pd.to_numeric(pd.Series(df[col]), errors='coerce').fillna(0).round().astype(np.int64)

    existing_pks = [k for k in p_keys if k in df.columns]
    if existing_pks:
        df = df.dropna(subset=existing_pks).drop_duplicates(subset=existing_pks, keep='first')
    
    return df.replace({np.nan: None})

def sync_table(table_name: str, df: pd.DataFrame, p_key_str: str):
    ready = clean_and_validate(df, table_name, p_key_str.split(','))
    if ready.empty: return
    try:
        supabase.table(table_name).upsert(ready.to_dict(orient="records"), on_conflict=p_key_str).execute()
        LOG.info(f"Synced {len(ready)} to {table_name}")
    except Exception as e:
        LOG.error(f"Sync failed for {table_name}: {e}")

def run_sync(mode="daily"):
    s_str, s_int = "20242025", 20242025
    teams_df = scrapeTeams(source="records")
    sync_table("teams", teams_df, "id")
    
    teams_df.columns = [c.lower() for c in teams_df.columns]
    active_teams = teams_df[teams_df['lastseasonid'].isna()]['teamabbrev'].dropna().unique().tolist()

    if mode == "debug":
        active_teams = active_teams[:2]
        LOG.info(f"DEBUG MODE: Syncing only {active_teams}")

    if mode == "catchup":
        for yr in range(2020, 2026):
            d_df = scrapeDraftRecords(str(yr))
            if not d_df.empty:
                d_df["year"] = yr
                sync_table("draft", d_df, "year,overall_pick")

    for team in active_teams:
        LOG.info(f"Processing Franchise: {team}")
        ros = scrapeRoster(team, s_str)
        if not ros.empty:
            ros['season'] = s_int
            ros['teamabbrev'] = team # Inject team name
            sync_table("players", ros.copy(), "id")
            sync_table("rosters", ros, "id,season")

        for goalie in [False, True]:
            st = scraper_legacy.scrapeTeamStats(team, s_str, goalies=goalie)
            if not st.empty:
                st = st.rename(columns={'playerId': 'playerid'})
                st['season'] = s_int
                st['team'] = team # Inject team name
                st['id'] = st.apply(lambda r: f"{r['playerid']}_{s_int}_{goalie}_{r.get('strength','all')}", axis=1)
                sync_table("player_stats", st, "id")

        sc = scrapeSchedule(team, s_str)
        if mode == "debug": sc = sc.head(3)
        sync_table("schedule", sc, "id")

        if mode != "debug":
            for gid in sc['id'].tolist():
                pl = scrapePlays(gid)
                if not pl.empty:
                    pl['id'] = pl.apply(lambda r: f"{gid}_{r.get('eventid', '0')}_{r.get('period', '1')}", axis=1)
                    pl['game_id'] = gid
                    sync_table("plays", pl, "id")

if __name__ == "__main__":
    sync_mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    run_sync(sync_mode)