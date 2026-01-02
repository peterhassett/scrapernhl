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
    except: return None

def clean_and_validate(df: pd.DataFrame, table_name: str, p_keys: list) -> pd.DataFrame:
    if df.empty: return df
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    if table_name == "draft":
        # Records API uses these variants; map them to our 'overall_pick' primary key
        for v in ["overallpick", "pickoverall", "draft_overall", "overall_pick_number", "pick_overall"]:
            if v in df.columns and "overall_pick" not in df.columns:
                df["overall_pick"] = df[v]

    intl = ('_cs', '_fi', '_sk', '_sv', '_de', '_fr', '_es')
    df = df.drop(columns=[c for c in df.columns if c.endswith(intl)], errors='ignore')

    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()

    # Aggressive casting to prevent 22P02 (bigint syntax error)
    num_pats = ['id', 'season', 'pick', 'goals', 'played', 'number', 'wins', 'points', 'shots', 'saves', 'started', 'type']
    for col in df.columns:
        if any(p in col for p in num_pats):
            if not (table_name in ["player_stats", "plays"] and col == "id"):
                # to_numeric then round avoids the "2.0" string issue
                df[col] = pd.to_numeric(pd.Series(df[col]), errors='coerce').fillna(0).round().astype(np.int64)
        if 'timeonice' in col:
            df[col] = df[col].apply(toi_to_decimal)

    existing_pks = [k for k in p_keys if k in df.columns]
    if existing_pks:
        df = df.dropna(subset=existing_pks)
        df = df.drop_duplicates(subset=existing_pks, keep='first')
    
    return df.replace({np.nan: None})

def sync_table(table_name: str, df: pd.DataFrame, p_key_str: str):
    ready = clean_and_validate(df, table_name, p_key_str.split(','))
    if ready.empty: return
    try:
        data = ready.to_dict(orient="records")
        supabase.table(table_name).upsert(data, on_conflict=p_key_str).execute()
        LOG.info(f"Synced {len(ready)} to {table_name}")
    except Exception as e:
        LOG.error(f"Sync failed for {table_name}: {e}")

def run_sync(mode="daily"):
    s_str, s_int = "20242025", 20242025
    sync_table("teams", scrapeTeams(source="records"), "id")
    sync_table("standings", scrapeStandings(), "date,teamabbrev_default")
    
    teams_clean = clean_and_validate(scrapeTeams(source="records"), "teams", ["id"])
    active_teams = teams_clean[teams_clean['lastseasonid'].isna()]['teamabbrev'].dropna().unique().tolist()

    if mode == "catchup":
        for yr in range(2020, 2026):
            d_df = scrapeDraftRecords(str(yr))
            if not d_df.empty:
                d_df["year"] = yr
                sync_table("draft", d_df, "year,overall_pick")

        for team in active_teams:
            LOG.info(f"Syncing {team}...")
            ros = scrapeRoster(team, s_str)
            if not ros.empty:
                ros['season'] = s_int
                sync_table("players", ros.copy(), "id")
                sync_table("rosters", ros, "id,season")

            for goalie in [False, True]:
                st = scraper_legacy.scrapeTeamStats(team, s_str, goalies=goalie)
                if not st.empty:
                    st = st.rename(columns={'playerId': 'playerid'})
                    st['season'] = s_int
                    st['id'] = st.apply(lambda r: f"{r['playerid']}_{s_int}_{goalie}_{r.get('strength','all')}", axis=1)
                    p_saf = st[['playerid']].rename(columns={'playerid': 'id'}).drop_duplicates()
                    sync_table("players", p_saf, "id")
                    sync_table("player_stats", st, "id")

            # SCHEDULE MUST BE SYNCED AND VALID BEFORE PLAYS LOOP
            sc = scrapeSchedule(team, s_str)
            sync_table("schedule", sc, "id")
            
            for gid in sc['id'].tolist():
                pl = scrapePlays(gid)
                if not pl.empty:
                    pl['id'] = pl.apply(lambda r: f"{gid}_{r.get('eventid', '0')}_{r.get('period', '1')}", axis=1)
                    pl['game_id'] = gid
                    sync_table("plays", pl, "id")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")