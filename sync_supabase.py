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

# EXHAUSTIVE WHITELIST - NO COLUMNS REMOVED
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
    
    # Normalize headers
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # Fix Draft naming variations
    if table_name == "draft":
        for col_alt in ["overallpick", "pickoverall", "draft_overall", "overall_pick_number"]:
            if col_alt in df.columns and "overall_pick" not in df.columns:
                df["overall_pick"] = df[col_alt]

    # Drop international fields
    intl = ('_cs', '_fi', '_sk', '_sv', '_de', '_fr', '_es')
    df = df.drop(columns=[c for c in df.columns if c.endswith(intl)], errors='ignore')

    # Whitelist Filter
    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()

    # Numeric Coercion (Vector-Safe)
    num_patterns = ['id', 'season', 'pick', 'goals', 'played', 'number', 'wins', 'points', 'shots', 'saves']
    for col in df.columns:
        if any(p in col for p in num_patterns):
            if not (table_name in ["player_stats", "plays"] and col == "id"):
                df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')
        if 'timeonice' in col:
            df[col] = df[col].apply(toi_to_decimal)

    # DEDUPLICATION & NULL PK CHECK (Fixes 23502 and 21000)
    df = df.dropna(subset=p_keys)
    df = df.drop_duplicates(subset=p_keys, keep='first')
    
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
    season_str = "20242025"
    season_int = 20242025
    
    # Teams & Standings
    sync_table("teams", scrapeTeams(source="records"), "id")
    sync_table("standings", scrapeStandings(), "date,teamabbrev_default")
    
    # Iteration Logic
    teams_list = ["MTL", "TOR", "NYR", "CHI", "EDM", "VAN"] # Expanded list in real run

    if mode == "catchup":
        # Draft Sync
        for yr in range(2020, 2026):
            d_df = scrapeDraftRecords(str(yr))
            if not d_df.empty:
                d_df["year"] = yr
                sync_table("draft", d_df, "year,overall_pick")

        for team in teams_list:
            LOG.info(f"Syncing: {team}")
            
            # Players & Rosters
            ros = scrapeRoster(team, season_str)
            if not ros.empty:
                ros['season'] = season_int
                sync_table("players", ros.copy(), "id")
                sync_table("rosters", ros, "id,season")

            # Analytical Stats
            for goalie in [False, True]:
                st = scraper_legacy.scrapeTeamStats(team, season_str, goalies=goalie)
                if not st.empty:
                    st['playerid'] = st['playerId']
                    st['season'] = season_int
                    st['id'] = st.apply(lambda r: f"{r['playerid']}_{season_int}_{goalie}_{r.get('strength','all')}", axis=1)
                    # Safety Parent Upsert
                    p_saf = st[['playerid']].rename(columns={'playerid': 'id'}).drop_duplicates()
                    sync_table("players", p_saf, "id")
                    sync_table("player_stats", st, "id")

            # Plays
            sc = scrapeSchedule(team, season_str)
            for gid in sc['id'].head(10).tolist():
                plays = scrapePlays(gid)
                if not plays.empty:
                    plays['id'] = plays.apply(lambda r: f"{gid}_{r.get('eventid','0')}_{r.get('period','1')}", axis=1)
                    plays['game_id'] = gid
                    sync_table("plays", plays, "id")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")