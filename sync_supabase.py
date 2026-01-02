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
    
    # 1. Standardize column names
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. DRAFT KEY RECOVERY (Search for any pick identifier)
    if table_name == "draft":
        pick_vars = ["pickoverall", "overallpick", "draft_overall", "overall_pick_number", "pick_overall", "pickinround"]
        for v in pick_vars:
            if v in df.columns and ("overall_pick" not in df.columns or df["overall_pick"].isnull().all()):
                df["overall_pick"] = df[v]

    # 3. Filter international junk
    df = df.drop(columns=[c for c in df.columns if c.endswith(('_cs', '_fi', '_sk', '_sv', '_de', '_fr', '_es'))], errors='ignore')

    # 4. Whitelist Filter
    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()

    # 5. Robust Type Casting
    num_pats = ['id', 'season', 'pick', 'goals', 'played', 'number', 'wins', 'points', 'shots', 'saves', 'started']
    for col in df.columns:
        if any(p in col for p in num_pats):
            if not (table_name in ["player_stats", "plays"] and col == "id"):
                # Fill missing with 0 for PKs, or keep as Null for metrics
                df[col] = pd.to_numeric(pd.Series(df[col]), errors='coerce').fillna(0).round().astype(np.int64)
        if 'timeonice' in col:
            df[col] = df[col].apply(toi_to_decimal)

    # 6. PK SAFETY (Crucial for 23502 and 21000)
    existing_pks = [k for k in p_keys if k in df.columns]
    if existing_pks:
        # We MUST have valid PKs to perform an upsert
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
    
    # Discovery
    teams_df = scrapeTeams(source="records")
    sync_table("teams", teams_df, "id")
    sync_table("standings", scrapeStandings(), "date,teamabbrev_default")
    
    teams_clean = clean_and_validate(teams_df.copy(), "teams", ["id"])
    active_teams = teams_clean[teams_clean['lastseasonid'].isna()]['teamabbrev'].dropna().unique().tolist()

    if mode == "catchup":
        # 1. Draft
        for yr in range(2020, 2026):
            d_df = scrapeDraftRecords(str(yr))
            if not d_df.empty:
                d_df["year"] = yr
                # Alexis Lafrenière and other top picks are often in different JSON nodes
                # sync_table will now map and drop nulls for safety
                sync_table("draft", d_df, "year,overall_pick")

        # 2. Iterate Franchises
        for team in active_teams:
            LOG.info(f"Syncing: {team}")
            
            # Players Profile
            ros = scrapeRoster(team, s_str)
            if not ros.empty:
                ros['season'] = s_int
                sync_table("players", ros.copy(), "id")
                sync_table("rosters", ros, "id,season")

            # Stats
            for goalie in [False, True]:
                st = scraper_legacy.scrapeTeamStats(team, s_str, goalies=goalie)
                if not st.empty:
                    st = st.rename(columns={'playerId': 'playerid'})
                    st['season'] = s_int
                    st['id'] = st.apply(lambda r: f"{r['playerid']}_{s_int}_{goalie}_{r.get('strength','all')}", axis=1)
                    
                    p_saf = st[['playerid']].rename(columns={'playerid': 'id'}).drop_duplicates()
                    sync_table("players", p_saf, "id")
                    sync_table("player_stats", st, "id")

            # Schedule & Plays
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