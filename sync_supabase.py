import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime
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

# MANUAL WHITELIST FOR TOTAL RIGOR (Pure English)
WHITELISTS = {
    "teams": ["id", "fullname", "teamabbrev", "teamcommonname", "teamplacename", "firstseasonid", "lastseasonid", "mostrecentteamid", "active_status", "conference_name", "division_name", "source"],
    "players": ["id", "firstname_default", "lastname_default", "headshot", "positioncode", "shootscatches", "heightininches", "heightincentimeters", "weightinpounds", "weightinkilograms", "birthdate", "birthcountry", "birthcity_default", "birthstateprovince_default", "source"],
    "rosters": ["id", "season", "teamabbrev", "sweaternumber", "positioncode"],
    "player_stats": ["id", "playerid", "season", "is_goalie", "team", "opp", "strength", "gamesplayed", "gamesstarted", "goals", "assists", "points", "plusminus", "penaltyminutes", "powerplaygoals", "shorthandedgoals", "gamewinninggoals", "overtimegoals", "shots", "shotsagainst", "saves", "goalsagainst", "shutouts", "shootingpctg", "savepercentage", "goalsagainstaverage", "cf", "ca", "cf_pct", "ff", "fa", "ff_pct", "sf", "sa", "sf_pct", "gf", "ga", "gf_pct", "xg", "xga", "xgf_pct", "pf", "pa", "give_for", "give_against", "take_for", "take_against", "seconds", "minutes", "avgtimeonicepergame", "avgshiftspergame", "faceoffwinpctg"],
    "schedule": ["id", "season", "gamedate", "gametype", "gamestate", "hometeam_id", "hometeam_abbrev", "hometeam_score", "hometeam_commonname_default", "hometeam_placename_default", "hometeam_logo", "awayteam_id", "awayteam_abbrev", "awayteam_score", "awayteam_commonname_default", "awayteam_placename_default", "awayteam_logo", "venue_default", "venue_location_default", "starttimeutc", "easternutcoffset", "venueutcoffset", "gamecenterlink"],
    "standings": ["date", "teamabbrev_default", "teamname_default", "teamcommonname_default", "conference_name", "division_name", "gamesplayed", "wins", "losses", "otlosses", "points", "pointpctg", "regulationwins", "row", "goalsfor", "goalsagainst", "goaldifferential", "streak_code", "streak_count"],
    "draft": ["year", "overall_pick", "round_number", "pick_in_round", "team_tricode", "player_id", "player_firstname", "player_lastname", "player_position", "player_birthcountry", "player_birthstateprovince", "player_years_pro", "amateurclubname", "amateurleague", "countrycode", "displayabbrev_default"],
    "plays": ["id", "game_id", "event_id", "period", "period_type", "time_in_period", "time_remaining", "situation_code", "home_team_defending_side", "event_type", "type_desc_key", "x_coord", "y_coord", "x_normalized", "y_normalized", "distance_from_goal", "zone_code", "ppt_replay_url"]
}

def toi_to_decimal(toi_str):
    if pd.isna(toi_str) or not isinstance(toi_str, str) or ':' not in toi_str:
        return None
    try:
        m, s = map(int, toi_str.split(':'))
        return round(m + (s / 60.0), 2)
    except: return None

def rigorous_clean(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    if df.empty: return df
    
    # 1. Flatten and Normalize
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 2. STRICT NO-INTL FILTER: DROP CS, FI, SK, SV, etc.
    intl_suffixes = ('_cs', '_fi', '_sk', '_sv', '_de', '_fr', '_es')
    df = df.drop(columns=[c for c in df.columns if c.endswith(intl_suffixes)], errors='ignore')

    # 3. Whitelist Intersection
    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()

    # 4. BigInt and Type Safety
    num_patterns = ['id', 'season', 'pick', 'goals', 'played', 'number', 'wins', 'points', 'shots', 'saves']
    for col in df.columns:
        if any(pat in col for pat in num_patterns):
            if not (table_name in ["player_stats", "plays"] and col == "id"):
                df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')
        if 'timeonice' in col:
            df[col] = df[col].apply(toi_to_decimal)

    return df.replace({np.nan: None, np.inf: None, -np.inf: None})

def sync_table(table_name: str, df: pd.DataFrame, p_key: str):
    ready = rigorous_clean(df, table_name)
    if ready.empty: return
    try:
        data = ready.to_dict(orient="records")
        supabase.table(table_name).upsert(data, on_conflict=p_key).execute()
        LOG.info(f"Synced {len(ready)} rows to {table_name}")
    except Exception as e:
        LOG.error(f"Sync failed for {table_name}: {e}")

def run_sync(mode="daily"):
    season_int = 20242025
    season_str = "20242025"
    
    # 1. Standings & Teams (Base discovery)
    sync_table("teams", scrapeTeams(source="records"), "id")
    sync_table("standings", scrapeStandings(), "date,teamabbrev_default")
    
    teams_clean = rigorous_clean(scrapeTeams(source="records"), "teams")
    active_teams = teams_clean[teams_clean['lastseasonid'].isna()]['teamabbrev'].dropna().unique().tolist()

    if mode == "catchup":
        # 2. Historical Draft
        for year in range(2020, 2026):
            sync_table("draft", scrapeDraftRecords(str(year)), "year,overall_pick")

        # 3. Loop through active NHL franchises
        for team in active_teams:
            LOG.info(f"Processing Franchise: {team}")
            
            # Players Profile Parent First
            roster = scrapeRoster(team, season_str)
            if not roster.empty:
                roster['season'] = season_int
                sync_table("players", roster.copy(), "id")
                sync_table("rosters", roster, "id,season")

            # Stats (Analytical Core)
            for is_goalie in [False, True]:
                stats = scraper_legacy.scrapeTeamStats(team, season_str, goalies=is_goalie)
                if not stats.empty:
                    stats['playerid'] = stats['playerId']
                    stats['season'] = season_int
                    stats['id'] = stats.apply(lambda r: f"{r['playerid']}_{season_int}_{is_goalie}_{r.get('strength','all')}", axis=1)
                    
                    # SAFETY PARENT UPSERT
                    p_saf = stats[['playerid']].rename(columns={'playerid': 'id'}).drop_duplicates()
                    sync_table("players", p_saf, "id")
                    
                    sync_table("player_stats", stats, "id")

            # Schedule & Plays
            sched = scrapeSchedule(team, season_str)
            sync_table("schedule", sched, "id")
            for gid in sched['id'].head(10).tolist(): # Limits for processing speed
                plays = scrapePlays(gid)
                if not plays.empty:
                    plays['id'] = plays.apply(lambda r: f"{gid}_{r.get('eventid','0')}_{r.get('period','1')}_{r.get('event_type','0')}", axis=1)
                    plays['game_id'] = gid
                    sync_table("plays", plays, "id")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")