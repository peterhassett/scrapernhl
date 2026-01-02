import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from supabase import create_client, Client

# Modular Imports from the Repository
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.stats import scrapeTeamStats
from scrapernhl.scrapers.draft import scrapeDraftRecords
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.games import scrapePlays
from scrapernhl import scraper_legacy

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# RIGOROUS WHITELISTS (Matches the SQL schema columns exactly)
WHITELISTS = {
    "teams": ["id", "fullname", "teamabbrev", "teamcommonname", "teamplacename", "firstseasonid", "lastseasonid", "mostrecentteamid", "active_status", "conference_name", "division_name"],
    "players": ["id", "firstname_default", "lastname_default", "headshot", "positioncode", "shootscatches", "heightininches", "heightincentimeters", "weightinpounds", "weightinkilograms", "birthdate", "birthcountry", "birthcity_default", "birthstateprovince_default"],
    "rosters": ["id", "season", "teamabbrev", "sweaternumber", "positioncode"],
    "player_stats": ["id", "playerid", "season", "is_goalie", "team", "opp", "strength", "gamesplayed", "gamesstarted", "goals", "assists", "points", "plusminus", "penaltyminutes", "powerplaygoals", "shorthandedgoals", "gamewinninggoals", "overtimegoals", "shots", "shootingpctg", "cf", "ca", "cf_pct", "ff", "fa", "ff_pct", "sf", "sa", "sf_pct", "gf", "ga", "gf_pct", "xg", "xga", "xgf_pct", "seconds", "minutes", "avgtimeonicepergame"],
    "schedule": ["id", "season", "gamedate", "gametype", "gamestate", "hometeam_id", "hometeam_abbrev", "hometeam_score", "hometeam_commonname_default", "awayteam_id", "awayteam_abbrev", "awayteam_score", "awayteam_commonname_default", "venue_default", "venue_location_default", "starttimeutc", "gamecenterlink"],
    "standings": ["date", "teamabbrev_default", "teamname_default", "conference_name", "division_name", "gamesplayed", "wins", "losses", "otlosses", "points", "pointpctg", "regulationwins", "row"],
    "draft": ["year", "overall_pick", "round_number", "pick_in_round", "team_tricode", "player_id", "player_firstname", "player_lastname", "amateurclubname", "amateurleague", "countrycode", "displayabbrev_default"],
    "plays": ["id", "game_id", "event_id", "period", "period_type", "time_in_period", "time_remaining", "situation_code", "home_team_defending_side", "event_type", "type_desc_key", "x_coord", "y_coord", "zone_code", "ppt_replay_url"]
}

def toi_to_decimal(toi_str):
    """Converts 'MM:SS' strings to decimal numeric values."""
    if pd.isna(toi_str) or not isinstance(toi_str, str) or ':' not in toi_str:
        return None
    try:
        m, s = map(int, toi_str.split(':'))
        return round(m + (s / 60.0), 2)
    except: return None

def clean_and_coerce(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """The rigorous data preparation engine."""
    if df.empty: return df
    
    # 1. Normalize Column Names (Matches json_normalize output)
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 2. Hard-Filter Internationalization noise
    intl = ('_cs', '_fr', '_fi', '_sk', '_de', '_sv', '_es')
    df = df.drop(columns=[c for c in df.columns if c.endswith(intl)], errors='ignore')

    # 3. Special Draft Mapping: overallpick -> overall_pick
    if table_name == "draft" and "overallpick" in df.columns:
        df["overall_pick"] = df["overallpick"]

    # 4. Whitelist Intersection (Prevents 400 Bad Requests)
    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()

    # 5. Type Conversions
    for col in df.columns:
        # Time string conversion
        if 'timeonice' in col:
            df[col] = df[col].apply(toi_to_decimal)
        # BIGINT integer safety (removes .0 from NaNs)
        if any(pat in col for pat in ['id', 'season', 'pick', 'goals', 'played', 'number', 'wins']):
            if not (table_name in ["player_stats", "plays"] and col == "id"): # Skip string PKs
                df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    return df.replace({np.nan: None, np.inf: None, -np.inf: None})

def sync_table(table_name: str, df: pd.DataFrame, p_key: str):
    """Executes the tabular upsert to Supabase."""
    ready_df = clean_and_coerce(df, table_name)
    
    # Ensure PK components are not null
    for pk in p_key.split(','):
        if pk in ready_df.columns:
            ready_df = ready_df[ready_df[pk].notna()]
            
    if ready_df.empty: return
    try:
        supabase.table(table_name).upsert(ready_df.to_dict(orient="records"), on_conflict=p_key).execute()
        LOG.info(f"Successfully synced {len(ready_df)} rows to {table_name}")
    except Exception as e:
        LOG.error(f"Sync failed for {table_name}: {e}")

def run_sync(mode="daily"):
    season_str = "20242025"
    season_int = 20242025
    
    LOG.info(f"Starting Master Sync: {mode}")

    # 1. Teams Sync (Discovery)
    teams_raw = scrapeTeams(source="records")
    sync_table("teams", teams_raw, "id")
    
    # Filter Active Franchises for iteration
    teams_clean = clean_and_coerce(teams_raw.copy(), "teams")
    active_teams = teams_clean[teams_clean['lastseasonid'].isna()]['teamabbrev'].dropna().unique().tolist()

    # 2. Standings Sync (Always Current Date)
    sync_table("standings", scrapeStandings(), "date,teamabbrev_default")

    if mode == "catchup":
        # 3. Draft Sync (Historical Range)
        for year in range(2020, 2026):
            sync_table("draft", scrapeDraftRecords(str(year)), "year,overall_pick")

        # 4. Full Franchise Sync
        for team in active_teams:
            LOG.info(f"Deep Syncing Franchise: {team}")
            
            # Schedule
            sched = scrapeSchedule(team, season_str)
            sync_table("schedule", sched, "id")
            
            # Play-by-Play (PBP) for the first 10 games of schedule for speed (Optional)
            if not sched.empty:
                game_ids = sched['id'].head(10).tolist()
                for gid in game_ids:
                    plays = scrapePlays(gid)
                    if not plays.empty:
                        plays['id'] = plays.apply(lambda r: f"{gid}_{r.get('eventid', r.get('id'))}", axis=1)
                        plays['game_id'] = gid
                        sync_table("plays", plays, "id")

            # Roster & Players (Inject Season PK part)
            roster = scrapeRoster(team, season_str)
            if not roster.empty:
                roster['season'] = season_int
                roster['teamabbrev'] = team
                sync_table("players", roster.copy(), "id")
                sync_table("rosters", roster, "id,season")

            # Analytical Player Stats (Inject Composite ID)
            for is_goalie in [False, True]:
                stats = scraper_legacy.scrapeTeamStats(team, season_str, goalies=is_goalie)
                if not stats.empty:
                    stats['playerid'] = stats['playerid' if 'playerid' in stats.columns else 'playerId']
                    stats['season'] = season_int
                    stats['is_goalie'] = is_goalie
                    # ID: playerid_season_isgoalie_strength
                    stats['id'] = stats.apply(lambda r: f"{r['playerid']}_{season_int}_{is_goalie}_{r.get('strength', 'all')}", axis=1)
                    sync_table("player_stats", stats, "id")

if __name__ == "__main__":
    sync_mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    run_sync(sync_mode)