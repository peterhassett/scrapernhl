import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from supabase import create_client, Client

# Modular Imports from your repository structure
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.stats import scrapeTeamStats
from scrapernhl.scrapers.draft import scrapeDraftRecords
from scrapernhl.scrapers.standings import scrapeStandings

# Logging Setup
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Initialize Supabase Client
# Using service_role key is recommended for bulk upserts to bypass RLS if needed
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# Intersection cache to prevent redundant schema lookups
PHYSICAL_SCHEMA_CACHE = {}

def get_actual_columns(table_name: str) -> list:
    """Interrogates Supabase to get a whitelist of columns actually in the DB."""
    if table_name in PHYSICAL_SCHEMA_CACHE:
        return PHYSICAL_SCHEMA_CACHE[table_name]
    try:
        # Fetching 1 row to inspect keys
        res = supabase.table(table_name).select("*").limit(1).execute()
        cols = list(res.data[0].keys()) if res.data else []
        PHYSICAL_SCHEMA_CACHE[table_name] = cols
        return cols
    except Exception as e:
        LOG.error(f"Failed to fetch schema for {table_name}: {e}")
        return []

def toi_to_decimal(toi_str):
    """Converts 'MM:SS' string format to numeric decimal minutes."""
    if pd.isna(toi_str) or not isinstance(toi_str, str) or ':' not in toi_str:
        return None
    try:
        minutes, seconds = map(int, toi_str.split(':'))
        return round(minutes + (seconds / 60.0), 2)
    except:
        return None

def rigorous_prepare(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Cleans, flattens, and whitelists data for Supabase.
    - Normalizes nested dot-notation to underscores (e.g., awayTeam.abbrev -> awayteam_abbrev).
    - Hard-filters internationalization fields.
    - Intersects with DB schema to prevent 'Column Not Found' errors.
    """
    if df.empty:
        return df

    # 1. Deduplicate and normalize headers
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # 2. Global Filter: Ignore all internationalized noise fields
    intl_suffixes = ('_cs', '_fr', '_fi', '_sk', '_de', '_sv', '_es')
    df = df.drop(columns=[c for c in df.columns if c.endswith(intl_suffixes)], errors='ignore')

    # 3. Handle 'MM:SS' to Numeric conversion for time fields
    time_cols = [c for c in df.columns if 'timeonice' in c]
    for col in time_cols:
        df[col] = df[col].apply(toi_to_decimal)

    # 4. Whitelist Intersection: Only keep columns that exist in the SQL schema
    db_cols = get_actual_columns(table_name)
    if db_cols:
        matching = [c for c in df.columns if c in db_cols]
        df = df[matching].copy()

    # 5. Type Protection: Drop nested lists/dicts to prevent TypeError in numeric casting
    for col in df.columns:
        if df[col].dtype == 'object':
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample, (list, dict, tuple)):
                df = df.drop(columns=[col])

    # 6. Final Clean: Replace NaNs/Infs with None for JSON compliance
    return df.replace({np.nan: None, np.inf: None, -np.inf: None})

def sync_table(table_name: str, df: pd.DataFrame, p_key: str):
    """Executes the upsert operation to Supabase."""
    df_ready = rigorous_prepare(df, table_name)
    if df_ready.empty:
        return
    try:
        data_dict = df_ready.to_dict(orient="records")
        supabase.table(table_name).upsert(data_dict, on_conflict=p_key).execute()
        LOG.info(f"Synced {len(df_ready)} rows to {table_name}")
    except Exception as e:
        LOG.error(f"Sync failed for {table_name}: {e}")

def run_sync(mode="daily"):
    """
    Master sync logic utilizing repo scrapers.
    """
    LOG.info(f"Starting Rigorous Sync: {mode}")
    current_season = "20242025"
    
    # 1. Teams Sync (Discovery)
    # Uses Records source to get mostrecentteamid
    teams_raw = scrapeTeams(source="records")
    sync_table("teams", teams_raw, "id")
    
    # Identify Active Teams (No lastSeasonId) for deeper sync
    teams_clean = rigorous_prepare(teams_raw.copy(), "teams")
    active_teams = teams_clean[teams_clean['lastseasonid'].isna()]['teamabbrev'].dropna().unique().tolist()
    
    # 2. Standings Sync (Current Date)
    today = datetime.now().strftime("%Y-%m-%d")
    standings = scrapeStandings(date=today)
    sync_table("standings", standings, "date,teamabbrev_default")

    if mode == "catchup":
        # 3. Draft Sync (Historical Registry)
        for year in range(2020, 2026):
            sync_table("draft", scrapeDraftRecords(str(year)), "year,overall_pick")

        # 4. Iterative Sync for all 32 active teams
        for team in active_teams:
            LOG.info(f"Processing Team: {team}")
            
            # Schedule Sync
            sync_table("schedule", scrapeSchedule(team, current_season), "id")
            
            # Roster Sync
            roster_raw = scrapeRoster(team, current_season)
            sync_table("players", roster_raw.copy(), "id")
            sync_table("rosters", roster_raw, "id,season")

            # Player Stats Sync (Skaters then Goalies)
            for is_goalie in [False, True]:
                stats = scrapeTeamStats(team, current_season, goalies=is_goalie)
                if not stats.empty:
                    # Create unique key for stats table
                    stats['id'] = stats['playerId'].astype(str) + "_" + current_season + "_" + str(is_goalie)
                    stats['playerid'] = stats['playerId']
                    stats['season'] = int(current_season)
                    stats['is_goalie'] = is_goalie
                    sync_table("player_stats", stats, "id")

if __name__ == "__main__":
    # Modes: 'daily' (Teams/Standings) or 'catchup' (Deep Full Sync)
    sync_mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    run_sync(sync_mode)