import subprocess
# Utility: Safe float conversion
def safe_float(val):
    try:
        if pd.isna(val):
            return np.nan
        return float(val)
    except Exception:
        return np.nan

# Universal DataFrame cleaning to handle NAType, NaN, None before analytics
def clean_dataframe_for_analytics(df):
    """
    Bulletproof DataFrame cleaning for analytics:
    - Replace all pd.NA with np.nan (regardless of dtype)
    - Coerce all columns to numeric where possible (errors become np.nan)
    - Replace pd.NA/None in non-numeric columns with empty string
    - Log columns that could not be converted to numeric
    """
    import warnings
    df = df.copy()
    # Replace all pd.NA with np.nan everywhere
    df = df.replace({pd.NA: np.nan})
    non_numeric_cols = []
    for col in df.columns:
        # Try to convert to numeric, if fails, keep as object
        try:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # If all values are nan after conversion, treat as non-numeric
            if df[col].isna().all():
                non_numeric_cols.append(col)
                df[col] = df[col].astype(str).replace({"<NA>": "", "nan": "", "None": ""})
        except Exception:
            non_numeric_cols.append(col)
            df[col] = df[col].astype(str).replace({"<NA>": "", "nan": "", "None": ""})
    if non_numeric_cols:
        LOG.info(f"[CLEAN] Non-numeric columns (left as string): {sorted(non_numeric_cols)}")
    return df
import os
import sys
import logging
import pandas as pd
import numpy as np
import json
from datetime import datetime
from supabase import create_client, Client

# Scrapers from your package
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.games import scrapePlays 
from scrapernhl import engineer_xg_features, predict_xg_for_pbp, on_ice_stats_by_player_strength

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

# Supabase Configuration
supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Local cache for schema-compliant columns based on final_schema.sql
DB_COLS = {}

def get_valid_cols(table_name):
    """
    Dynamically fetches column names from the database. 
    This allows the script to 'ignore' any scraper data not in your SQL.
    """
    if table_name in DB_COLS: return DB_COLS[table_name]
    try:
        res = supabase.table(table_name).select("*").limit(1).execute()
        DB_COLS[table_name] = list(res.data[0].keys()) if res.data else []
        return DB_COLS[table_name]
    except Exception as e:
        LOG.warning(f"Metadata fetch failed for {table_name}: {e}")
        return []

def literal_sync(table_name, df, p_key):
    """
    Synchronizes DataFrame to Supabase with strict column alignment.
    Ignores extra data to prevent PGRST204 errors and neutralizes NAType.
    """
    if df.empty:
        return

    # 1. Column Alignment (dots to underscores, lowercase)
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]

    # 2. Whitelist Filtering: Only keep columns that exist in your SQL schema
    valid = get_valid_cols(table_name)
    if valid:
        original_cols = set(df.columns)
        valid_set = set(valid)
        drop_cols = original_cols - valid_set
        if drop_cols:
            LOG.info(f"[{table_name}] Dropping columns not in DB schema: {sorted(drop_cols)}")
        df = df[[c for c in df.columns if c in valid]]
    else:
        LOG.warning(f"[{table_name}] No valid columns found in DB schema; skipping sync.")
        return

    # 3. CRITICAL: Manual NAType and Float Sanitation
    # Converts pandas 2.0 NAType sentinels to standard Python None
    def clean_cell(val):
        if pd.isna(val): return None
        if isinstance(val, (np.integer, int)): return int(val)
        if isinstance(val, (np.floating, float)): return float(val)
        return val

    records = []
    for _, row in df.iterrows():
        # Clean every cell to ensure standard types reach the DB driver
        record = {k: clean_cell(v) for k, v in row.to_dict().items()}

        # 4. JSONB Serialization for columns like 'teams' or 'tvbroadcasts'
        for k, v in record.items():
            if isinstance(v, (list, dict)):
                record[k] = json.dumps(v, default=str)
        records.append(record)

    # 5. Deduplicate Payload
    pk_list = [k.strip() for k in p_key.split(',')]
    unique_map = {tuple(r.get(k) for k in pk_list): r for r in records}
    payload = list(unique_map.values())

    try:
        supabase.table(table_name).upsert(payload, on_conflict=p_key).execute()
        LOG.info(f"Sync Success: {len(payload)} records to '{table_name}'")
    except Exception as e:
        LOG.error(f"Sync Failure for '{table_name}': {e}")

def run_sync(mode="daily"):
    # Using 2024-2025 Regular Season as requested
    S_STR, S_INT = "20242025", 20242025
    LOG.info(f"--- STARTING PRODUCTION SYNC | Mode: {mode} ---")

    # 1. Base Tables (Teams, Standings) - Use CLI to generate clean files
    subprocess.run(["python", "-m", "scrapernhl.cli", "teams", "--db-schema", "--output", "teams.csv"], check=True)
    teams_df = pd.read_csv("teams.csv")
    literal_sync("teams", teams_df, "id")

    # Standings: fallback to in-process for now (can be CLI-ized if needed)
    std = scrapeStandings()
    if not std.empty:
        std.columns = [str(c).replace('.', '_').lower() for c in std.columns]
        std['id'] = std['date'].astype(str) + "_" + std['teamabbrev_default'].astype(str)
        literal_sync("standings", std, "id")

    # 2. Roster and Schedule Discovery
    # Use CLI to generate clean rosters and schedules for each team
    active_teams = ['MTL', 'BUF'] if mode == "debug" else teams_df['abbrev'].unique().tolist()
    global_games = set()

    for team in active_teams:
        LOG.info(f"Processing context for team: {team}")
        # Roster
        roster_out = f"{team}_roster.csv"
        subprocess.run(["python", "-m", "scrapernhl.cli", "roster", team, S_STR, "--db-schema", "--output", roster_out], check=True)
        ros = pd.read_csv(roster_out)
        if not ros.empty:
            ros['season'] = S_INT
            ros['teamabbrev'] = team
            literal_sync("players", ros.copy(), "id")
            literal_sync("rosters", ros, "id,season")

        # Schedule
        sched_out = f"{team}_schedule.csv"
        subprocess.run(["python", "-m", "scrapernhl.cli", "schedule", team, S_STR, "--db-schema", "--output", sched_out], check=True)
        sched = pd.read_csv(sched_out)
        # Filter strictly for Regular Season (GameType 2)
        sched_f = sched[(sched['gametype'] == 2) & (sched['gamestate'].isin(['FINAL', 'OFF']))]
        global_games.update(sched_f['id'].tolist())

    # 3. Analytics Processing (Game Phase)
    game_list = sorted(list(global_games))
    if mode == "debug": game_list = game_list[:3]
    
    all_game_stats = []
    for gid in game_list:
        try:
            LOG.info(f"Ingesting Analytics for Game: {gid}")
            if df.empty:
                LOG.warning(f"[{table_name}] DataFrame is empty, skipping sync.")
                return

            # 1. Column Alignment (dots to underscores, lowercase)
            df.columns = [str(c).replace('.', '_').lower() for c in df.columns]

            # 2. Whitelist Filtering: Only keep columns that exist in your SQL schema
            valid = get_valid_cols(table_name)
            if valid:
                original_cols = set(df.columns)
                valid_set = set(valid)
                drop_cols = original_cols - valid_set
                if drop_cols:
                    LOG.info(f"[{table_name}] Dropping columns not in DB schema: {sorted(drop_cols)}")
                df = df[[c for c in df.columns if c in valid]]
            else:
                LOG.warning(f"[{table_name}] No valid columns found in DB schema; skipping sync.")
                return

            # 3. Explicit type casting (optional: customize per table/column as needed)
            # Example: enforce int for id columns, float for numeric, str for text
            for col in df.columns:
                if col.endswith('id') or col in ('id', 'season'):
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                    except Exception:
                        pass
                elif df[col].dtype.kind in 'fiu':
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    except Exception:
                        pass
                else:
                    try:
                        df[col] = df[col].astype(str).replace({"<NA>": "", "nan": "", "None": ""})
                    except Exception:
                        pass

            # 4. Primary Key Null Check
            pk_list = [k.strip() for k in p_key.split(',')]
            if df[pk_list].isnull().any().any():
                LOG.error(f"[{table_name}] Null value(s) in primary key columns: {pk_list}. Skipping sync.")
                return

            # 5. CRITICAL: Manual NAType and Float Sanitation
            def clean_cell(val):
                if pd.isna(val): return None
                if isinstance(val, (np.integer, int)): return int(val)
                if isinstance(val, (np.floating, float)): return float(val)
                return val

            records = []
            for _, row in df.iterrows():
                # Clean every cell to ensure standard types reach the DB driver
                record = {k: clean_cell(v) for k, v in row.to_dict().items()}

                # 6. JSONB Serialization for columns like 'teams' or 'tvbroadcasts'
                for k, v in record.items():
                    if isinstance(v, (list, dict)):
                        record[k] = json.dumps(v, default=str)
                records.append(record)

            # 7. Deduplicate Payload
            unique_map = {tuple(r.get(k) for k in pk_list): r for r in records}
            payload = list(unique_map.values())

            try:
                supabase.table(table_name).upsert(payload, on_conflict=p_key).execute()
                LOG.info(f"Sync Success: {len(payload)} records to '{table_name}'")
            except Exception as e:
                LOG.error(f"Sync Failure for '{table_name}': {e}")
                print("==== stats_clean.iloc[0].to_dict() ====")
                print(stats_clean.iloc[0].to_dict())

            all_game_stats.append(on_ice_stats_by_player_strength(stats_clean, include_goalies=False))
            LOG.info(f"Analytics completed for game {gid}")
        except Exception as e:
            LOG.error(f"Processing error for Game {gid}: {e}")

    # 4. Final Aggregation and Player Registry
    if all_game_stats:
        LOG.info("Finalizing Player Registry from game evidence...")
        combined = pd.concat(all_game_stats)
        combined.columns = [str(c).replace('.', '_').lower() for c in combined.columns]
        
        # Register any player ID found in games not on official team rosters
        u_pids = combined[['player1id', 'player1name']].dropna().drop_duplicates()
        u_pids = u_pids.rename(columns={'player1name': 'firstname_default', 'player1id': 'id'})
        literal_sync("players", u_pids, "id")

        # Rollup seasonal player stats
        agg = combined.groupby(['player1id', 'player1name', 'eventteam', 'strength']).sum(numeric_only=True).reset_index()
        agg['season'] = S_INT
        agg['id'] = agg.apply(lambda r: f"{int(r.player1id)}_{S_INT}_{r.strength}", axis=1)
        # Note: player_stats table must be created to receive this data
        literal_sync("player_stats", agg, "id")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["daily", "catchup", "debug"], default="daily", nargs="?")
    args = parser.parse_args()
    run_sync(args.mode)