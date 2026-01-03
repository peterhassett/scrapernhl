import pandas as pd
import numpy as np
import logging
import os

# Scrapers from your package
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.draft import scrapeDraftData
from scrapernhl.scrapers.games import scrapePlays 
from scrapernhl import engineer_xg_features, predict_xg_for_pbp, on_ice_stats_by_player_strength

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

def get_sql_type(col_name, series):
    """Infers SQL type based on regular season data depth."""
    if "id" in col_name.lower() or "season" in col_name.lower():
        return "BIGINT"
    if pd.api.types.is_numeric_dtype(series):
        return "NUMERIC"
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if "date" in col_name.lower() or "birthdate" in col_name.lower():
        return "DATE"
    
    # Check for complex JSON/Nested types
    sample = series.dropna().iloc[0] if not series.dropna().empty else ""
    if isinstance(sample, (list, dict)) or str(sample).startswith(('[', '{')):
        return "JSONB"
    
    return "TEXT"

def generate_master_schema():
    LOG.info("--- Starting Regular Season Column Discovery ---")
    
    # 1. Base Tables
    t_df = pd.concat([scrapeTeams(s) for s in ["calendar", "franchise", "records"]])
    std_df = scrapeStandings()
    draft_df = pd.concat([scrapeDraftData(y) for y in [2023, 2024, 2025]])
    
    # 2. Team Context (Using 24-25 for full reliability)
    discovery_teams = ['MTL', 'BUF', 'NYR', 'TOR', 'EDM', 'FLA']
    r_df = pd.concat([scrapeRoster(t, "20242025") for t in discovery_teams])
    
    # 3. Schedule - Filter for Regular Season (GameType 2)
    s_df = pd.concat([scrapeSchedule(t, "20242025") for t in discovery_teams])
    reg_season_games = s_df[(s_df['gameType'] == 2) & (s_df['gameState'] == 'FINAL')]['id'].unique()
    
    LOG.info(f"Discovery: Analyzing {min(len(reg_season_games), 20)} Regular Season games...")
    
    plays_collector = []
    stats_collector = []

    # 4. Deep Game Scan
    for gid in reg_season_games[:20]:
        try:
            # scrapePlays uses the modern JSON API, more reliable for field discovery
            pbp = scrapePlays(gid) 
            if pbp.empty: continue
            
            pbp = predict_xg_for_pbp(engineer_xg_features(pbp))
            plays_collector.append(pbp)
            
            # This handles the aggregation logic we need for player_stats
            stats = on_ice_stats_by_player_strength(pbp)
            stats_collector.append(stats)
            LOG.info(f"Sampled Regular Season Game: {gid}")
        except Exception as e:
            LOG.warning(f"Skipping game {gid} during discovery: {e}")

    # Build the column union
    p_df = pd.concat(plays_collector).drop_duplicates() if plays_collector else pd.DataFrame()
    st_df = pd.concat(stats_collector).drop_duplicates() if stats_collector else pd.DataFrame()

    all_tables = {
        "teams": t_df,
        "players": r_df,
        "rosters": r_df, 
        "schedule": s_df,
        "plays": p_df,
        "player_stats": st_df,
        "standings": std_df,
        "draft": draft_df
    }

    with open("final_schema.sql", "w") as f:
        f.write("-- MASTER SCHEMA GENERATED FROM REGULAR SEASON DATA --\n\n")
        for table, df in all_tables.items():
            if df.empty: continue
            
            df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
            unique_cols = df.columns.unique()
            
            f.write(f"DROP TABLE IF EXISTS {table} CASCADE;\n")
            f.write(f"CREATE TABLE {table} (\n")
            
            col_lines = []
            pk = "id"
            if table == "rosters": pk = "id, season"
            
            for col in unique_cols:
                sql_type = get_sql_type(col, df[col])
                pk_suffix = " PRIMARY KEY" if col == "id" and "," not in pk else ""
                col_lines.append(f"    {col} {sql_type}{pk_suffix}")
            
            f.write(",\n".join(col_lines))
            if "," in pk:
                f.write(f",\n    PRIMARY KEY ({pk})")
            f.write("\n);\n\n")
            
    LOG.info("--- SUCCESS: final_schema.sql contains the union of all regular season columns ---")

if __name__ == "__main__":
    generate_master_schema()