"""
NHL DATA SCRAPER - MAIN RUNNER (Resilient Version)
Managed Tables:
- games: game_id, season_id, home_team, away_team, game_date (UPSERT)
- players: player_id, first_name, last_name, birth_date, height_in_centimeters, 
           weight_in_pounds, shoots_catches, headshot_url (UPSERT)
- player_game_stats: player_id, game_id, strength, toi_sec, cf, ca, ff, fa, sf, sa, 
                    gf, ga, xgf, xga, pf, pa, give_for, give_against, 
                    take_for, take_against (UPSERT)
- team_season_stats: team_id, season_id, strength, gp, toi_sec, cf, ca, 
                    gf, ga, xgf, xga (ACCUMULATE)
- standings: teamname_default, conferenceabbr, divisionabbr, points, wins, 
             losses, otlosses, goalfor, goalagainst, goaldifferential (REPLACE)
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# 1. ROBUST IMPORTS
try:
    from scrapernhl.scraper import (
        pipeline, on_ice_stats_by_player_strength, scrapeStandings, scrapeRoster
    )
except ImportError:
    try:
        from scraper import (
            pipeline, on_ice_stats_by_player_strength, scrapeStandings, scrapeRoster
        )
    except ImportError as e:
        print(f"CRITICAL ERROR: Could not find scraper functions! {e}")
        sys.exit(1)

# Force unbuffered output for cleaner logs
sys.stdout.reconfigure(line_buffering=True)

# 2. DATABASE SETUP
DB_URL = os.getenv("DATABASE_URL")
if DB_URL and DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DB_URL, connect_args={'sslmode': 'require'}, pool_pre_ping=True)

# --- RESILIENCY HELPERS ---

def get_safe_df(engine, df, target_table):
    """Returns a DataFrame containing only columns that exist in the target DB table."""
    with engine.connect() as conn:
        query = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{target_table}'")
        db_cols = [row[0] for row in conn.execute(query)]
    
    safe_cols = [c for c in df.columns if c in db_cols]
    return df[safe_cols]

def upsert_table(engine, df, table_name, constraint_cols, is_accumulation=False):
    """Robustly upserts any dataframe into any table by matching columns automatically."""
    if df.empty: return
    
    # 1. Standardize column names
    df.columns = [c.lower().replace('.', '_') for c in df.columns]
    
    # 2. Global Rename Map (Matches your Schema exactly)
    rename_map = {
        'player1id': 'player_id', 'id': 'player_id', 'seconds': 'toi_sec',
        'xg': 'xgf', 'birthdate': 'birth_date', 'headshot': 'headshot_url',
        'heightincentimeters': 'height_in_centimeters', 'weightinpounds': 'weight_in_pounds'
    }
    df = df.rename(columns=rename_map)
    
    # 3. Filter to only columns existing in the DB
    df_safe = get_safe_df(engine, df, table_name)
    df_safe.to_sql("temp_staging", engine, if_exists="replace", index=False)
    
    # 4. Build dynamic SQL
    cols = [f'"{c}"' for c in df_safe.columns]
    col_list = ", ".join(cols)
    
    if is_accumulation:
        update_parts = []
        for c in df_safe.columns:
            if c not in constraint_cols:
                if c == 'gp': update_parts.append(f"{c} = {table_name}.{c} + 1")
                else: update_parts.append(f"{c} = {table_name}.{c} + EXCLUDED.{c}")
        update_list = ", ".join(update_parts)
    else:
        update_list = ", ".join([f'{c} = EXCLUDED.{c}' for c in cols if c.strip('"') not in constraint_cols])
    
    upsert_query = f"""
        INSERT INTO {table_name} ({col_list})
        SELECT {col_list} FROM temp_staging
        ON CONFLICT ({", ".join(constraint_cols)}) 
        DO UPDATE SET {update_list};
    """
    
    with engine.begin() as conn:
        conn.execute(text(upsert_query))

def get_team_stats(pbp_wide):
    """Aggregates play-by-play data into team-level totals."""
    results = []
    time_col = next((c for c in ['seconds_elapsed', 'seconds', 'timeInPeriod'] if c in pbp_wide.columns), None)
    
    for s in ['EV', 'PP', 'PK']:
        df_s = pbp_wide[pbp_wide['strength'] == s]
        if df_s.empty: continue
        
        h_id, a_id = str(df_s['homeTeam'].iloc[0]), str(df_s['awayTeam'].iloc[0])
        toi = len(df_s[time_col].unique()) if time_col else 0

        for t_id, opp_id in [(h_id, a_id), (a_id, h_id)]:
            results.append({
                'team_id': t_id, 'strength': s, 'toi_sec': toi, 'gp': 1,
                'cf': len(df_s[df_s['eventTeam'] == t_id]),
                'ca': len(df_s[df_s['eventTeam'] == opp_id]),
                'gf': len(df_s[(df_s['eventTeam'] == t_id) & (df_s['Event'] == 'GOAL')]),
                'ga': len(df_s[(df_s['eventTeam'] != t_id) & (df_s['Event'] == 'GOAL')]),
                'xgf': df_s[df_s['eventTeam'] == t_id]['xG'].sum() if 'xG' in df_s.columns else 0,
                'xga': df_s[df_s['eventTeam'] != t_id]['xG'].sum() if 'xG' in df_s.columns else 0
            })
    return pd.DataFrame(results)

# --- MAIN RUNNER ---

def run_pipeline(game_id):
    print(f"RUNNER: Starting Sync for Game {game_id}")
    try:
        pbp_wide, _ = pipeline(game_id)
        start_year = int(str(game_id)[:4])
        season = f"{start_year}{start_year + 1}"

        # 0. GAMES (Insert Metadata First to satisfy Foreign Key constraints)
        game_meta = pd.DataFrame([{
            'game_id': game_id, 'season_id': int(season),
            'home_team': str(pbp_wide['homeTeam'].iloc[0]),
            'away_team': str(pbp_wide['awayTeam'].iloc[0]),
            'game_date': pd.to_datetime(pbp_wide['gameDate'].iloc[0]).date() if 'gameDate' in pbp_wide.columns else None
        }])
        upsert_table(engine, game_meta, "games", ['game_id'])

        # 1. PLAYER STATS
        p_stats = on_ice_stats_by_player_strength(pbp_wide)
        p_stats['game_id'] = game_id
        upsert_table(engine, p_stats, "player_game_stats", ['player_id', 'game_id', 'strength'])

        # 2. TEAM STATS
        t_stats = get_team_stats(pbp_wide)
        t_stats['season_id'] = int(season)
        upsert_table(engine, t_stats, "team_season_stats", ['team_id', 'season_id', 'strength'], is_accumulation=True)

        # 3. PLAYER BIOS
        h_team, a_team = pbp_wide['homeTeam'].dropna().iloc[0], pbp_wide['awayTeam'].dropna().iloc[0]
        players_df = pd.concat([scrapeRoster(h_team, season), scrapeRoster(a_team, season)])
        upsert_table(engine, players_df, "players", ['player_id'])

        # 4. STANDINGS
        standings = scrapeStandings()
        standings.columns = [c.lower().replace('.', '_') for c in standings.columns]
        standings.to_sql("standings", engine, if_exists="replace", index=False)

        print(f"SUCCESS: Game {game_id} and all related tables are synced.")
    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline(2024020123)
