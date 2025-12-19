import os
import sys
import argparse 
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

sys.stdout.reconfigure(line_buffering=True)
DB_URL = os.getenv("DATABASE_URL")
if DB_URL and DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)
engine = create_engine(DB_URL, connect_args={'sslmode': 'require'}, pool_pre_ping=True)

def get_safe_df(engine, df, target_table):
    with engine.connect() as conn:
        query = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{target_table}'")
        db_cols = [row[0] for row in conn.execute(query)]
    return df[[c for c in df.columns if c in db_cols]]

def upsert_table(engine, df, table_name, constraint_cols, is_accumulation=False):
    if df.empty: return
    df.columns = [c.lower().replace('.', '_') for c in df.columns]
    rename_map = {
        'player1id': 'player_id', 'id': 'player_id', 'seconds': 'toi_sec',
        'xg': 'xgf', 'birthdate': 'birth_date', 'headshot': 'headshot_url',
        'heightincentimeters': 'height_in_centimeters', 'weightinpounds': 'weight_in_pounds'
    }
    df = df.rename(columns=rename_map)
    df_safe = get_safe_df(engine, df, table_name)
    df_safe.to_sql("temp_staging", engine, if_exists="replace", index=False)
    
    cols = []
    select_parts = []
    for c in df_safe.columns:
        cols.append(f'"{c}"')
        if c in ['birth_date', 'game_date']:
            select_parts.append(f'"{c}"::DATE')
        else:
            select_parts.append(f'"{c}"')

    col_list, select_list = ", ".join(cols), ", ".join(select_parts)
    if is_accumulation:
        update_parts = [f"{c} = {table_name}.{c} + (CASE WHEN '{c}'='gp' THEN 1 ELSE EXCLUDED.{c} END)" for c in df_safe.columns if c not in constraint_cols]
        update_list = ", ".join(update_parts)
    else:
        update_list = ", ".join([f'{c} = EXCLUDED.{c}' for c in cols if c.strip('"') not in constraint_cols])
    
    upsert_query = f"INSERT INTO {table_name} ({col_list}) SELECT {select_list} FROM temp_staging ON CONFLICT ({', '.join(constraint_cols)}) DO UPDATE SET {update_list};"
    with engine.begin() as conn:
        conn.execute(text(upsert_query))

def get_team_stats(pbp_wide):
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
                'ca': len(df_s[df_s['eventTeam'] != t_id]),
                'gf': len(df_s[(df_s['eventTeam'] == t_id) & (df_s['Event'] == 'GOAL')]),
                'ga': len(df_s[(df_s['eventTeam'] != t_id) & (df_s['Event'] == 'GOAL')]),
                'xgf': df_s[df_s['eventTeam'] == t_id]['xG'].sum() if 'xG' in df_s.columns else 0,
                'xga': df_s[df_s['eventTeam'] != t_id]['xG'].sum() if 'xG' in df_s.columns else 0
            })
    return pd.DataFrame(results)

def run_pipeline(game_id):
    print(f"RUNNER: Starting Resilient Pipeline for Game {game_id}")
    try:
        pbp_wide, _ = pipeline(game_id)
        start_year = int(str(game_id)[:4])
        season = f"{start_year}{start_year + 1}"
        game_meta = pd.DataFrame([{'game_id': game_id, 'season_id': int(season), 'home_team': str(pbp_wide['homeTeam'].iloc[0]), 'away_team': str(pbp_wide['awayTeam'].iloc[0]), 'game_date': pd.to_datetime(pbp_wide['gameDate'].iloc[0]).date()}])
        upsert_table(engine, game_meta, "games", ['game_id'])
        p_stats = on_ice_stats_by_player_strength(pbp_wide)
        p_stats['game_id'] = game_id
        upsert_table(engine, p_stats, "player_game_stats", ['player_id', 'game_id', 'strength'])
        t_stats = get_team_stats(pbp_wide)
        t_stats['season_id'] = int(season)
        upsert_table(engine, t_stats, "team_season_stats", ['team_id', 'season_id', 'strength'], is_accumulation=True)
        players_df = pd.concat([scrapeRoster(pbp_wide['homeTeam'].iloc[0], season), scrapeRoster(pbp_wide['awayTeam'].iloc[0], season)])
        upsert_table(engine, players_df, "players", ['player_id'])
        print(f"SUCCESS: Game {game_id} fully synced.")
    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # 1. Set up arg parser
    parser = argparse.ArgumentParser(description="NHL Game Scraper Runner")
    
    # 2. Add game_id argument
    # Set a default too
    parser.add_argument(
        "game_id", 
        type=int, 
        nargs="?", 
        default=2025020539, 
        help="The 10-digit NHL Game ID to scrape"
    )
    
    # 3. Parse the input
    args = parser.parse_args()
    
    # 4. Run the pipeline with the dynamic ID
    run_pipeline(args.game_id)