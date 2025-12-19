import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
import argparse

# 1. PRE-FLIGHT CHECK: Ensure the environment is ready before importing the scraper
try:
    import selectolax
    import xgboost
except ImportError as e:
    print(f"ENVIRONMENT ERROR: Missing library {e.name}. Check GitHub Action installation step.")
    sys.exit(1)

# 2. IMPORT SCRAPER: Now that we know dependencies exist
try:
    import scrapernhl
except ImportError as e:
    print(f"PACKAGE ERROR: Could not find scrapernhl folder. {e}")
    sys.exit(1)

# Database Setup
DB_URL = os.getenv("DATABASE_URL")
if DB_URL and DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)
engine = create_engine(DB_URL, connect_args={'sslmode': 'require'}, pool_pre_ping=True)

def get_safe_df(engine, df, target_table):
    with engine.connect() as conn:
        query = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{target_table}'")
        db_cols = [row[0] for row in conn.execute(query)]
    return df[[c for c in df.columns if c in db_cols]]

def upsert_table(engine, df, table_name, constraint_cols):
    if df.empty: return
    df.columns = [c.lower().replace('.', '_') for c in df.columns]
    
    # Custom mapping for your specific DB schema
    rename_map = {
        'player1id': 'player_id', 'id': 'player_id', 'seconds': 'toi_sec',
        'xg': 'xgf', 'headshot': 'headshot_url'
    }
    df = df.rename(columns=rename_map)
    df_safe = get_safe_df(engine, df, table_name)
    df_safe.to_sql("temp_staging", engine, if_exists="replace", index=False)
    
    cols = [f'"{c}"' for c in df_safe.columns]
    col_list = ", ".join(cols)
    update_list = ", ".join([f'{c} = EXCLUDED.{c}' for c in cols if c.strip('"') not in constraint_cols])
    
    upsert_query = f"""
    INSERT INTO {table_name} ({col_list}) 
    SELECT {col_list} FROM temp_staging 
    ON CONFLICT ({', '.join(constraint_cols)}) 
    DO UPDATE SET {update_list};
    """
    with engine.begin() as conn:
        conn.execute(text(upsert_query))

def run_pipeline(game_id):
    print(f"RUNNER: Processing Game {game_id}")
    try:
        # Accessing functions via the scrapernhl package
        pbp_wide, players_df = scrapernhl.pipeline(game_id)
        
        start_year = int(str(game_id)[:4])
        season = f"{start_year}{start_year + 1}"
        
        # Sync Game Metadata
        game_meta = pd.DataFrame([{
            'game_id': game_id, 
            'season_id': int(season), 
            'game_date': pd.to_datetime(pbp_wide['gameDate'].iloc[0]).date()
        }])
        upsert_table(engine, game_meta, "games", ['game_id'])

        # Sync Stats
        p_stats = scrapernhl.on_ice_stats_by_player_strength(pbp_wide)
        p_stats['game_id'] = game_id
        upsert_table(engine, p_stats, "player_game_stats", ['player_id', 'game_id', 'strength'])

        # Sync Players
        upsert_table(engine, players_df, "players", ['player_id'])
        
        print(f"SUCCESS: Game {game_id} synced.")
    except Exception as e:
        print(f"FAILED Game {game_id}: {str(e)}")
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("game_id", type=int)
    args = parser.parse_args()
    run_pipeline(args.game_id)