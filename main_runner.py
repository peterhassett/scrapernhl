import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# 1. HANDLE IMPORTS AND PATHING
try:
    from scrapernhl.scraper import pipeline, on_ice_stats_by_player_strength, MODEL_PATH
except ImportError:
    from scraper import pipeline, on_ice_stats_by_player_strength, MODEL_PATH

sys.stdout.reconfigure(line_buffering=True)

print("PYTHON STARTING...")
print(f"Current Working Directory: {os.getcwd()}")

# Validate Model Path before starting expensive scraping
if not os.path.exists(MODEL_PATH):
    print(f"Model not found at: {MODEL_PATH}")
    # Automatic fallback: check if we are in a nested directory
    alt_path = os.path.join(os.getcwd(), "scrapernhl", "models", "xgboost_xG_model1.json")
    if os.path.exists(alt_path):
        print(f"Found model at alternate path: {alt_path}")
    else:
        print(f"CRITICAL: Could not find model file. Directory content: {os.listdir('.')}")
        sys.exit(1)

# 2. DATABASE SETUP
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    print("ERROR: DATABASE_URL missing from GitHub Secrets.")
    sys.exit(1)

if DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DB_URL, 
    connect_args={'sslmode': 'require'},
    pool_pre_ping=True
)

def run_pipeline(game_id):
    print(f"Processing Game: {game_id}")
    
    try:
        # SCRAPER PIPELINE
        pbp_wide, players_df = pipeline(game_id)
        stats_df = on_ice_stats_by_player_strength(pbp_wide)
        
        # 4. PREPARE GAME METADATA
        game_meta = {
            'game_id': game_id,
            'season_id': int(str(game_id)[:8]),
            'game_date': pbp_wide['gameDate'].iloc[0],
            'home_team': pbp_wide['homeTeam'].iloc[0],
            'away_team': pbp_wide['awayTeam'].iloc[0],
            'home_score': int(pbp_wide['homeScore'].max()),
            'away_score': int(pbp_wide['awayScore'].max()),
            'game_type': int(pbp_wide['gameType'].iloc[0]),
            'venue': pbp_wide['venue'].iloc[0]
        }
        game_df = pd.DataFrame([game_meta])

        #  DATABASE UPLOAD 
        with engine.begin() as conn:
            
            # PLAYERS
            players_df.columns = [c.lower() for c in players_df.columns]
            players_df.to_sql("temp_players", conn, if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO players (player_id, full_name, default_pos, headshot_url)
                SELECT playerid, fullname, positioncode, headshot FROM temp_players
                ON CONFLICT (player_id) DO UPDATE SET 
                    full_name = EXCLUDED.full_name, headshot_url = EXCLUDED.headshot_url;
            """))

            # GAMES
            game_df.to_sql("temp_game", conn, if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO games (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_type, venue)
                SELECT game_id, season_id, game_date::date, home_team, away_team, home_score, away_score, game_type, venue FROM temp_game
                ON CONFLICT (game_id) DO UPDATE SET 
                    home_score = EXCLUDED.home_score, away_score = EXCLUDED.away_score;
            """))

            # PLAYER_GAME_STATS
            stats_df['game_id'] = game_id
            stats_df.columns = [c.lower() for c in stats_df.columns]
            stats_df.to_sql("temp_stats", conn, if_exists="replace", index=False)
            
            conn.execute(text("""
                INSERT INTO player_game_stats (player_id, game_id, strength, toi_sec, cf, ca, gf, ga, xgf, xga)
                SELECT 
                    player1id, game_id, strength, seconds, 
                    cf, ca, gf, ga, xg::float, xga::float 
                FROM temp_stats
                ON CONFLICT (player_id, game_id, strength) 
                DO UPDATE SET 
                    toi_sec = EXCLUDED.toi_sec, cf = EXCLUDED.cf, xgf = EXCLUDED.xgf;
            """))
            
        print(f"SUCCESS: Game {game_id} is live in Supabase.")

    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    # Starting with the test game ID provided
    run_pipeline(2024020123)
