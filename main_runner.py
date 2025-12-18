import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

try:
    from scrapernhl.scraper import pipeline, on_ice_stats_by_player_strength
except ImportError:
    # Fallback for the PYTHONPATH method
    from scraper import pipeline, on_ice_stats_by_player_strength
    
sys.stdout.reconfigure(line_buffering=True)

print("PYTHON STARTING...")
print(f"Current Directory: {os.getcwd()}")

# DB Connection
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    print("❌ ERROR: DATABASE_URL not found in environment secrets.")
    sys.exit(1)

if DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DB_URL)

def run_pipeline(game_id):
    print(f"🚀 Processing Game: {game_id}")
    
    try:
        pbp_wide, players_df = pipeline(game_id)
        stats_df = on_ice_stats_by_player_strength(pbp_wide)
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
        
        with engine.begin() as conn:
            
            print("--- Pushing Players ---")
            players_df.to_sql("temp_players", conn, if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO players (player_id, full_name, default_pos, headshot_url)
                SELECT "playerId", "fullName", "positionCode", "headshot" FROM temp_players
                ON CONFLICT (player_id) DO UPDATE SET 
                    full_name = EXCLUDED.full_name, headshot_url = EXCLUDED.headshot_url;
            """))

            print("--- Pushing Game Metadata ---")
            game_df.to_sql("temp_game", conn, if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO games (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_type, venue)
                SELECT game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_type, venue FROM temp_game
                ON CONFLICT (game_id) DO UPDATE SET 
                    home_score = EXCLUDED.home_score, away_score = EXCLUDED.away_score;
            """))

            print("--- Pushing Player Game Stats ---")
            stats_df.to_sql("temp_stats", conn, if_exists="replace", index=False)
            # Note: Double quotes are used for mixed-case columns from your scraper functions
            conn.execute(text("""
                INSERT INTO player_game_stats (player_id, game_id, strength, toi_sec, cf, ca, gf, ga, xgf, xga)
                SELECT "player1Id", "gameId", strength, seconds, "CF", "CA", "GF", "GA", "xG", "xGA" FROM temp_stats
                ON CONFLICT (player_id, game_id, strength) 
                DO UPDATE SET 
                    toi_sec = EXCLUDED.toi_sec,
                    cf = EXCLUDED.cf,
                    xgf = EXCLUDED.xgf;
            """))
            
        print(f"✅ SUCCESS: Game {game_id} is live in Supabase.")

    except Exception as e:
        print(f"❌ CRITICAL ERROR: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Test with one game. 
    run_pipeline(2024020123)
