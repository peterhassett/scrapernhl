import os
import pandas as pd
from sqlalchemy import create_engine, text
from scraper import pipeline, on_ice_stats_by_player_strength

DB_URL = os.getenv("DATABASE_URL")
if DB_URL and DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DB_URL)

def run_pipeline(game_id):
    print(f"🚀 Starting Scraper for Game: {game_id}")
    
    pbp_wide, players_df = pipeline(game_id)

    stats_df = on_ice_stats_by_player_strength(pbp_wide)
    
    game_meta = {
        'game_id': game_id,
        'season_id': int(str(game_id)[:8]),
        'game_date': pbp_wide['gameDate'].iloc[0],
        'home_team': pbp_wide['homeTeam'].iloc[0],
        'away_team': pbp_wide['awayTeam'].iloc[0],
        'home_score': pbp_wide['homeScore'].max(),
        'away_score': pbp_wide['awayScore'].max(),
        'game_type': pbp_wide['gameType'].iloc[0]
    }
    game_df = pd.DataFrame([game_meta])

    with engine.connect() as conn:
        print("Saving Players...")
        players_df.to_sql("temp_players", conn, if_exists="replace", index=False)
        conn.execute(text("""
            INSERT INTO players (player_id, full_name, default_pos, headshot_url)
            SELECT playerId, fullName, positionCode, headshot FROM temp_players
            ON CONFLICT (player_id) DO UPDATE SET 
                full_name = EXCLUDED.full_name, headshot_url = EXCLUDED.headshot_url;
        """))

        print("Saving Game Metadata...")
        game_df.to_sql("temp_game", conn, if_exists="replace", index=False)
        conn.execute(text("""
            INSERT INTO games (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_type)
            SELECT game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_type FROM temp_game
            ON CONFLICT (game_id) DO UPDATE SET home_score = EXCLUDED.home_score, away_score = EXCLUDED.away_score;
        """))

        print("Saving Player Game Stats...")

        stats_df.to_sql("temp_stats", conn, if_exists="replace", index=False)
        conn.execute(text("""
            INSERT INTO player_game_stats (player_id, game_id, strength, toi_sec, cf, ca, gf, ga, xgf, xga)
            SELECT player1Id, gameId, strength, seconds, CF, CA, GF, GA, xG, xGA FROM temp_stats
            ON CONFLICT (player_id, game_id, strength) 
            DO UPDATE SET toi_sec = EXCLUDED.toi_sec, cf = EXCLUDED.cf, xgf = EXCLUDED.xgf;
        """))
        
        conn.commit()
    print(f"✅ Game {game_id} fully updated in Supabase.")

if __name__ == "__main__":
    # will loop multiple games 
    run_pipeline(2024020123)
