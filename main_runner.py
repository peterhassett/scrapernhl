import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

#  IMPORTS AND PATHING
try:
    from scrapernhl.scraper import pipeline, on_ice_stats_by_player_strength, MODEL_PATH
except ImportError:
    from scraper import pipeline, on_ice_stats_by_player_strength, MODEL_PATH

sys.stdout.reconfigure(line_buffering=True)

# DATABASE 
DB_URL = os.getenv("DATABASE_URL")
if DB_URL and DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DB_URL, connect_args={'sslmode': 'require'}, pool_pre_ping=True)

def run_pipeline(game_id):
    print(f"Processing Game: {game_id}")
    try:
        # SCRAPE
        pbp_wide, players_df = pipeline(game_id)
        stats_df = on_ice_stats_by_player_strength(pbp_wide)
        
        # METADATA
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
            players_df.columns = [c.lower().replace('.', '_') for c in players_df.columns]
            players_df.to_sql("temp_players", conn, if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO players (player_id, full_name, first_name, last_name, default_pos, headshot_url)
                SELECT playerid, fullname, firstname_default, lastname_default, positioncode, headshot 
                FROM temp_players
                ON CONFLICT (player_id) DO UPDATE SET 
                    full_name = EXCLUDED.full_name, 
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    headshot_url = EXCLUDED.headshot_url;
            """))

            # --- GAMES PUSH ---
            game_df.to_sql("temp_game", conn, if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO games (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_type, venue)
                SELECT game_id, season_id, game_date::date, home_team, away_team, home_score, away_score, game_type, venue FROM temp_game
                ON CONFLICT (game_id) DO UPDATE SET home_score = EXCLUDED.home_score, away_score = EXCLUDED.away_score;
            """))

            stats_df['game_id'] = game_id
            stats_df.columns = [c.lower() for c in stats_df.columns]
            stats_df.to_sql("temp_stats", conn, if_exists="replace", index=False)
            
            conn.execute(text("""
                INSERT INTO player_game_stats (
                    player_id, game_id, strength, toi_sec, 
                    cf, ca, ff, fa, sf, sa, gf, ga, xgf, xga, pf, pa
                )
                SELECT 
                    player1id, game_id, strength, seconds, 
                    cf, ca, ff, fa, sf, sa, gf, ga, 
                    xg::float, xga::float, pf, pa
                FROM temp_stats
                ON CONFLICT (player_id, game_id, strength) 
                DO UPDATE SET 
                    toi_sec = EXCLUDED.toi_sec,
                    cf = EXCLUDED.cf, ca = EXCLUDED.ca,
                    ff = EXCLUDED.ff, fa = EXCLUDED.fa,
                    sf = EXCLUDED.sf, sa = EXCLUDED.sa,
                    gf = EXCLUDED.gf, ga = EXCLUDED.ga,
                    xgf = EXCLUDED.xgf, xga = EXCLUDED.xga,
                    pf = EXCLUDED.pf, pa = EXCLUDED.pa;
            """))
            
        print(f"SUCCESS: Game {game_id} fully updated.")

    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline(2024020123)
