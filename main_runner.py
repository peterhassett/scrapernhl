"""
NHL DATA SCRAPER - MAIN RUNNER
Managed Tables and Columns:
- players: player_id, first_name, last_name, birth_date, height_in_centimeters, weight_in_pounds, shoots_catches, headshot_url (UPSERT on player_id)
- player_game_stats: player_id, game_id, strength, toi_sec, cf, ca, ff, fa, sf, sa, gf, ga, xgf, xga, pf, pa, giveaways_for, giveaways_against, takeaways_for, takeaways_against (UPSERT on player_id, game_id, strength)
- standings: snapshot of current league rankings (REPLACE full table on each run)
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# 1. HANDLE IMPORTS
try:
    from scrapernhl.scraper import (
        pipeline, 
        on_ice_stats_by_player_strength, 
        scrapeStandings, 
        scrapeRoster
    )
except ImportError:
    from scraper import (
        pipeline, 
        on_ice_stats_by_player_strength, 
        scrapeStandings, 
        scrapeRoster
    )

# Force unbuffered output for cleaner logs
sys.stdout.reconfigure(line_buffering=True)

# 2. DATABASE SETUP
DB_URL = os.getenv("DATABASE_URL")
if DB_URL and DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DB_URL, 
    connect_args={'sslmode': 'require'}, 
    pool_pre_ping=True
)

def run_pipeline(game_id):
    print(f"RUNNER: Starting process for Game {game_id}")
    try:
        # SCRAPE CORE GAME DATA
        pbp_wide, _ = pipeline(game_id)
        stats_df = on_ice_stats_by_player_strength(pbp_wide)
        
        # EXTRACT TEAMS AND VALID 8-DIGIT SEASON FOR BIO SCRAPING
        home_team = pbp_wide['homeTeam'].dropna().iloc[0]
        away_team = pbp_wide['awayTeam'].dropna().iloc[0]
        
        # Correctly derive the 8-digit season (e.g., 2024 -> 20242025)
        start_year = int(str(game_id)[:4])
        season = f"{start_year}{start_year + 1}"

        # GET FULL BIOS (This defines players_df)
        print(f"RUNNER: Fetching full roster bios for {home_team} and {away_team} for season {season}")
        home_bio = scrapeRoster(home_team, season)
        away_bio = scrapeRoster(away_team, season)
        players_df = pd.concat([home_bio, away_bio])

        with engine.begin() as conn:
            # --- PUSH PLAYER BIOS ---
            players_df.columns = [c.lower().replace('.', '_') for c in players_df.columns]
            players_df.to_sql("temp_players", conn, if_exists="replace", index=False)
            
            conn.execute(text("""
                INSERT INTO players (
                    player_id, first_name, last_name, birth_date, 
                    height_in_centimeters, weight_in_pounds, shoots_catches, headshot_url
                )
                SELECT 
                    id, 
                    firstname_default, 
                    lastname_default, 
                    birthdate::DATE, 
                    heightincentimeters, 
                    weightinpounds, 
                    shootscatches, 
                    headshot 
                FROM temp_players
                ON CONFLICT (player_id) DO UPDATE SET 
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    birth_date = EXCLUDED.birth_date,
                    height_in_centimeters = EXCLUDED.height_in_centimeters, 
                    weight_in_pounds = EXCLUDED.weight_in_pounds,
                    shoots_catches = EXCLUDED.shoots_catches;
            """))

            # --- PUSH GAME STATS ---
            stats_df['game_id'] = game_id
            stats_df.columns = [c.lower() for c in stats_df.columns]
            stats_df.to_sql("temp_stats", conn, if_exists="replace", index=False)
            
            conn.execute(text("""
                INSERT INTO player_game_stats (
                    player_id, game_id, strength, toi_sec, 
                    cf, ca, ff, fa, sf, sa, gf, ga, xgf, xga, pf, pa,
                    giveaways_for, giveaways_against, takeaways_for, takeaways_against
                )
                SELECT 
                    player1id, 
                    game_id, 
                    strength, 
                    seconds, 
                    cf, ca, 
                    ff, fa, 
                    sf, sa, 
                    gf, ga, 
                    xg::float, 
                    xga::float, 
                    pf, pa,
                    give_for, 
                    give_against, 
                    take_for, 
                    take_against
                FROM temp_stats
                ON CONFLICT (player_id, game_id, strength) 
                DO UPDATE SET 
                    toi_sec = EXCLUDED.toi_sec,
                    cf = EXCLUDED.cf,
                    ff = EXCLUDED.ff,
                    sf = EXCLUDED.sf,
                    xgf = EXCLUDED.xgf,
                    giveaways_for = EXCLUDED.giveaways_for,
                    takeaways_for = EXCLUDED.takeaways_for;
            """))

            # --- PUSH LEAGUE STANDINGS ---
            print("RUNNER: Updating current NHL standings")
            standings = scrapeStandings()
            standings.columns = [c.lower().replace('.', '_') for c in standings.columns]
            standings.to_sql("standings", conn, if_exists="replace", index=False)

        print(f"SUCCESS: Game {game_id} and Standings successfully synced to database.")

    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline(2024020123)
