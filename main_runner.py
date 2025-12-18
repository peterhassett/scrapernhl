"""
NHL DATA SCRAPER - MAIN RUNNER
Managed Tables and Columns:
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

def get_team_stats(pbp_wide):
    """Aggregates play-by-play data into team-level totals."""
    results = []
    for s in ['EV', 'PP', 'PK']:
        df_s = pbp_wide[pbp_wide['strength'] == s]
        if df_s.empty: continue
        
        # FIX: Team IDs are strings (e.g., 'PHI'), not ints
        home_id = str(df_s['homeTeam'].iloc[0])
        away_id = int(df_s['awayTeam'].iloc[0]) if str(df_s['awayTeam'].iloc[0]).isdigit() else str(df_s['awayTeam'].iloc[0])
        
        toi = len(df_s['seconds_elapsed'].unique())

        for t_id, opp_id in [(home_id, away_id), (away_id, home_id)]:
            results.append({
                'team_id': t_id,
                'strength': s,
                'toi': toi,
                'cf': len(df_s[df_s['eventTeam'] == t_id]),
                'ca': len(df_s[df_s['eventTeam'] == opp_id]),
                'gf': len(df_s[(df_s['eventTeam'] == t_id) & (df_s['Event'] == 'GOAL')]),
                'ga': len(df_s[(df_s['eventTeam'] == opp_id) & (df_s['Event'] == 'GOAL')]),
                'xgf': df_s[df_s['eventTeam'] == t_id]['xG'].sum(),
                'xga': df_s[df_s['eventTeam'] == opp_id]['xG'].sum()
            })
    return pd.DataFrame(results)

def run_pipeline(game_id):
    print(f"RUNNER: Starting process for Game {game_id}")
    try:
        # SCRAPE CORE GAME DATA
        pbp_wide, _ = pipeline(game_id)
        p_stats = on_ice_stats_by_player_strength(pbp_wide)
        t_stats = get_team_stats(pbp_wide)
        
        # EXTRACT METADATA
        home_team = pbp_wide['homeTeam'].dropna().iloc[0]
        away_team = pbp_wide['awayTeam'].dropna().iloc[0]
        start_year = int(str(game_id)[:4])
        season = f"{start_year}{start_year + 1}"

        # GET ROSTERS
        print(f"RUNNER: Fetching rosters for {home_team} and {away_team}")
        players_df = pd.concat([
            scrapeRoster(home_team, season), 
            scrapeRoster(away_team, season)
        ])

        with engine.begin() as conn:
            # --- 1. PLAYERS (UPSERT) ---
            players_df.columns = [c.lower().replace('.', '_') for c in players_df.columns]
            players_df.to_sql("temp_players", conn, if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO players (
                    player_id, first_name, last_name, birth_date, 
                    height_in_centimeters, weight_in_pounds, shoots_catches, headshot_url
                )
                SELECT 
                    id, firstname_default, lastname_default, birthdate::DATE, 
                    heightincentimeters, weightinpounds, shootscatches, headshot 
                FROM temp_players
                ON CONFLICT (player_id) DO UPDATE SET 
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    birth_date = EXCLUDED.birth_date,
                    height_in_centimeters = EXCLUDED.height_in_centimeters,
                    weight_in_pounds = EXCLUDED.weight_in_pounds,
                    shoots_catches = EXCLUDED.shoots_catches;
            """))

            # --- 2. PLAYER GAME STATS (UPSERT) ---
            p_stats['game_id'] = game_id
            p_stats.columns = [c.lower() for c in p_stats.columns]
            p_stats.to_sql("temp_p_stats", conn, if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO player_game_stats (
                    player_id, game_id, strength, toi_sec, 
                    cf, ca, ff, fa, sf, sa, gf, ga, xgf, xga, pf, pa, 
                    give_for, give_against, take_for, take_against
                )
                SELECT 
                    player1id, game_id, strength, seconds, 
                    cf, ca, ff, fa, sf, sa, gf, ga, xg::float, xga::float, pf, pa, 
                    give_for, give_against, take_for, take_against 
                FROM temp_p_stats
                ON CONFLICT (player_id, game_id, strength) DO UPDATE SET 
                    toi_sec = EXCLUDED.toi_sec, 
                    cf = EXCLUDED.cf,
                    give_for = EXCLUDED.give_for,
                    take_for = EXCLUDED.take_for;
            """))

            # --- 3. TEAM SEASON STATS (ACCUMULATE) ---
            t_stats['season_id'] = int(season)
            t_stats.to_sql("temp_t_stats", conn, if_exists="replace", index=False)
            conn.execute(text("""
                INSERT INTO team_season_stats (
                    team_id, season_id, strength, gp, toi_sec, 
                    cf, ca, gf, ga, xgf, xga
                )
                SELECT team_id, season_id, strength, 1, toi, cf, ca, gf, ga, xgf, xga 
                FROM temp_t_stats
                ON CONFLICT (team_id, season_id, strength) DO UPDATE SET 
                    gp = team_season_stats.gp + 1,
                    toi_sec = team_season_stats.toi_sec + EXCLUDED.toi_sec,
                    cf = team_season_stats.cf + EXCLUDED.cf,
                    ca = team_season_stats.ca + EXCLUDED.ca,
                    gf = team_season_stats.gf + EXCLUDED.gf,
                    ga = team_season_stats.ga + EXCLUDED.ga,
                    xgf = team_season_stats.xgf + EXCLUDED.xgf,
                    xga = team_season_stats.xga + EXCLUDED.xga;
            """))

            # --- 4. STANDINGS (REPLACE) ---
            print("RUNNER: Updating standings snapshot")
            standings = scrapeStandings()
            standings.columns = [c.lower().replace('.', '_') for c in standings.columns]
            standings.to_sql("standings", conn, if_exists="replace", index=False)

        print(f"SUCCESS: Game {game_id} processed successfully.")

    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    # Example Game ID
    run_pipeline(2024020123)
