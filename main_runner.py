import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# ... (Helper functions get_safe_df and upsert_table remain the same) ...

def run_pipeline(game_id):
    print(f"RUNNER: Starting Exact-Map Pipeline for Game {game_id}")
    try:
        # Fetch the raw data
        pbp_wide, _ = pipeline(game_id)
        start_year = int(str(game_id)[:4])
        season = f"{start_year}{start_year + 1}"

        # --- 0. GAME METADATA (EXACT KEYS) ---
        game_meta = pd.DataFrame([{
            'game_id': game_id,
            'season_id': int(season),
            'home_team': pbp_wide['homeTeam'].iloc[0],
            'away_team': pbp_wide['awayTeam'].iloc[0],
            'game_date': pd.to_datetime(pbp_wide['gameDate'].iloc[0]).date()
        }])
        upsert_table(engine, game_meta, "games", ['game_id'])

        # --- 1. PLAYER STATS (EXACT KEYS) ---
        p_stats = on_ice_stats_by_player_strength(pbp_wide)
        # Force the scraper's 'player1id' to your DB's 'player_id'
        # Force 'seconds' to 'toi_sec'
        p_stats = p_stats.rename(columns={
            'player1id': 'player_id', 
            'seconds': 'toi_sec',
            'xg': 'xgf'
        })
        upsert_table(engine, p_stats, "player_game_stats", ['player_id', 'game_id', 'strength'])

        # --- 2. TEAM STATS ---
        t_stats = get_team_stats(pbp_wide)
        t_stats['season_id'] = int(season)
        upsert_table(engine, t_stats, "team_season_stats", ['team_id', 'season_id', 'strength'], is_accumulation=True)

        # --- 3. PLAYER BIOS (EXACT KEYS) ---
        h_team = pbp_wide['homeTeam'].dropna().iloc[0]
        a_team = pbp_wide['awayTeam'].dropna().iloc[0]
        
        # Scrape rosters and map keys exactly
        players_df = pd.concat([scrapeRoster(h_team, season), scrapeRoster(a_team, season)])
        players_df = players_df.rename(columns={
            'id': 'player_id',
            'birthDate': 'birth_date',
            'heightInCentimeters': 'height_in_centimeters',
            'weightInPounds': 'weight_in_pounds',
            'headshot': 'headshot_url'
        })
        upsert_table(engine, players_df, "players", ['player_id'])

        print(f"SUCCESS: Exact-map sync complete for Game {game_id}")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline(2024020123)
