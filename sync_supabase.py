import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from supabase import create_client, Client

# Repo Scrapers
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.stats import scrapeTeamStats
from scrapernhl.scrapers.draft import scrapeDraftRecords
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.games import scrapePlays
from scrapernhl import scraper_legacy

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# COMPLETE WHITELIST (Matches SQL Columns)
WHITELISTS = {
    "teams": ["id", "fullname", "teamabbrev", "teamcommonname", "teamplacename", "firstseasonid", "lastseasonid", "mostrecentteamid", "active_status", "conference_name", "division_name", "source"],
    "players": ["id", "firstname_default", "lastname_default", "headshot", "positioncode", "shootscatches", "heightininches", "heightincentimeters", "weightinpounds", "weightinkilograms", "birthdate", "birthcountry", "birthcity_default", "birthstateprovince_default", "source"],
    "rosters": ["id", "season", "teamabbrev", "sweaternumber", "positioncode"],
    "player_stats": ["id", "playerid", "season", "is_goalie", "team", "opp", "strength", "gamesplayed", "gamesstarted", "goals", "assists", "points", "plusminus", "penaltyminutes", "powerplaygoals", "shorthandedgoals", "gamewinninggoals", "overtimegoals", "shots", "shootingpctg", "cf", "ca", "cf_pct", "ff", "fa", "ff_pct", "sf", "sa", "sf_pct", "gf", "ga", "gf_pct", "xg", "xga", "xgf_pct", "seconds", "minutes", "avgtimeonicepergame"],
    "schedule": ["id", "season", "gamedate", "gametype", "gamestate", "hometeam_id", "hometeam_abbrev", "hometeam_score", "hometeam_commonname_default", "awayteam_id", "awayteam_abbrev", "awayteam_score", "awayteam_commonname_default", "venue_default", "venue_location_default", "starttimeutc", "gamecenterlink"],
    "plays": ["id", "game_id", "event_id", "period", "period_type", "time_in_period", "time_remaining", "situation_code", "home_team_defending_side", "event_type", "type_desc_key", "x_coord", "y_coord", "zone_code", "ppt_replay_url"],
    "draft": ["year", "overall_pick", "round_number", "pick_in_round", "team_tricode", "player_id", "player_firstname", "player_lastname", "amateurclubname", "amateurleague", "countrycode", "displayabbrev_default"],
    "standings": ["date", "teamabbrev_default", "teamname_default", "conference_name", "division_name", "gamesplayed", "wins", "losses", "otlosses", "points", "pointpctg", "regulationwins", "row"]
}

def rigorous_clean(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    if df.empty: return df
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # Mapping collisions
    if table_name == "draft" and "overallpick" in df.columns:
        df["overall_pick"] = df["overallpick"]
    
    allowed = WHITELISTS.get(table_name, [])
    df = df[[c for c in df.columns if c in allowed]].copy()
    
    # Deduplicate within batch (Fixes Error 21000)
    if table_name in ["plays", "draft", "rosters", "standings"]:
        pk = ["id"] if table_name == "plays" else (["year", "overall_pick"] if table_name == "draft" else (["id", "season"] if table_name == "rosters" else ["date", "teamabbrev_default"]))
        df = df.drop_duplicates(subset=pk, keep='first')

    # Float to BigInt Safety (Fixes Error 22P02)
    for col in df.columns:
        if any(x in col for x in ['id', 'season', 'pick', 'goals', 'played', 'number']):
            if not (table_name in ["player_stats", "plays"] and col == "id"):
                df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    return df.replace({np.nan: None})

def sync_table(table_name: str, df: pd.DataFrame, p_key: str):
    ready = rigorous_clean(df, table_name)
    if ready.empty: return
    try:
        supabase.table(table_name).upsert(ready.to_dict(orient="records"), on_conflict=p_key).execute()
        print(f"Synced {len(ready)} to {table_name}")
    except Exception as e:
        print(f"Failed {table_name}: {e}")

def run_sync(mode="daily"):
    current_season = "20242025"
    
    # 1. SYNC PARENT TEAMS
    teams_df = scrapeTeams(source="records")
    sync_table("teams", teams_df, "id")
    
    active_teams = rigorous_clean(teams_df.copy(), "teams")
    team_list = active_teams[active_teams['lastseasonid'].isna()]['teamabbrev'].dropna().unique().tolist()

    if mode == "catchup":
        for team in team_list:
            print(f"Processing Franchise: {team}")
            
            # 2. SYNC PLAYERS (Parent for Roster/Stats - Fixes Error 23503)
            roster_raw = scrapeRoster(team, current_season)
            if not roster_raw.empty:
                roster_raw['season'] = int(current_season)
                sync_table("players", roster_raw.copy(), "id")
                sync_table("rosters", roster_raw, "id,season")

            # 3. SYNC ANALYTICAL STATS
            for is_goalie in [False, True]:
                stats = scraper_legacy.scrapeTeamStats(team, current_season, goalies=is_goalie)
                if not stats.empty:
                    stats['playerid'] = stats['playerId']
                    stats['season'] = int(current_season)
                    # Granular ID for Analytics
                    stats['id'] = stats.apply(lambda r: f"{r['playerid']}_{current_season}_{is_goalie}_{r.get('strength','all')}", axis=1)
                    
                    # Ensure foreign key exists for every player in stats batch
                    player_safety = stats[['playerid']].rename(columns={'playerid': 'id'}).drop_duplicates()
                    sync_table("players", player_safety, "id")
                    
                    sync_table("player_stats", stats, "id")

            # 4. SYNC SCHEDULE & PLAYS
            sched = scrapeSchedule(team, current_season)
            sync_table("schedule", sched, "id")
            for gid in sched['id'].head(10).tolist():
                plays = scrapePlays(gid)
                if not plays.empty:
                    plays['id'] = plays.apply(lambda r: f"{gid}_{r.get('eventid', '0')}_{r.get('period', '1')}_{r.get('event_type','sub')}", axis=1)
                    plays['game_id'] = gid
                    sync_table("plays", plays, "id")

if __name__ == "__main__":
    run_sync(sys.argv[1] if len(sys.argv) > 1 else "daily")