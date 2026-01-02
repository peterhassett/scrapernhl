# /// script
# dependencies = [
#   "pandas",
#   "scrapernhl",
# ]
# ///
import pandas as pd
import json
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl import scrape_game, on_ice_stats_by_player_strength

def print_truth(name, df):
    print(f"\n{'='*20} {name} {'='*20}")
    if df.empty:
        print("Dataframe is empty.")
        return
    print(f"Columns: {df.columns.tolist()}")
    print("\nSample Row (JSON):")
    print(json.dumps(df.iloc[0].to_dict(), indent=2, default=str))

# 1. Teams
print_truth("TEAMS", scrapeTeams(source="records"))

# 2. Roster (using MTL as sample)
print_truth("ROSTER", scrapeRoster("MTL", "20252026"))

# 3. Schedule (using MTL as sample)
print_truth("SCHEDULE", scrapeSchedule("MTL", "20252026"))

# 4. PBP & Aggregated Stats (using a recent Game ID)
# We'll use a hardcoded sample ID or you can grab one from the schedule
sample_game_id = 2024020001 
pbp = scrape_game(sample_game_id)
print_truth("PBP (RAW)", pbp)

stats = on_ice_stats_by_player_strength(pbp, include_goalies=False)
print_truth("PLAYER_STATS (AGGREGATED)", stats)