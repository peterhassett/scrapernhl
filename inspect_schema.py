import pandas as pd
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.roster import scrapeRoster
from scrapernhl.scrapers.schedule import scrapeSchedule
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.draft import scrapeDraftData
from scrapernhl.scrapers.games import scrapePlays
from scrapernhl.scrapers.stats import scrapeTeamStats

def check(name, df):
    print(f"\n=== {name} ===")
    if isinstance(df, pd.DataFrame) and not df.empty:
        print(f"Columns ({len(df.columns)}): {df.columns.tolist()}")
        # This line is for the VS Code Debugger
        breakpoint() 
    else:
        print("No data returned or empty DataFrame.")

if __name__ == "__main__":
    # 1. Teams (Records source has the most detail)
    check("Teams", scrapeTeams(source="records"))
    
    # 2. Roster
    check("Roster", scrapeRoster(team="MTL", season="20242025"))
    
    # 3. Schedule
    check("Schedule", scrapeSchedule(team="MTL", season="20242025"))
    
    # 4. Standings
    check("Standings", scrapeStandings(date="2024-12-01"))
    
    # 5. Draft
    check("Draft", scrapeDraftData(year="2024"))
    
    # 6. Plays (Requires a specific Game ID)
    check("Plays", scrapePlays(game="2024020001"))
    
    # 7. Stats
    check("Skaters", scrapeTeamStats(team="MTL", season="20242025", goalies=False))