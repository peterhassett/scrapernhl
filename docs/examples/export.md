# Data Export Examples

Learn how to save scraped data to various file formats.

## Setup

```python
import pandas as pd
import os
from datetime import datetime

# Create output directory
os.makedirs('output', exist_ok=True)
print("Created output/ directory for exported files")
```

## Export to CSV

```python
from scrapernhl.scrapers.teams import scrapeTeams
from scrapernhl.scrapers.schedule import scrapeSchedule

# Scrape teams
teams = scrapeTeams()
teams.to_csv('output/nhl_teams.csv', index=False)
print(f"✅ Saved {len(teams)} teams to output/nhl_teams.csv")

# Scrape schedule
schedule = scrapeSchedule("MTL", "20252026")
schedule.to_csv('output/mtl_schedule.csv', index=False)
print(f"✅ Saved {len(schedule)} games to output/mtl_schedule.csv")
```

## Export to Excel (Multiple Sheets)

```python
from scrapernhl.scrapers.standings import scrapeStandings
from scrapernhl.scrapers.stats import scrapeTeamStats

# Scrape data
today = datetime.now().strftime("%Y-%m-%d")
standings = scrapeStandings(today)
skaters = scrapeTeamStats("MTL", "20252026", goalies=False)
goalies = scrapeTeamStats("MTL", "20252026", goalies=True)

# Save to Excel with multiple sheets
with pd.ExcelWriter('output/nhl_data.xlsx', engine='openpyxl') as writer:
    standings.to_excel(writer, sheet_name='Standings', index=False)
    skaters.to_excel(writer, sheet_name='Skaters', index=False)
    goalies.to_excel(writer, sheet_name='Goalies', index=False)

print(f"✅ Saved Excel file with 3 sheets to output/nhl_data.xlsx")
print(f"   - Standings: {len(standings)} teams")
print(f"   - Skaters: {len(skaters)} players")
print(f"   - Goalies: {len(goalies)} goalies")
```

## Export to JSON

```python
from scrapernhl.scrapers.games import scrapePlays

# Get a completed game
completed_games = schedule[schedule['gameState'] == 'OFF']
if len(completed_games) > 0:
    game_id = completed_games.iloc[0]['id']
    
    # Scrape play-by-play
    pbp = scrapePlays(game_id)
    
    # Save as JSON (pretty printed)
    pbp.to_json('output/game_pbp.json', orient='records', indent=2)
    print(f"✅ Saved {len(pbp)} events to output/game_pbp.json (pretty format)")
    
    # Save as JSON lines (more efficient for large files)
    pbp.to_json('output/game_pbp.jsonl', orient='records', lines=True)
    print(f"✅ Saved {len(pbp)} events to output/game_pbp.jsonl (lines format)")
    
    # Compare file sizes
    json_size = os.path.getsize('output/game_pbp.json') / 1024
    jsonl_size = os.path.getsize('output/game_pbp.jsonl') / 1024
    print(f"   File sizes: JSON={json_size:.1f}KB, JSONL={jsonl_size:.1f}KB")
else:
    print("No completed games found")
```

## Export to Parquet (Recommended for Large Datasets)

```python
# Scrape multiple games
game_ids = completed_games.head(3)['id'].tolist() if len(completed_games) >= 3 else []

if game_ids:
    all_pbp = []
    for gid in game_ids:
        print(f"Scraping game {gid}...")
        pbp = scrapePlays(gid)
        all_pbp.append(pbp)
    
    # Combine
    combined = pd.concat(all_pbp, ignore_index=True)
    
    # Save as Parquet (compressed)
    combined.to_parquet('output/games_pbp.parquet', index=False, compression='snappy')
    print(f"✅ Saved {len(combined)} events to output/games_pbp.parquet")
    
    # Read it back to verify
    df = pd.read_parquet('output/games_pbp.parquet')
    print(f"✅ Verified: Loaded {len(df)} events from parquet")
    
    # Compare with CSV
    combined.to_csv('output/games_pbp.csv', index=False)
    parquet_size = os.path.getsize('output/games_pbp.parquet') / 1024
    csv_size = os.path.getsize('output/games_pbp.csv') / 1024
    print(f"   File sizes: Parquet={parquet_size:.1f}KB, CSV={csv_size:.1f}KB")
    print(f"   Compression: {100 * (1 - parquet_size/csv_size):.1f}% smaller")
else:
    print("No completed games available")
```

## Export with Polars (Faster for Large Datasets)

```python
# Get data as Polars DataFrame
teams_pl = scrapeTeams(output_format="polars")

# Export to various formats
teams_pl.write_csv('output/teams_polars.csv')
teams_pl.write_json('output/teams_polars.json')
teams_pl.write_parquet('output/teams_polars.parquet')

print(f"✅ Exported {len(teams_pl)} teams using Polars to:")
print("   - output/teams_polars.csv")
print("   - output/teams_polars.json")
print("   - output/teams_polars.parquet")
```

## Export to SQLite Database

```python
import sqlite3

# Create database connection
conn = sqlite3.connect('output/nhl_data.db')

# Save multiple tables
teams.to_sql('teams', conn, if_exists='replace', index=False)
standings.to_sql('standings', conn, if_exists='replace', index=False)
schedule.to_sql('schedule', conn, if_exists='replace', index=False)

print("✅ Saved to SQLite database: output/nhl_data.db")
print(f"   Tables: teams ({len(teams)} rows), standings ({len(standings)} rows), schedule ({len(schedule)} rows)")

# Query the database
query = "SELECT fullName, id, teamPlaceName FROM teams LIMIT 5"
result = pd.read_sql(query, conn)
print("\n   Sample query result:")
result

conn.close()
```

## Incremental Export (Append Mode)

```python
output_file = 'output/incremental_games.csv'

# Remove if exists (for demo)
if os.path.exists(output_file):
    os.remove(output_file)

# Scrape games incrementally
for gid in game_ids[:2] if game_ids else []:
    print(f"Scraping game {gid}...")
    pbp = scrapePlays(gid)
    
    # Append to CSV (create if doesn't exist)
    if os.path.exists(output_file):
        pbp.to_csv(output_file, mode='a', header=False, index=False)
        print(f"   Appended {len(pbp)} events")
    else:
        pbp.to_csv(output_file, mode='w', header=True, index=False)
        print(f"   Created file with {len(pbp)} events")

if os.path.exists(output_file):
    total_rows = len(pd.read_csv(output_file))
    print(f"\n✅ Total events in incremental file: {total_rows}")
```

## Export Selected Columns

```python
from scrapernhl.scrapers.roster import scrapeRoster

# Scrape roster
roster = scrapeRoster("MTL", "20252026")

# Export only specific columns
columns_to_export = ['firstName.default', 'lastName.default', 'sweaterNumber', 'positionCode', 'heightInInches', 'weightInPounds']
roster[columns_to_export].to_csv('output/mtl_roster_simple.csv', index=False)
print(f"✅ Saved simplified roster ({len(columns_to_export)} columns) to output/mtl_roster_simple.csv")
```

## Export with Custom Formatting

```python
# Add custom columns and formatting
schedule_formatted = schedule.copy()
schedule_formatted['gameDate'] = pd.to_datetime(schedule_formatted['gameDate']).dt.strftime('%Y-%m-%d')
schedule_formatted['season'] = '2025-26'
schedule_formatted['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Export with custom column order
column_order = ['gameDate', 'homeTeam.abbrev', 'awayTeam.abbrev', 'gameState', 'season', 'scraped_at']
schedule_formatted[column_order].to_csv('output/mtl_schedule_formatted.csv', index=False)
print("✅ Saved formatted schedule to output/mtl_schedule_formatted.csv")
print("   Custom columns: season, scraped_at")
print("   Date format: YYYY-MM-DD")
```

## List All Exported Files

```python
# List all files in output directory
output_files = os.listdir('output')
print(f"\n📁 Exported {len(output_files)} files to output/ directory:\n")

for file in sorted(output_files):
    filepath = os.path.join('output', file)
    size = os.path.getsize(filepath) / 1024
    print(f"   {file:<30} {size:>8.1f} KB")
```

## See Also

- [Basic Scraping](scraping.md) - Getting the data
- [Advanced Examples](advanced.md) - Data processing
- [API Reference](../api.md) - Function documentation

