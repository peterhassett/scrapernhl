# /// script
# dependencies = [
#   "pandas",
#   "scrapernhl",
# ]
# ///
import pandas as pd
from scrapernhl.scrapers.teams import scrapeTeams

print("Fetching teams data...")
teams_df = scrapeTeams(source="records")

print("\n--- RAW COLUMN NAMES ---")
print(teams_df.columns.tolist())

print("\n--- FIRST ROW SAMPLE ---")
# This will show us exactly how the data is nested or named
print(teams_df.iloc[0].to_dict())

# Let's test the specific normalization that failed
print("\n--- TESTING NORMALIZATION ---")
normalized_cols = [str(c).replace('.', '_').lower() for c in teams_df.columns]
print(f"Normalized columns: {normalized_cols}")

if 'activestatus' in normalized_cols:
    print("SUCCESS: found 'activestatus'")
elif 'active_status' in normalized_cols:
    print("SUCCESS: found 'active_status'")
else:
    print("FAILURE: neither found. Need to check raw names above.")