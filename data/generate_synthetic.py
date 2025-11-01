"""
Generate a small synthetic football matches dataset for testing.

This version keeps the CSV output for compatibility, and also optionally imports into the DB
if you want the DB to be immediately populated. To import automatically set IMPORT_TO_DB=1 env var.
"""
import random
import pandas as pd
from datetime import datetime, timedelta
import os

random.seed(0)

TEAMS = [f"Team {i}" for i in range(1, 21)]
LEAGUE = "Premier"

start_date = datetime(2023, 1, 1)
matches = []
date = start_date
# create random fixtures over ~600 matches
for _ in range(600):
    home, away = random.sample(TEAMS, 2)
    # bias a bit towards low scores
    home_goals = max(0, int(random.gauss(1.3, 1.2)))
    away_goals = max(0, int(random.gauss(1.0, 1.1)))
    matches.append({
        "date": date.strftime("%Y-%m-%d"),
        "home_team": home,
        "away_team": away,
        "home_score": home_goals,
        "away_score": away_goals,
        "league": LEAGUE
    })
    date += timedelta(days=random.randint(1, 3))

df = pd.DataFrame(matches)
os.makedirs("data", exist_ok=True)
csv_path = "data/matches.csv"
df.to_csv(csv_path, index=False)
print("Wrote data/matches.csv with", len(df), "rows")

# optional: import into DB immediately
if os.environ.get("IMPORT_TO_DB") == "1":
    try:
        from src.import_csv_to_db import import_csv
        inserted = import_csv(csv_path)
        print(f"Imported {inserted} rows into DB")
    except Exception as e:
        print("Failed to import to DB:", e)
