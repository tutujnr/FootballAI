"""
Import existing data/matches.csv into the database.

This script:
 - creates tables if they don't exist
 - reads CSV (if present)
 - inserts rows skipping duplicates (based on date, home_team, away_team)
"""
import os
import pandas as pd
from datetime import datetime
from src.db import init_db, SessionLocal
from src.models import Match

CSV_PATH = "data/matches.csv"


def parse_row_to_match(row):
    # normalize and parse date
    try:
        date = pd.to_datetime(row["date"]).date()
    except Exception:
        date = None
    hs = None if row.get("home_score") in (None, "") else int(row.get("home_score"))
    aa = None if row.get("away_score") in (None, "") else int(row.get("away_score"))
    return {
        "date": date,
        "home_team": str(row["home_team"]),
        "away_team": str(row["away_team"]),
        "home_score": hs,
        "away_score": aa,
        "league": row.get("league", None),
    }


def import_csv(path=CSV_PATH):
    init_db()
    if not os.path.exists(path):
        print("No CSV found at", path)
        return 0

    df = pd.read_csv(path, dtype=str)
    df = df.fillna("")

    inserted = 0
    with SessionLocal() as session:
        for _, r in df.iterrows():
            m = parse_row_to_match(r)
            if m["date"] is None:
                # skip rows without valid date
                continue
            # check exists
            exists = session.query(Match).filter(
                Match.date == m["date"],
                Match.home_team == m["home_team"],
                Match.away_team == m["away_team"],
            ).first()
            if exists:
                continue
            obj = Match(
                date=m["date"],
                home_team=m["home_team"],
                away_team=m["away_team"],
                home_score=m["home_score"],
                away_score=m["away_score"],
                league=m["league"],
            )
            session.add(obj)
            inserted += 1
        session.commit()
    print(f"Imported {inserted} rows into DB")
    return inserted


if __name__ == "__main__":
    import_csv()
