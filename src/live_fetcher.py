"""
Polling fetcher adapted to insert into the DB (SQLAlchemy) instead of appending CSV.

It keeps the fetch_matches() function (unchanged) and replaces append_matches with insert_matches_db().
"""
import os
import requests
import pandas as pd
from datetime import datetime, timedelta

from src.db import init_db, SessionLocal
from src.models import Match

API_URL = "https://api.football-data.org/v4/matches"
TOKEN = os.environ.get("FOOTBALL_DATA_TOKEN")  # set this in your environment


def fetch_matches(date_from: str, date_to: str):
    """
    date_from/date_to: ISO date strings YYYY-MM-DD
    Returns: DataFrame with columns date, home_team, away_team, home_score, away_score, league
    """
    headers = {}
    if TOKEN:
        headers["X-Auth-Token"] = TOKEN

    params = {"dateFrom": date_from, "dateTo": date_to}
    resp = requests.get(API_URL, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for m in data.get("matches", []):
        utc = m.get("utcDate")
        hteam = m.get("homeTeam", {}).get("name") or m.get("homeTeam", {}).get("shortName") or str(m.get("homeTeam", {}).get("id", ""))
        ateam = m.get("awayTeam", {}).get("name") or m.get("awayTeam", {}).get("shortName") or str(m.get("awayTeam", {}).get("id", ""))
        score = m.get("score", {}).get("fullTime", {})
        hs = score.get("home")
        aa = score.get("away")
        comp = m.get("competition", {}).get("name") or m.get("competition", {}).get("id") or "Unknown"

        # if score is None (match not finished), we set None
        hs_val = hs if hs is not None else None
        aa_val = aa if aa is not None else None

        # normalize date to YYYY-MM-DD
        try:
            date = datetime.fromisoformat(utc.replace("Z", "+00:00")).date().isoformat()
        except Exception:
            date = (datetime.utcnow().date()).isoformat()

        rows.append({
            "date": date,
            "home_team": hteam,
            "away_team": ateam,
            "home_score": hs_val,
            "away_score": aa_val,
            "league": comp
        })

    df = pd.DataFrame(rows)
    return df


def insert_matches_db(df_new):
    """
    Insert matches into the DB, avoiding duplicates using the unique constraint check.
    Returns the number of inserted rows.
    """
    if df_new.empty:
        return 0

    init_db()
    inserted = 0
    with SessionLocal() as session:
        for _, row in df_new.iterrows():
            try:
                date = pd.to_datetime(row["date"]).date()
            except Exception:
                continue
            home = str(row["home_team"])
            away = str(row["away_team"])
            hs = None if pd.isna(row.get("home_score")) else row.get("home_score")
            aa = None if pd.isna(row.get("away_score")) else row.get("away_score")
            league = row.get("league", None)

            exists = session.query(Match).filter(
                Match.date == date,
                Match.home_team == home,
                Match.away_team == away,
            ).first()
            if exists:
                # optionally update scores if previously unknown and now available
                updated = False
                if (exists.home_score is None or exists.away_score is None) and (hs is not None and aa is not None):
                    exists.home_score = hs
                    exists.away_score = aa
                    updated = True
                if updated:
                    session.add(exists)
                continue

            m = Match(
                date=date,
                home_team=home,
                away_team=away,
                home_score=hs,
                away_score=aa,
                league=league,
            )
            session.add(m)
            inserted += 1
        session.commit()
    return inserted


if __name__ == "__main__":
    # quick local test: fetch last 2 days and insert into DB
    to_date = datetime.utcnow().date()
    from_date = to_date - timedelta(days=2)
    print("Fetching matches from", from_date.isoformat(), "to", to_date.isoformat())
    df = fetch_matches(from_date.isoformat(), to_date.isoformat())
    print("Fetched", len(df), "matches")
    inserted = insert_matches_db(df)
    print("Inserted", inserted, "new rows into DB")
