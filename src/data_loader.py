"""
Loads the dataset from the DB and applies feature engineering.

If you still have a CSV you can import it using src/import_csv_to_db.py
"""
import pandas as pd
from src.features import compute_recent_stats
from src.db import init_db, SessionLocal
from src.models import Match


def load_matches_from_db(limit=None):
    """
    Return a pandas DataFrame of matches ordered by date asc.

    limit: optional number of rows to return (most recent when limit provided)
    """
    init_db()
    with SessionLocal() as session:
        q = session.query(Match).order_by(Match.date.asc())
        if limit:
            q = q.limit(limit)
        rows = q.all()
        # build DataFrame
        data = []
        for m in rows:
            data.append({
                "date": m.date.isoformat() if m.date is not None else None,
                "home_team": m.home_team,
                "away_team": m.away_team,
                "home_score": m.home_score if m.home_score is not None else -1,
                "away_score": m.away_score if m.away_score is not None else -1,
                "league": m.league or "Unknown",
            })
        df = pd.DataFrame(data)
    return df


def load_and_featurize_from_db(last_n=5):
    df = load_matches_from_db()
    if df.empty:
        # return empty placeholders
        X = pd.DataFrame(columns=[
            "home_avg_scored", "home_avg_conceded", "home_form",
            "away_avg_scored", "away_avg_conceded", "away_form",
        ])
        return X, pd.Series(dtype=int), pd.DataFrame(), {}
    df_features, team_stats = compute_recent_stats(df, last_n=last_n)

    # features used for modeling
    feature_columns = [
        "home_avg_scored", "home_avg_conceded", "home_form",
        "away_avg_scored", "away_avg_conceded", "away_form",
    ]

    X = df_features[feature_columns].copy()
    y = df_features["target"].copy()
    df_meta = df_features[["date", "home_team", "away_team", "league"]].copy()

    return X, y, df_meta, team_stats


# Backwards-compatible name for train.py
def load_and_featurize(path="data/matches.csv", last_n=5):
    """
    If a path is passed we try to import CSV into DB then read from DB.
    Otherwise just load from DB.
    """
    if path and isinstance(path, str) and path.endswith(".csv") and path != "":
        # try to import CSV into DB so DB stays canonical
        try:
            from src.import_csv_to_db import import_csv
            import_csv(path)
        except Exception:
            pass
    return load_and_featurize_from_db(last_n=last_n)
