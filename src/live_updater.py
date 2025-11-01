"""
Scheduler that periodically fetches recent matches, inserts them into the DB,
updates artifacts/team_stats.joblib, and optionally retrains the model.

This version uses the DB as canonical storage.
"""
import os
import time
import joblib
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

from src.live_fetcher import fetch_matches, insert_matches_db
from src.data_loader import load_and_featurize_from_db
from src.model import train_and_save, TEAM_STATS_PATH, MODEL_PATH

LIVE_POLL_MINUTES = int(os.environ.get("LIVE_POLL_MINUTES", "10"))
RETRAIN_THRESHOLD = int(os.environ.get("RETRAIN_THRESHOLD", "20"))
MAX_LOOKBACK_DAYS = int(os.environ.get("MAX_LOOKBACK_DAYS", "2"))

_last_row_count = None


def poll_and_update():
    global _last_row_count
    to_date = datetime.utcnow().date()
    from_date = to_date - timedelta(days=MAX_LOOKBACK_DAYS)

    try:
        df = fetch_matches(from_date.isoformat(), to_date.isoformat())
    except Exception as e:
        print("[live_updater] fetch failed:", e)
        return

    try:
        added = insert_matches_db(df)
        print(f"[live_updater] fetched {len(df)} matches, inserted {added} new rows into DB")
    except Exception as e:
        print("[live_updater] db insert failed:", e)
        return

    # load and featurize from DB: updates team_stats
    try:
        X, y, meta, team_stats = load_and_featurize_from_db(last_n=5)
        # save updated team_stats to artifact
        joblib.dump(team_stats, TEAM_STATS_PATH)
        print("[live_updater] updated team_stats artifact")
    except Exception as e:
        print("[live_updater] featurize failed:", e)
        return

    # decide whether to retrain
    try:
        total_rows = len(X)
        if _last_row_count is None:
            _last_row_count = total_rows

        new_rows = total_rows - _last_row_count
        if new_rows >= RETRAIN_THRESHOLD:
            print(f"[live_updater] {new_rows} new rows >= {RETRAIN_THRESHOLD}, retraining model...")
            train_and_save(X, y, team_stats, test_size=0.2)
            _last_row_count = total_rows
        else:
            print(f"[live_updater] {new_rows} new rows < {RETRAIN_THRESHOLD}, skipping retrain")
    except Exception as e:
        print("[live_updater] retrain check failed:", e)


def main():
    # initial poll to set baseline and create artifacts/db
    poll_and_update()

    scheduler = BackgroundScheduler()
    scheduler.add_job(poll_and_update, "interval", minutes=LIVE_POLL_MINUTES, next_run_time=datetime.utcnow())
    scheduler.start()
    print(f"[live_updater] started scheduler: polling every {LIVE_POLL_MINUTES} minutes")

    try:
        while True:
            time.sleep(5)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("[live_updater] stopped")


if __name__ == "__main__":
    main()
