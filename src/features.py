"""
Feature engineering: compute recent team statistics (last N matches).
Implements an online-style pass over the matches sorted by date; for each match
we compute features based on previous matches only.
"""
from collections import defaultdict, deque
import pandas as pd
import numpy as np


def compute_recent_stats(df, last_n=5):
    """
    Input:
      df: DataFrame with columns ['date','home_team','away_team','home_score','away_score','league']
    Returns:
      df_features: original df with added columns (home_avg_scored, home_avg_conceded, away_avg_scored, away_avg_conceded,
                    home_form, away_form). Form features are numeric where:
        - avg_scored / avg_conceded: mean over last_n matches (NaN replaced by global mean)
        - form: sum of outcomes where win=1, draw=0, loss=-1 over last_n matches
      team_stats: dict mapping team -> latest stats dict (used by API for predictions)
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # structures to keep last_n values per team
    goals_scored = defaultdict(lambda: deque(maxlen=last_n))
    goals_conceded = defaultdict(lambda: deque(maxlen=last_n))
    outcomes = defaultdict(lambda: deque(maxlen=last_n))  # 1 win, 0 draw, -1 loss

    # lists to collect features
    h_avg_scored = []
    h_avg_conceded = []
    h_form = []
    a_avg_scored = []
    a_avg_conceded = []
    a_form = []

    # global fallback means
    global_scored = []
    global_conceded = []

    for _, row in df.iterrows():
        h = row['home_team']
        a = row['away_team']

        # compute features from previous history
        def avg_or_nan(seq):
            return float(np.mean(seq)) if len(seq) > 0 else np.nan

        h_avg_scored.append(avg_or_nan(goals_scored[h]))
        h_avg_conceded.append(avg_or_nan(goals_conceded[h]))
        h_form.append(float(np.sum(outcomes[h])) if len(outcomes[h]) > 0 else np.nan)

        a_avg_scored.append(avg_or_nan(goals_scored[a]))
        a_avg_conceded.append(avg_or_nan(goals_conceded[a]))
        a_form.append(float(np.sum(outcomes[a])) if len(outcomes[a]) > 0 else np.nan)

        # after collecting, update histories with current match result
        hs = row['home_score']
        aa = row['away_score']
        # update globals
        global_scored.append(hs)
        global_scored.append(aa)
        global_conceded.append(aa)
        global_conceded.append(hs)

        goals_scored[h].append(hs)
        goals_conceded[h].append(aa)

        goals_scored[a].append(aa)
        goals_conceded[a].append(hs)

        if hs > aa:
            outcomes[h].append(1)
            outcomes[a].append(-1)
        elif hs < aa:
            outcomes[h].append(-1)
            outcomes[a].append(1)
        else:
            outcomes[h].append(0)
            outcomes[a].append(0)

    df['home_avg_scored'] = h_avg_scored
    df['home_avg_conceded'] = h_avg_conceded
    df['home_form'] = h_form
    df['away_avg_scored'] = a_avg_scored
    df['away_avg_conceded'] = a_avg_conceded
    df['away_form'] = a_form

    # replace NaNs with global means
    fallback_scored = float(np.mean(global_scored)) if len(global_scored) > 0 else 1.0
    fallback_conceded = float(np.mean(global_conceded)) if len(global_conceded) > 0 else 1.0
    fallback_form = 0.0

    df['home_avg_scored'] = df['home_avg_scored'].fillna(fallback_scored)
    df['home_avg_conceded'] = df['home_avg_conceded'].fillna(fallback_conceded)
    df['home_form'] = df['home_form'].fillna(fallback_form)
    df['away_avg_scored'] = df['away_avg_scored'].fillna(fallback_scored)
    df['away_avg_conceded'] = df['away_avg_conceded'].fillna(fallback_conceded)
    df['away_form'] = df['away_form'].fillna(fallback_form)

    # target: 0 home win, 1 draw, 2 away win
    def outcome_label(hs, aa):
        if hs > aa:
            return 0
        if hs == aa:
            return 1
        return 2

    df['target'] = df.apply(lambda r: outcome_label(r['home_score'], r['away_score']), axis=1)

    # create team_stats: latest statistics (after last row)
    team_stats = {}
    for team in set(df['home_team']).union(df['away_team']):
        team_stats[team] = {
            "avg_scored": float(avg_or_nan(goals_scored[team])) if len(goals_scored[team]) > 0 else fallback_scored,
            "avg_conceded": float(avg_or_nan(goals_conceded[team])) if len(goals_conceded[team]) > 0 else fallback_conceded,
            "form": float(np.sum(outcomes[team])) if len(outcomes[team]) > 0 else fallback_form
        }

    return df, team_stats
