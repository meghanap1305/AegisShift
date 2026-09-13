"""
AegisShift - stream consumer / detection pipeline

Reads events from Redis Stream (real-time event bus, replaces Kafka
for this prototype), builds a per-user behavioral profile, and runs:

  1. Mahalanobis distance   -> how far is today's behavior from this
                               user's own historical normal?
  2. CUSUM                  -> is that deviation persistent (a real
                               drift) or a one-off blip?
  3. Rule-based state layer -> NORMAL / ELEVATED / SUSPICIOUS / CRITICAL
                               (stands in for the trained HMM - same
                               interface, swap in a real HMM later
                               once there's enough sequence data)
  4. Context fusion         -> checks the HR/IT feed; if a logged,
                               legitimate reason explains the drift,
                               the risk score is suppressed

Output: a SQLite table of per-user, per-day risk snapshots that the
dashboard (app.py) reads and displays.
"""

import json
import sqlite3
import numpy as np
import redis
from collections import defaultdict

STREAM_KEY = "aegisshift:events"
DB_PATH = "aegisshift.db"
HR_FEED_PATH = "hr_context.json"

# Continuous behavioral features go into Mahalanobis/CUSUM - these are
# the ones with meaningful day-to-day variance to measure drift against.
# new_device / privilege_change are deliberately NOT in here: they're rare
# binary flags with near-zero baseline variance, so a single occurrence
# blows up the distance score disproportionately. Correctly, these belong
# in the context fusion layer (Bayesian layer) as explanatory signals, not
# as inputs to the raw anomaly score - exactly the role they were designed
# for in the architecture.
FEATURES = ["n_files", "avg_sensitivity", "avg_hour", "usb_rate"]

# CUSUM tuning: slack is expressed in units of the baseline's own
# standard deviation of Mahalanobis distance, not an absolute number -
# this matters because "typical" distance scales with feature count.
# DECAY keeps this a "persistent drift" detector rather than a pure
# cumulative sum: a plain max(0, cusum + x) with no decay is a reflected
# random walk, which drifts upward over many days from noise ALONE, even
# with zero true signal. Decaying it means only SUSTAINED deviation above
# baseline accumulates - a few noisy days relax back down on their own.
CUSUM_SLACK_STD = 1.75
CUSUM_DECAY = 0.85

STATE_THRESHOLDS = {
    "NORMAL": 0,
    "ELEVATED": 4,
    "SUSPICIOUS": 10,
    "CRITICAL": 20,
}


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DROP TABLE IF EXISTS risk_scores")
    conn.execute("""
        CREATE TABLE risk_scores (
            user_id TEXT, peer_group TEXT, day INTEGER, mahalanobis REAL, cusum REAL,
            state TEXT, context_note TEXT, suppressed INTEGER,
            final_risk REAL
        )
    """)
    conn.execute("DROP TABLE IF EXISTS events_raw")
    conn.execute("""
        CREATE TABLE events_raw (
            user_id TEXT, peer_group TEXT, day INTEGER, resource TEXT, sensitivity INTEGER
        )
    """)
    conn.commit()
    return conn


def load_hr_feed():
    try:
        with open(HR_FEED_PATH) as f:
            records = json.load(f)
    except FileNotFoundError:
        records = []
    individual = defaultdict(list)   # user_id -> [(day, note)]
    group = defaultdict(list)        # peer_group -> [(day, note)]
    for rec in records:
        if rec.get("scope") == "group":
            group[rec["peer_group"]].append((rec["day"], rec["note"]))
        else:
            individual[rec["user_id"]].append((rec["day"], rec["note"]))
    return individual, group


def load_peer_groups():
    try:
        with open("peer_groups.json") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def daily_features(events):
    """Aggregate a list of raw events into one continuous feature vector
    for a day, for the Mahalanobis/CUSUM statistical layers."""
    if not events:
        return None
    n_files = sum(1 for e in events if e["event_type"] == "file_access")
    sens = [e["sensitivity"] for e in events if e["event_type"] == "file_access"]
    return np.array([
        n_files,
        np.mean(sens) if sens else 0,
        np.mean([e["hour"] for e in events]),
        np.mean([e["usb"] for e in events]),
    ], dtype=float)


def daily_context_flags(events):
    """Rare binary signals, handled separately by the context fusion
    layer rather than folded into the statistical anomaly score."""
    if not events:
        return {"new_device": 0, "privilege_change": 0}
    return {
        "new_device": max(e["new_device"] for e in events),
        "privilege_change": max(e["privilege_change"] for e in events),
    }


def mahalanobis(x, mean, cov_inv):
    diff = x - mean
    return float(np.sqrt(max(diff @ cov_inv @ diff.T, 0)))


def state_for_cusum(cusum_val):
    state = "NORMAL"
    for name, thresh in STATE_THRESHOLDS.items():
        if cusum_val >= thresh:
            state = name
    return state


def context_explains(user_id, day, hr_feed_individual, peer_group, hr_feed_group, window=5):
    """Bayesian-style context fusion (rule-based approximation for the
    prototype): checks whether a logged HR/IT record - either individual
    OR a group-wide change affecting this user's whole peer group -
    explains a spike. A bare new_device/privilege_change flag with NO
    matching record is deliberately NOT treated as an explanation."""
    for rec_day, note in hr_feed_individual.get(user_id, []):
        if 0 <= day - rec_day <= window:
            return note
    for rec_day, note in hr_feed_group.get(peer_group, []):
        if 0 <= day - rec_day <= window:
            return note
    return None


def run():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    entries = r.xrange(STREAM_KEY, min="-", max="+")
    print(f"Read {len(entries)} events from stream")

    by_user_day = defaultdict(list)
    for _id, fields in entries:
        ev = json.loads(fields["data"])
        by_user_day[(ev["user_id"], ev["day"])].append(ev)

    users = sorted(set(u for u, _ in by_user_day.keys()))
    hr_feed_individual, hr_feed_group = load_hr_feed()
    peer_groups = load_peer_groups()
    conn = init_db()

    # also persist raw per-event resource/sensitivity rows for the graph layer
    raw_rows = []
    for (user_id, day), evs in by_user_day.items():
        pg = evs[0].get("peer_group", peer_groups.get(user_id, "unknown"))
        for e in evs:
            if e["event_type"] == "file_access":
                raw_rows.append((user_id, pg, day, e.get("resource", "unknown"), e["sensitivity"]))
    conn.executemany("INSERT INTO events_raw VALUES (?,?,?,?,?)", raw_rows)
    conn.commit()

    for user_id in users:
        peer_group = peer_groups.get(user_id, "unknown")
        days = sorted(d for u, d in by_user_day.keys() if u == user_id)
        vectors_flags = [(daily_features(by_user_day[(user_id, d)]),
                          daily_context_flags(by_user_day[(user_id, d)])) for d in days]
        days = [d for d, (v, _) in zip(days, vectors_flags) if v is not None]
        flags_list = [f for v, f in vectors_flags if v is not None]
        vectors = [v for v, _ in vectors_flags if v is not None]
        if len(vectors) < 8:
            continue

        # Baseline = first 40% of history (rolling reconciliation window)
        split = max(5, int(len(vectors) * 0.4))
        baseline = np.array(vectors[:split])
        mean = baseline.mean(axis=0)
        cov = np.cov(baseline.T) + np.eye(len(FEATURES)) * 1e-3  # regularize
        cov_inv = np.linalg.pinv(cov)

        # Calibrate CUSUM against the baseline's OWN distance distribution -
        # a Mahalanobis distance of e.g. 2.4 is "normal" by construction
        # (expected distance grows with feature count), so CUSUM must
        # accumulate deviation ABOVE that expected level, not above zero.
        baseline_dists = [mahalanobis(v, mean, cov_inv) for v in baseline]
        base_mean_dist = float(np.mean(baseline_dists))
        base_std_dist = float(np.std(baseline_dists)) + 1e-6
        slack = base_mean_dist + CUSUM_SLACK_STD * base_std_dist

        cusum = 0.0
        rows = []
        for day, vec, flags in zip(days, vectors, flags_list):
            dist = mahalanobis(vec, mean, cov_inv)
            # CUSUM accumulates deviation above the baseline's expected
            # distance + slack, with decay so only PERSISTENT deviation
            # (not one noisy day, and not slow noise accumulation over
            # dozens of days) builds up.
            cusum = max(0.0, cusum * CUSUM_DECAY + (dist - slack))
            state = state_for_cusum(cusum)

            note = context_explains(user_id, day, hr_feed_individual, peer_group, hr_feed_group)
            base_risk = min(100.0, cusum * 8)
            suppressed = 0
            if note and state != "NORMAL":
                base_risk *= 0.15  # context fusion: strongly discount explained drift
                suppressed = 1
            elif state != "NORMAL" and (flags["new_device"] or flags["privilege_change"]) and not note:
                # access/device change with NO paperwork trail - this is a
                # risk amplifier, not a mitigator (fair-play equivalent of
                # "new laptop, no ticket, sudden sensitive access")
                base_risk = min(100.0, base_risk * 1.25)
                note = "unexplained device/privilege change"

            rows.append((user_id, peer_group, day, dist, cusum, state, note or "", suppressed, base_risk))

        conn.executemany(
            "INSERT INTO risk_scores VALUES (?,?,?,?,?,?,?,?,?)", rows
        )
        conn.commit()

    conn.close()
    print(f"Wrote risk scores for {len(users)} users to {DB_PATH}")


if __name__ == "__main__":
    run()
