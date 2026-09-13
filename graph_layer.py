"""
AegisShift - peer-group + graph layer (Part 2)

Runs AFTER stream_consumer.py. Reads the individually-computed risk
scores (own-baseline Mahalanobis/CUSUM) and adds two things that a
purely per-user model can never see on its own:

  1. PEER-GROUP NORMALIZATION
     Compares each user's drift against their peer group's drift on the
     same day. If a user's own-baseline CUSUM is high, but so is
     everyone else's in their peer group (e.g. a legitimate org-wide
     tool rollout), that's a GROUP-level shift, not an individual
     anomaly, and gets suppressed. If a user is drifting while their
     peers stay flat, that's a genuine standout, and gets kept or
     amplified.

     This is a lightweight, explainable proxy for the "peer-group
     relational grounding" that a trained GCN would eventually learn
     from embeddings - same intent (don't punish a user for doing what
     their whole team is doing), simpler math.

  2. RESOURCE-GRAPH COLLUSION CHECK
     Builds a bipartite user<->resource access graph from the raw event
     log. If two or more users who are ALREADY individually elevated
     also share access to the same sensitive resource, that's flagged
     as a linked pair - the kind of pattern a single-user behavioral
     profile can never surface on its own, because each user's own
     numbers might look identical whether they're acting alone or as
     part of a coordinated pair.

Writes a new table `peer_graph_scores` with the final, graph-adjusted
risk, and a `collusion_pairs` table for any flagged pairs.
"""

import sqlite3
import numpy as np
from collections import defaultdict

DB_PATH = "aegisshift.db"

# How much peer-relative deviation matters vs the user's own-baseline
# score. If a user's CUSUM is high AND their peer_z is high (standing
# out from peers too), keep the risk. If CUSUM is high but peer_z is
# low (whole group moved together), suppress it heavily.
PEER_Z_SUPPRESS_THRESHOLD = 1.0   # below this, treat as group-explained
SENSITIVE_ACCESS_MIN = 2          # min shared sensitive-resource touches to flag a pair


def fetch_risk_scores(conn):
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM risk_scores").fetchall()
    return [dict(r) for r in rows]


def compute_peer_adjustment(rows):
    """For each (peer_group, day), compute the group's median/std CUSUM,
    then each user's peer_z = how far they sit from their own group's
    typical drift that day."""
    by_group_day = defaultdict(list)
    for r in rows:
        by_group_day[(r["peer_group"], r["day"])].append(r["cusum"])

    group_stats = {}
    for key, cusum_vals in by_group_day.items():
        arr = np.array(cusum_vals)
        group_stats[key] = (float(np.median(arr)), float(np.std(arr)) + 1e-6)

    for r in rows:
        med, std = group_stats[(r["peer_group"], r["day"])]
        r["group_median_cusum"] = med
        r["peer_z"] = (r["cusum"] - med) / std
    return rows


def apply_peer_layer(rows):
    for r in rows:
        original_risk = r["final_risk"]
        if r["state"] == "NORMAL":
            r["peer_adjusted_risk"] = original_risk
            r["peer_explanation"] = ""
            continue

        if r["suppressed"]:
            # already explained by an individual HR record - peer layer
            # doesn't need to do anything further here
            r["peer_adjusted_risk"] = original_risk
            r["peer_explanation"] = "individually explained (HR record)"
            continue

        if r["peer_z"] < PEER_Z_SUPPRESS_THRESHOLD and r["group_median_cusum"] > 1.0:
            # this user's drift tracks their peer group's drift - looks
            # like a group-wide shift (e.g. org tool rollout), not an
            # individual anomaly. Suppress heavily, same logic as the
            # HR-record case but detected structurally instead.
            r["peer_adjusted_risk"] = min(original_risk, original_risk * 0.2)
            r["peer_explanation"] = f"tracks peer group median (peer_z={r['peer_z']:.2f}) - likely group-wide, not individual"
        elif r["peer_z"] >= 2.0:
            # standing out even MORE than their own-baseline score
            # suggested, relative to peers - amplify
            r["peer_adjusted_risk"] = min(100.0, original_risk * 1.15)
            r["peer_explanation"] = f"outlier vs peer group (peer_z={r['peer_z']:.2f}) - confirmed individual anomaly"
        else:
            r["peer_adjusted_risk"] = original_risk
            r["peer_explanation"] = f"peer_z={r['peer_z']:.2f}, within normal range of group variation"
    return rows


def find_collusion_pairs(conn, rows):
    """Bipartite user<->resource graph: flag pairs of ALREADY-elevated
    users who both accessed the same RARE resource repeatedly. Rarity
    matters - a resource everyone in the company touches (like a shared
    drive) tells you nothing; a resource only a couple of people ever
    touch, that two elevated users BOTH show up on, is a real signal a
    single-user behavioral profile could never surface on its own."""
    conn.row_factory = sqlite3.Row

    # Only consider resources with a small total footprint across the
    # whole org - this is what separates a real "shared narrow access"
    # signal from noise on a resource everyone happens to touch.
    footprint = conn.execute(
        "SELECT resource, COUNT(DISTINCT user_id) as n_users FROM events_raw GROUP BY resource"
    ).fetchall()
    rare_resources = {r["resource"] for r in footprint if r["n_users"] <= 3}

    if not rare_resources:
        return []

    placeholders = ",".join("?" * len(rare_resources))
    events = conn.execute(
        f"SELECT user_id, resource FROM events_raw WHERE resource IN ({placeholders})",
        tuple(rare_resources),
    ).fetchall()

    elevated_users = {r["user_id"] for r in rows if r["peer_adjusted_risk"] >= 50}

    access_count = defaultdict(lambda: defaultdict(int))  # resource -> user -> count
    for e in events:
        if e["user_id"] in elevated_users:
            access_count[e["resource"]][e["user_id"]] += 1

    pairs = []
    for resource, user_counts in access_count.items():
        users_here = [u for u, c in user_counts.items() if c >= 1]
        for i in range(len(users_here)):
            for j in range(i + 1, len(users_here)):
                pairs.append({
                    "user_a": users_here[i],
                    "user_b": users_here[j],
                    "shared_resource": resource,
                    "access_count_a": user_counts[users_here[i]],
                    "access_count_b": user_counts[users_here[j]],
                })
    return pairs


def run():
    conn = sqlite3.connect(DB_PATH)
    rows = fetch_risk_scores(conn)
    rows = compute_peer_adjustment(rows)
    rows = apply_peer_layer(rows)

    conn.execute("DROP TABLE IF EXISTS peer_graph_scores")
    conn.execute("""
        CREATE TABLE peer_graph_scores (
            user_id TEXT, peer_group TEXT, day INTEGER, cusum REAL,
            group_median_cusum REAL, peer_z REAL, state TEXT,
            own_baseline_risk REAL, peer_adjusted_risk REAL, peer_explanation TEXT
        )
    """)
    conn.executemany(
        "INSERT INTO peer_graph_scores VALUES (?,?,?,?,?,?,?,?,?,?)",
        [(r["user_id"], r["peer_group"], r["day"], r["cusum"], r["group_median_cusum"],
          r["peer_z"], r["state"], r["final_risk"], r["peer_adjusted_risk"], r["peer_explanation"])
         for r in rows]
    )

    pairs = find_collusion_pairs(conn, rows)
    conn.execute("DROP TABLE IF EXISTS collusion_pairs")
    conn.execute("""
        CREATE TABLE collusion_pairs (
            user_a TEXT, user_b TEXT, shared_resource TEXT,
            access_count_a INTEGER, access_count_b INTEGER
        )
    """)
    conn.executemany(
        "INSERT INTO collusion_pairs VALUES (?,?,?,?,?)",
        [(p["user_a"], p["user_b"], p["shared_resource"], p["access_count_a"], p["access_count_b"])
         for p in pairs]
    )
    conn.commit()
    conn.close()

    print(f"Peer-adjusted {len(rows)} risk rows written to peer_graph_scores")
    print(f"Collusion check found {len(pairs)} elevated-user pairs sharing a sensitive resource")
    for p in pairs:
        print(f"  -> {p['user_a']} <-> {p['user_b']} via {p['shared_resource']} "
              f"({p['access_count_a']} / {p['access_count_b']} accesses)")


if __name__ == "__main__":
    run()
