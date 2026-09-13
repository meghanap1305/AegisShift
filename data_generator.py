"""
AegisShift - synthetic event generator
Creates a fake company's worth of user behavior events and publishes
them to a Redis Stream (standing in for the real-time event bus).

Storylines injected so the pipeline has something real to detect:
  - "insider_1", "insider_2": slow, persistent malicious drift
    (rising sensitive-file access + off-hours activity + USB use,
    with NO matching HR/IT change record -> should get flagged)
  - "promo_1": a legitimate access-level jump, but WITH a matching
    HR record (role change) -> should be suppressed by context layer
  - "finance" peer group: an ORG-WIDE legitimate shift (new tool
    rollout affecting the whole team at once) -> individual own-baseline
    drift would look identical to an insider, but PEER comparison
    should suppress it because everyone in the group moved together
  - insider_1 and insider_2 also share access to a "confidential_project"
    resource late in the timeline -> the graph/collusion layer should
    flag them as a linked pair, on top of their individual risk
  - everyone else: normal background noise, small day-to-day variation
"""

import json
import random
import time
import redis
import numpy as np
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

STREAM_KEY = "aegisshift:events"
HR_FEED_PATH = "hr_context.json"

PEER_GROUPS = ["engineering", "sales", "finance", "hr", "ops"]
NUM_NORMAL_USERS = 25   # 5 per peer group
DAYS = 30
EVENTS_PER_DAY = 6

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# Resource pool - most events touch generic resources; a couple of users
# share access to one flagged "confidential_project" resource late on,
# which is what the collusion/graph check looks for.
GENERIC_RESOURCES = [f"resource_{i}" for i in range(12)]
SENSITIVE_RESOURCE = "confidential_project_x"


def assign_peer_groups(users):
    groups = {}
    for i, u in enumerate(users):
        groups[u] = PEER_GROUPS[i % len(PEER_GROUPS)]
    return groups


def normal_day_events(user_id, day_index, peer_group, org_shift_group=None, shift_day=20):
    """Small, stable variation around a personal baseline.
    If this user's peer_group matches org_shift_group and we're past
    shift_day, everyone in that group legitimately ramps up together
    (e.g. a new tool rollout) - a real behavior change, but a GROUP one,
    not an individual anomaly."""
    events = []
    group_shifted = (peer_group == org_shift_group and day_index >= shift_day)
    base_files = 8 if not group_shifted else 15
    n_files = max(0, int(random.gauss(base_files, 2)))
    for _ in range(n_files):
        events.append({
            "user_id": user_id,
            "peer_group": peer_group,
            "event_type": "file_access",
            "resource": random.choice(GENERIC_RESOURCES),
            "sensitivity": random.choices([1, 2, 3], weights=[70, 25, 5] if not group_shifted else [50, 35, 15])[0],
            "hour": int(random.gauss(13, 3)) % 24,
            "usb": 0,
            "new_device": 0,
            "privilege_change": 0,
            "day": day_index,
        })
    events.append({
        "user_id": user_id,
        "peer_group": peer_group,
        "event_type": "login",
        "resource": "auth_system",
        "sensitivity": 1,
        "hour": int(random.gauss(9, 1)) % 24,
        "usb": 0,
        "new_device": 1 if random.random() < 0.01 else 0,
        "privilege_change": 0,
        "day": day_index,
    })
    return events


def insider_drift_events(user_id, day_index, peer_group):
    """Slow, persistent escalation with no legitimate explanation."""
    progress = max(0, day_index - DAYS * 0.4) / (DAYS * 0.6)
    events = []
    n_files = int(6 + progress * 18)
    for _ in range(n_files):
        touches_sensitive_resource = progress > 0.6 and random.random() < 0.3
        events.append({
            "user_id": user_id,
            "peer_group": peer_group,
            "event_type": "file_access",
            "resource": SENSITIVE_RESOURCE if touches_sensitive_resource else random.choice(GENERIC_RESOURCES),
            "sensitivity": random.choices([1, 2, 3], weights=[40, 30, 30 + int(progress * 40)])[0],
            "hour": int(random.gauss(13 + progress * 8, 3)) % 24,
            "usb": 1 if random.random() < progress * 0.4 else 0,
            "new_device": 1 if random.random() < progress * 0.05 else 0,
            "privilege_change": 0,
            "day": day_index,
        })
    return events


def legit_promotion_events(user_id, day_index, peer_group):
    """A real jump in access level, but with a matching HR record."""
    promoted = day_index >= 15
    events = []
    n_files = int(8 + (10 if promoted else 0))
    for _ in range(n_files):
        events.append({
            "user_id": user_id,
            "peer_group": peer_group,
            "event_type": "file_access",
            "resource": random.choice(GENERIC_RESOURCES),
            "sensitivity": random.choices([1, 2, 3], weights=[50, 30, 20] if promoted else [70, 25, 5])[0],
            "hour": int(random.gauss(10, 2)) % 24,
            "usb": 0,
            "new_device": 1 if day_index == 15 else 0,
            "privilege_change": 1 if day_index == 15 else 0,
            "day": day_index,
        })
    return events


def main():
    r.delete(STREAM_KEY)
    users = [f"user_{i:03d}" for i in range(NUM_NORMAL_USERS)]
    peer_of = assign_peer_groups(users)
    # storyline users get explicit peer group assignments too
    peer_of.update({"insider_1": "engineering", "insider_2": "sales", "promo_1": "hr"})
    org_shift_group = "finance"  # this whole team legitimately shifts together

    special = {
        "insider_1": lambda u, d: insider_drift_events(u, d, peer_of[u]),
        "insider_2": lambda u, d: insider_drift_events(u, d, peer_of[u]),
        "promo_1": lambda u, d: legit_promotion_events(u, d, peer_of[u]),
    }

    hr_feed = [
        {"user_id": "promo_1", "day": 15, "type": "role_change",
         "note": "Promoted to Senior Analyst - access scope expanded", "scope": "individual"},
        {"user_id": None, "peer_group": "finance", "day": 20, "type": "org_change",
         "note": "New reporting tool rollout - finance team", "scope": "group"},
    ]
    with open(HR_FEED_PATH, "w") as f:
        json.dump(hr_feed, f, indent=2)

    with open("peer_groups.json", "w") as f:
        json.dump(peer_of, f, indent=2)

    base_time = datetime.now() - timedelta(days=DAYS)
    total = 0
    for day in range(DAYS):
        for u in users:
            for ev in normal_day_events(u, day, peer_of[u], org_shift_group=org_shift_group):
                ts = (base_time + timedelta(days=day, hours=ev["hour"])).isoformat()
                ev["timestamp"] = ts
                r.xadd(STREAM_KEY, {"data": json.dumps(ev)})
                total += 1
        for name, fn in special.items():
            for ev in fn(name, day):
                ts = (base_time + timedelta(days=day, hours=ev["hour"])).isoformat()
                ev["timestamp"] = ts
                r.xadd(STREAM_KEY, {"data": json.dumps(ev)})
                total += 1

    print(f"Published {total} synthetic events to Redis stream '{STREAM_KEY}'")
    print(f"Users: {len(users)} normal (5 per peer group: {PEER_GROUPS}), plus insider_1, insider_2, promo_1")
    print(f"Org-wide legitimate shift injected in peer group: {org_shift_group} (day {20}+)")
    print(f"HR/context feed written to {HR_FEED_PATH}")
    print(f"Peer group map written to peer_groups.json")


if __name__ == "__main__":
    main()

