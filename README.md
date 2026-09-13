# AegisShift — prototype

Context-aware UEBA for early insider threat detection.
Round 1 prototype (Manipal Hackathon 2026, Round 1 — this problem
statement will be reassigned within the same Domain/SDG for Round 2).

## What this actually is

A working, end-to-end slice of the AegisShift pipeline, running on
synthetic (not real) event data, computing real statistics — not a
mockup or a set of pre-baked screenshots.

**Pipeline implemented here:**

```
Synthetic event generator
        |
Redis Stream (real-time event bus)
        |
Feature extraction (per user, per day)
        |
Mahalanobis distance  --------> how far is today from this user's own normal?
        |
CUSUM (with decay)    --------> is the deviation PERSISTENT, not a one-off?
        |
Rule-based state layer  ------> NORMAL -> ELEVATED -> SUSPICIOUS -> CRITICAL
        |                       (stands in for a trained HMM - same
        |                        interface; swap in a real HMM once
        |                        there's enough sequence data to train on)
        |
Context fusion  --------------> checks an HR/IT change feed; a logged,
        |                       legitimate reason (promotion, transfer)
        |                       suppresses the alert. An unexplained
        |                       new device / privilege change AMPLIFIES it.
        |
Peer-group normalization -----> compares each user's drift against their
        |                       OWN peer group's drift the same day. A
        |                       group-wide legitimate shift (e.g. a team
        |                       tool rollout) gets suppressed; an
        |                       individual standing out from flat peers
        |                       gets confirmed/amplified. (rule-based
        |                       proxy for what a trained GCN would learn
        |                       from graph embeddings)
        |
Resource-graph collusion check -> flags pairs of already-elevated users
        |                        who BOTH repeatedly touch the same RARE,
        |                        narrowly-shared resource - a signal no
        |                        single-user profile can surface alone
        |
Final risk score (0-100) + explainable dashboard
```

**Two scores are computed and shown side by side in the dashboard:**
`own-baseline risk` (from the individual statistical layers alone) and
`peer-adjusted risk` (after the peer-group/graph layer runs). Showing
both, rather than overwriting one with the other, is deliberate — it's
what makes the "this would have been a false alarm without peer
context" story visible and explainable rather than a black box.

**Architecture note — Kafka -> Redis:** the original design used Kafka
as the event bus. For this prototype we use **Redis Streams** instead:
same role (ordered, replayable, real-time event log that a consumer
reads from), but a single binary with no cluster/ZooKeeper to stand up,
which matters a lot inside a 36-hour build. The consumer code
(`stream_consumer.py`) reads via `XRANGE`/`XADD` the same way it would
read from a Kafka topic — swapping back to Kafka later is a
drop-in replacement of the transport layer only, not a redesign.

## What's real vs. what's a stand-in

| Layer | Status |
|---|---|
| Event bus | Real (Redis Streams) |
| Feature extraction | Real |
| Mahalanobis distance | Real, computed per-user from actual (synthetic) history |
| CUSUM | Real, with decay to avoid false drift from noise alone |
| State layer | Rule-based thresholds standing in for a trained HMM (see below) |
| Context fusion (HR/IT feed) | Rule-based (hand-specified), standing in for a trained Bayesian layer |
| Peer-group normalization | Real, computed from actual per-group statistics (median/std of CUSUM); rule-based proxy for a trained GCN's peer-grounding |
| Resource-graph collusion check | Real, computed from an actual bipartite user-resource access graph, filtered to rare/narrowly-shared resources |
| Data | 100% synthetic, generated with 4 deliberate storylines (see below) |

We're upfront about this in the pitch: HR/IT-review data needed to
train the HMM and the Bayesian context weights properly doesn't
exist yet for a hackathon-scale project, and a full trained GCN needs
real historical org-graph data to learn embeddings from. The rule-based
versions here implement the *same interface and logic* the trained
versions would — swapping in a trained HMM/Bayesian/GCN later is a
parameter-fitting exercise, not an architecture change.

## Storylines in the synthetic data (`data_generator.py`)

- **insider_1, insider_2** — slow, persistent malicious drift (rising
  sensitive file access, later hours, USB use) with **no** matching
  HR record. Should be correctly flagged CRITICAL. They also share
  repeated access to one rare, narrowly-held resource
  (`confidential_project_x`, touched by only these two users in the
  whole org) — the graph layer flags this as a collusion signal.
- **promo_1** — a real, large access-level jump, but **with** a logged
  HR role-change record. Should be suppressed while the record is
  recent, then re-flagged once it's stale (>5 days old) if the
  elevated behavior persists — showing the system doesn't give a
  permanent pass, just a time-boxed benefit of the doubt.
- **finance peer group (5 users)** — an org-wide legitimate shift
  (new tool rollout) from day 20 onward: everyone in the group ramps
  up file access together. Own-baseline Mahalanobis/CUSUM sees this as
  individual drift for each of the 5 people; the peer-group layer
  correctly recognizes it as a group-wide shift and suppresses most of
  them, while still allowing a member whose drift genuinely exceeds
  even the elevated group median to stay flagged.
- **25 normal users total (5 per peer group)** — background noise
  only. All correctly stay at low risk scores throughout.

## Running it

```bash
pip install -r requirements.txt
redis-server --daemonize yes          # start the event bus
python3 data_generator.py             # publish synthetic events
python3 stream_consumer.py            # individual statistical layers
python3 graph_layer.py                # peer-group + collusion layer
python3 app.py                        # start the dashboard at :5050
```

Open `http://localhost:5050` for the watchlist, click any user for
their full explainable timeline (Mahalanobis / CUSUM / own-baseline
risk / peer-adjusted risk over time, with context and peer
annotations). A banner at the top of the watchlist and on any involved
user's page surfaces the collusion pair, if any is found.

## Roadmap beyond Round 1

1. Real HR/IT change feed integration (currently a hand-written JSON stub)
2. Train the HMM on accumulated real behavioral sequences once available
3. Train/calibrate the Bayesian context weights against analyst-confirmed
   true/false positive outcomes
4. Replace the rule-based peer-group/collusion layer with a trained GCN
   over the real organizational graph (reporting lines, shared projects,
   device/resource graphs) once there's enough historical graph data
5. Swap Redis Streams for Kafka if/when the org needs multi-consumer
   fan-out, longer retention, or cross-datacenter replication at scale
