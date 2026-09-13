"""AegisShift - explainable investigation dashboard."""
import sqlite3
from flask import Flask, render_template, jsonify

DB_PATH = "aegisshift.db"
app = Flask(__name__)


def query(sql, params=()):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    conn.close()
    return rows


@app.route("/")
def index():
    latest = query("""
        SELECT r.user_id, r.day, r.state, r.final_risk, r.suppressed, r.context_note, r.peer_group
        FROM risk_scores r
        INNER JOIN (
            SELECT user_id, MAX(day) as max_day FROM risk_scores GROUP BY user_id
        ) m ON r.user_id = m.user_id AND r.day = m.max_day
        ORDER BY r.final_risk DESC
    """)
    peer_latest = {r["user_id"]: r for r in query("""
        SELECT p.user_id, p.peer_adjusted_risk, p.peer_explanation, p.peer_z
        FROM peer_graph_scores p
        INNER JOIN (
            SELECT user_id, MAX(day) as max_day FROM peer_graph_scores GROUP BY user_id
        ) m ON p.user_id = m.user_id AND p.day = m.max_day
    """)}
    for row in latest:
        pg = peer_latest.get(row["user_id"], {})
        row["peer_adjusted_risk"] = pg.get("peer_adjusted_risk", row["final_risk"])
        row["peer_explanation"] = pg.get("peer_explanation", "")

    collusion = query("SELECT * FROM collusion_pairs")
    latest.sort(key=lambda r: r["peer_adjusted_risk"], reverse=True)
    return render_template("index.html", users=latest, collusion=collusion)


@app.route("/user/<user_id>")
def user_detail(user_id):
    history = query(
        "SELECT * FROM risk_scores WHERE user_id=? ORDER BY day", (user_id,)
    )
    peer_history = query(
        "SELECT * FROM peer_graph_scores WHERE user_id=? ORDER BY day", (user_id,)
    )
    peer_by_day = {p["day"]: p for p in peer_history}
    for h in history:
        p = peer_by_day.get(h["day"], {})
        h["group_median_cusum"] = p.get("group_median_cusum")
        h["peer_z"] = p.get("peer_z")
        h["peer_adjusted_risk"] = p.get("peer_adjusted_risk", h["final_risk"])
        h["peer_explanation"] = p.get("peer_explanation", "")
    collusion = query(
        "SELECT * FROM collusion_pairs WHERE user_a=? OR user_b=?", (user_id, user_id)
    )
    return render_template("user.html", user_id=user_id, history=history, collusion=collusion)


@app.route("/api/history/<user_id>")
def api_history(user_id):
    history = query(
        "SELECT r.day, r.mahalanobis, r.cusum, r.final_risk, p.peer_adjusted_risk, "
        "p.group_median_cusum, p.peer_z, r.peer_group "
        "FROM risk_scores r LEFT JOIN peer_graph_scores p "
        "ON r.user_id=p.user_id AND r.day=p.day WHERE r.user_id=? ORDER BY r.day",
        (user_id,),
    )
    return jsonify(history)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=False)
