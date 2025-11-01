"""
Small SSE (Server-Sent Events) endpoint that emits a message when artifacts (team_stats or model)
are updated on disk. This is a convenient way to get near-real-time notifications in a UI.

Usage:
  python app/streaming.py

Connect from browser / client:
  const es = new EventSource("http://127.0.0.1:5001/updates");
  es.onmessage = (e) => console.log("update:", e.data);
"""
from flask import Flask, Response
import time
import os
import json

ARTIFACTS = ["artifacts/team_stats.joblib", "artifacts/model.joblib"]

app = Flask(__name__)


def file_mtimes():
    return {p: os.path.getmtime(p) if os.path.exists(p) else 0 for p in ARTIFACTS}


@app.route("/updates")
def updates():
    def stream():
        last = file_mtimes()
        # initial ping
        yield f"data: {json.dumps({'status': 'connected'})}\n\n"
        while True:
            time.sleep(2)
            current = file_mtimes()
            changed = []
            for p in ARTIFACTS:
                if current[p] != last[p]:
                    changed.append(p)
            if changed:
                payload = {"changed": changed, "timestamp": time.time()}
                yield f"data: {json.dumps(payload)}\n\n"
                last = current
    return Response(stream(), mimetype="text/event-stream")


if __name__ == "__main__":
    app.run(port=5001, debug=False)
