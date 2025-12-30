import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from flask import Flask, jsonify, render_template, request

from app.db import init_db, insert_webhook, list_webhooks, get_webhook, delete_all


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

def create_app() -> Flask:
    app = Flask(__name__)
    init_db()

    @app.get("/health")
    def health():
        return jsonify(status="ok")

    @app.post("/webhook")
    def webhook():
        received_at = utc_now_iso()
        source_ip = request.headers.get("X-Forwarded-For", request.remote_addr)

        headers: Dict[str, str] = {k: v for k, v in request.headers.items()}
        headers_json = safe_json_dumps(headers)

        raw_body = request.get_data(as_text=True) or ""
        body_json: Optional[str] = None

        try:
            parsed = request.get_json(silent=True)
            if parsed is not None:
                body_json = safe_json_dumps(parsed)
        except Exception:
            body_json = None

        new_id = insert_webhook(
            received_at=received_at,
            source_ip=source_ip,
            headers_json=headers_json,
            body_json=body_json,
            raw_body=raw_body,
        )

        return jsonify(ok=True, id=new_id, received_at=received_at)

    @app.get("/api/webhooks")
    def api_webhooks():
        limit = int(request.args.get("limit", "50"))
        limit = max(1, min(limit, 200))
        return jsonify(items=list_webhooks(limit=limit))

    @app.get("/api/webhooks/<int:webhook_id>")
    def api_webhook_one(webhook_id: int):
        item = get_webhook(webhook_id)
        if not item:
            return jsonify(error="not_found"), 404
        return jsonify(item=item)

    @app.delete("/api/webhooks")
    def api_delete_all():
        if request.headers.get("X-Confirm") != "YES":
            return jsonify(error="missing_confirmation_header"), 400
        deleted = delete_all()
        return jsonify(ok=True, deleted=deleted)

    @app.get("/")
    def home():
        return render_template("index.html")

    return app

app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)