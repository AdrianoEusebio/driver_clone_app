import os, threading
from flask import Flask, render_template, request, redirect, url_for, jsonify, session
from werkzeug.middleware.proxy_fix import ProxyFix
from dotenv import load_dotenv
from typing import Dict
from storage import ensure_dir, read_json, atomic_write_json, new_job_id, utc_iso
from drive_api import build_flow_for_web, save_token, get_creds
from sync_worker import run_job

load_dotenv()

DATA_DIR   = os.getenv("DATA_DIR", "/data")
STATE_PATH = os.getenv("STATE_PATH", os.path.join(DATA_DIR, "state.json"))
JOBS_DIR   = os.getenv("JOBS_DIR", os.path.join(DATA_DIR, "jobs"))
LOGS_DIR   = os.getenv("LOGS_DIR", os.path.join(DATA_DIR, "logs"))
CLIENT_SECRETS = os.getenv("GOOGLE_CLIENT_SECRETS", os.path.join(DATA_DIR, "credentials.json"))
PORT = int(os.getenv("PORT", "8000"))

ensure_dir(DATA_DIR); ensure_dir(JOBS_DIR); ensure_dir(LOGS_DIR)

app = Flask(__name__, template_folder="templates", static_folder="static")
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)
app.secret_key = os.getenv("SECRET_KEY","dev-secret")

# --- OAuth ---

@app.route("/auth")
def auth():
    redirect_uri = url_for("oauth2callback", _external=True)  # ex.: http://localhost:8000/oauth2callback
    flow = build_flow_for_web(CLIENT_SECRETS, redirect_uri)
    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent"  # força refresh_token
    )
    session["oauth_state"] = state
    # guardamos flow na sessão? não é serializável; melhor guardar state apenas.
    # refazemos flow na callback com o mesmo redirect_uri.
    session["redirect_uri"] = redirect_uri
    return redirect(authorization_url)

@app.route("/oauth2callback")
def oauth2callback():
    state = session.get("oauth_state")
    redirect_uri = session.get("redirect_uri")
    if not redirect_uri:
        return "Missing redirect_uri in session", 400
    flow = build_flow_for_web(CLIENT_SECRETS, redirect_uri)
    flow.fetch_token(authorization_response=request.url)
    creds = flow.credentials
    save_token(os.path.join(DATA_DIR, "token.json"), creds)
    session["authed"] = True
    return redirect(url_for("index"))

def is_authed() -> bool:
    try:
        get_creds(DATA_DIR)
        return True
    except Exception:
        return False

# --- Páginas ---

@app.get("/")
def index():
    authed = is_authed()
    return render_template("index.html", authed=authed)

@app.post("/start")
def start():
    if not is_authed():
        return redirect(url_for("auth"))

    src_id = request.form.get("src_id","").strip()
    dest_id = request.form.get("dest_id","").strip()
    repeat_min = int(request.form.get("repeat_interval_min") or "0")

    if not src_id or not dest_id:
        return "SRC e DEST são obrigatórios.", 400

    job_id = new_job_id()
    job_path = os.path.join(JOBS_DIR, f"{job_id}.json")
    job = {
        "job_id": job_id,
        "status": "queued",
        "message": "Job criado",
        "src_id": src_id,
        "dest_id": dest_id,
        "repeat_interval_min": repeat_min,
        "started_at": utc_iso(),
        "updated_at": utc_iso(),
        "cancel_requested": False,
        "progress": {
            "items_done": 0, "items_total": 0,
            "bytes_done": 0, "bytes_total": 0,
            "percent_items": 0.0, "percent_bytes": 0.0
        }
    }
    atomic_write_json(job_path, job)

    # lança Worker em thread que cria um Process interno
    t = threading.Thread(target=run_job, args=(DATA_DIR, job_path, STATE_PATH), daemon=True)
    t.start()

    return redirect(url_for("status_page", job_id=job_id))

@app.get("/status/<job_id>")
def status_page(job_id: str):
    return render_template("status.html", job_id=job_id)

@app.get("/api/status/<job_id>")
def api_status(job_id: str):
    job_path = os.path.join(JOBS_DIR, f"{job_id}.json")
    job = read_json(job_path, {})
    if not job:
        return jsonify({"error":"job not found"}), 404
    return jsonify(job)

@app.post("/cancel/<job_id>")
def cancel(job_id: str):
    job_path = os.path.join(JOBS_DIR, f"{job_id}.json")
    job = read_json(job_path, {})
    if not job:
        return "job not found", 404
    job["cancel_requested"] = True
    atomic_write_json(job_path, job)
    return redirect(url_for("status_page", job_id=job_id))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
