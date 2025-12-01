#!/usr/bin/env python3
from flask import Flask, jsonify, request
import requests
import threading
import time
import os

app = Flask(__name__)

# ---------------- CONFIG ----------------
# CHANGE THIS to your game's Universe ID (not Place ID!)
# Find it: Go to game page > click "..." > Copy Universe ID
GAME_ID = "109983668079237"  # <-- CHANGE THIS

ROBLOX_API_URL = f"https://games.roblox.com/v1/games/{GAME_ID}/servers/Public?sortOrder=Asc&limit=100"

CACHE_LIMIT = 50000
CACHE_REFRESH_INTERVAL = 5
CACHE_LOW_THRESHOLD = 5000
BLACKLIST_DURATION = 180   # 5 minutes blacklist
ROBLOX_TIMEOUT = 10
CACHE_CLEAR_INTERVAL = 1800  # Clear every 30 min
# ---------------------------------------

server_cache = []
jobs_assigned = 0
blacklist = {}
lock = threading.Lock()

def cleanup_blacklist():
    now = time.time()
    to_remove = [job for job, exp in blacklist.items() if exp <= now]
    for job in to_remove:
        del blacklist[job]

def fetch_job_ids():
    global server_cache, blacklist
    while True:
        try:
            response = requests.get(ROBLOX_API_URL, timeout=ROBLOX_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            servers = data.get("data", [])

            new_jobs = []
            with lock:
                cleanup_blacklist()
                existing_ids = set(server_cache) | set(blacklist.keys())
                for s in servers:
                    job_id = s.get("id")
                    if job_id and job_id not in existing_ids:
                        new_jobs.append(job_id)
                server_cache.extend(new_jobs)
                if len(server_cache) > CACHE_LIMIT:
                    server_cache = server_cache[:CACHE_LIMIT]

            print(f"[CACHE] +{len(new_jobs)} | Total: {len(server_cache)} | Blacklist: {len(blacklist)}")

        except Exception as e:
            print(f"[ERROR] fetch_job_ids: {e}")

        for _ in range(CACHE_REFRESH_INTERVAL):
            time.sleep(1)
            with lock:
                if len(server_cache) < CACHE_LOW_THRESHOLD:
                    print("[LOW CACHE] Refreshing early...")
                    break

def clear_cache_periodically():
    global server_cache, blacklist
    while True:
        time.sleep(CACHE_CLEAR_INTERVAL)
        with lock:
            server_cache.clear()
            blacklist.clear()
            print("[CLEARED] Cache and blacklist reset")

# ------------------- ENDPOINTS -------------------

@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "Main API running", "cache": len(server_cache)})

@app.route("/status", methods=["GET"])
def status():
    with lock:
        cleanup_blacklist()
        return jsonify({
            "cache_jobs": len(server_cache),
            "jobs_assigned": jobs_assigned,
            "blacklist_jobs": len(blacklist),
        })

@app.route("/get-server", methods=["GET"])
def get_server():
    global jobs_assigned
    with lock:
        cleanup_blacklist()
        if not server_cache:
            return jsonify({"error": "No servers available"}), 404

        job_id = server_cache.pop(0)
        jobs_assigned += 1
        blacklist[job_id] = time.time() + BLACKLIST_DURATION

        print(f"[ASSIGNED] {job_id[:20]}... | Left: {len(server_cache)}")
        return jsonify({"job_id": job_id})

@app.route("/get-servers", methods=["GET"])
def get_servers():
    with lock:
        cleanup_blacklist()
        return jsonify({"job_ids": server_cache[:100]})  # Return first 100

@app.route("/add-pool", methods=["POST"])
def add_pool():
    global server_cache
    data = request.get_json()
    if not data or "servers" not in data:
        return jsonify({"error": "Missing 'servers' field"}), 400

    servers = data["servers"]
    if not isinstance(servers, list):
        return jsonify({"error": "'servers' must be a list"}), 400

    added = 0
    with lock:
        cleanup_blacklist()
        existing_ids = set(server_cache) | set(blacklist.keys())
        for job_id in servers:
            if job_id and job_id not in existing_ids:
                server_cache.append(job_id)
                added += 1
        if len(server_cache) > CACHE_LIMIT:
            server_cache = server_cache[:CACHE_LIMIT]

    print(f"[ADD POOL] +{added} | Total: {len(server_cache)}")
    return jsonify({"added": added})

# ------------------- MAIN -------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    threading.Thread(target=fetch_job_ids, daemon=True).start()
    threading.Thread(target=clear_cache_periodically, daemon=True).start()
    print(f"Main API running on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
