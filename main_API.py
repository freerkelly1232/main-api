#!/usr/bin/env python3
"""
COVERAGE MAIN API v3.0 - STRICT NO OVERLAP
Every bot gets a unique server. No sharing. Maximum spread.
"""

from flask import Flask, jsonify, request
import requests
import threading
import time
import os
from collections import deque
import random

app = Flask(__name__)

# ================== CONFIG ==================
GAME_ID = "109983668079237"
PLACE_IDS = ["109983668079237"]

ROBLOX_API_BASE = "https://games.roblox.com/v1/games/{}/servers/Public?sortOrder=Asc&limit=100"

# Cache settings
CACHE_LIMIT = 100000             # Big cache for unique distribution
CACHE_REFRESH_INTERVAL = 4       # Fast refresh
CACHE_LOW_THRESHOLD = 5000
ROBLOX_TIMEOUT = 15

# STRICT BLACKLIST - ensures no overlap
BLACKLIST_DURATION = 600         # 10 MINUTES - server won't be given to ANY bot
BOT_BLACKLIST_DURATION = 1800    # 30 MINUTES - bot-specific blacklist

# Fetch settings
MAX_PAGES_PER_FETCH = 20         # More pages = more unique servers
OPTIMAL_PLAYER_RANGE = (2, 7)    # Accept wider range for more options

# ================== DATA ==================
server_queue = deque(maxlen=CACHE_LIMIT)
global_blacklist = {}            # {job_id: expiry_time} - NO bot gets these
bot_blacklist = {}               # {bot_id: {job_id: expiry_time}} - per-bot tracking

lock = threading.RLock()

stats = {
    "total_assigned": 0,
    "unique_servers": 0,
    "collisions_prevented": 0,
    "total_fetched": 0,
    "fetch_errors": 0,
    "last_fetch": None,
    "uptime_start": time.time()
}

# ================== CLEANUP ==================
def cleanup_global_blacklist():
    now = time.time()
    expired = [job for job, exp in global_blacklist.items() if exp <= now]
    for job in expired:
        del global_blacklist[job]
    return len(expired)

def cleanup_bot_blacklist(bot_id):
    if bot_id not in bot_blacklist:
        return 0
    now = time.time()
    expired = [job for job, exp in bot_blacklist[bot_id].items() if exp <= now]
    for job in expired:
        del bot_blacklist[bot_id][job]
    return len(expired)

def cleanup_all_bot_blacklists():
    now = time.time()
    for bot_id in list(bot_blacklist.keys()):
        expired = [job for job, exp in bot_blacklist[bot_id].items() if exp <= now]
        for job in expired:
            del bot_blacklist[bot_id][job]
        if not bot_blacklist[bot_id]:
            del bot_blacklist[bot_id]

# ================== SERVER SCORING ==================
def score_server(server_data):
    """Simple scoring - just check player count"""
    players = server_data.get("playing", 0)
    max_players = server_data.get("maxPlayers", 8)
    
    # Skip full servers
    if players >= max_players:
        return -1
    
    # Skip empty servers
    if players < 1:
        return 0
    
    # Prefer servers in optimal range
    if OPTIMAL_PLAYER_RANGE[0] <= players <= OPTIMAL_PLAYER_RANGE[1]:
        return 100 + random.randint(0, 20)
    
    return 50 + random.randint(0, 10)

# ================== FETCH ==================
def fetch_servers_from_page(place_id, cursor=None):
    url = ROBLOX_API_BASE.format(place_id)
    if cursor:
        url += f"&cursor={cursor}"
    
    try:
        response = requests.get(url, timeout=ROBLOX_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return data.get("data", []), data.get("nextPageCursor")
    except Exception as e:
        stats["fetch_errors"] += 1
        return [], None

def fetch_job_ids():
    """Main fetch loop"""
    while True:
        try:
            total_new = 0
            
            for place_id in PLACE_IDS:
                cursor = None
                pages = 0
                
                while pages < MAX_PAGES_PER_FETCH:
                    servers, next_cursor = fetch_servers_from_page(place_id, cursor)
                    
                    if not servers:
                        break
                    
                    with lock:
                        cleanup_global_blacklist()
                        existing = set(server_queue) | set(global_blacklist.keys())
                        
                        # Score and sort servers
                        scored = []
                        for s in servers:
                            job_id = s.get("id")
                            if not job_id or job_id in existing:
                                continue
                            score = score_server(s)
                            if score > 0:
                                scored.append((score, job_id))
                        
                        # Sort by score (highest first)
                        scored.sort(reverse=True)
                        
                        # Add to queue
                        for _, job_id in scored:
                            server_queue.append(job_id)
                            total_new += 1
                        
                        stats["total_fetched"] += len(scored)
                    
                    pages += 1
                    if not next_cursor:
                        break
                    cursor = next_cursor
                    time.sleep(0.1)
            
            stats["last_fetch"] = time.time()
            stats["unique_servers"] = len(server_queue)
            print(f"[CACHE] +{total_new} | Queue: {len(server_queue)} | Blacklist: {len(global_blacklist)}")
            
        except Exception as e:
            print(f"[ERROR] fetch: {e}")
            stats["fetch_errors"] += 1
        
        # Dynamic refresh
        if len(server_queue) < CACHE_LOW_THRESHOLD:
            time.sleep(2)
        elif len(server_queue) > CACHE_LIMIT * 0.8:
            time.sleep(8)
        else:
            time.sleep(CACHE_REFRESH_INTERVAL)

# ================== UNIQUE SERVER DISTRIBUTION ==================
def get_unique_server(bot_id=None):
    """
    Get a server that:
    1. No other bot has been assigned recently (global blacklist)
    2. This specific bot hasn't visited recently (bot blacklist)
    """
    with lock:
        cleanup_global_blacklist()
        if bot_id:
            cleanup_bot_blacklist(bot_id)
        
        # Get bot's personal blacklist
        bot_bl = bot_blacklist.get(bot_id, {}) if bot_id else {}
        
        # Find a server not in any blacklist
        attempts = 0
        max_attempts = min(100, len(server_queue))
        
        while server_queue and attempts < max_attempts:
            job_id = server_queue.popleft()
            
            # Check global blacklist
            if job_id in global_blacklist:
                stats["collisions_prevented"] += 1
                attempts += 1
                continue
            
            # Check bot-specific blacklist
            if job_id in bot_bl:
                # Put it back at the end for other bots
                server_queue.append(job_id)
                stats["collisions_prevented"] += 1
                attempts += 1
                continue
            
            # Found a unique server!
            # Add to GLOBAL blacklist - no other bot gets this
            global_blacklist[job_id] = time.time() + BLACKLIST_DURATION
            
            # Add to bot's personal blacklist
            if bot_id:
                if bot_id not in bot_blacklist:
                    bot_blacklist[bot_id] = {}
                bot_blacklist[bot_id][job_id] = time.time() + BOT_BLACKLIST_DURATION
            
            stats["total_assigned"] += 1
            return job_id
        
        return None

def get_unique_servers_batch(count, bot_id=None):
    """Get multiple unique servers"""
    servers = []
    for _ in range(count):
        job_id = get_unique_server(bot_id)
        if job_id:
            servers.append(job_id)
        else:
            break
    return servers

# ================== ENDPOINTS ==================

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "Coverage API v3.0 - No Overlap",
        "queue": len(server_queue),
        "global_blacklist": len(global_blacklist),
        "bots_tracked": len(bot_blacklist)
    })

@app.route("/status", methods=["GET"])
def status():
    with lock:
        cleanup_global_blacklist()
        cleanup_all_bot_blacklists()
        return jsonify({
            "queue_size": len(server_queue),
            "global_blacklist": len(global_blacklist),
            "bots_tracked": len(bot_blacklist),
            "total_assigned": stats["total_assigned"],
            "collisions_prevented": stats["collisions_prevented"],
            "total_fetched": stats["total_fetched"],
            "fetch_errors": stats["fetch_errors"],
            "uptime": int(time.time() - stats["uptime_start"])
        })

@app.route("/get-server", methods=["GET"])
def get_server():
    """Get a single unique server"""
    bot_id = request.args.get("bot")
    
    job_id = get_unique_server(bot_id)
    
    if job_id:
        return jsonify({"job_id": job_id})
    else:
        return jsonify({"error": "No unique servers available"}), 404

@app.route("/get-batch", methods=["GET"])
def get_batch():
    """Get multiple unique servers"""
    count = request.args.get("count", default=5, type=int)
    count = min(count, 20)  # Max 20 per request
    bot_id = request.args.get("bot")
    
    servers = get_unique_servers_batch(count, bot_id)
    
    return jsonify({
        "servers": [{"job_id": j} for j in servers],
        "count": len(servers),
        "remaining": len(server_queue)
    })

@app.route("/return-server", methods=["POST"])
def return_server():
    """Return a server (e.g., if teleport failed)"""
    data = request.get_json()
    if not data or "job_id" not in data:
        return jsonify({"error": "Missing job_id"}), 400
    
    job_id = data["job_id"]
    bot_id = data.get("bot_id")
    
    with lock:
        # Remove from global blacklist
        if job_id in global_blacklist:
            del global_blacklist[job_id]
        
        # Remove from bot blacklist
        if bot_id and bot_id in bot_blacklist:
            if job_id in bot_blacklist[bot_id]:
                del bot_blacklist[bot_id][job_id]
        
        # Add back to queue (at front for quick reuse)
        if job_id not in server_queue:
            server_queue.appendleft(job_id)
    
    return jsonify({"status": "returned"})

@app.route("/add-pool", methods=["POST"])
def add_pool():
    """Add servers from mini API"""
    data = request.get_json()
    if not data or "servers" not in data:
        return jsonify({"error": "Missing servers"}), 400
    
    added = 0
    with lock:
        existing = set(server_queue) | set(global_blacklist.keys())
        for job_id in data["servers"]:
            if job_id and job_id not in existing:
                server_queue.append(job_id)
                added += 1
    
    return jsonify({"added": added, "total": len(server_queue)})

@app.route("/stats", methods=["GET"])
def get_stats():
    uptime = int(time.time() - stats["uptime_start"])
    with lock:
        return jsonify({
            "queue_size": len(server_queue),
            "global_blacklist": len(global_blacklist),
            "bots_tracked": len(bot_blacklist),
            "total_assigned": stats["total_assigned"],
            "collisions_prevented": stats["collisions_prevented"],
            "servers_per_minute": round(stats["total_assigned"] / max(uptime / 60, 1), 1),
            "uptime_seconds": uptime
        })

@app.route("/bot-stats/<bot_id>", methods=["GET"])
def bot_stats(bot_id):
    """Get stats for a specific bot"""
    with lock:
        cleanup_bot_blacklist(bot_id)
        bl = bot_blacklist.get(bot_id, {})
        return jsonify({
            "bot_id": bot_id,
            "servers_visited": len(bl),
            "blacklist_expires": [
                {"job_id": j[:12], "expires_in": int(exp - time.time())}
                for j, exp in sorted(bl.items(), key=lambda x: x[1])[:10]
            ]
        })

@app.route("/clear-bot/<bot_id>", methods=["POST"])
def clear_bot(bot_id):
    """Clear a bot's blacklist (e.g., if restarting)"""
    with lock:
        if bot_id in bot_blacklist:
            count = len(bot_blacklist[bot_id])
            del bot_blacklist[bot_id]
            return jsonify({"cleared": count})
    return jsonify({"cleared": 0})

# ================== MAIN ==================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    
    threading.Thread(target=fetch_job_ids, daemon=True).start()
    
    print(f"""
╔═══════════════════════════════════════════════╗
║     COVERAGE MAIN API v3.0                    ║
╠═══════════════════════════════════════════════╣
║  🎯 STRICT NO OVERLAP MODE                    ║
║  ⏱️  Global Blacklist: {BLACKLIST_DURATION}s                  ║
║  🤖 Bot Blacklist: {BOT_BLACKLIST_DURATION}s                    ║
║  📊 Every bot = unique server                 ║
╚═══════════════════════════════════════════════╝
    """)
    
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
