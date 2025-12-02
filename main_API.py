#!/usr/bin/env python3
from flask import Flask, jsonify, request
import requests
import threading
import time
import os
from collections import deque
import random

app = Flask(__name__)

# ---------------- CONFIG ----------------
GAME_ID = "109983668079237"  # Universe ID

# Multiple Place IDs to fetch from (main game + rebirth servers)
PLACE_IDS = [
    "109983668079237",  # Main game
]

# API URLs
ROBLOX_API_BASE = "https://games.roblox.com/v1/games/{}/servers/Public?sortOrder=Asc&limit=100"

# Cache settings - Optimized for 300 bots
CACHE_LIMIT = 50000              # 50k servers max
CACHE_REFRESH_INTERVAL = 5       # Fetch every 5 seconds
CACHE_LOW_THRESHOLD = 5000       # Refresh early if below 5k
BLACKLIST_DURATION = 180         # 3 min blacklist (shorter for more servers)
ROBLOX_TIMEOUT = 15
CACHE_CLEAR_INTERVAL = 3600      # Clear every 1 hour

# Rate limiting
MAX_REQUESTS_PER_SECOND = 100    # Max requests per second
BATCH_SIZE = 50                  # Servers per batch request

# Pagination - fetch multiple pages
MAX_PAGES_PER_FETCH = 10         # Fetch up to 10 pages (1000 servers) per cycle
# ---------------------------------------

# Use deque for O(1) popleft operations
server_cache = deque(maxlen=CACHE_LIMIT)
jobs_assigned = 0
blacklist = {}
lock = threading.RLock()  # Reentrant lock for nested calls

# Stats tracking
stats = {
    "total_assigned": 0,
    "total_fetched": 0,
    "fetch_errors": 0,
    "last_fetch": None,
    "uptime_start": time.time()
}

def cleanup_blacklist():
    """Remove expired blacklist entries"""
    now = time.time()
    expired = [job for job, exp in blacklist.items() if exp <= now]
    for job in expired:
        del blacklist[job]
    return len(expired)

def fetch_servers_from_page(place_id, cursor=None):
    """Fetch a single page of servers"""
    url = ROBLOX_API_BASE.format(place_id)
    if cursor:
        url += f"&cursor={cursor}"
    
    try:
        response = requests.get(url, timeout=ROBLOX_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        servers = data.get("data", [])
        
        # Sort by player count (higher = better, more brainrots)
        servers.sort(key=lambda x: x.get("playing", 0), reverse=True)
        
        return servers, data.get("nextPageCursor")
    except Exception as e:
        stats["fetch_errors"] += 1
        print(f"[ERROR] Fetch failed: {e}")
        return [], None

def fetch_job_ids():
    """Main fetch loop - fetches from multiple pages"""
    global server_cache
    
    while True:
        try:
            total_new = 0
            
            for place_id in PLACE_IDS:
                cursor = None
                pages_fetched = 0
                
                while pages_fetched < MAX_PAGES_PER_FETCH:
                    servers, next_cursor = fetch_servers_from_page(place_id, cursor)
                    
                    if not servers:
                        break
                    
                    with lock:
                        cleanup_blacklist()
                        existing = set(server_cache) | set(blacklist.keys())
                        
                        new_jobs = []
                        for s in servers:
                            job_id = s.get("id")
                            players = s.get("playing", 0)
                            # Prioritize servers with 3-7 players (active but room to join)
                            if job_id and job_id not in existing and 3 <= players <= 7:
                                new_jobs.append(job_id)
                        
                        # Add high-player servers to FRONT of queue (priority)
                        for job in new_jobs:
                            server_cache.appendleft(job)
                        
                        total_new += len(new_jobs)
                        stats["total_fetched"] += len(new_jobs)
                    
                    pages_fetched += 1
                    
                    if not next_cursor:
                        break
                    cursor = next_cursor
                    
                    # Small delay between pages to avoid rate limit
                    time.sleep(0.1)
            
            stats["last_fetch"] = time.time()
            print(f"[CACHE] +{total_new} | Total: {len(server_cache)} | Blacklist: {len(blacklist)}")
            
        except Exception as e:
            print(f"[ERROR] fetch_job_ids: {e}")
            stats["fetch_errors"] += 1
        
        # Dynamic refresh based on cache size
        wait_time = CACHE_REFRESH_INTERVAL
        if len(server_cache) < CACHE_LOW_THRESHOLD:
            wait_time = 2  # Faster refresh when low
        elif len(server_cache) > CACHE_LIMIT * 0.8:
            wait_time = 10  # Slower when cache is full
        
        time.sleep(wait_time)

def clear_cache_periodically():
    """Periodic full cache clear to remove stale servers"""
    while True:
        time.sleep(CACHE_CLEAR_INTERVAL)
        with lock:
            old_size = len(server_cache)
            server_cache.clear()
            blacklist.clear()
            print(f"[CLEARED] Cache reset (was {old_size} servers)")

# ------------------- ENDPOINTS -------------------

@app.route("/", methods=["GET"])
def home():
    """Health check"""
    uptime = int(time.time() - stats["uptime_start"])
    return jsonify({
        "status": "Main API running",
        "cache": len(server_cache),
        "uptime_seconds": uptime,
        "version": "2.0"
    })

@app.route("/status", methods=["GET"])
def status():
    """Detailed status"""
    with lock:
        cleanup_blacklist()
        uptime = int(time.time() - stats["uptime_start"])
        return jsonify({
            "cache_jobs": len(server_cache),
            "jobs_assigned": stats["total_assigned"],
            "jobs_fetched": stats["total_fetched"],
            "blacklist_jobs": len(blacklist),
            "fetch_errors": stats["fetch_errors"],
            "last_fetch": stats["last_fetch"],
            "uptime_seconds": uptime,
            "cache_limit": CACHE_LIMIT
        })

@app.route("/get-server", methods=["GET"])
def get_server():
    """Get a single server for a bot"""
    global jobs_assigned
    
    with lock:
        cleanup_blacklist()
        
        if not server_cache:
            return jsonify({"error": "No servers available"}), 404
        
        job_id = server_cache.popleft()  # O(1) operation
        stats["total_assigned"] += 1
        blacklist[job_id] = time.time() + BLACKLIST_DURATION
        
        return jsonify({"job_id": job_id})

@app.route("/get-servers", methods=["GET"])
def get_servers():
    """Get multiple servers at once (for batch operations)"""
    count = request.args.get("count", default=10, type=int)
    count = min(count, 100)  # Max 100 per request
    
    with lock:
        cleanup_blacklist()
        
        if not server_cache:
            return jsonify({"job_ids": []})
        
        job_ids = []
        for _ in range(min(count, len(server_cache))):
            job_id = server_cache.popleft()
            job_ids.append(job_id)
            blacklist[job_id] = time.time() + BLACKLIST_DURATION
            stats["total_assigned"] += 1
        
        return jsonify({"job_ids": job_ids, "count": len(job_ids)})

@app.route("/get-batch", methods=["GET"])
def get_batch():
    """Get a batch of servers for multiple bots"""
    count = request.args.get("count", default=BATCH_SIZE, type=int)
    count = min(count, 200)  # Max 200 per batch
    
    with lock:
        cleanup_blacklist()
        
        servers = []
        for _ in range(min(count, len(server_cache))):
            job_id = server_cache.popleft()
            servers.append({"job_id": job_id})
            blacklist[job_id] = time.time() + BLACKLIST_DURATION
            stats["total_assigned"] += 1
        
        return jsonify({
            "servers": servers,
            "count": len(servers),
            "remaining": len(server_cache)
        })

@app.route("/peek", methods=["GET"])
def peek():
    """Preview servers without removing them"""
    count = request.args.get("count", default=10, type=int)
    count = min(count, 100)
    
    with lock:
        servers = list(server_cache)[:count]
        return jsonify({
            "job_ids": servers,
            "total_available": len(server_cache)
        })

@app.route("/add-pool", methods=["POST"])
def add_pool():
    """Add servers from external source"""
    data = request.get_json()
    
    if not data or "servers" not in data:
        return jsonify({"error": "Missing 'servers' field"}), 400
    
    servers = data["servers"]
    if not isinstance(servers, list):
        return jsonify({"error": "'servers' must be a list"}), 400
    
    added = 0
    with lock:
        cleanup_blacklist()
        existing = set(server_cache) | set(blacklist.keys())
        
        for job_id in servers:
            if job_id and job_id not in existing:
                server_cache.append(job_id)
                added += 1
    
    print(f"[ADD POOL] +{added} | Total: {len(server_cache)}")
    return jsonify({"added": added, "total": len(server_cache)})

@app.route("/return-server", methods=["POST"])
def return_server():
    """Return a server back to the pool (e.g., if bot failed to join)"""
    data = request.get_json()
    
    if not data or "job_id" not in data:
        return jsonify({"error": "Missing 'job_id' field"}), 400
    
    job_id = data["job_id"]
    
    with lock:
        # Remove from blacklist and add back to cache
        if job_id in blacklist:
            del blacklist[job_id]
        
        if job_id not in server_cache:
            server_cache.appendleft(job_id)  # Add to front (priority)
    
    return jsonify({"status": "returned", "job_id": job_id})

@app.route("/blacklist", methods=["POST"])
def add_to_blacklist():
    """Manually blacklist a server"""
    data = request.get_json()
    
    if not data or "job_id" not in data:
        return jsonify({"error": "Missing 'job_id' field"}), 400
    
    job_id = data["job_id"]
    duration = data.get("duration", BLACKLIST_DURATION)
    
    with lock:
        blacklist[job_id] = time.time() + duration
        # Remove from cache if present
        if job_id in server_cache:
            server_cache.remove(job_id)
    
    return jsonify({"status": "blacklisted", "job_id": job_id, "duration": duration})

@app.route("/clear-blacklist", methods=["POST"])
def clear_blacklist_endpoint():
    """Clear all blacklisted servers"""
    with lock:
        count = len(blacklist)
        blacklist.clear()
    
    return jsonify({"cleared": count})

@app.route("/stats", methods=["GET"])
def get_stats():
    """Get detailed statistics"""
    uptime = int(time.time() - stats["uptime_start"])
    hours = uptime // 3600
    minutes = (uptime % 3600) // 60
    
    with lock:
        return jsonify({
            "cache_size": len(server_cache),
            "cache_limit": CACHE_LIMIT,
            "cache_percent": round(len(server_cache) / CACHE_LIMIT * 100, 1),
            "blacklist_size": len(blacklist),
            "total_assigned": stats["total_assigned"],
            "total_fetched": stats["total_fetched"],
            "fetch_errors": stats["fetch_errors"],
            "uptime": f"{hours}h {minutes}m",
            "uptime_seconds": uptime,
            "servers_per_minute": round(stats["total_assigned"] / max(uptime / 60, 1), 1)
        })

# ------------------- MAIN -------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    
    # Start background threads
    threading.Thread(target=fetch_job_ids, daemon=True).start()
    threading.Thread(target=clear_cache_periodically, daemon=True).start()
    
    print(f"""
╔══════════════════════════════════════════╗
║       MAIN API - Server Pool v2.0        ║
╠══════════════════════════════════════════╣
║  Port: {port}                              
║  Cache Limit: {CACHE_LIMIT:,}                   
║  Refresh: Every {CACHE_REFRESH_INTERVAL}s                    
║  Blacklist Duration: {BLACKLIST_DURATION}s               
║  Optimized for: 300+ bots                ║
╚══════════════════════════════════════════╝
    """)
    
    # Use threaded mode for better concurrency
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
