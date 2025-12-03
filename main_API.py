#!/usr/bin/env python3
"""
OPTIMIZED MAIN API - For 400+ Bot Server Hopping
- Fetches 100 servers, refills when below 10
- Bot gets server = removed from pool
"""

import os
import time
import logging
import threading
from collections import deque
from flask import Flask, jsonify, request
import requests

# ==================== CONFIG ====================
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# Roblox Config
GAME_ID = os.environ.get("GAME_ID", "109983668079237")
BASE_URL = f"https://games.roblox.com/v1/games/{GAME_ID}/servers/Public"

# Pool Settings
MAX_SERVERS = 100           # Keep 100 servers
MIN_THRESHOLD = 10          # Refill when below 10
REQUEST_TIMEOUT = 5

# ==================== SERVER POOL ====================
class ServerPool:
    def __init__(self):
        self._lock = threading.Lock()
        self._servers = deque()
        self._seen = set()
        self._stats = {'total_given': 0, 'total_fetched': 0}
    
    def add_servers(self, servers):
        added = 0
        with self._lock:
            for job_id, players in servers:
                if job_id not in self._seen:
                    self._servers.append((job_id, players))
                    self._seen.add(job_id)
                    added += 1
            self._stats['total_fetched'] += added
        return added
    
    def get_server(self):
        with self._lock:
            if not self._servers:
                return None
            job_id, players = self._servers.popleft()
            self._stats['total_given'] += 1
            return {'job_id': job_id, 'players': players}
    
    def get_batch(self, count):
        results = []
        with self._lock:
            for _ in range(min(count, len(self._servers))):
                job_id, players = self._servers.popleft()
                results.append({'job_id': job_id, 'players': players})
                self._stats['total_given'] += 1
        return results
    
    def count(self):
        with self._lock:
            return len(self._servers)
    
    def needs_refill(self):
        return self.count() < MIN_THRESHOLD
    
    def get_stats(self):
        with self._lock:
            return {
                'available': len(self._servers),
                'total_given': self._stats['total_given'],
                'total_fetched': self._stats['total_fetched']
            }
    
    def clear_seen(self):
        with self._lock:
            current = {s[0] for s in self._servers}
            self._seen = current

pool = ServerPool()

# ==================== FETCHING ====================
def fetch_page(cursor=None):
    try:
        params = {'sortOrder': 'Desc', 'limit': 100}
        if cursor:
            params['cursor'] = cursor
        
        resp = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        
        if resp.status_code == 429:
            time.sleep(2)
            return [], None
        
        resp.raise_for_status()
        data = resp.json()
        
        servers = [(s['id'], s.get('playing', 0)) for s in data.get('data', []) if s.get('id')]
        return servers, data.get('nextPageCursor')
        
    except Exception as e:
        log.error(f"[FETCH] Error: {e}")
        return [], None

def fetch_servers():
    """Fetch up to 100 servers"""
    all_servers = []
    cursor = None
    
    while len(all_servers) < MAX_SERVERS:
        servers, cursor = fetch_page(cursor)
        if servers:
            all_servers.extend(servers)
        if not cursor:
            break
        time.sleep(0.1)
    
    return all_servers[:MAX_SERVERS]

def fetch_loop():
    """Background loop - refill when below 10"""
    while True:
        try:
            if pool.needs_refill():
                log.info(f"[FETCH] Pool at {pool.count()}, refilling to 100...")
                servers = fetch_servers()
                added = pool.add_servers(servers)
                log.info(f"[FETCH] Added {added}, pool now {pool.count()}")
            
            # Clear seen every 5 min
            if int(time.time()) % 300 == 0:
                pool.clear_seen()
                
        except Exception as e:
            log.exception(f"[FETCH] Error: {e}")
        
        time.sleep(2)

# ==================== API ENDPOINTS ====================
@app.route('/status', methods=['GET'])
def status():
    return jsonify(pool.get_stats())

@app.route('/get-server', methods=['GET'])
def get_server():
    server = pool.get_server()
    
    if not server:
        log.warning("[GET] Empty, emergency fetch...")
        servers = fetch_servers()
        pool.add_servers(servers)
        server = pool.get_server()
    
    if not server:
        return jsonify({'error': 'No servers'}), 404
    
    log.info(f"[GIVE] {server['job_id'][:8]}... ({pool.count()} left)")
    return jsonify(server)

@app.route('/get-batch', methods=['GET'])
def get_batch():
    count = request.args.get('count', default=10, type=int)
    count = min(count, 50)
    
    servers = pool.get_batch(count)
    
    if len(servers) < count:
        fetched = fetch_servers()
        pool.add_servers(fetched)
        more = pool.get_batch(count - len(servers))
        servers.extend(more)
    
    log.info(f"[BATCH] Gave {len(servers)} ({pool.count()} left)")
    return jsonify({'servers': servers, 'count': len(servers)})

@app.route('/add-pool', methods=['POST'])
def add_pool():
    data = request.get_json() or {}
    servers = data.get('servers', [])
    
    server_tuples = [(s, 0) if isinstance(s, str) else (s.get('id', s), s.get('players', 0)) for s in servers]
    added = pool.add_servers(server_tuples)
    
    return jsonify({'added': added})

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'servers': pool.count()})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    
    # Initial fetch
    log.info("[INIT] Fetching initial 100 servers...")
    servers = fetch_servers()
    pool.add_servers(servers)
    log.info(f"[INIT] Ready with {pool.count()} servers")
    
    # Start background loop
    threading.Thread(target=fetch_loop, daemon=True).start()
    
    log.info(f"🚀 Main API on port {port}")
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
