#!/usr/bin/env python3
"""
OPTIMIZED MAIN API - For 400+ Bot Server Hopping
Features:
- Atomic server assignment (no duplicates ever)
- Parallel Roblox API fetching
- Smart prioritization by player count
- High-throughput thread-safe design
- Batch assignment for efficiency
"""

import os
import time
import logging
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# Performance Tuning
MAX_CACHE_SIZE = 10000              # Store up to 10k servers
MIN_CACHE_THRESHOLD = 200           # Trigger fetch when below this
BLACKLIST_DURATION = 180            # Seconds before server can be reused
FETCH_WORKERS = 8                   # Parallel fetch threads
FETCH_PAGES_PER_CYCLE = 50          # Max pages to fetch per cycle
FETCH_INTERVAL = 5                  # Seconds between fetch cycles
CACHE_CLEAR_INTERVAL = 600          # Clear stale data every 10 min
REQUEST_TIMEOUT = 8                 # Roblox API timeout

# ==================== DATA STRUCTURES ====================
class AtomicServerPool:
    """Thread-safe server pool with atomic operations"""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._servers = deque()           # (job_id, players, timestamp)
        self._assigned = set()            # Currently assigned job_ids
        self._blacklist = {}              # job_id -> expiry_time
        self._stats = {
            'total_assigned': 0,
            'total_fetched': 0,
            'duplicates_blocked': 0
        }
    
    def add_servers(self, servers):
        """Add multiple servers atomically, returns count added"""
        added = 0
        now = time.time()
        
        with self._lock:
            self._cleanup_blacklist(now)
            existing = {s[0] for s in self._servers}
            blocked = existing | self._assigned | set(self._blacklist.keys())
            
            for job_id, players in servers:
                if job_id not in blocked:
                    self._servers.append((job_id, players, now))
                    added += 1
                else:
                    self._stats['duplicates_blocked'] += 1
            
            # Sort by player count (higher = better) and keep limit
            if len(self._servers) > MAX_CACHE_SIZE:
                sorted_servers = sorted(self._servers, key=lambda x: -x[1])
                self._servers = deque(sorted_servers[:MAX_CACHE_SIZE])
            
            self._stats['total_fetched'] += added
        
        return added
    
    def get_server(self):
        """Get single best server atomically"""
        with self._lock:
            self._cleanup_blacklist(time.time())
            
            if not self._servers:
                return None
            
            # Get server with most players (first after sort)
            best_idx = 0
            best_players = -1
            for i, (job_id, players, _) in enumerate(self._servers):
                if players > best_players:
                    best_players = players
                    best_idx = i
            
            # Remove and blacklist
            job_id, players, _ = self._servers[best_idx]
            del self._servers[best_idx]
            
            self._assigned.add(job_id)
            self._blacklist[job_id] = time.time() + BLACKLIST_DURATION
            self._stats['total_assigned'] += 1
            
            return {'job_id': job_id, 'players': players}
    
    def get_batch(self, count):
        """Get multiple unique servers atomically - CRITICAL for 400 bots"""
        results = []
        
        with self._lock:
            self._cleanup_blacklist(time.time())
            
            # Sort by players descending
            sorted_servers = sorted(self._servers, key=lambda x: -x[1])
            
            for job_id, players, ts in sorted_servers:
                if len(results) >= count:
                    break
                if job_id not in self._assigned:
                    results.append({'job_id': job_id, 'players': players})
                    self._assigned.add(job_id)
                    self._blacklist[job_id] = time.time() + BLACKLIST_DURATION
                    self._stats['total_assigned'] += 1
            
            # Remove assigned servers from cache
            assigned_ids = {r['job_id'] for r in results}
            self._servers = deque(
                s for s in self._servers if s[0] not in assigned_ids
            )
        
        return results
    
    def release_server(self, job_id):
        """Release a server back (bot finished with it)"""
        with self._lock:
            self._assigned.discard(job_id)
            # Keep in blacklist to prevent immediate reassignment
    
    def _cleanup_blacklist(self, now):
        """Remove expired blacklist entries"""
        expired = [k for k, exp in self._blacklist.items() if exp <= now]
        for k in expired:
            del self._blacklist[k]
            self._assigned.discard(k)
    
    def clear_all(self):
        """Full reset"""
        with self._lock:
            self._servers.clear()
            self._assigned.clear()
            self._blacklist.clear()
    
    def get_stats(self):
        """Get current statistics"""
        with self._lock:
            self._cleanup_blacklist(time.time())
            return {
                'available': len(self._servers),
                'assigned': len(self._assigned),
                'blacklisted': len(self._blacklist),
                'total_assigned': self._stats['total_assigned'],
                'total_fetched': self._stats['total_fetched'],
                'duplicates_blocked': self._stats['duplicates_blocked']
            }

# Global pool instance
pool = AtomicServerPool()

# ==================== PARALLEL FETCHING ====================
def fetch_page(cursor=None, page_num=0):
    """Fetch single page from Roblox API"""
    try:
        params = {'sortOrder': 'Desc', 'limit': 100}  # Desc = fuller servers first
        if cursor:
            params['cursor'] = cursor
        
        resp = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        
        if resp.status_code == 429:
            log.warning(f"[Page {page_num}] Rate limited, backing off...")
            time.sleep(2)
            return [], None, False
        
        resp.raise_for_status()
        data = resp.json()
        
        servers = [(s['id'], s.get('playing', 0)) for s in data.get('data', []) if s.get('id')]
        next_cursor = data.get('nextPageCursor')
        
        return servers, next_cursor, True
        
    except Exception as e:
        log.error(f"[Page {page_num}] Fetch error: {e}")
        return [], None, False

def parallel_fetch_all():
    """Fetch multiple pages in parallel"""
    all_servers = []
    cursors_to_fetch = [None]  # Start with first page
    page_count = 0
    
    while cursors_to_fetch and page_count < FETCH_PAGES_PER_CYCLE:
        batch_cursors = cursors_to_fetch[:FETCH_WORKERS]
        cursors_to_fetch = cursors_to_fetch[FETCH_WORKERS:]
        
        with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as executor:
            futures = {
                executor.submit(fetch_page, cursor, page_count + i): cursor 
                for i, cursor in enumerate(batch_cursors)
            }
            
            for future in as_completed(futures):
                servers, next_cursor, success = future.result()
                if success:
                    all_servers.extend(servers)
                    if next_cursor:
                        cursors_to_fetch.append(next_cursor)
                page_count += 1
        
        # Small delay between batches
        time.sleep(0.3)
    
    return all_servers

def fetch_loop():
    """Background fetch loop"""
    while True:
        try:
            stats = pool.get_stats()
            
            # Fetch if cache is low
            if stats['available'] < MIN_CACHE_THRESHOLD:
                log.info(f"[FETCH] Cache low ({stats['available']}), fetching more...")
                servers = parallel_fetch_all()
                added = pool.add_servers(servers)
                log.info(f"[FETCH] Added {added} servers (fetched {len(servers)}, blocked {len(servers)-added} dupes)")
            else:
                log.debug(f"[FETCH] Cache OK: {stats['available']} servers")
            
        except Exception as e:
            log.exception(f"[FETCH] Error: {e}")
        
        time.sleep(FETCH_INTERVAL)

def cleanup_loop():
    """Periodic full cleanup"""
    while True:
        time.sleep(CACHE_CLEAR_INTERVAL)
        log.info("[CLEANUP] Clearing stale data...")
        pool.clear_all()

# ==================== API ENDPOINTS ====================

@app.route('/status', methods=['GET'])
def status():
    """Get pool statistics"""
    return jsonify(pool.get_stats())

@app.route('/get-server', methods=['GET'])
def get_server():
    """Get single best server"""
    server = pool.get_server()
    if not server:
        return jsonify({'error': 'No servers available'}), 404
    
    log.info(f"[ASSIGN] {server['job_id']} (players: {server['players']})")
    return jsonify(server)

@app.route('/get-batch', methods=['GET'])
def get_batch():
    """Get multiple unique servers - USE THIS FOR 400 BOTS"""
    count = request.args.get('count', default=10, type=int)
    count = min(count, 100)  # Cap at 100 per request
    
    servers = pool.get_batch(count)
    log.info(f"[BATCH] Assigned {len(servers)} servers")
    return jsonify({'servers': servers, 'count': len(servers)})

@app.route('/get-servers', methods=['GET'])
def get_servers():
    """List all available servers (for monitoring)"""
    stats = pool.get_stats()
    return jsonify({
        'available': stats['available'],
        'job_ids': []  # Don't expose full list for security
    })

@app.route('/release', methods=['POST'])
def release_server():
    """Release a server back to pool when bot is done"""
    data = request.get_json() or {}
    job_id = data.get('job_id')
    if job_id:
        pool.release_server(job_id)
        return jsonify({'released': job_id})
    return jsonify({'error': 'Missing job_id'}), 400

@app.route('/add-pool', methods=['POST'])
def add_pool():
    """Add servers from external source (mini_api)"""
    data = request.get_json() or {}
    servers = data.get('servers', [])
    
    if not isinstance(servers, list):
        return jsonify({'error': "'servers' must be a list"}), 400
    
    # Convert to (job_id, players) format
    server_tuples = [(s, 0) if isinstance(s, str) else (s.get('id', s), s.get('players', 0)) for s in servers]
    added = pool.add_servers(server_tuples)
    
    log.info(f"[ADD-POOL] Added {added}/{len(servers)} servers")
    return jsonify({'added': added})

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'timestamp': time.time()})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    
    # Start background threads
    threading.Thread(target=fetch_loop, daemon=True).start()
    threading.Thread(target=cleanup_loop, daemon=True).start()
    
    log.info(f"🚀 Optimized Main API starting on port {port}")
    log.info(f"   Max cache: {MAX_CACHE_SIZE}, Workers: {FETCH_WORKERS}")
    
    # Use threaded mode for concurrent requests
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
