#!/usr/bin/env python3
"""
MAIN API - Production Server Pool
- Self-fetches + receives from Mini API
- Distributes to 400+ bots
- Dead server tracking
- 30 min TTL
"""

import os
import time
import logging
import threading
import requests
from collections import deque, OrderedDict
from flask import Flask, jsonify, request
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import hashlib

# ==================== CONFIG ====================
SERVER_TTL = 1800  # 30 minutes
PLACE_ID = 109983668079237

# NAProxy Config
PROXY_HOST = "us.naproxy.net"
PROXY_PORT = "1000"
PROXY_USER = "proxy-e5a1ntzmrlr3_area-US"
PROXY_PASS = "Ol43jGdsIuPUNacc"

# Fetching config - AGGRESSIVE
FETCH_THREADS = 20          # More parallel threads
FETCH_INTERVAL = 1          # Fetch every second
SERVERS_PER_REQUEST = 100
CURSORS_PER_CYCLE = 100     # More cursors = more unique servers

# Bot tracking
BOT_BATCH_SIZE = 100        # Servers per bot batch
MAX_POOL_SIZE = 100000      # Max servers to hold

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ==================== PROXY ROTATION ====================
class ProxyManager:
    def __init__(self):
        self.session_counter = 0
        self.lock = threading.Lock()
        self.failed_sessions = set()
    
    def get_proxy(self):
        with self.lock:
            self.session_counter += 1
            session_id = f"s{self.session_counter}r{random.randint(10000, 99999)}"
        
        proxy_url = f"http://{PROXY_USER}_session-{session_id}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
        return {'http': proxy_url, 'https': proxy_url}
    
    def get_sticky_proxy(self, bot_id):
        """Same proxy for same bot (for consistency)"""
        session_hash = hashlib.md5(bot_id.encode()).hexdigest()[:8]
        proxy_url = f"http://{PROXY_USER}_session-{session_hash}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
        return {'http': proxy_url, 'https': proxy_url}

proxy_mgr = ProxyManager()

# ==================== SERVER POOL ====================
class ServerPool:
    def __init__(self):
        self._lock = threading.Lock()
        self._servers = deque(maxlen=MAX_POOL_SIZE)
        self._seen_jobs = OrderedDict()  # job_id -> timestamp (for LRU cleanup)
        self._dead_servers = set()       # Reported dead by bots
        self._given_servers = {}         # job_id -> timestamp (track what we gave out)
        self._stats = {
            'total_given': 0,
            'total_received': 0,
            'expired': 0,
            'duplicates_skipped': 0,
            'dead_skipped': 0,
            'fetch_errors': 0,
            'fetch_success': 0,
            'dead_reports': 0
        }
    
    def _clean_expired(self):
        now = time.time()
        expired = 0
        
        # Clean expired from queue
        while self._servers:
            job_id, players, added_time = self._servers[0]
            if now - added_time > SERVER_TTL:
                self._servers.popleft()
                if job_id in self._seen_jobs:
                    del self._seen_jobs[job_id]
                expired += 1
            else:
                break
        
        # Clean old seen jobs (keep last 200k)
        while len(self._seen_jobs) > 200000:
            self._seen_jobs.popitem(last=False)
        
        # Clean old dead servers (reset after 10 min)
        # This allows re-trying servers that might be alive again
        
        # Clean old given servers (older than TTL)
        old_given = [k for k, v in self._given_servers.items() if now - v > SERVER_TTL]
        for k in old_given:
            del self._given_servers[k]
        
        if expired:
            self._stats['expired'] += expired
        return expired
    
    def add_servers(self, servers, source="unknown"):
        added = 0
        duplicates = 0
        dead = 0
        now = time.time()
        
        with self._lock:
            self._clean_expired()
            
            for item in servers:
                if isinstance(item, str):
                    job_id = item
                    players = 0
                elif isinstance(item, dict):
                    job_id = item.get('id') or item.get('job_id')
                    players = item.get('playing', item.get('players', 0))
                else:
                    continue
                
                if not job_id:
                    continue
                
                # Skip dead servers
                if job_id in self._dead_servers:
                    dead += 1
                    continue
                
                # Skip duplicates
                if job_id in self._seen_jobs:
                    duplicates += 1
                    continue
                
                # Skip servers we already gave out recently
                if job_id in self._given_servers:
                    duplicates += 1
                    continue
                
                self._servers.append((job_id, players, now))
                self._seen_jobs[job_id] = now
                added += 1
            
            self._stats['total_received'] += added
            self._stats['duplicates_skipped'] += duplicates
            self._stats['dead_skipped'] += dead
        
        return added
    
    def get_server(self):
        now = time.time()
        with self._lock:
            attempts = 0
            while self._servers and attempts < 100:
                attempts += 1
                job_id, players, added_time = self._servers.popleft()
                
                # Skip expired
                if now - added_time > SERVER_TTL:
                    self._stats['expired'] += 1
                    if job_id in self._seen_jobs:
                        del self._seen_jobs[job_id]
                    continue
                
                # Skip dead
                if job_id in self._dead_servers:
                    self._stats['dead_skipped'] += 1
                    continue
                
                # Track that we gave this out
                self._given_servers[job_id] = now
                self._stats['total_given'] += 1
                
                return {
                    'job_id': job_id,
                    'players': players,
                    'age': int(now - added_time)
                }
            
            return None
    
    def get_batch(self, count):
        results = []
        now = time.time()
        
        with self._lock:
            attempts = 0
            while self._servers and len(results) < count and attempts < count * 2:
                attempts += 1
                job_id, players, added_time = self._servers.popleft()
                
                if now - added_time > SERVER_TTL:
                    self._stats['expired'] += 1
                    if job_id in self._seen_jobs:
                        del self._seen_jobs[job_id]
                    continue
                
                if job_id in self._dead_servers:
                    self._stats['dead_skipped'] += 1
                    continue
                
                self._given_servers[job_id] = now
                self._stats['total_given'] += 1
                
                results.append({
                    'job_id': job_id,
                    'players': players,
                    'age': int(now - added_time)
                })
        
        return results
    
    def report_dead(self, job_id):
        with self._lock:
            self._dead_servers.add(job_id)
            self._stats['dead_reports'] += 1
            
            # Limit dead server set size
            if len(self._dead_servers) > 50000:
                # Remove oldest (convert to list, slice, back to set)
                self._dead_servers = set(list(self._dead_servers)[-40000:])
    
    def count(self):
        with self._lock:
            self._clean_expired()
            return len(self._servers)
    
    def record_fetch_error(self):
        with self._lock:
            self._stats['fetch_errors'] += 1
    
    def record_fetch_success(self):
        with self._lock:
            self._stats['fetch_success'] += 1
    
    def get_stats(self):
        with self._lock:
            self._clean_expired()
            return {
                'available': len(self._servers),
                'seen_total': len(self._seen_jobs),
                'dead_servers': len(self._dead_servers),
                'given_tracking': len(self._given_servers),
                'total_given': self._stats['total_given'],
                'total_received': self._stats['total_received'],
                'expired': self._stats['expired'],
                'duplicates_skipped': self._stats['duplicates_skipped'],
                'dead_skipped': self._stats['dead_skipped'],
                'dead_reports': self._stats['dead_reports'],
                'fetch_errors': self._stats['fetch_errors'],
                'fetch_success': self._stats['fetch_success'],
                'ttl_seconds': SERVER_TTL
            }

pool = ServerPool()

# ==================== ROBLOX FETCHER ====================
class RobloxFetcher:
    def __init__(self):
        self.running = False
        self.cursors = deque(maxlen=1000)
        self.cursor_lock = threading.Lock()
        self.last_reset = time.time()
        self.servers_this_minute = 0
        self.minute_lock = threading.Lock()
    
    def fetch_page(self, cursor=None):
        try:
            url = f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public"
            params = {'sortOrder': 'Asc', 'limit': SERVERS_PER_REQUEST}
            if cursor:
                params['cursor'] = cursor
            
            proxies = proxy_mgr.get_proxy()
            
            response = requests.get(
                url,
                params=params,
                proxies=proxies,
                timeout=10,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json'
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                servers = data.get('data', [])
                next_cursor = data.get('nextPageCursor')
                pool.record_fetch_success()
                return servers, next_cursor
            elif response.status_code == 429:
                log.warning("[FETCH] Rate limited, rotating proxy...")
                time.sleep(0.5)
                return [], None
            else:
                pool.record_fetch_error()
                return [], None
                
        except Exception as e:
            pool.record_fetch_error()
            return [], None
    
    def get_cursor(self):
        with self.cursor_lock:
            if self.cursors:
                return self.cursors.popleft()
            return None
    
    def add_cursor(self, cursor):
        if cursor:
            with self.cursor_lock:
                self.cursors.append(cursor)
    
    def fetch_cycle(self):
        # Build list of cursors to fetch
        cursors_to_fetch = [None]  # Always start fresh
        
        # Add saved cursors
        with self.cursor_lock:
            cursors_to_fetch.extend(list(self.cursors)[:CURSORS_PER_CYCLE - 1])
        
        total_added = 0
        new_cursors = []
        
        with ThreadPoolExecutor(max_workers=FETCH_THREADS) as executor:
            futures = {executor.submit(self.fetch_page, c): c for c in cursors_to_fetch}
            
            for future in as_completed(futures):
                try:
                    servers, next_cursor = future.result()
                    
                    if servers:
                        added = pool.add_servers(servers, source="fetcher")
                        total_added += added
                    
                    if next_cursor:
                        new_cursors.append(next_cursor)
                        
                except Exception as e:
                    log.error(f"[FETCH] Future error: {e}")
        
        # Save new cursors
        for c in new_cursors:
            self.add_cursor(c)
        
        # Track rate
        with self.minute_lock:
            self.servers_this_minute += total_added
            now = time.time()
            if now - self.last_reset >= 60:
                rate = self.servers_this_minute
                stats = pool.get_stats()
                log.info(f"[RATE] {rate}/min | Pool: {stats['available']} | Seen: {stats['seen_total']} | Dead: {stats['dead_servers']} | Given: {stats['total_given']}")
                self.servers_this_minute = 0
                self.last_reset = now
        
        return total_added
    
    def run(self):
        self.running = True
        log.info("[FETCHER] Started - NAProxy rotating sessions")
        
        while self.running:
            try:
                self.fetch_cycle()
            except Exception as e:
                log.error(f"[FETCHER] Cycle error: {e}")
            
            time.sleep(FETCH_INTERVAL)
    
    def start(self):
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()

fetcher = RobloxFetcher()

# ==================== API ENDPOINTS ====================

@app.route('/status', methods=['GET'])
def status():
    stats = pool.get_stats()
    with fetcher.cursor_lock:
        stats['cursors_cached'] = len(fetcher.cursors)
    return jsonify(stats)

@app.route('/get-server', methods=['GET'])
def get_server():
    server = pool.get_server()
    if not server:
        return jsonify({'error': 'No servers available'}), 404
    return jsonify(server)

@app.route('/get-batch', methods=['GET'])
def get_batch():
    count = request.args.get('count', default=BOT_BATCH_SIZE, type=int)
    count = min(count, 200)  # Max 200 per request
    
    servers = pool.get_batch(count)
    
    if not servers:
        return jsonify({'servers': [], 'count': 0}), 200
    
    return jsonify({'servers': servers, 'count': len(servers)})

@app.route('/add-pool', methods=['POST'])
def add_pool():
    """Receive servers from Mini API"""
    data = request.get_json() or {}
    servers = data.get('servers', [])
    source = data.get('source', 'mini-api')
    
    if not servers:
        return jsonify({'added': 0}), 200
    
    added = pool.add_servers(servers, source=source)
    log.info(f"[RECEIVE] +{added} from {source} (pool: {pool.count()})")
    return jsonify({'added': added, 'pool_size': pool.count()})

@app.route('/report-dead', methods=['POST'])
def report_dead():
    """Bots report dead/failed servers"""
    data = request.get_json() or {}
    job_id = data.get('job_id')
    bot_id = data.get('bot_id', 'unknown')
    reason = data.get('reason', 'unknown')
    
    if job_id:
        pool.report_dead(job_id)
        log.debug(f"[DEAD] {job_id[:12]}... from {bot_id} ({reason})")
    
    return jsonify({'status': 'noted'})

@app.route('/health', methods=['GET'])
def health():
    count = pool.count()
    status = 'healthy' if count > 100 else 'low' if count > 0 else 'empty'
    return jsonify({'status': status, 'servers': count})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    
    log.info(f"[STARTUP] Main API on port {port}")
    log.info(f"[CONFIG] TTL={SERVER_TTL}s | Threads={FETCH_THREADS} | Interval={FETCH_INTERVAL}s")
    log.info(f"[PROXY] {PROXY_HOST}:{PROXY_PORT}")
    
    # Start fetcher
    fetcher.start()
    time.sleep(2)
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
