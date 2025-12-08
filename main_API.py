#!/usr/bin/env python3
"""
MAIN API - ULTIMATE SERVER HOPPER
- Deep pagination (5000+ cursors)
- Both Asc + Desc fetching
- Smart priority (6-7 players first)
- Dead server blacklist
- No duplicate servers to bots
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

# ==================== CONFIG ====================
SERVER_TTL = 1800  # 30 min
PLACE_ID = 109983668079237

# NAProxy
PROXY_HOST = "us.naproxy.net"
PROXY_PORT = "1000"
PROXY_USER = "proxy-e5a1ntzmrlr3_area-US"
PROXY_PASS = "Ol43jGdsIuPUNacc"

# Gentle fetching (proxy friendly)
FETCH_THREADS = 10
FETCH_INTERVAL = 2
SERVERS_PER_REQUEST = 100
CURSORS_PER_CYCLE = 50
MAX_STORED_CURSORS = 1000

# Cooldowns
GIVEN_COOLDOWN = 90  # Don't re-give server for 90 seconds

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ==================== PROXY ====================
def get_proxy():
    session_id = f"main{random.randint(100000, 999999)}"
    proxy_url = f"http://{PROXY_USER}_session-{session_id}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    return {'http': proxy_url, 'https': proxy_url}

# ==================== SERVER POOL ====================
class ServerPool:
    def __init__(self):
        self._lock = threading.Lock()
        self._servers = {}  # job_id -> {players, added_time, priority}
        self._seen_jobs = OrderedDict()  # All seen servers
        self._dead_servers = set()  # Failed teleports
        self._given_servers = {}  # job_id -> timestamp
        
        self._stats = {
            'total_given': 0,
            'total_received': 0,
            'duplicates_skipped': 0,
            'dead_skipped': 0,
            'cooldown_skipped': 0,
            'low_player_skipped': 0,
            'fetch_errors': 0,
            'fetch_success': 0
        }
    
    def _clean_expired(self):
        now = time.time()
        
        # Remove expired servers
        expired = [jid for jid, data in self._servers.items() 
                   if now - data['added_time'] > SERVER_TTL]
        for jid in expired:
            del self._servers[jid]
            if jid in self._seen_jobs:
                del self._seen_jobs[jid]
        
        # Trim seen jobs to prevent memory bloat
        while len(self._seen_jobs) > 150000:
            self._seen_jobs.popitem(last=False)
        
        # Clean old given servers
        old_given = [k for k, v in self._given_servers.items() if now - v > GIVEN_COOLDOWN]
        for k in old_given:
            del self._given_servers[k]
        
        # Trim dead servers
        if len(self._dead_servers) > 20000:
            self._dead_servers = set(list(self._dead_servers)[-10000:])
    
    def add_servers(self, servers, source="unknown"):
        added = 0
        duplicates = 0
        low_player = 0
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
                    continue
                
                # Skip already seen
                if job_id in self._seen_jobs:
                    duplicates += 1
                    continue
                
                # Skip low player servers (less valuable)
                if players < 5:
                    low_player += 1
                    continue
                
                # Priority scoring
                if players == 6 or players == 7:
                    priority = 100  # BEST
                elif players == 5:
                    priority = 50
                elif players == 8:
                    priority = 30
                else:
                    priority = 10
                
                self._servers[job_id] = {
                    'players': players,
                    'added_time': now,
                    'priority': priority
                }
                self._seen_jobs[job_id] = now
                added += 1
            
            self._stats['total_received'] += added
            self._stats['duplicates_skipped'] += duplicates
            self._stats['low_player_skipped'] += low_player
        
        return added, duplicates
    
    def get_server(self, bot_id=None):
        now = time.time()
        
        with self._lock:
            self._clean_expired()
            
            # Build list of available servers
            available = []
            cooldown_skipped = 0
            
            for job_id, data in self._servers.items():
                # Skip dead
                if job_id in self._dead_servers:
                    continue
                
                # Skip recently given (prevent same server to multiple bots)
                if job_id in self._given_servers:
                    if now - self._given_servers[job_id] < GIVEN_COOLDOWN:
                        cooldown_skipped += 1
                        continue
                
                # Skip expired
                if now - data['added_time'] > SERVER_TTL:
                    continue
                
                available.append((job_id, data))
            
            self._stats['cooldown_skipped'] += cooldown_skipped
            
            if not available:
                return None
            
            # Sort by priority (highest first), then by freshness (newest first)
            available.sort(key=lambda x: (x[1]['priority'], -x[1]['added_time']), reverse=True)
            
            # Get best server
            job_id, data = available[0]
            
            # Mark as given
            self._given_servers[job_id] = now
            
            # Remove from pool
            del self._servers[job_id]
            
            self._stats['total_given'] += 1
            
            return {
                'job_id': job_id,
                'players': data['players'],
                'priority': data['priority'],
                'age': int(now - data['added_time'])
            }
    
    def report_dead(self, job_id, reason=None):
        with self._lock:
            self._dead_servers.add(job_id)
            if job_id in self._servers:
                del self._servers[job_id]
            self._stats['dead_skipped'] += 1
    
    def count(self):
        with self._lock:
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
            
            # Count by priority
            priority_100 = sum(1 for s in self._servers.values() if s['priority'] == 100)
            priority_50 = sum(1 for s in self._servers.values() if s['priority'] == 50)
            priority_30 = sum(1 for s in self._servers.values() if s['priority'] == 30)
            
            return {
                'available': len(self._servers),
                'priority_6_7': priority_100,
                'priority_5': priority_50,
                'priority_8': priority_30,
                'seen_total': len(self._seen_jobs),
                'dead_servers': len(self._dead_servers),
                'given_cooldown': len(self._given_servers),
                'total_given': self._stats['total_given'],
                'total_received': self._stats['total_received'],
                'duplicates_skipped': self._stats['duplicates_skipped'],
                'cooldown_skipped': self._stats['cooldown_skipped'],
                'low_player_skipped': self._stats['low_player_skipped'],
                'dead_reports': self._stats['dead_skipped'],
                'fetch_errors': self._stats['fetch_errors'],
                'fetch_success': self._stats['fetch_success']
            }

pool = ServerPool()

# ==================== DEEP FETCHER ====================
class DeepFetcher:
    def __init__(self):
        self.running = False
        self.cursor_lock = threading.Lock()
        self.cursors_asc = deque(maxlen=MAX_STORED_CURSORS)
        self.cursors_desc = deque(maxlen=MAX_STORED_CURSORS)
        
        # Stats
        self.last_reset = time.time()
        self.servers_this_minute = 0
        self.pages_this_minute = 0
    
    def fetch_page(self, cursor=None, sort_order='Asc'):
        try:
            url = f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public"
            params = {'sortOrder': sort_order, 'limit': SERVERS_PER_REQUEST}
            if cursor:
                params['cursor'] = cursor
            
            response = requests.get(
                url,
                params=params,
                proxies=get_proxy(),
                timeout=10,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json',
                    'Accept-Language': 'en-US,en;q=0.9'
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                servers = data.get('data', [])
                next_cursor = data.get('nextPageCursor')
                pool.record_fetch_success()
                return servers, next_cursor, sort_order
            elif response.status_code == 429:
                time.sleep(1)
                return [], None, sort_order
            else:
                pool.record_fetch_error()
                return [], None, sort_order
                
        except Exception as e:
            pool.record_fetch_error()
            return [], None, sort_order
    
    def fetch_cycle(self):
        fetches = []
        
        # Always start fresh (both directions)
        fetches.append((None, 'Asc'))
        fetches.append((None, 'Desc'))
        
        # Sample cursors from different depths
        with self.cursor_lock:
            asc_list = list(self.cursors_asc)
            desc_list = list(self.cursors_desc)
            
            # Sample evenly across all depths for Asc
            if asc_list:
                step = max(1, len(asc_list) // 60)
                for i in range(0, len(asc_list), step):
                    fetches.append((asc_list[i], 'Asc'))
            
            # Sample evenly across all depths for Desc
            if desc_list:
                step = max(1, len(desc_list) // 60)
                for i in range(0, len(desc_list), step):
                    fetches.append((desc_list[i], 'Desc'))
        
        # Limit per cycle
        fetches = fetches[:CURSORS_PER_CYCLE]
        
        total_added = 0
        new_cursors_asc = []
        new_cursors_desc = []
        pages_fetched = 0
        
        # Execute in parallel
        with ThreadPoolExecutor(max_workers=FETCH_THREADS) as executor:
            futures = {executor.submit(self.fetch_page, c, s): (c, s) for c, s in fetches}
            
            for future in as_completed(futures):
                try:
                    servers, next_cursor, sort_order = future.result()
                    pages_fetched += 1
                    
                    if servers:
                        added, _ = pool.add_servers(servers, source=f"main-{sort_order}")
                        total_added += added
                    
                    if next_cursor:
                        if sort_order == 'Asc':
                            new_cursors_asc.append(next_cursor)
                        else:
                            new_cursors_desc.append(next_cursor)
                except:
                    pass
        
        # Store new cursors for deeper fetching
        with self.cursor_lock:
            for c in new_cursors_asc:
                self.cursors_asc.append(c)
            for c in new_cursors_desc:
                self.cursors_desc.append(c)
        
        # Stats logging
        self.servers_this_minute += total_added
        self.pages_this_minute += pages_fetched
        
        now = time.time()
        if now - self.last_reset >= 60:
            stats = pool.get_stats()
            with self.cursor_lock:
                cursor_count = len(self.cursors_asc) + len(self.cursors_desc)
            
            log.info(f"[MAIN] +{self.servers_this_minute}/min | Pool: {stats['available']} (6-7: {stats['priority_6_7']}) | Pages: {self.pages_this_minute} | Cursors: {cursor_count} | Given: {stats['total_given']}")
            
            self.servers_this_minute = 0
            self.pages_this_minute = 0
            self.last_reset = now
        
        return total_added
    
    def run(self):
        self.running = True
        log.info("[MAIN FETCHER] Started - Deep pagination enabled")
        log.info(f"[CONFIG] Threads: {FETCH_THREADS} | Interval: {FETCH_INTERVAL}s | Cursors/cycle: {CURSORS_PER_CYCLE} | Max cursors: {MAX_STORED_CURSORS}")
        
        while self.running:
            try:
                self.fetch_cycle()
            except Exception as e:
                log.error(f"[FETCHER] Error: {e}")
            time.sleep(FETCH_INTERVAL)
    
    def start(self):
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()

fetcher = DeepFetcher()

# ==================== API ENDPOINTS ====================

@app.route('/status', methods=['GET'])
def status():
    stats = pool.get_stats()
    with fetcher.cursor_lock:
        stats['cursors_asc'] = len(fetcher.cursors_asc)
        stats['cursors_desc'] = len(fetcher.cursors_desc)
        stats['cursors_total'] = stats['cursors_asc'] + stats['cursors_desc']
    return jsonify(stats)

@app.route('/get-server', methods=['GET'])
def get_server():
    bot_id = request.args.get('bot_id', 'unknown')
    server = pool.get_server(bot_id=bot_id)
    if not server:
        return jsonify({'error': 'No servers available'}), 404
    return jsonify(server)

@app.route('/report-dead', methods=['POST'])
def report_dead():
    data = request.get_json() or {}
    job_id = data.get('job_id')
    reason = data.get('reason', 'unknown')
    if job_id:
        pool.report_dead(job_id, reason)
    return jsonify({'status': 'noted'})

@app.route('/add-pool', methods=['POST'])
def add_pool():
    data = request.get_json() or {}
    servers = data.get('servers', [])
    source = data.get('source', 'mini-api')
    
    if not servers:
        return jsonify({'added': 0}), 200
    
    added, dupes = pool.add_servers(servers, source=source)
    return jsonify({
        'added': added,
        'duplicates': dupes,
        'pool_size': pool.count()
    })

@app.route('/health', methods=['GET'])
def health():
    stats = pool.get_stats()
    return jsonify({
        'status': 'ok',
        'servers': stats['available'],
        'given': stats['total_given']
    })

@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'service': 'Main API - Ultimate Server Hopper',
        'status': 'running',
        'servers': pool.count()
    })

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    
    log.info("=" * 60)
    log.info("  MAIN API - ULTIMATE SERVER HOPPER")
    log.info("=" * 60)
    log.info(f"  Port: {port}")
    log.info(f"  Fetch Threads: {FETCH_THREADS}")
    log.info(f"  Max Cursors: {MAX_STORED_CURSORS}")
    log.info(f"  Server TTL: {SERVER_TTL}s")
    log.info("=" * 60)
    
    fetcher.start()
    time.sleep(2)
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)

