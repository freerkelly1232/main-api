#!/usr/bin/env python3
"""
MAIN API v2 - Deep Pagination + Smart Distribution
- Fetches DEEP into server list (not just first pages)
- Tracks scanned servers (don't re-scan within cooldown)
- Spreads bots across MORE unique servers
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
SERVER_TTL = 1800  # 30 minutes
PLACE_ID = 109983668079237

# NAProxy Config
PROXY_HOST = "us.naproxy.net"
PROXY_PORT = "1000"
PROXY_USER = "proxy-e5a1ntzmrlr3_area-US"
PROXY_PASS = "Ol43jGdsIuPUNacc"

# DEEP FETCHING - go way deeper into server list
FETCH_THREADS = 25          # More parallel fetches
FETCH_INTERVAL = 1          # Every second
SERVERS_PER_REQUEST = 100
CURSORS_PER_CYCLE = 150     # Fetch 150 different pages per cycle
MAX_STORED_CURSORS = 2000   # Store more cursors to go deeper

# SMART DISTRIBUTION
SCAN_COOLDOWN = 300         # Don't re-give scanned server for 5 minutes
GIVEN_COOLDOWN = 60         # Don't re-give server for 60 seconds
MAX_BOTS_PER_SERVER = 1     # Only 1 bot per server at a time

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ==================== PROXY ====================
def get_proxy():
    session_id = f"s{random.randint(100000, 999999)}"
    proxy_url = f"http://{PROXY_USER}_session-{session_id}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    return {'http': proxy_url, 'https': proxy_url}

# ==================== SERVER POOL ====================
class ServerPool:
    def __init__(self):
        self._lock = threading.Lock()
        
        # Server storage - use dict for priority access
        self._servers = {}  # job_id -> {players, added_time, priority}
        self._server_queue = deque(maxlen=100000)  # For ordering
        
        # Tracking sets
        self._seen_jobs = OrderedDict()      # All seen servers
        self._dead_servers = set()            # Failed servers
        self._given_servers = {}              # job_id -> timestamp (recently given)
        self._scanned_servers = {}            # job_id -> timestamp (bot finished scanning)
        self._active_scans = {}               # job_id -> bot_id (currently being scanned)
        
        self._stats = {
            'total_given': 0,
            'total_received': 0,
            'total_unique': 0,
            'duplicates_skipped': 0,
            'cooldown_skipped': 0,
            'dead_skipped': 0,
            'scanned_skipped': 0,
            'fetch_errors': 0,
            'fetch_success': 0
        }
    
    def _clean_expired(self):
        now = time.time()
        
        # Clean old servers
        expired_jobs = []
        for job_id, data in list(self._servers.items()):
            if now - data['added_time'] > SERVER_TTL:
                expired_jobs.append(job_id)
        
        for job_id in expired_jobs:
            del self._servers[job_id]
            if job_id in self._seen_jobs:
                del self._seen_jobs[job_id]
        
        # Clean old seen jobs (keep 100k)
        while len(self._seen_jobs) > 100000:
            self._seen_jobs.popitem(last=False)
        
        # Clean old given tracking
        old_given = [k for k, v in self._given_servers.items() if now - v > GIVEN_COOLDOWN]
        for k in old_given:
            del self._given_servers[k]
        
        # Clean old scanned tracking (keep for cooldown period)
        old_scanned = [k for k, v in self._scanned_servers.items() if now - v > SCAN_COOLDOWN]
        for k in old_scanned:
            del self._scanned_servers[k]
        
        # Clean old active scans (timeout after 30s)
        old_active = [k for k, v in self._active_scans.items() if now - v.get('time', 0) > 30]
        for k in old_active:
            del self._active_scans[k]
        
        # Clean dead servers (keep last 10k)
        if len(self._dead_servers) > 10000:
            self._dead_servers = set(list(self._dead_servers)[-5000:])
    
    def add_servers(self, servers, source="unknown"):
        added = 0
        duplicates = 0
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
                
                # Calculate priority score
                # Lower player count = higher priority (less likely to be scanned)
                # Prefer 3-6 players
                if players <= 2:
                    priority = 50  # Too empty, might be dead
                elif players <= 4:
                    priority = 100  # Best - low competition
                elif players <= 6:
                    priority = 80   # Good
                elif players == 7:
                    priority = 40   # Getting full
                else:
                    priority = 10   # Full, skip mostly
                
                self._servers[job_id] = {
                    'players': players,
                    'added_time': now,
                    'priority': priority
                }
                self._seen_jobs[job_id] = now
                self._server_queue.append(job_id)
                added += 1
            
            self._stats['total_received'] += added
            self._stats['duplicates_skipped'] += duplicates
            if added > 0:
                self._stats['total_unique'] = len(self._seen_jobs)
        
        return added, duplicates
    
    def get_server(self, bot_id=None):
        """Get best available server for a bot"""
        now = time.time()
        
        with self._lock:
            self._clean_expired()
            
            # Sort servers by priority (highest first)
            available = []
            for job_id, data in self._servers.items():
                # Skip if dead
                if job_id in self._dead_servers:
                    continue
                
                # Skip if recently given to another bot
                if job_id in self._given_servers:
                    if now - self._given_servers[job_id] < GIVEN_COOLDOWN:
                        self._stats['cooldown_skipped'] += 1
                        continue
                
                # Skip if recently scanned
                if job_id in self._scanned_servers:
                    if now - self._scanned_servers[job_id] < SCAN_COOLDOWN:
                        self._stats['scanned_skipped'] += 1
                        continue
                
                # Skip if currently being scanned by another bot
                if job_id in self._active_scans:
                    continue
                
                # Skip expired
                if now - data['added_time'] > SERVER_TTL:
                    continue
                
                available.append((job_id, data))
            
            if not available:
                return None
            
            # Sort by priority (highest first), then by freshness
            available.sort(key=lambda x: (x[1]['priority'], -x[1]['added_time']), reverse=True)
            
            # Get best server
            job_id, data = available[0]
            
            # Mark as given and active
            self._given_servers[job_id] = now
            self._active_scans[job_id] = {'bot': bot_id, 'time': now}
            
            # Remove from pool (one-time use)
            del self._servers[job_id]
            
            self._stats['total_given'] += 1
            
            return {
                'job_id': job_id,
                'players': data['players'],
                'priority': data['priority'],
                'age': int(now - data['added_time'])
            }
    
    def report_scanned(self, job_id, bot_id=None, found_count=0):
        """Bot reports it finished scanning a server"""
        with self._lock:
            self._scanned_servers[job_id] = time.time()
            if job_id in self._active_scans:
                del self._active_scans[job_id]
    
    def report_dead(self, job_id):
        """Mark server as dead"""
        with self._lock:
            self._dead_servers.add(job_id)
            if job_id in self._servers:
                del self._servers[job_id]
            if job_id in self._active_scans:
                del self._active_scans[job_id]
    
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
            return {
                'available': len(self._servers),
                'total_unique_seen': len(self._seen_jobs),
                'dead_servers': len(self._dead_servers),
                'given_cooldown': len(self._given_servers),
                'scanned_cooldown': len(self._scanned_servers),
                'active_scans': len(self._active_scans),
                'total_given': self._stats['total_given'],
                'total_received': self._stats['total_received'],
                'duplicates_skipped': self._stats['duplicates_skipped'],
                'cooldown_skipped': self._stats['cooldown_skipped'],
                'scanned_skipped': self._stats['scanned_skipped'],
                'fetch_errors': self._stats['fetch_errors'],
                'fetch_success': self._stats['fetch_success']
            }

pool = ServerPool()

# ==================== DEEP FETCHER ====================
class DeepFetcher:
    def __init__(self):
        self.running = False
        self.cursor_lock = threading.Lock()
        
        # Store cursors at different depths
        self.cursors_asc = deque(maxlen=MAX_STORED_CURSORS)   # Ascending cursors
        self.cursors_desc = deque(maxlen=MAX_STORED_CURSORS)  # Descending cursors
        
        # Stats
        self.last_reset = time.time()
        self.servers_this_minute = 0
        self.pages_fetched = 0
    
    def fetch_page(self, cursor=None, sort_order='Asc'):
        """Fetch a page of servers"""
        try:
            url = f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public"
            params = {
                'sortOrder': sort_order,
                'limit': SERVERS_PER_REQUEST
            }
            if cursor:
                params['cursor'] = cursor
            
            response = requests.get(
                url,
                params=params,
                proxies=get_proxy(),
                timeout=10,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/json'
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                servers = data.get('data', [])
                next_cursor = data.get('nextPageCursor')
                pool.record_fetch_success()
                return servers, next_cursor, sort_order
            elif response.status_code == 429:
                time.sleep(0.5)
                return [], None, sort_order
            else:
                pool.record_fetch_error()
                return [], None, sort_order
                
        except Exception as e:
            pool.record_fetch_error()
            return [], None, sort_order
    
    def fetch_cycle(self):
        """Fetch from multiple depths - both Asc and Desc"""
        
        # Build list of fetches to do
        fetches = []
        
        # Always fetch fresh (no cursor) - both directions
        fetches.append((None, 'Asc'))
        fetches.append((None, 'Desc'))
        
        # Add stored cursors from different depths
        with self.cursor_lock:
            # Get cursors from Asc (spread across different depths)
            asc_list = list(self.cursors_asc)
            if asc_list:
                # Sample from different parts of the list (shallow, middle, deep)
                sample_count = min(len(asc_list), CURSORS_PER_CYCLE // 2)
                indices = [int(i * len(asc_list) / sample_count) for i in range(sample_count)]
                for i in indices:
                    if i < len(asc_list):
                        fetches.append((asc_list[i], 'Asc'))
            
            # Same for Desc
            desc_list = list(self.cursors_desc)
            if desc_list:
                sample_count = min(len(desc_list), CURSORS_PER_CYCLE // 2)
                indices = [int(i * len(desc_list) / sample_count) for i in range(sample_count)]
                for i in indices:
                    if i < len(desc_list):
                        fetches.append((desc_list[i], 'Desc'))
        
        # Limit fetches per cycle
        fetches = fetches[:CURSORS_PER_CYCLE]
        
        total_added = 0
        new_cursors_asc = []
        new_cursors_desc = []
        
        # Execute fetches in parallel
        with ThreadPoolExecutor(max_workers=FETCH_THREADS) as executor:
            futures = {executor.submit(self.fetch_page, cursor, sort): (cursor, sort) 
                      for cursor, sort in fetches}
            
            for future in as_completed(futures):
                try:
                    servers, next_cursor, sort_order = future.result()
                    
                    if servers:
                        added, _ = pool.add_servers(servers, source=f"fetcher-{sort_order}")
                        total_added += added
                        self.pages_fetched += 1
                    
                    if next_cursor:
                        if sort_order == 'Asc':
                            new_cursors_asc.append(next_cursor)
                        else:
                            new_cursors_desc.append(next_cursor)
                            
                except Exception as e:
                    pass
        
        # Store new cursors
        with self.cursor_lock:
            for c in new_cursors_asc:
                self.cursors_asc.append(c)
            for c in new_cursors_desc:
                self.cursors_desc.append(c)
        
        # Log stats every minute
        self.servers_this_minute += total_added
        now = time.time()
        if now - self.last_reset >= 60:
            stats = pool.get_stats()
            with self.cursor_lock:
                cursor_count = len(self.cursors_asc) + len(self.cursors_desc)
            log.info(f"[RATE] +{self.servers_this_minute}/min | Pool: {stats['available']} | Unique: {stats['total_unique_seen']} | Cursors: {cursor_count} | Pages: {self.pages_fetched}")
            self.servers_this_minute = 0
            self.pages_fetched = 0
            self.last_reset = now
        
        return total_added
    
    def run(self):
        self.running = True
        log.info("[FETCHER] Started - Deep pagination enabled")
        log.info(f"[CONFIG] Threads={FETCH_THREADS} | Cursors/cycle={CURSORS_PER_CYCLE} | Max cursors={MAX_STORED_CURSORS}")
        
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
    return jsonify(stats)

@app.route('/get-server', methods=['GET'])
def get_server():
    bot_id = request.args.get('bot_id', 'unknown')
    server = pool.get_server(bot_id=bot_id)
    if not server:
        return jsonify({'error': 'No servers available'}), 404
    return jsonify(server)

@app.route('/scanned', methods=['POST'])
def scanned():
    """Bot reports it finished scanning a server"""
    data = request.get_json() or {}
    job_id = data.get('job_id')
    bot_id = data.get('bot_id')
    found_count = data.get('found_count', 0)
    
    if job_id:
        pool.report_scanned(job_id, bot_id=bot_id, found_count=found_count)
    
    return jsonify({'status': 'noted'})

@app.route('/report-dead', methods=['POST'])
def report_dead():
    data = request.get_json() or {}
    job_id = data.get('job_id')
    if job_id:
        pool.report_dead(job_id)
    return jsonify({'status': 'noted'})

@app.route('/add-pool', methods=['POST'])
def add_pool():
    """Receive servers from mini API"""
    data = request.get_json() or {}
    servers = data.get('servers', [])
    source = data.get('source', 'mini-api')
    
    if not servers:
        return jsonify({'added': 0}), 200
    
    added, dupes = pool.add_servers(servers, source=source)
    log.info(f"[RECEIVE] {source}: +{added} new, {dupes} dupes | Pool: {pool.count()}")
    return jsonify({'added': added, 'duplicates': dupes, 'pool_size': pool.count()})

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'servers': pool.count()})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    
    log.info(f"[STARTUP] Main API v2 on port {port}")
    log.info(f"[CONFIG] Deep pagination + Smart distribution enabled")
    log.info(f"[CONFIG] Scan cooldown: {SCAN_COOLDOWN}s | Given cooldown: {GIVEN_COOLDOWN}s")
    
    fetcher.start()
    time.sleep(2)
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
