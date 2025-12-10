#!/usr/bin/env python3
"""
MAIN API - High Volume Server Pool with NAProxy
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
PROXY_USER = "proxy-e5a1ntzmrlr3_area-BR-"
PROXY_PASS = "Ol43jGdsIuPUNacc"

# Reduced fetching (proxy friendly)
FETCH_THREADS = 8
FETCH_INTERVAL = 2
SERVERS_PER_REQUEST = 100
CURSORS_PER_CYCLE = 30

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
        self._servers = deque(maxlen=100000)
        self._seen_jobs = OrderedDict()
        self._dead_servers = set()
        self._given_servers = {}
        self._stats = {
            'total_given': 0,
            'total_received': 0,
            'total_sent_to_us': 0,
            'expired': 0,
            'duplicates_skipped': 0,
            'dead_skipped': 0,
            'fetch_errors': 0,
            'fetch_success': 0
        }
    
    def _clean_expired(self):
        now = time.time()
        expired = 0
        
        while self._servers:
            job_id, players, added_time, priority = self._servers[0]
            if now - added_time > SERVER_TTL:
                self._servers.popleft()
                if job_id in self._seen_jobs:
                    del self._seen_jobs[job_id]
                expired += 1
            else:
                break
        
        # Expire old seen jobs (allow re-discovery after 10 min)
        old_seen = [k for k, v in self._seen_jobs.items() if now - v > 600]
        for k in old_seen:
            del self._seen_jobs[k]
        
        # Trim if still too big
        while len(self._seen_jobs) > 50000:
            self._seen_jobs.popitem(last=False)
        
        old_given = [k for k, v in self._given_servers.items() if now - v > 600]
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
            self._stats['total_sent_to_us'] += len(servers)
            
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
                
                if job_id in self._dead_servers:
                    dead += 1
                    continue
                
                if job_id in self._seen_jobs:
                    duplicates += 1
                    continue
                
                if job_id in self._given_servers:
                    duplicates += 1
                    continue
                
                # Skip servers with less than 5 players (low value)
                if players < 5:
                    continue
                
                # Priority based on player count
                if players == 6 or players == 7:
                    priority = 100  # Best
                elif players == 5:
                    priority = 50
                else:
                    priority = 10
                
                self._servers.append((job_id, players, now, priority))
                self._seen_jobs[job_id] = now
                added += 1
            
            self._stats['total_received'] += added
            self._stats['duplicates_skipped'] += duplicates
            self._stats['dead_skipped'] += dead
        
        return added, duplicates, dead, len(servers)
    
    def get_server(self):
        now = time.time()
        with self._lock:
            # Convert to list and sort by priority
            server_list = list(self._servers)
            server_list.sort(key=lambda x: x[3], reverse=True)  # Sort by priority (highest first)
            
            for job_id, players, added_time, priority in server_list:
                if now - added_time > SERVER_TTL:
                    self._stats['expired'] += 1
                    continue
                
                if job_id in self._dead_servers:
                    self._stats['dead_skipped'] += 1
                    continue
                
                if job_id in self._given_servers:
                    continue
                
                # Remove from queue
                try:
                    self._servers.remove((job_id, players, added_time, priority))
                except:
                    pass
                
                self._given_servers[job_id] = now
                self._stats['total_given'] += 1
                
                return {'job_id': job_id, 'players': players, 'priority': priority, 'age': int(now - added_time)}
            
            return None
    
    def get_batch(self, count):
        results = []
        for _ in range(count):
            server = self.get_server()
            if server:
                results.append(server)
            else:
                break
        return results
    
    def report_dead(self, job_id):
        with self._lock:
            self._dead_servers.add(job_id)
            if len(self._dead_servers) > 10000:
                self._dead_servers = set(list(self._dead_servers)[-5000:])
    
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
                'total_sent_to_us': self._stats['total_sent_to_us'],
                'expired': self._stats['expired'],
                'duplicates_skipped': self._stats['duplicates_skipped'],
                'dead_skipped': self._stats['dead_skipped'],
                'fetch_errors': self._stats['fetch_errors'],
                'fetch_success': self._stats['fetch_success'],
                'ttl_seconds': SERVER_TTL
            }

pool = ServerPool()

# ==================== ROBLOX FETCHER ====================
class RobloxFetcher:
    def __init__(self):
        self.running = False
        self.cursors_asc = deque(maxlen=2000)  # Deep Asc cursors
        self.cursors_desc = deque(maxlen=2000)  # Deep Desc cursors
        self.cursor_lock = threading.Lock()
        self.last_reset = time.time()
        self.servers_this_minute = 0
        self.minute_lock = threading.Lock()
    
    def fetch_page(self, cursor=None, sort_order='Asc'):
        max_retries = 2
        for attempt in range(max_retries):
            try:
                url = f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public"
                params = {
                    'sortOrder': sort_order,
                    'limit': SERVERS_PER_REQUEST,
                    'excludeFullGames': 'true'
                }
                if cursor:
                    params['cursor'] = cursor
                
                proxies = get_proxy()
                
                response = requests.get(
                    url,
                    params=params,
                    proxies=proxies,
                    timeout=15,
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
                    time.sleep(2)
                    return [], None, sort_order
                else:
                    pool.record_fetch_error()
                    return [], None, sort_order
                    
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError):
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                pool.record_fetch_error()
                return [], None, sort_order
            except Exception as e:
                pool.record_fetch_error()
                return [], None, sort_order
        
        return [], None, sort_order
    
    def fetch_cycle(self):
        fetches = []
        
        # Always start fresh from both ends
        fetches.append((None, 'Asc'))
        fetches.append((None, 'Desc'))
        
        # Sample DEEP cursors from both directions
        with self.cursor_lock:
            asc_list = list(self.cursors_asc)
            desc_list = list(self.cursors_desc)
            
            # Take cursors from END of list (deepest into the middle)
            if asc_list:
                # Prioritize DEEP cursors (end of list = closer to middle)
                deep_asc = asc_list[-min(20, len(asc_list)):]  # Last 20 (deepest)
                for c in deep_asc:
                    fetches.append((c, 'Asc'))
            
            if desc_list:
                # Prioritize DEEP cursors
                deep_desc = desc_list[-min(20, len(desc_list)):]
                for c in deep_desc:
                    fetches.append((c, 'Desc'))
        
        # Limit per cycle
        fetches = fetches[:CURSORS_PER_CYCLE]
        
        total_added = 0
        new_cursors_asc = []
        new_cursors_desc = []
        
        with ThreadPoolExecutor(max_workers=FETCH_THREADS) as executor:
            futures = {executor.submit(self.fetch_page, c, s): (c, s) for c, s in fetches}
            
            for future in as_completed(futures):
                try:
                    servers, next_cursor, sort_order = future.result()
                    if servers:
                        added, dupes, dead, total = pool.add_servers(servers, source="fetcher")
                        total_added += added
                    if next_cursor:
                        if sort_order == 'Asc':
                            new_cursors_asc.append(next_cursor)
                        else:
                            new_cursors_desc.append(next_cursor)
                except Exception as e:
                    pass
        
        # Store new cursors (deeper into the list)
        with self.cursor_lock:
            for c in new_cursors_asc:
                self.cursors_asc.append(c)
            for c in new_cursors_desc:
                self.cursors_desc.append(c)
        
        with self.minute_lock:
            self.servers_this_minute += total_added
            now = time.time()
            if now - self.last_reset >= 60:
                rate = self.servers_this_minute
                stats = pool.get_stats()
                with self.cursor_lock:
                    depth = len(self.cursors_asc) + len(self.cursors_desc)
                log.info(f"[RATE] +{rate}/min | Pool: {stats['available']} | Seen: {stats['seen_total']} | Given: {stats['total_given']} | Depth: {depth}")
                self.servers_this_minute = 0
                self.last_reset = now
        
        return total_added
    
    def run(self):
        self.running = True
        log.info("[FETCHER] Started - Deep pagination to find middle servers")
        
        while self.running:
            try:
                self.fetch_cycle()
            except Exception as e:
                log.error(f"[FETCHER] Error: {e}")
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
    count = request.args.get('count', default=100, type=int)
    count = min(count, 200)
    servers = pool.get_batch(count)
    if not servers:
        return jsonify({'servers': [], 'count': 0}), 200
    return jsonify({'servers': servers, 'count': len(servers)})

@app.route('/add-pool', methods=['POST'])
def add_pool():
    data = request.get_json() or {}
    servers = data.get('servers', [])
    source = data.get('source', 'mini-api')
    
    if not servers:
        return jsonify({'added': 0}), 200
    
    added, dupes, dead, total = pool.add_servers(servers, source=source)
    log.info(f"[RECEIVE] {source}: {total} sent -> {added} new, {dupes} dupes, {dead} dead | Pool: {pool.count()}")
    return jsonify({'added': added, 'duplicates': dupes, 'dead': dead, 'pool_size': pool.count()})

@app.route('/report-dead', methods=['POST'])
def report_dead():
    data = request.get_json() or {}
    job_id = data.get('job_id')
    if job_id:
        pool.report_dead(job_id)
    return jsonify({'status': 'noted'})

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'servers': pool.count()})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    
    log.info(f"[STARTUP] Main API on port {port}")
    log.info(f"[CONFIG] TTL={SERVER_TTL}s | Threads={FETCH_THREADS}")
    
    fetcher.start()
    time.sleep(2)
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
