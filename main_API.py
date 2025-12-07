#!/usr/bin/env python3
"""
MAIN API v3 - Wealth-Based Server Targeting
- Deep pagination for more servers
- Wealth scoring from bot scouts
- Rich servers get highest priority
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

# DEEP FETCHING
FETCH_THREADS = 25
FETCH_INTERVAL = 1
SERVERS_PER_REQUEST = 100
CURSORS_PER_CYCLE = 200  # More pages
MAX_STORED_CURSORS = 3000  # Way deeper

# Priority settings
WEALTH_PRIORITY_BOOST = 500  # Big boost for wealthy servers
WEALTH_EXPIRE = 600  # Wealth data expires after 10 min

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
        self._servers = {}  # job_id -> {players, added_time, priority, wealth_score}
        self._seen_jobs = OrderedDict()
        self._dead_servers = set()
        self._given_servers = {}
        self._wealth_data = {}  # job_id -> {score, timestamp, details}
        self._stats = {
            'total_given': 0,
            'total_received': 0,
            'duplicates_skipped': 0,
            'dead_skipped': 0,
            'wealth_reports': 0,
            'fetch_errors': 0,
            'fetch_success': 0
        }
    
    def _clean_expired(self):
        now = time.time()
        
        # Clean expired servers
        expired = [jid for jid, data in self._servers.items() 
                   if now - data['added_time'] > SERVER_TTL]
        for jid in expired:
            del self._servers[jid]
            if jid in self._seen_jobs:
                del self._seen_jobs[jid]
        
        # Clean old seen jobs
        while len(self._seen_jobs) > 100000:
            self._seen_jobs.popitem(last=False)
        
        # Clean old given
        old_given = [k for k, v in self._given_servers.items() if now - v > 120]
        for k in old_given:
            del self._given_servers[k]
        
        # Clean old wealth data
        old_wealth = [k for k, v in self._wealth_data.items() if now - v['timestamp'] > WEALTH_EXPIRE]
        for k in old_wealth:
            del self._wealth_data[k]
        
        # Clean dead servers
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
                
                if job_id in self._dead_servers:
                    continue
                
                if job_id in self._seen_jobs:
                    duplicates += 1
                    continue
                
                # Base priority by player count
                if players == 5:
                    priority = 50
                elif players == 6 or players == 7:
                    priority = 100
                elif players == 8:
                    priority = 20
                else:
                    priority = 10
                
                # Check if we have wealth data for this server
                wealth_score = 0
                if job_id in self._wealth_data:
                    wealth_score = self._wealth_data[job_id]['score']
                    priority += wealth_score  # Boost priority based on wealth
                
                self._servers[job_id] = {
                    'players': players,
                    'added_time': now,
                    'priority': priority,
                    'wealth_score': wealth_score
                }
                self._seen_jobs[job_id] = now
                added += 1
            
            self._stats['total_received'] += added
            self._stats['duplicates_skipped'] += duplicates
        
        return added, duplicates
    
    def report_wealth(self, job_id, wealth_score, details=None):
        """Bot reports wealth score for a server"""
        now = time.time()
        
        with self._lock:
            self._wealth_data[job_id] = {
                'score': wealth_score,
                'timestamp': now,
                'details': details or {}
            }
            
            # Boost priority if server still in pool
            if job_id in self._servers:
                self._servers[job_id]['wealth_score'] = wealth_score
                self._servers[job_id]['priority'] += wealth_score
            
            self._stats['wealth_reports'] += 1
            
            if wealth_score >= 50:
                log.info(f"[WEALTH] High value server found! Score: {wealth_score} | Details: {details}")
    
    def get_server(self, bot_id=None):
        """Get best available server - prioritizes wealthy servers"""
        now = time.time()
        
        with self._lock:
            self._clean_expired()
            
            # Get available servers
            available = []
            for job_id, data in self._servers.items():
                if job_id in self._dead_servers:
                    continue
                if job_id in self._given_servers:
                    continue
                if now - data['added_time'] > SERVER_TTL:
                    continue
                available.append((job_id, data))
            
            if not available:
                return None
            
            # Sort by priority (highest first)
            available.sort(key=lambda x: x[1]['priority'], reverse=True)
            
            # Get best server
            job_id, data = available[0]
            
            # Mark as given
            self._given_servers[job_id] = now
            del self._servers[job_id]
            
            self._stats['total_given'] += 1
            
            return {
                'job_id': job_id,
                'players': data['players'],
                'priority': data['priority'],
                'wealth_score': data.get('wealth_score', 0),
                'age': int(now - data['added_time'])
            }
    
    def report_dead(self, job_id):
        with self._lock:
            self._dead_servers.add(job_id)
            if job_id in self._servers:
                del self._servers[job_id]
    
    def count(self):
        with self._lock:
            return len(self._servers)
    
    def get_wealthy_count(self):
        with self._lock:
            return len([s for s in self._servers.values() if s.get('wealth_score', 0) > 0])
    
    def record_fetch_error(self):
        with self._lock:
            self._stats['fetch_errors'] += 1
    
    def record_fetch_success(self):
        with self._lock:
            self._stats['fetch_success'] += 1
    
    def get_stats(self):
        with self._lock:
            self._clean_expired()
            wealthy = len([s for s in self._servers.values() if s.get('wealth_score', 0) > 0])
            return {
                'available': len(self._servers),
                'wealthy_servers': wealthy,
                'wealth_data_cached': len(self._wealth_data),
                'seen_total': len(self._seen_jobs),
                'dead_servers': len(self._dead_servers),
                'total_given': self._stats['total_given'],
                'total_received': self._stats['total_received'],
                'wealth_reports': self._stats['wealth_reports'],
                'duplicates_skipped': self._stats['duplicates_skipped'],
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
        self.last_reset = time.time()
        self.servers_this_minute = 0
    
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
        fetches = []
        
        # Fresh fetches
        fetches.append((None, 'Asc'))
        fetches.append((None, 'Desc'))
        
        # Deep cursors - sample from different depths
        with self.cursor_lock:
            asc_list = list(self.cursors_asc)
            desc_list = list(self.cursors_desc)
            
            # Sample evenly across all depths
            step = max(1, len(asc_list) // 50)
            for i in range(0, len(asc_list), step):
                fetches.append((asc_list[i], 'Asc'))
            
            step = max(1, len(desc_list) // 50)
            for i in range(0, len(desc_list), step):
                fetches.append((desc_list[i], 'Desc'))
        
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
                        added, _ = pool.add_servers(servers, source=f"fetcher-{sort_order}")
                        total_added += added
                    if next_cursor:
                        if sort_order == 'Asc':
                            new_cursors_asc.append(next_cursor)
                        else:
                            new_cursors_desc.append(next_cursor)
                except:
                    pass
        
        with self.cursor_lock:
            for c in new_cursors_asc:
                self.cursors_asc.append(c)
            for c in new_cursors_desc:
                self.cursors_desc.append(c)
        
        # Log stats
        self.servers_this_minute += total_added
        now = time.time()
        if now - self.last_reset >= 60:
            stats = pool.get_stats()
            with self.cursor_lock:
                cursor_count = len(self.cursors_asc) + len(self.cursors_desc)
            log.info(f"[RATE] +{self.servers_this_minute}/min | Pool: {stats['available']} | Wealthy: {stats['wealthy_servers']} | Cursors: {cursor_count}")
            self.servers_this_minute = 0
            self.last_reset = now
        
        return total_added
    
    def run(self):
        self.running = True
        log.info("[FETCHER] Started - Deep pagination + Wealth targeting")
        
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

@app.route('/report-wealth', methods=['POST'])
def report_wealth():
    """Bot reports wealth score for a server"""
    data = request.get_json() or {}
    job_id = data.get('job_id')
    wealth_score = data.get('wealth_score', 0)
    details = data.get('details', {})
    
    if job_id and wealth_score > 0:
        pool.report_wealth(job_id, wealth_score, details)
        log.info(f"[WEALTH] {job_id[:12]}... score={wealth_score}")
    
    return jsonify({'status': 'noted', 'score': wealth_score})

@app.route('/report-dead', methods=['POST'])
def report_dead():
    data = request.get_json() or {}
    job_id = data.get('job_id')
    if job_id:
        pool.report_dead(job_id)
    return jsonify({'status': 'noted'})

@app.route('/add-pool', methods=['POST'])
def add_pool():
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
    
    log.info(f"[STARTUP] Main API v3 - Wealth Targeting on port {port}")
    log.info(f"[CONFIG] Deep fetch: {CURSORS_PER_CYCLE} pages/cycle, {MAX_STORED_CURSORS} max cursors")
    
    fetcher.start()
    time.sleep(2)
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
