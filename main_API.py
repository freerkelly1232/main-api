#!/usr/bin/env python3
"""
MAIN API - Deep Pagination + 5-8 Priority
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
SERVER_TTL = 1800
PLACE_ID = 109983668079237

# NAProxy
PROXY_HOST = "us.naproxy.net"
PROXY_PORT = "1000"
PROXY_USER = "proxy-e5a1ntzmrlr3_area-US"
PROXY_PASS = "Ol43jGdsIuPUNacc"

# Deep fetching
FETCH_THREADS = 25
FETCH_INTERVAL = 1
SERVERS_PER_REQUEST = 100
CURSORS_PER_CYCLE = 200
MAX_STORED_CURSORS = 3000

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
        self._servers = {}
        self._seen_jobs = OrderedDict()
        self._dead_servers = set()
        self._given_servers = {}
        self._stats = {
            'total_given': 0,
            'total_received': 0,
            'duplicates_skipped': 0,
            'dead_skipped': 0,
            'fetch_errors': 0,
            'fetch_success': 0
        }
    
    def _clean_expired(self):
        now = time.time()
        
        expired = [jid for jid, data in self._servers.items() 
                   if now - data['added_time'] > SERVER_TTL]
        for jid in expired:
            del self._servers[jid]
            if jid in self._seen_jobs:
                del self._seen_jobs[jid]
        
        while len(self._seen_jobs) > 100000:
            self._seen_jobs.popitem(last=False)
        
        old_given = [k for k, v in self._given_servers.items() if now - v > 120]
        for k in old_given:
            del self._given_servers[k]
        
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
                
                # Priority: 5=50, 6-7=100, 8=20
                if players == 5:
                    priority = 50
                elif players == 6 or players == 7:
                    priority = 100
                elif players == 8:
                    priority = 20
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
        
        return added, duplicates
    
    def get_server(self):
        now = time.time()
        
        with self._lock:
            self._clean_expired()
            
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
            
            available.sort(key=lambda x: x[1]['priority'], reverse=True)
            
            job_id, data = available[0]
            
            self._given_servers[job_id] = now
            del self._servers[job_id]
            
            self._stats['total_given'] += 1
            
            return {
                'job_id': job_id,
                'players': data['players'],
                'priority': data['priority'],
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
                'total_given': self._stats['total_given'],
                'total_received': self._stats['total_received'],
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
                
        except:
            pool.record_fetch_error()
            return [], None, sort_order
    
    def fetch_cycle(self):
        fetches = []
        
        fetches.append((None, 'Asc'))
        fetches.append((None, 'Desc'))
        
        with self.cursor_lock:
            asc_list = list(self.cursors_asc)
            desc_list = list(self.cursors_desc)
            
            step = max(1, len(asc_list) // 50)
            for i in range(0, len(asc_list), step):
                fetches.append((asc_list[i], 'Asc'))
            
            step = max(1, len(desc_list) // 50)
            for i in range(0, len(desc_list), step):
                fetches.append((desc_list[i], 'Desc'))
        
        fetches = fetches[:CURSORS_PER_CYCLE]
        
        total_added = 0
        new_asc = []
        new_desc = []
        
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
                            new_asc.append(next_cursor)
                        else:
                            new_desc.append(next_cursor)
                except:
                    pass
        
        with self.cursor_lock:
            for c in new_asc:
                self.cursors_asc.append(c)
            for c in new_desc:
                self.cursors_desc.append(c)
        
        self.servers_this_minute += total_added
        now = time.time()
        if now - self.last_reset >= 60:
            stats = pool.get_stats()
            with self.cursor_lock:
                cursor_count = len(self.cursors_asc) + len(self.cursors_desc)
            log.info(f"[RATE] +{self.servers_this_minute}/min | Pool: {stats['available']} | Cursors: {cursor_count}")
            self.servers_this_minute = 0
            self.last_reset = now
        
        return total_added
    
    def run(self):
        self.running = True
        log.info("[FETCHER] Started - Deep pagination")
        
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
    server = pool.get_server()
    if not server:
        return jsonify({'error': 'No servers available'}), 404
    return jsonify(server)

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

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    log.info(f"[STARTUP] Main API on port {port}")
    fetcher.start()
    time.sleep(2)
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
