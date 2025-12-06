#!/usr/bin/env python3
"""
MAIN API - High Volume Server Pool with NAProxy
"""

import os
import time
import logging
import threading
import requests
from collections import deque
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

# Fetching config
FETCH_THREADS = 10
FETCH_INTERVAL = 2
SERVERS_PER_REQUEST = 100
CURSORS_PER_CYCLE = 50

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ==================== PROXY ====================
def get_proxy():
    session_id = f"{random.randint(100000, 999999)}"
    proxy_url = f"http://{PROXY_USER}_session-{session_id}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    return {
        'http': proxy_url,
        'https': proxy_url
    }

# ==================== SERVER POOL ====================
class ServerPool:
    def __init__(self):
        self._lock = threading.Lock()
        self._servers = deque()
        self._seen_jobs = set()
        self._stats = {
            'total_given': 0, 
            'total_received': 0, 
            'expired': 0,
            'duplicates_skipped': 0,
            'fetch_errors': 0,
            'fetch_success': 0
        }
    
    def _clean_expired(self):
        now = time.time()
        expired = 0
        expired_jobs = []
        
        while self._servers:
            job_id, players, added_time = self._servers[0]
            if now - added_time > SERVER_TTL:
                self._servers.popleft()
                expired_jobs.append(job_id)
                expired += 1
            else:
                break
        
        for job_id in expired_jobs:
            self._seen_jobs.discard(job_id)
        
        if expired:
            self._stats['expired'] += expired
        return expired
    
    def add_servers(self, servers):
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
                
                if job_id and job_id not in self._seen_jobs:
                    self._servers.append((job_id, players, now))
                    self._seen_jobs.add(job_id)
                    added += 1
                else:
                    duplicates += 1
            
            self._stats['total_received'] += added
            self._stats['duplicates_skipped'] += duplicates
        
        return added
    
    def get_server(self):
        now = time.time()
        with self._lock:
            while self._servers:
                job_id, players, added_time = self._servers.popleft()
                
                if now - added_time > SERVER_TTL:
                    self._stats['expired'] += 1
                    self._seen_jobs.discard(job_id)
                    continue
                
                self._stats['total_given'] += 1
                return {'job_id': job_id, 'players': players, 'age': int(now - added_time)}
            
            return None
    
    def get_batch(self, count):
        results = []
        now = time.time()
        
        with self._lock:
            while self._servers and len(results) < count:
                job_id, players, added_time = self._servers.popleft()
                
                if now - added_time > SERVER_TTL:
                    self._stats['expired'] += 1
                    self._seen_jobs.discard(job_id)
                    continue
                
                results.append({
                    'job_id': job_id, 
                    'players': players,
                    'age': int(now - added_time)
                })
                self._stats['total_given'] += 1
        
        return results
    
    def count(self):
        with self._lock:
            self._clean_expired()
            return len(self._servers)
    
    def seen_count(self):
        with self._lock:
            return len(self._seen_jobs)
    
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
                'total_given': self._stats['total_given'],
                'total_received': self._stats['total_received'],
                'expired': self._stats['expired'],
                'duplicates_skipped': self._stats['duplicates_skipped'],
                'fetch_errors': self._stats['fetch_errors'],
                'fetch_success': self._stats['fetch_success'],
                'ttl_seconds': SERVER_TTL
            }

pool = ServerPool()

# ==================== ROBLOX FETCHER ====================
class RobloxFetcher:
    def __init__(self):
        self.running = False
        self.cursors = set()
        self.last_reset = time.time()
        self.servers_this_minute = 0
        self.minute_lock = threading.Lock()
    
    def fetch_page(self, cursor=None):
        try:
            url = f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public?sortOrder=Asc&limit={SERVERS_PER_REQUEST}"
            if cursor:
                url += f"&cursor={cursor}"
            
            proxies = get_proxy()
            
            response = requests.get(
                url,
                proxies=proxies,
                timeout=15,
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
                return servers, next_cursor
            else:
                log.warning(f"Roblox API returned {response.status_code}")
                pool.record_fetch_error()
                return [], None
                
        except Exception as e:
            log.error(f"Fetch error: {str(e)[:50]}")
            pool.record_fetch_error()
            return [], None
    
    def fetch_cycle(self):
        cursors_to_fetch = [None]
        saved_cursors = list(self.cursors)[:CURSORS_PER_CYCLE - 1]
        cursors_to_fetch.extend(saved_cursors)
        
        total_added = 0
        new_cursors = []
        
        with ThreadPoolExecutor(max_workers=FETCH_THREADS) as executor:
            futures = {executor.submit(self.fetch_page, c): c for c in cursors_to_fetch}
            
            for future in as_completed(futures):
                try:
                    servers, next_cursor = future.result()
                    
                    if servers:
                        added = pool.add_servers(servers)
                        total_added += added
                    
                    if next_cursor:
                        new_cursors.append(next_cursor)
                        
                except Exception as e:
                    log.error(f"Future error: {e}")
        
        for c in new_cursors:
            self.cursors.add(c)
        
        if len(self.cursors) > 500:
            self.cursors = set(list(self.cursors)[-300:])
        
        with self.minute_lock:
            self.servers_this_minute += total_added
            if time.time() - self.last_reset >= 60:
                rate = self.servers_this_minute
                log.info(f"[STATS] Rate: {rate} servers/min | Pool: {pool.count()} | Seen: {pool.seen_count()}")
                self.servers_this_minute = 0
                self.last_reset = time.time()
        
        return total_added
    
    def run(self):
        self.running = True
        log.info("[FETCHER] Started with NAProxy")
        
        while self.running:
            try:
                self.fetch_cycle()
            except Exception as e:
                log.error(f"Fetch cycle error: {e}")
            
            time.sleep(FETCH_INTERVAL)
    
    def start(self):
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        log.info("[FETCHER] Background thread started")
    
    def stop(self):
        self.running = False

fetcher = RobloxFetcher()

# ==================== API ENDPOINTS ====================

@app.route('/status', methods=['GET'])
def status():
    stats = pool.get_stats()
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
    count = request.args.get('count', default=10, type=int)
    count = min(count, 100)
    
    servers = pool.get_batch(count)
    
    if not servers:
        return jsonify({'servers': [], 'count': 0}), 200
    
    log.info(f"[BATCH] Gave {len(servers)} ({pool.count()} left)")
    return jsonify({'servers': servers, 'count': len(servers)})

@app.route('/add-pool', methods=['POST'])
def add_pool():
    data = request.get_json() or {}
    servers = data.get('servers', [])
    
    if not servers:
        return jsonify({'added': 0, 'error': 'No servers provided'}), 400
    
    added = pool.add_servers(servers)
    return jsonify({'added': added, 'pool_size': pool.count()})

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'servers': pool.count()})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    
    log.info(f"[STARTUP] Main API on port {port}")
    log.info(f"[CONFIG] TTL: {SERVER_TTL}s | Proxy: {PROXY_HOST}:{PROXY_PORT}")
    
    fetcher.start()
    time.sleep(3)
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
