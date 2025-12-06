#!/usr/bin/env python3
"""
MAIN API - Fetches ASCENDING servers
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
SERVER_TTL = 1800
PLACE_ID = 109983668079237
SORT_ORDER = "Asc"

# NAProxy US
PROXY_HOST = "us.naproxy.net"
PROXY_PORT = "1000"
PROXY_USER = "proxy-e5a1ntzmrlr3_area-US"
PROXY_PASS = "Ol43jGdsIuPUNacc"

FETCH_THREADS = 20
FETCH_INTERVAL = 1
SERVERS_PER_REQUEST = 100
CURSORS_PER_CYCLE = 100
MAX_POOL_SIZE = 100000

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

def get_proxy():
    session_id = f"main{random.randint(100000, 999999)}"
    proxy_url = f"http://{PROXY_USER}_session-{session_id}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    return {'http': proxy_url, 'https': proxy_url}

class ServerPool:
    def __init__(self):
        self._lock = threading.Lock()
        self._servers = deque(maxlen=MAX_POOL_SIZE)
        self._dead_servers = set()
        self._stats = {
            'total_given': 0,
            'total_received': 0,
            'expired': 0,
            'dead_skipped': 0,
            'fetch_errors': 0,
            'fetch_success': 0,
            'from_mini': 0,
            'from_fetcher': 0
        }
    
    def _clean_expired(self):
        now = time.time()
        expired = 0
        while self._servers:
            job_id, players, added_time = self._servers[0]
            if now - added_time > SERVER_TTL:
                self._servers.popleft()
                expired += 1
            else:
                break
        if expired:
            self._stats['expired'] += expired
    
    def add_servers(self, servers, source="unknown"):
        added = 0
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
                
                if not job_id or job_id in self._dead_servers:
                    continue
                
                self._servers.append((job_id, players, now))
                added += 1
                
                if source == "mini-api":
                    self._stats['from_mini'] += 1
                else:
                    self._stats['from_fetcher'] += 1
            
            self._stats['total_received'] += added
        
        return added
    
    def get_server(self):
        now = time.time()
        with self._lock:
            while self._servers:
                job_id, players, added_time = self._servers.popleft()
                
                if now - added_time > SERVER_TTL:
                    self._stats['expired'] += 1
                    continue
                
                if job_id in self._dead_servers:
                    self._stats['dead_skipped'] += 1
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
                
                if now - added_time > SERVER_TTL or job_id in self._dead_servers:
                    continue
                
                self._stats['total_given'] += 1
                results.append({'job_id': job_id, 'players': players, 'age': int(now - added_time)})
        
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
                'dead_servers': len(self._dead_servers),
                'total_given': self._stats['total_given'],
                'total_received': self._stats['total_received'],
                'from_mini': self._stats['from_mini'],
                'from_fetcher': self._stats['from_fetcher'],
                'expired': self._stats['expired'],
                'fetch_errors': self._stats['fetch_errors'],
                'fetch_success': self._stats['fetch_success'],
                'sort_order': SORT_ORDER
            }

pool = ServerPool()

class RobloxFetcher:
    def __init__(self):
        self.running = False
        self.cursors = deque(maxlen=1000)
        self.last_reset = time.time()
        self.servers_this_minute = 0
    
    def fetch_page(self, cursor=None):
        try:
            url = f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public"
            params = {'sortOrder': SORT_ORDER, 'limit': SERVERS_PER_REQUEST}
            if cursor:
                params['cursor'] = cursor
            
            response = requests.get(url, params=params, proxies=get_proxy(), timeout=10,
                                   headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'})
            
            if response.status_code == 200:
                data = response.json()
                pool.record_fetch_success()
                return data.get('data', []), data.get('nextPageCursor')
            else:
                pool.record_fetch_error()
                return [], None
        except:
            pool.record_fetch_error()
            return [], None
    
    def fetch_cycle(self):
        cursors_to_fetch = [None] + list(self.cursors)[:CURSORS_PER_CYCLE - 1]
        total_added = 0
        new_cursors = []
        
        with ThreadPoolExecutor(max_workers=FETCH_THREADS) as executor:
            futures = {executor.submit(self.fetch_page, c): c for c in cursors_to_fetch}
            for future in as_completed(futures):
                try:
                    servers, next_cursor = future.result()
                    if servers:
                        total_added += pool.add_servers(servers, source="fetcher")
                    if next_cursor:
                        new_cursors.append(next_cursor)
                except:
                    pass
        
        for c in new_cursors:
            self.cursors.append(c)
        
        self.servers_this_minute += total_added
        now = time.time()
        if now - self.last_reset >= 60:
            stats = pool.get_stats()
            log.info(f"[RATE] Fetcher: {self.servers_this_minute}/min | Pool: {stats['available']} | Mini: {stats['from_mini']} | Given: {stats['total_given']}")
            self.servers_this_minute = 0
            self.last_reset = now
        
        return total_added
    
    def run(self):
        self.running = True
        log.info(f"[FETCHER] Started - {SORT_ORDER}")
        while self.running:
            try:
                self.fetch_cycle()
            except Exception as e:
                log.error(f"Error: {e}")
            time.sleep(FETCH_INTERVAL)
    
    def start(self):
        threading.Thread(target=self.run, daemon=True).start()

fetcher = RobloxFetcher()

@app.route('/status', methods=['GET'])
def status():
    stats = pool.get_stats()
    stats['cursors'] = len(fetcher.cursors)
    return jsonify(stats)

@app.route('/get-server', methods=['GET'])
def get_server():
    server = pool.get_server()
    if not server:
        return jsonify({'error': 'No servers'}), 404
    return jsonify(server)

@app.route('/get-batch', methods=['GET'])
def get_batch():
    count = min(request.args.get('count', defa
