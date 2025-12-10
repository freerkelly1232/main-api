#!/usr/bin/env python3
"""
MAIN API - UK/Brazil/US Peak Hour Rotation
Fixed for SOCKS5 proxies from NAProxy
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
from datetime import datetime, timezone, timedelta

# ==================== CONFIG ====================
SERVER_TTL = 300
GIVEN_TTL = 300
PLACE_ID = 109983668079237

# NAProxy API
NAPROXY_BASE = "https://api.naproxy.com/web_v1/ip/get-ip-v3"
NAPROXY_KEY = "b6e6673dfb2404d26759eddd1bdf9d12"

# Proxy settings
PROXY_COUNT = 20
PROXY_LIFE = 30
MIN_PROXIES = 10

# Peak hours
PEAK_START = 17
PEAK_END = 20

# Regions
REGIONS = [
    {"name": "UK", "cc": "GB", "state": "", "utc_offset": 0},
    {"name": "Brazil", "cc": "BR", "state": "", "utc_offset": -3},
    {"name": "US-East", "cc": "US", "state": "NY", "utc_offset": -5},
    {"name": "US-Central", "cc": "US", "state": "TX", "utc_offset": -6},
    {"name": "US-West", "cc": "US", "state": "CA", "utc_offset": -8},
]

# Fetching config
FETCH_THREADS = 16
FETCH_INTERVAL = 1.5
SERVERS_PER_REQUEST = 100
CURSORS_PER_CYCLE = 50

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ==================== PEAK FINDER ====================
def get_hour(offset):
    utc = datetime.now(timezone.utc)
    return (utc + timedelta(hours=offset)).hour

def get_peak_regions():
    return [r for r in REGIONS if PEAK_START <= get_hour(r['utc_offset']) < PEAK_END]

def get_all_status():
    return [
        {
            'name': r['name'],
            'hour': get_hour(r['utc_offset']),
            'is_peak': PEAK_START <= get_hour(r['utc_offset']) < PEAK_END
        }
        for r in REGIONS
    ]

# ==================== PROXY POOL ====================
class ProxyPool:
    def __init__(self):
        self.lock = threading.Lock()
        self.proxies = deque()
        self.region = "Starting"
        self.stats = {'fetched': 0, 'used': 0, 'success': 0, 'errors': 0, 'api_errors': 0}
        self.last_error = None
        
        threading.Thread(target=self._loop, daemon=True).start()
    
    def _build_url(self, region=None):
        """Build NAProxy URL"""
        url = f"{NAPROXY_BASE}?app_key={NAPROXY_KEY}&pt=9&ep=&num={PROXY_COUNT}"
        
        if region:
            url += f"&cc={region.get('cc', '')}&state={region.get('state', '')}"
        else:
            url += "&cc=&state="
        
        url += f"&city=&life={PROXY_LIFE}&protocol=1&format=txt&lb=%5Cr%5Cn"
        
        return url
    
    def _fetch(self, region=None):
        """Fetch proxies from NAProxy"""
        name = region['name'] if region else 'Any'
        
        try:
            url = self._build_url(region)
            resp = requests.get(url, timeout=15)
            text = resp.text.strip()
            
            if 'allow list' in text.lower() or 'whitelist' in text.lower():
                self.last_error = "IP not whitelisted"
                log.error(f"[NAPROXY] {self.last_error}")
                return 0
            
            if text.startswith('{') or 'error' in text.lower():
                self.last_error = f"API Error: {text[:80]}"
                log.warning(f"[NAPROXY] {self.last_error}")
                return 0
            
            if not text or ':' not in text:
                self.last_error = "Empty response"
                return 0
            
            # Parse IPs - format is ip:port
            lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
            expire = time.time() + PROXY_LIFE
            count = 0
            
            with self.lock:
                for line in lines:
                    line = line.strip()
                    if line and ':' in line and not line.startswith('{'):
                        # Use SOCKS5 protocol since protocol=1
                        proxy_url = f"socks5://{line}"
                        self.proxies.append((proxy_url, expire))
                        count += 1
                
                if count > 0:
                    self.stats['fetched'] += count
                    self.region = name
                    self.last_error = None
            
            if count > 0:
                log.info(f"[NAPROXY] +{count} IPs from {name} (pool: {len(self.proxies)})")
            
            return count
            
        except Exception as e:
            self.last_error = f"Error: {e}"
            log.error(f"[NAPROXY] {self.last_error}")
        
        return 0
    
    def _cleanup(self):
        now = time.time()
        with self.lock:
            self.proxies = deque(p for p in self.proxies if p[1] > now)
    
    def _loop(self):
        while True:
            try:
                self._cleanup()
                
                with self.lock:
                    size = len(self.proxies)
                
                if size < MIN_PROXIES:
                    peak = get_peak_regions()
                    region = random.choice(peak) if peak else random.choice(REGIONS)
                    self._fetch(region)
                
                time.sleep(5)
            except Exception as e:
                log.error(f"[POOL] {e}")
                time.sleep(2)
    
    def get(self):
        self._cleanup()
        
        with self.lock:
            if self.proxies:
                idx = random.randint(0, len(self.proxies) - 1)
                url, exp = self.proxies[idx]
                if exp - time.time() > 5:
                    self.stats['used'] += 1
                    # Return both http and https pointing to socks5
                    return {'http': url, 'https': url}, url
        
        # Need more
        peak = get_peak_regions()
        self._fetch(random.choice(peak) if peak else random.choice(REGIONS))
        
        with self.lock:
            if self.proxies:
                url, _ = self.proxies[0]
                self.stats['used'] += 1
                return {'http': url, 'https': url}, url
        
        return None, None
    
    def success(self):
        with self.lock:
            self.stats['success'] += 1
    
    def error(self, url=None):
        with self.lock:
            self.stats['errors'] += 1
            if url:
                self.proxies = deque(p for p in self.proxies if p[0] != url)
    
    def get_stats(self):
        with self.lock:
            total = self.stats['success'] + self.stats['errors']
            return {
                'pool_size': len(self.proxies),
                'current_region': self.region,
                'fetched': self.stats['fetched'],
                'used': self.stats['used'],
                'success_rate': round(self.stats['success'] / max(1, total) * 100, 1),
                'api_errors': self.stats['api_errors'],
                'last_error': self.last_error
            }

proxy_pool = ProxyPool()

# ==================== SERVER POOL ====================
class ServerPool:
    def __init__(self):
        self._lock = threading.Lock()
        self._available = OrderedDict()
        self._assigned = {}
        self._dead = {}
        self._stats = {'total_given': 0, 'total_received': 0, 'recycled': 0}
    
    def _cleanup(self):
        now = time.time()
        
        old = [k for k, v in self._dead.items() if now - v > 600]
        for k in old:
            del self._dead[k]
        
        expired = [(k, v) for k, v in self._assigned.items() if now - v > GIVEN_TTL]
        for job_id, _ in expired:
            del self._assigned[job_id]
            self._stats['recycled'] += 1
        
        old = [k for k, (p, t, pr) in self._available.items() if now - t > SERVER_TTL]
        for k in old:
            del self._available[k]
    
    def add(self, servers):
        added = 0
        now = time.time()
        
        with self._lock:
            self._cleanup()
            
            for item in servers:
                if isinstance(item, dict):
                    job_id = item.get('id') or item.get('job_id')
                    players = item.get('playing', item.get('players', 0))
                else:
                    continue
                
                if not job_id or players < 5:
                    continue
                
                if job_id in self._dead or job_id in self._assigned or job_id in self._available:
                    continue
                
                priority = 100 if players in (6, 7) else 50 if players == 5 else 10
                self._available[job_id] = (players, now, priority)
                added += 1
            
            self._stats['total_received'] += added
        
        return added
    
    def get(self):
        with self._lock:
            self._cleanup()
            
            if not self._available:
                return None
            
            sorted_servers = sorted(self._available.items(), key=lambda x: x[1][2], reverse=True)
            
            for job_id, (players, added_time, priority) in sorted_servers:
                if job_id in self._dead:
                    continue
                
                del self._available[job_id]
                self._assigned[job_id] = time.time()
                self._stats['total_given'] += 1
                
                return {'job_id': job_id, 'players': players, 'pool_size': len(self._available)}
            
            return None
    
    def get_batch(self, count):
        results = []
        for _ in range(count):
            server = self.get()
            if server:
                results.append(server)
            else:
                break
        return results
    
    def report_dead(self, job_id):
        with self._lock:
            self._dead[job_id] = time.time()
            self._available.pop(job_id, None)
            self._assigned.pop(job_id, None)
    
    def count(self):
        with self._lock:
            self._cleanup()
            return len(self._available)
    
    def stats(self):
        with self._lock:
            self._cleanup()
            return {
                'available': len(self._available),
                'assigned': len(self._assigned),
                'dead': len(self._dead),
                'total_given': self._stats['total_given'],
                'total_received': self._stats['total_received']
            }

pool = ServerPool()

# ==================== FETCHER ====================
class Fetcher:
    def __init__(self):
        self.cursors_asc = deque(maxlen=3000)
        self.cursors_desc = deque(maxlen=3000)
        self.lock = threading.Lock()
        self.servers_min = 0
        self.last_log = time.time()
        self.fetch_errors = 0
    
    def fetch_page(self, cursor=None, sort='Asc'):
        proxy, url = proxy_pool.get()
        if not proxy:
            return [], None
        
        try:
            params = {'sortOrder': sort, 'limit': SERVERS_PER_REQUEST, 'excludeFullGames': 'true'}
            if cursor:
                params['cursor'] = cursor
            
            resp = requests.get(
                f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public",
                params=params,
                proxies=proxy,
                timeout=15,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            )
            
            if resp.status_code == 200:
                data = resp.json()
                proxy_pool.success()
                return data.get('data', []), data.get('nextPageCursor')
            else:
                proxy_pool.error(url)
                self.fetch_errors += 1
        except Exception as e:
            proxy_pool.error(url)
            self.fetch_errors += 1
            # Log first few errors to debug
            if self.fetch_errors <= 5:
                log.error(f"[FETCH] Error: {e}")
        
        return [], None
    
    def cycle(self):
        fetches = [(None, 'Asc'), (None, 'Desc')]
        
        with self.lock:
            for c in list(self.cursors_asc)[-30:]:
                fetches.append((c, 'Asc'))
            for c in list(self.cursors_desc)[-30:]:
                fetches.append((c, 'Desc'))
        
        fetches = fetches[:CURSORS_PER_CYCLE]
        
        added = 0
        new_asc, new_desc = [], []
        
        with ThreadPoolExecutor(max_workers=FETCH_THREADS) as ex:
            futures = {ex.submit(self.fetch_page, c, s): s for c, s in fetches}
            
            for f in as_completed(futures):
                try:
                    servers, cursor = f.result()
                    if servers:
                        added += pool.add(servers)
                    if cursor:
                        (new_asc if futures[f] == 'Asc' else new_desc).append(cursor)
                except:
                    pass
        
        with self.lock:
            self.cursors_asc.extend(new_asc)
            self.cursors_desc.extend(new_desc)
        
        self.servers_min += added
        
        if time.time() - self.last_log >= 60:
            stats = pool.stats()
            pstats = proxy_pool.get_stats()
            peak = get_peak_regions()
            regions = [r['name'] for r in peak] if peak else ['Rotating']
            
            log.info(f"[{', '.join(regions)}] +{self.servers_min}/min | Avail: {stats['available']} | Given: {stats['total_given']} | Proxies: {pstats['pool_size']} | {pstats['success_rate']}%")
            
            if pstats['last_error']:
                log.warning(f"[PROXY] {pstats['last_error']}")
            
            if self.fetch_errors > 0:
                log.warning(f"[FETCH] {self.fetch_errors} errors this minute")
                self.fetch_errors = 0
            
            self.servers_min = 0
            self.last_log = time.time()
    
    def run(self):
        log.info("[FETCHER] Started - UK/Brazil/US rotation")
        while True:
            try:
                self.cycle()
                time.sleep(FETCH_INTERVAL)
            except Exception as e:
                log.error(f"[FETCHER] {e}")
                time.sleep(1)

fetcher = Fetcher()

# ==================== ENDPOINTS ====================
@app.route('/status', methods=['GET'])
def status():
    peak = get_peak_regions()
    return jsonify({
        'servers': pool.stats(),
        'proxy': proxy_pool.get_stats(),
        'regions': get_all_status(),
        'peak_now': [r['name'] for r in peak] if peak else ['Rotating']
    })

@app.route('/get-server', methods=['GET'])
def get_server():
    server = pool.get()
    if not server:
        return jsonify({'error': 'No servers', 'retry_ms': 500}), 404
    return jsonify(server)

@app.route('/get-batch', methods=['GET'])
def get_batch():
    count = min(request.args.get('count', 1, type=int), 50)
    servers = pool.get_batch(count)
    return jsonify({'servers': servers, 'count': len(servers), 'available': pool.count()})

@app.route('/add-pool', methods=['POST'])
def add_pool():
    data = request.get_json() or {}
    added = pool.add(data.get('servers', []))
    return jsonify({'added': added, 'available': pool.count()})

@app.route('/report-dead', methods=['POST'])
def report_dead():
    data = request.get_json() or {}
    if data.get('job_id'):
        pool.report_dead(data['job_id'])
    return jsonify({'status': 'ok'})

@app.route('/health', methods=['GET'])
def health():
    pstats = proxy_pool.get_stats()
    return jsonify({
        'status': 'ok',
        'available': pool.count(),
        'proxies': pstats['pool_size'],
        'proxy_error': pstats['last_error']
    })

@app.route('/regions', methods=['GET'])
def regions():
    return jsonify({'regions': get_all_status()})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    
    log.info(f"[STARTUP] Main API on port {port}")
    log.info(f"[CONFIG] Regions: UK, Brazil, US")
    log.info(f"[CONFIG] Using SOCKS5 proxies")
    
    # Show status
    log.info("[REGIONS]")
    for r in get_all_status():
        status = "🟢 PEAK" if r['is_peak'] else ""
        log.info(f"  {r['name']}: {r['hour']}:00 {status}")
    
    # Fetch initial proxies
    log.info("[NAPROXY] Fetching initial proxies...")
    peak = get_peak_regions()
    proxy_pool._fetch(random.choice(peak) if peak else random.choice(REGIONS))
    
    # Start fetcher
    threading.Thread(target=fetcher.run, daemon=True).start()
    time.sleep(1)
    
    app.run(host='0.0.0.0', port=port, threaded=True)
