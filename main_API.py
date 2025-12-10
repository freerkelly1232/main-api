#!/usr/bin/env python3
"""
MAIN API - UK/Brazil/US Peak Hour Rotation
- Switches between UK, Brazil, US based on where it's 5-8 PM
- Uses NAProxy API for fresh residential IPs
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
NAPROXY_API_URL = "https://api.naproxy.com/web_v1/ip/get-ip-v3"
NAPROXY_API_KEY = "b6e6673dfb2404d26759eddd1bdf9d12"

# Proxy settings
PROXY_COUNT = 20
PROXY_LIFE = 30
MIN_PROXIES = 10

# Peak hours
PEAK_START = 17  # 5 PM
PEAK_END = 20    # 8 PM

# Target regions - UK, Brazil, US (East/Central/West)
REGIONS = [
    {"name": "UK", "cc": "GB", "utc_offset": 0},
    {"name": "Brazil", "cc": "BR", "utc_offset": -3},
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

# ==================== PEAK REGION FINDER ====================
class PeakFinder:
    def __init__(self):
        self.lock = threading.Lock()
        self.cache = None
        self.cache_time = 0
    
    def get_hour(self, utc_offset):
        """Get current hour in a timezone"""
        utc_now = datetime.now(timezone.utc)
        local = utc_now + timedelta(hours=utc_offset)
        return local.hour
    
    def get_peak_regions(self):
        """Find regions where it's currently 5-8 PM"""
        with self.lock:
            now = time.time()
            if self.cache and now - self.cache_time < 60:
                return self.cache
            
            peak = []
            for r in REGIONS:
                hour = self.get_hour(r['utc_offset'])
                if PEAK_START <= hour < PEAK_END:
                    peak.append(r)
            
            self.cache = peak
            self.cache_time = now
            
            if peak:
                names = [r['name'] for r in peak]
                log.info(f"[PEAK REGIONS] {', '.join(names)}")
            
            return peak
    
    def get_status(self):
        """Get all regions with their current hour"""
        status = []
        for r in REGIONS:
            hour = self.get_hour(r['utc_offset'])
            is_peak = PEAK_START <= hour < PEAK_END
            status.append({
                'name': r['name'],
                'country': r['cc'],
                'local_hour': hour,
                'local_time': f"{hour}:00",
                'is_peak': is_peak
            })
        return status

peak_finder = PeakFinder()

# ==================== NAPROXY POOL ====================
class ProxyPool:
    def __init__(self):
        self.lock = threading.Lock()
        self.proxies = deque()
        self.current_region = "Starting..."
        self.stats = {
            'api_calls': 0,
            'ips_fetched': 0,
            'ips_used': 0,
            'success': 0,
            'errors': 0,
            'by_region': {}
        }
        
        self.running = True
        threading.Thread(target=self._refresh_loop, daemon=True).start()
    
    def _fetch(self, region=None):
        """Fetch proxies from NAProxy API"""
        try:
            params = {
                'app_key': NAPROXY_API_KEY,
                'pt': 9,
                'num': PROXY_COUNT,
                'life': PROXY_LIFE,
                'protocol': 1,
                'format': 'txt',
                'lb': '%5Cr%5Cn'
            }
            
            # Set region
            if region:
                params['cc'] = region.get('cc', '')
                params['state'] = region.get('state', '')
                region_name = region['name']
            else:
                params['cc'] = ''
                params['state'] = ''
                region_name = 'Any'
            
            params['city'] = ''
            params['ep'] = ''
            
            resp = requests.get(NAPROXY_API_URL, params=params, timeout=10)
            
            if resp.ok:
                text = resp.text.strip()
                
                # Check for error response
                if not text or text.startswith('{') or 'error' in text.lower() or 'fail' in text.lower():
                    log.warning(f"[NAPROXY] Bad response: {text[:80]}")
                    return 0
                
                lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
                expire = time.time() + PROXY_LIFE
                count = 0
                
                with self.lock:
                    for line in lines:
                        line = line.strip()
                        if line and ':' in line:
                            self.proxies.append((f"http://{line}", expire))
                            count += 1
                    
                    self.stats['api_calls'] += 1
                    self.stats['ips_fetched'] += count
                    self.current_region = region_name
                    
                    if region_name not in self.stats['by_region']:
                        self.stats['by_region'][region_name] = 0
                    self.stats['by_region'][region_name] += count
                
                log.info(f"[NAPROXY] +{count} IPs from {region_name} (pool: {len(self.proxies)})")
                return count
            else:
                log.error(f"[NAPROXY] HTTP {resp.status_code}")
                
        except Exception as e:
            log.error(f"[NAPROXY] Error: {e}")
        
        return 0
    
    def _cleanup(self):
        """Remove expired proxies"""
        now = time.time()
        with self.lock:
            self.proxies = deque(p for p in self.proxies if p[1] > now)
    
    def _refresh_loop(self):
        """Keep pool fresh with peak region IPs"""
        while self.running:
            try:
                self._cleanup()
                
                with self.lock:
                    size = len(self.proxies)
                
                if size < MIN_PROXIES:
                    peak = peak_finder.get_peak_regions()
                    
                    if peak:
                        # Pick random peak region
                        region = random.choice(peak)
                        self._fetch(region)
                    else:
                        # No peak - rotate through all
                        region = random.choice(REGIONS)
                        self._fetch(region)
                
                time.sleep(5)
                
            except Exception as e:
                log.error(f"[POOL] Error: {e}")
                time.sleep(2)
    
    def get(self):
        """Get a proxy"""
        self._cleanup()
        
        with self.lock:
            if self.proxies:
                idx = random.randint(0, len(self.proxies) - 1)
                url, expire = self.proxies[idx]
                if expire - time.time() > 5:
                    self.stats['ips_used'] += 1
                    return {'http': url, 'https': url}, url
        
        # Need more
        peak = peak_finder.get_peak_regions()
        if peak:
            self._fetch(random.choice(peak))
        else:
            self._fetch(random.choice(REGIONS))
        
        with self.lock:
            if self.proxies:
                url, _ = self.proxies[0]
                self.stats['ips_used'] += 1
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
                'current_region': self.current_region,
                'api_calls': self.stats['api_calls'],
                'ips_fetched': self.stats['ips_fetched'],
                'ips_used': self.stats['ips_used'],
                'success_rate': round(self.stats['success'] / max(1, total) * 100, 1),
                'by_region': self.stats['by_region']
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
        
        # Clean dead
        old = [k for k, v in self._dead.items() if now - v > 600]
        for k in old:
            del self._dead[k]
        
        # Recycle assigned
        expired = [(k, v) for k, v in self._assigned.items() if now - v > GIVEN_TTL]
        for job_id, _ in expired:
            del self._assigned[job_id]
            self._stats['recycled'] += 1
        
        # Clean available
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
            
            # Sort by priority
            sorted_servers = sorted(
                self._available.items(),
                key=lambda x: x[1][2],
                reverse=True
            )
            
            for job_id, (players, added_time, priority) in sorted_servers:
                if job_id in self._dead:
                    continue
                
                del self._available[job_id]
                self._assigned[job_id] = time.time()
                self._stats['total_given'] += 1
                
                return {
                    'job_id': job_id,
                    'players': players,
                    'pool_size': len(self._available)
                }
            
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
                'total_received': self._stats['total_received'],
                'recycled': self._stats['recycled']
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
    
    def fetch_page(self, cursor=None, sort='Asc'):
        proxy, url = proxy_pool.get()
        if not proxy:
            return [], None
        
        try:
            resp = requests.get(
                f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public",
                params={
                    'sortOrder': sort,
                    'limit': SERVERS_PER_REQUEST,
                    'excludeFullGames': 'true',
                    'cursor': cursor
                } if cursor else {
                    'sortOrder': sort,
                    'limit': SERVERS_PER_REQUEST,
                    'excludeFullGames': 'true'
                },
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
                return [], None
                
        except:
            proxy_pool.error(url)
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
                        if futures[f] == 'Asc':
                            new_asc.append(cursor)
                        else:
                            new_desc.append(cursor)
                except:
                    pass
        
        with self.lock:
            self.cursors_asc.extend(new_asc)
            self.cursors_desc.extend(new_desc)
        
        self.servers_min += added
        
        # Log every minute
        if time.time() - self.last_log >= 60:
            stats = pool.stats()
            pstats = proxy_pool.get_stats()
            peak = peak_finder.get_peak_regions()
            regions = [r['name'] for r in peak] if peak else ['Rotating']
            
            log.info(f"[{', '.join(regions)}] +{self.servers_min}/min | Avail: {stats['available']} | Given: {stats['total_given']} | Proxies: {pstats['pool_size']} | {pstats['success_rate']}%")
            
            self.servers_min = 0
            self.last_log = time.time()
    
    def run(self):
        log.info("[FETCHER] Started - UK/Brazil/US Peak Rotation")
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
    return jsonify({
        'servers': pool.stats(),
        'proxy': proxy_pool.get_stats(),
        'regions': peak_finder.get_status(),
        'peak_regions': [r['name'] for r in peak_finder.get_peak_regions()] or ['Rotating all']
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
    servers = data.get('servers', [])
    added = pool.add(servers) if servers else 0
    return jsonify({'added': added, 'available': pool.count()})

@app.route('/report-dead', methods=['POST'])
def report_dead():
    data = request.get_json() or {}
    if data.get('job_id'):
        pool.report_dead(data['job_id'])
    return jsonify({'status': 'ok'})

@app.route('/health', methods=['GET'])
def health():
    peak = peak_finder.get_peak_regions()
    return jsonify({
        'status': 'ok',
        'available': pool.count(),
        'proxies': proxy_pool.get_stats()['pool_size'],
        'peak_regions': [r['name'] for r in peak] if peak else ['Rotating']
    })

@app.route('/regions', methods=['GET'])
def regions():
    return jsonify({
        'status': peak_finder.get_status(),
        'peak_now': [r['name'] for r in peak_finder.get_peak_regions()] or ['None']
    })

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    
    log.info(f"[STARTUP] Main API on port {port}")
    log.info(f"[CONFIG] Regions: UK, Brazil, US (East/Central/West)")
    log.info(f"[CONFIG] Peak hours: {PEAK_START}:00-{PEAK_END}:00 local time")
    
    # Show current status
    for r in peak_finder.get_status():
        status = "🟢 PEAK" if r['is_peak'] else "⚪"
        log.info(f"  {r['name']}: {r['local_time']} {status}")
    
    # Pre-fetch proxies
    log.info("[NAPROXY] Fetching initial proxies...")
    peak = peak_finder.get_peak_regions()
    if peak:
        proxy_pool._fetch(random.choice(peak))
    else:
        proxy_pool._fetch(random.choice(REGIONS))
    
    # Start fetcher
    threading.Thread(target=fetcher.run, daemon=True).start()
    time.sleep(1)
    
    app.run(host='0.0.0.0', port=port, threaded=True)
