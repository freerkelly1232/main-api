#!/usr/bin/env python3
"""
MAIN API - NAProxy API Integration
- Fetches fresh residential IPs via NAProxy API
- Smart Region for best routing
- Auto-refreshes IP pool
- No static IPs needed
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
SERVER_TTL = 300
GIVEN_TTL = 300
PLACE_ID = 109983668079237

# NAProxy API Config
NAPROXY_API_KEY = "b6e6673dfb2404d26759eddd1bdf9d12"
NAPROXY_API_URL = "https://api.naproxy.com/web_v1/ip/get-ip-v3"

# Proxy settings
PROXY_COUNT = 20          # IPs to fetch per batch
PROXY_LIFE = 30           # Session life in seconds
PROXY_PROTOCOL = 1        # 1 = HTTPS/SOCKS5
PROXY_REGION = "Smart Region"
PROXY_COUNTRY = ""        # Empty = random
PROXY_STATE = ""
PROXY_CITY = ""

# How often to refresh proxy pool
PROXY_REFRESH_INTERVAL = 25  # Refresh before they expire (30s life)
MIN_PROXIES = 10             # Fetch more when below this

# Peak hours
PEAK_START_HOUR = 17
PEAK_END_HOUR = 20

# Fetching config
FETCH_THREADS_PEAK = 20
FETCH_THREADS_NORMAL = 10
FETCH_INTERVAL_PEAK = 1
FETCH_INTERVAL_NORMAL = 2
SERVERS_PER_REQUEST = 100
CURSORS_PER_CYCLE_PEAK = 60
CURSORS_PER_CYCLE_NORMAL = 30

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ==================== AUTO TIMEZONE ====================
class TimezoneDetector:
    def __init__(self):
        self.timezone = 'America/New_York'
        self.utc_offset = -5
        self._detect()
    
    def _detect(self):
        try:
            resp = requests.get('http://ip-api.com/json/', timeout=5)
            if resp.ok:
                data = resp.json()
                self.timezone = data.get('timezone', 'America/New_York')
                self.utc_offset = self._get_offset(self.timezone)
                log.info(f"[TIMEZONE] {self.timezone} (UTC{self.utc_offset:+d})")
                return
        except:
            pass
    
    def _get_offset(self, tz_name):
        offsets = {
            'America/New_York': -5, 'America/Chicago': -6,
            'America/Denver': -7, 'America/Los_Angeles': -8,
            'Europe/London': 0, 'Europe/Paris': 1,
        }
        return offsets.get(tz_name, -5)
    
    def get_local_hour(self):
        utc_hour = time.gmtime().tm_hour
        return (utc_hour + self.utc_offset) % 24
    
    def is_peak(self):
        hour = self.get_local_hour()
        return PEAK_START_HOUR <= hour < PEAK_END_HOUR

tz_detector = TimezoneDetector()

def is_peak_hours():
    return tz_detector.is_peak()

def get_fetch_config():
    if is_peak_hours():
        return {
            'threads': FETCH_THREADS_PEAK,
            'interval': FETCH_INTERVAL_PEAK,
            'cursors': CURSORS_PER_CYCLE_PEAK,
            'mode': 'PEAK'
        }
    return {
        'threads': FETCH_THREADS_NORMAL,
        'interval': FETCH_INTERVAL_NORMAL,
        'cursors': CURSORS_PER_CYCLE_NORMAL,
        'mode': 'NORMAL'
    }

# ==================== NAPROXY IP POOL ====================
class NAProxyPool:
    def __init__(self):
        self.lock = threading.Lock()
        self.proxies = deque()  # (proxy_url, expire_time)
        self.last_fetch = 0
        self.stats = {
            'api_calls': 0,
            'ips_fetched': 0,
            'ips_used': 0,
            'ips_expired': 0,
            'api_errors': 0,
            'request_success': 0,
            'request_errors': 0
        }
        
        # Start background refresh thread
        self.running = True
        self.refresh_thread = threading.Thread(target=self._refresh_loop, daemon=True)
        self.refresh_thread.start()
    
    def _fetch_proxies(self):
        """Fetch fresh proxies from NAProxy API"""
        try:
            params = {
                'app_key': NAPROXY_API_KEY,
                'pt': 9,  # Proxy type
                'num': PROXY_COUNT,
                'cc': PROXY_COUNTRY,
                'state': PROXY_STATE,
                'city': PROXY_CITY,
                'life': PROXY_LIFE,
                'protocol': PROXY_PROTOCOL,
                'format': 'txt',
                'lb': '%5Cr%5Cn'  # \r\n separator
            }
            
            # Add sep parameter for Smart Region
            params['sep'] = ''
            
            resp = requests.get(NAPROXY_API_URL, params=params, timeout=10)
            
            if resp.ok:
                text = resp.text.strip()
                if text and not text.startswith('{'):  # Not an error JSON
                    lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
                    proxies = []
                    expire_time = time.time() + PROXY_LIFE
                    
                    for line in lines:
                        line = line.strip()
                        if line and ':' in line:
                            # Format: ip:port or ip:port:user:pass
                            parts = line.split(':')
                            if len(parts) >= 2:
                                proxy_url = f"http://{line}"
                                proxies.append((proxy_url, expire_time))
                    
                    with self.lock:
                        self.stats['api_calls'] += 1
                        self.stats['ips_fetched'] += len(proxies)
                        for p in proxies:
                            self.proxies.append(p)
                    
                    log.info(f"[NAPROXY] Fetched {len(proxies)} fresh IPs (pool: {len(self.proxies)})")
                    return len(proxies)
                else:
                    log.warning(f"[NAPROXY] API response: {text[:100]}")
            else:
                log.error(f"[NAPROXY] API error: {resp.status_code}")
                with self.lock:
                    self.stats['api_errors'] += 1
                    
        except Exception as e:
            log.error(f"[NAPROXY] Fetch error: {e}")
            with self.lock:
                self.stats['api_errors'] += 1
        
        return 0
    
    def _cleanup_expired(self):
        """Remove expired proxies"""
        now = time.time()
        expired = 0
        
        with self.lock:
            while self.proxies and self.proxies[0][1] < now:
                self.proxies.popleft()
                expired += 1
            
            if expired:
                self.stats['ips_expired'] += expired
        
        return expired
    
    def _refresh_loop(self):
        """Background thread to keep proxy pool fresh"""
        while self.running:
            try:
                self._cleanup_expired()
                
                with self.lock:
                    pool_size = len(self.proxies)
                
                # Fetch more if running low
                if pool_size < MIN_PROXIES:
                    self._fetch_proxies()
                
                time.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                log.error(f"[NAPROXY] Refresh error: {e}")
                time.sleep(1)
    
    def get_proxy(self):
        """Get a proxy from the pool"""
        self._cleanup_expired()
        
        with self.lock:
            if not self.proxies:
                # Pool empty, fetch synchronously
                pass
            else:
                # Get random proxy from pool (don't remove, can reuse within lifetime)
                idx = random.randint(0, len(self.proxies) - 1)
                proxy_url, expire_time = self.proxies[idx]
                
                # Skip if about to expire (< 5s left)
                if expire_time - time.time() > 5:
                    self.stats['ips_used'] += 1
                    return {'http': proxy_url, 'https': proxy_url}
        
        # Fetch fresh if needed
        self._fetch_proxies()
        
        with self.lock:
            if self.proxies:
                proxy_url, _ = self.proxies[0]
                self.stats['ips_used'] += 1
                return {'http': proxy_url, 'https': proxy_url}
        
        return None
    
    def report_success(self):
        with self.lock:
            self.stats['request_success'] += 1
    
    def report_error(self, proxy_url=None):
        with self.lock:
            self.stats['request_errors'] += 1
            
            # Remove bad proxy from pool
            if proxy_url:
                self.proxies = deque(
                    (p, e) for p, e in self.proxies if p != proxy_url
                )
    
    def get_stats(self):
        with self.lock:
            return {
                'pool_size': len(self.proxies),
                'api_calls': self.stats['api_calls'],
                'ips_fetched': self.stats['ips_fetched'],
                'ips_used': self.stats['ips_used'],
                'ips_expired': self.stats['ips_expired'],
                'api_errors': self.stats['api_errors'],
                'request_success': self.stats['request_success'],
                'request_errors': self.stats['request_errors'],
                'success_rate': round(
                    self.stats['request_success'] / max(1, self.stats['request_success'] + self.stats['request_errors']) * 100, 1
                )
            }

proxy_pool = NAProxyPool()

# ==================== SERVER POOL ====================
class ServerPool:
    def __init__(self):
        self._lock = threading.Lock()
        self._available = OrderedDict()
        self._assigned = {}
        self._dead = {}
        self._stats = {
            'total_given': 0,
            'total_received': 0,
            'recycled': 0,
            'duplicates_skipped': 0,
            'dead_skipped': 0,
            'peak_servers': 0,
            'normal_servers': 0
        }
    
    def _cleanup(self):
        now = time.time()
        
        old_dead = [k for k, v in self._dead.items() if now - v > 600]
        for k in old_dead:
            del self._dead[k]
        
        recycled = 0
        expired_assigned = [(k, v) for k, v in self._assigned.items() if now - v > GIVEN_TTL]
        for job_id, _ in expired_assigned:
            del self._assigned[job_id]
            recycled += 1
        
        if recycled:
            self._stats['recycled'] += recycled
        
        expired_available = [k for k, (p, t, pr) in self._available.items() if now - t > SERVER_TTL]
        for k in expired_available:
            del self._available[k]
    
    def add_servers(self, servers, source="unknown"):
        added = 0
        duplicates = 0
        dead = 0
        now = time.time()
        is_peak = is_peak_hours()
        
        with self._lock:
            self._cleanup()
            
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
                
                if job_id in self._dead:
                    dead += 1
                    continue
                
                if job_id in self._assigned or job_id in self._available:
                    duplicates += 1
                    continue
                
                if players < 5:
                    continue
                
                if players in (6, 7):
                    priority = 100
                elif players == 5:
                    priority = 50
                else:
                    priority = 10
                
                self._available[job_id] = (players, now, priority)
                added += 1
            
            self._stats['total_received'] += added
            self._stats['duplicates_skipped'] += duplicates
            self._stats['dead_skipped'] += dead
            
            if is_peak:
                self._stats['peak_servers'] += added
            else:
                self._stats['normal_servers'] += added
        
        return added, duplicates, dead, len(servers)
    
    def get_server(self):
        now = time.time()
        
        with self._lock:
            self._cleanup()
            
            if not self._available:
                return None
            
            sorted_servers = sorted(
                self._available.items(),
                key=lambda x: (x[1][2], -x[1][1]),
                reverse=True
            )
            
            for job_id, (players, added_time, priority) in sorted_servers:
                if job_id in self._dead:
                    continue
                
                del self._available[job_id]
                self._assigned[job_id] = now
                self._stats['total_given'] += 1
                
                return {
                    'job_id': job_id,
                    'players': players,
                    'priority': priority,
                    'age': int(now - added_time),
                    'pool_size': len(self._available)
                }
            
            return None
    
    def get_batch(self, count):
        results = []
        with self._lock:
            self._cleanup()
            
            if not self._available:
                return results
            
            now = time.time()
            
            sorted_servers = sorted(
                self._available.items(),
                key=lambda x: (x[1][2], -x[1][1]),
                reverse=True
            )
            
            for job_id, (players, added_time, priority) in sorted_servers:
                if len(results) >= count:
                    break
                
                if job_id in self._dead:
                    continue
                
                del self._available[job_id]
                self._assigned[job_id] = now
                self._stats['total_given'] += 1
                
                results.append({
                    'job_id': job_id,
                    'players': players,
                    'priority': priority,
                    'age': int(now - added_time)
                })
        
        return results
    
    def report_dead(self, job_id):
        with self._lock:
            now = time.time()
            self._dead[job_id] = now
            self._available.pop(job_id, None)
            self._assigned.pop(job_id, None)
    
    def release_server(self, job_id):
        with self._lock:
            self._assigned.pop(job_id, None)
    
    def count(self):
        with self._lock:
            self._cleanup()
            return len(self._available)
    
    def get_stats(self):
        with self._lock:
            self._cleanup()
            return {
                'available': len(self._available),
                'assigned': len(self._assigned),
                'dead': len(self._dead),
                'total_given': self._stats['total_given'],
                'total_received': self._stats['total_received'],
                'recycled': self._stats['recycled'],
                'duplicates_skipped': self._stats['duplicates_skipped'],
                'dead_skipped': self._stats['dead_skipped'],
                'peak_servers': self._stats['peak_servers'],
                'normal_servers': self._stats['normal_servers'],
                'ttl_seconds': SERVER_TTL
            }

pool = ServerPool()

# ==================== ROBLOX FETCHER ====================
class RobloxFetcher:
    def __init__(self):
        self.running = False
        self.cursors_asc = deque(maxlen=3000)
        self.cursors_desc = deque(maxlen=3000)
        self.cursor_lock = threading.Lock()
        self.last_reset = time.time()
        self.servers_this_minute = 0
        self.minute_lock = threading.Lock()
    
    def fetch_page(self, cursor=None, sort_order='Asc'):
        proxy = proxy_pool.get_proxy()
        if not proxy:
            return [], None, sort_order
        
        proxy_url = proxy.get('http', '')
        
        try:
            url = f"https://games.roblox.com/v1/games/{PLACE_ID}/servers/Public"
            params = {
                'sortOrder': sort_order,
                'limit': SERVERS_PER_REQUEST,
                'excludeFullGames': 'true'
            }
            if cursor:
                params['cursor'] = cursor
            
            response = requests.get(
                url,
                params=params,
                proxies=proxy,
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
                proxy_pool.report_success()
                return servers, next_cursor, sort_order
                
            elif response.status_code == 429:
                proxy_pool.report_error(proxy_url)
                return [], None, sort_order
            else:
                proxy_pool.report_error(proxy_url)
                return [], None, sort_order
                
        except Exception as e:
            proxy_pool.report_error(proxy_url)
            return [], None, sort_order
    
    def fetch_cycle(self):
        config = get_fetch_config()
        
        fetches = []
        fetches.append((None, 'Asc'))
        fetches.append((None, 'Desc'))
        
        with self.cursor_lock:
            asc_list = list(self.cursors_asc)
            desc_list = list(self.cursors_desc)
            
            depth = 35 if config['mode'] == 'PEAK' else 20
            
            if asc_list:
                for c in asc_list[-min(depth, len(asc_list)):]:
                    fetches.append((c, 'Asc'))
            
            if desc_list:
                for c in desc_list[-min(depth, len(desc_list)):]:
                    fetches.append((c, 'Desc'))
        
        fetches = fetches[:config['cursors']]
        
        total_added = 0
        new_cursors_asc = []
        new_cursors_desc = []
        
        with ThreadPoolExecutor(max_workers=config['threads']) as executor:
            futures = {executor.submit(self.fetch_page, c, s): (c, s) for c, s in fetches}
            
            for future in as_completed(futures):
                try:
                    servers, next_cursor, sort_order = future.result()
                    if servers:
                        added, _, _, _ = pool.add_servers(servers, source="fetcher")
                        total_added += added
                    if next_cursor:
                        if sort_order == 'Asc':
                            new_cursors_asc.append(next_cursor)
                        else:
                            new_cursors_desc.append(next_cursor)
                except Exception:
                    pass
        
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
                proxy_stats = proxy_pool.get_stats()
                with self.cursor_lock:
                    depth = len(self.cursors_asc) + len(self.cursors_desc)
                log.info(f"[{config['mode']}] +{rate}/min | Avail: {stats['available']} | Given: {stats['total_given']} | Proxies: {proxy_stats['pool_size']} | Success: {proxy_stats['success_rate']}%")
                self.servers_this_minute = 0
                self.last_reset = now
        
        return total_added
    
    def run(self):
        self.running = True
        log.info(f"[FETCHER] Started with NAProxy API")
        log.info(f"[FETCHER] Peak: {PEAK_START_HOUR}:00-{PEAK_END_HOUR}:00 ({tz_detector.timezone})")
        
        while self.running:
            try:
                config = get_fetch_config()
                self.fetch_cycle()
                time.sleep(config['interval'])
            except Exception as e:
                log.error(f"[FETCHER] Error: {e}")
                time.sleep(1)
    
    def start(self):
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()

fetcher = RobloxFetcher()

# ==================== API ENDPOINTS ====================

@app.route('/status', methods=['GET'])
def status():
    stats = pool.get_stats()
    config = get_fetch_config()
    proxy_stats = proxy_pool.get_stats()
    
    with fetcher.cursor_lock:
        stats['cursors_asc'] = len(fetcher.cursors_asc)
        stats['cursors_desc'] = len(fetcher.cursors_desc)
    
    stats['mode'] = config['mode']
    stats['is_peak'] = is_peak_hours()
    stats['local_hour'] = tz_detector.get_local_hour()
    stats['timezone'] = tz_detector.timezone
    stats['peak_hours'] = f"{PEAK_START_HOUR}:00-{PEAK_END_HOUR}:00"
    stats['proxy'] = proxy_stats
    stats['threads'] = config['threads']
    
    return jsonify(stats)

@app.route('/get-server', methods=['GET'])
def get_server():
    server = pool.get_server()
    if not server:
        return jsonify({'error': 'No servers available', 'retry_ms': 500}), 404
    return jsonify(server)

@app.route('/get-batch', methods=['GET'])
def get_batch():
    count = request.args.get('count', default=1, type=int)
    count = min(count, 50)
    servers = pool.get_batch(count)
    return jsonify({
        'servers': servers,
        'count': len(servers),
        'available': pool.count()
    })

@app.route('/add-pool', methods=['POST'])
def add_pool():
    data = request.get_json() or {}
    servers = data.get('servers', [])
    source = data.get('source', 'mini-api')
    
    if not servers:
        return jsonify({'added': 0}), 200
    
    added, dupes, dead, total = pool.add_servers(servers, source=source)
    log.info(f"[RECEIVE] {source}: +{added} | Avail: {pool.count()}")
    return jsonify({
        'added': added,
        'duplicates': dupes,
        'dead': dead,
        'available': pool.count()
    })

@app.route('/report-dead', methods=['POST'])
def report_dead():
    data = request.get_json() or {}
    job_id = data.get('job_id')
    if job_id:
        pool.report_dead(job_id)
    return jsonify({'status': 'noted'})

@app.route('/release', methods=['POST'])
def release():
    data = request.get_json() or {}
    job_id = data.get('job_id')
    if job_id:
        pool.release_server(job_id)
    return jsonify({'status': 'released'})

@app.route('/health', methods=['GET'])
def health():
    config = get_fetch_config()
    proxy_stats = proxy_pool.get_stats()
    return jsonify({
        'status': 'ok',
        'available': pool.count(),
        'mode': config['mode'],
        'proxies': proxy_stats['pool_size'],
        'local_hour': tz_detector.get_local_hour()
    })

@app.route('/proxy', methods=['GET'])
def proxy_status():
    """Detailed proxy stats"""
    return jsonify(proxy_pool.get_stats())

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    
    log.info(f"[STARTUP] Main API on port {port}")
    log.info(f"[CONFIG] TTL={SERVER_TTL}s | Peak={PEAK_START_HOUR}-{PEAK_END_HOUR}")
    log.info(f"[NAPROXY] Using API with Smart Region")
    
    # Pre-fetch some proxies
    log.info("[NAPROXY] Fetching initial proxy pool...")
    proxy_pool._fetch_proxies()
    
    fetcher.start()
    time.sleep(2)
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
