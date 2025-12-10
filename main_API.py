#!/usr/bin/env python3
"""
MAIN API - Static IPs with Residential Fallback
- 4 Static IPs as primary
- Residential proxy fallback when rate limited
- Auto timezone detection
- Peak hours: 5-8 PM local time
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
SERVER_TTL = 300  # 5 minutes
GIVEN_TTL = 300
PLACE_ID = 109983668079237

# Static IPs (Primary - faster, limited)
STATIC_IPS = [
    {"host": "45.82.245.107", "port": "6006", "user": "proxy-jjvnubn4uo17", "pass": "CeNOkLV1A3ObkRy4"},
    {"host": "163.171.161.247", "port": "6006", "user": "proxy-jjvnubn4uo17", "pass": "CeNOkLV1A3ObkRy4"},
    {"host": "43.153.175.48", "port": "6006", "user": "proxy-jjvnubn4uo17", "pass": "CeNOkLV1A3ObkRy4"},
    {"host": "185.92.209.125", "port": "6006", "user": "proxy-jjvnubn4uo17", "pass": "CeNOkLV1A3ObkRy4"},
]

# Residential Proxy (Fallback - unlimited IPs)
RESIDENTIAL_PROXY = {
    "host": "us.naproxy.net",
    "port": "1000",
    "user": "proxy-e5a1ntzmrlr3",
    "pass": "Ol43jGdsIuPUNacc"
}

# Peak hours (5 PM - 8 PM in detected timezone)
PEAK_START_HOUR = 17
PEAK_END_HOUR = 20

# Fetching config
FETCH_THREADS_PEAK = 16
FETCH_THREADS_NORMAL = 6
FETCH_INTERVAL_PEAK = 1
FETCH_INTERVAL_NORMAL = 3
SERVERS_PER_REQUEST = 100
CURSORS_PER_CYCLE_PEAK = 50
CURSORS_PER_CYCLE_NORMAL = 20

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
        self.timezone = None
        self.utc_offset = 0
        self.detected = False
        self._detect()
    
    def _detect(self):
        """Auto-detect timezone from multiple sources"""
        # Method 1: Try IP geolocation
        try:
            resp = requests.get('http://ip-api.com/json/', timeout=5)
            if resp.ok:
                data = resp.json()
                self.timezone = data.get('timezone', 'America/New_York')
                self.utc_offset = self._get_offset(self.timezone)
                self.detected = True
                log.info(f"[TIMEZONE] Detected: {self.timezone} (UTC{self.utc_offset:+d})")
                return
        except:
            pass
        
        # Method 2: Try worldtimeapi
        try:
            resp = requests.get('http://worldtimeapi.org/api/ip', timeout=5)
            if resp.ok:
                data = resp.json()
                self.timezone = data.get('timezone', 'America/New_York')
                self.utc_offset = int(data.get('utc_offset', '-05:00').split(':')[0])
                self.detected = True
                log.info(f"[TIMEZONE] Detected: {self.timezone} (UTC{self.utc_offset:+d})")
                return
        except:
            pass
        
        # Fallback to EST
        self.timezone = 'America/New_York'
        self.utc_offset = -5
        log.info(f"[TIMEZONE] Fallback: {self.timezone} (UTC{self.utc_offset:+d})")
    
    def _get_offset(self, tz_name):
        """Get UTC offset for timezone"""
        try:
            import pytz
            from datetime import datetime
            tz = pytz.timezone(tz_name)
            now = datetime.now(tz)
            return int(now.utcoffset().total_seconds() / 3600)
        except:
            # Common timezone offsets
            offsets = {
                'America/New_York': -5, 'America/Chicago': -6,
                'America/Denver': -7, 'America/Los_Angeles': -8,
                'Europe/London': 0, 'Europe/Paris': 1,
                'Asia/Tokyo': 9, 'Australia/Sydney': 11
            }
            return offsets.get(tz_name, -5)
    
    def get_local_hour(self):
        """Get current hour in detected timezone"""
        import time
        utc_hour = time.gmtime().tm_hour
        local_hour = (utc_hour + self.utc_offset) % 24
        return local_hour
    
    def is_peak(self):
        """Check if current time is peak hours (5-8 PM local)"""
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

# ==================== SMART IP ROTATION ====================
class SmartIPRotator:
    def __init__(self, static_ips, residential):
        self.static_ips = static_ips
        self.residential = residential
        self.static_index = 0
        self.lock = threading.Lock()
        
        # Track each static IP
        self.static_status = {}
        for i, ip in enumerate(static_ips):
            self.static_status[i] = {
                'usage': 0,
                'success': 0,
                'errors': 0,
                'last_error': 0,
                'cooldown_until': 0,
                'consecutive_errors': 0
            }
        
        # Track residential usage
        self.residential_stats = {
            'usage': 0,
            'success': 0,
            'errors': 0,
            'fallback_count': 0
        }
    
    def _build_proxy_url(self, ip_config, session_id=None):
        """Build proxy URL from config"""
        user = ip_config['user']
        if session_id:
            user = f"{user}_session-{session_id}"
        return f"http://{user}:{ip_config['pass']}@{ip_config['host']}:{ip_config['port']}"
    
    def get_proxy(self):
        """Get best available proxy - static first, residential fallback"""
        with self.lock:
            now = time.time()
            
            # Try static IPs first (round-robin, skip cooldowns)
            for _ in range(len(self.static_ips)):
                idx = self.static_index
                self.static_index = (self.static_index + 1) % len(self.static_ips)
                status = self.static_status[idx]
                
                # Skip if on cooldown
                if now < status['cooldown_until']:
                    continue
                
                # Use this static IP
                status['usage'] += 1
                ip_config = self.static_ips[idx]
                proxy_url = self._build_proxy_url(ip_config)
                
                return {
                    'http': proxy_url,
                    'https': proxy_url
                }, 'static', idx
            
            # All static IPs on cooldown - use residential
            self.residential_stats['usage'] += 1
            self.residential_stats['fallback_count'] += 1
            
            session_id = f"s{random.randint(100000, 999999)}"
            proxy_url = self._build_proxy_url(self.residential, session_id)
            
            return {
                'http': proxy_url,
                'https': proxy_url
            }, 'residential', -1
    
    def report_success(self, proxy_type, idx):
        """Report successful request"""
        with self.lock:
            if proxy_type == 'static' and idx >= 0:
                status = self.static_status[idx]
                status['success'] += 1
                status['consecutive_errors'] = 0
                # Reduce cooldown on success
                if status['cooldown_until'] > 0:
                    status['cooldown_until'] = max(0, status['cooldown_until'] - 10)
            else:
                self.residential_stats['success'] += 1
    
    def report_error(self, proxy_type, idx, is_rate_limit=False):
        """Report failed request"""
        with self.lock:
            now = time.time()
            
            if proxy_type == 'static' and idx >= 0:
                status = self.static_status[idx]
                status['errors'] += 1
                status['last_error'] = now
                status['consecutive_errors'] += 1
                
                # Progressive cooldown based on consecutive errors
                if is_rate_limit or status['consecutive_errors'] >= 2:
                    # 30s base + 15s per consecutive error (max 2 min)
                    cooldown = min(30 + (status['consecutive_errors'] * 15), 120)
                    status['cooldown_until'] = now + cooldown
                    log.warning(f"[IP] Static #{idx} cooldown {cooldown}s (errors: {status['consecutive_errors']})")
            else:
                self.residential_stats['errors'] += 1
    
    def get_stats(self):
        """Get rotation statistics"""
        with self.lock:
            now = time.time()
            static_stats = []
            
            for i, status in self.static_status.items():
                ip = self.static_ips[i]['host']
                cooldown_remaining = max(0, int(status['cooldown_until'] - now))
                static_stats.append({
                    'ip': ip,
                    'usage': status['usage'],
                    'success': status['success'],
                    'errors': status['errors'],
                    'cooldown': cooldown_remaining,
                    'active': cooldown_remaining == 0
                })
            
            return {
                'static': static_stats,
                'residential': self.residential_stats.copy(),
                'active_static': sum(1 for s in static_stats if s['active'])
            }

ip_rotator = SmartIPRotator(STATIC_IPS, RESIDENTIAL_PROXY)

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
            'fetch_errors': 0,
            'fetch_success': 0,
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
        
        return recycled
    
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
                
                if job_id in self._assigned:
                    duplicates += 1
                    continue
                
                if job_id in self._available:
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
            if job_id in self._available:
                del self._available[job_id]
            if job_id in self._assigned:
                del self._assigned[job_id]
    
    def release_server(self, job_id):
        with self._lock:
            if job_id in self._assigned:
                del self._assigned[job_id]
    
    def count(self):
        with self._lock:
            self._cleanup()
            return len(self._available)
    
    def record_fetch_error(self):
        with self._lock:
            self._stats['fetch_errors'] += 1
    
    def record_fetch_success(self):
        with self._lock:
            self._stats['fetch_success'] += 1
    
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
                'fetch_errors': self._stats['fetch_errors'],
                'fetch_success': self._stats['fetch_success'],
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
        max_retries = 2
        proxy, proxy_type, ip_idx = ip_rotator.get_proxy()
        
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
                    pool.record_fetch_success()
                    ip_rotator.report_success(proxy_type, ip_idx)
                    return servers, next_cursor, sort_order
                    
                elif response.status_code == 429:
                    ip_rotator.report_error(proxy_type, ip_idx, is_rate_limit=True)
                    # Get new proxy for retry
                    proxy, proxy_type, ip_idx = ip_rotator.get_proxy()
                    time.sleep(1)
                    continue
                else:
                    pool.record_fetch_error()
                    ip_rotator.report_error(proxy_type, ip_idx)
                    return [], None, sort_order
                    
            except Exception as e:
                ip_rotator.report_error(proxy_type, ip_idx)
                if attempt < max_retries - 1:
                    proxy, proxy_type, ip_idx = ip_rotator.get_proxy()
                    time.sleep(0.5)
                    continue
                pool.record_fetch_error()
                return [], None, sort_order
        
        return [], None, sort_order
    
    def fetch_cycle(self):
        config = get_fetch_config()
        
        fetches = []
        fetches.append((None, 'Asc'))
        fetches.append((None, 'Desc'))
        
        with self.cursor_lock:
            asc_list = list(self.cursors_asc)
            desc_list = list(self.cursors_desc)
            
            depth = 30 if config['mode'] == 'PEAK' else 15
            
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
                        added, dupes, dead, total = pool.add_servers(servers, source="fetcher")
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
                ip_stats = ip_rotator.get_stats()
                with self.cursor_lock:
                    depth = len(self.cursors_asc) + len(self.cursors_desc)
                log.info(f"[{config['mode']}] +{rate}/min | Avail: {stats['available']} | Given: {stats['total_given']} | Static: {ip_stats['active_static']}/4 | Resi: {ip_stats['residential']['fallback_count']}")
                self.servers_this_minute = 0
                self.last_reset = now
        
        return total_added
    
    def run(self):
        self.running = True
        log.info(f"[FETCHER] Started - 4 Static IPs + Residential fallback")
        log.info(f"[FETCHER] Peak hours: {PEAK_START_HOUR}:00-{PEAK_END_HOUR}:00 ({tz_detector.timezone})")
        
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
    ip_stats = ip_rotator.get_stats()
    
    with fetcher.cursor_lock:
        stats['cursors_asc'] = len(fetcher.cursors_asc)
        stats['cursors_desc'] = len(fetcher.cursors_desc)
    
    stats['mode'] = config['mode']
    stats['is_peak'] = is_peak_hours()
    stats['local_hour'] = tz_detector.get_local_hour()
    stats['timezone'] = tz_detector.timezone
    stats['peak_hours'] = f"{PEAK_START_HOUR}:00-{PEAK_END_HOUR}:00"
    stats['ip_stats'] = ip_stats
    stats['threads'] = config['threads']
    stats['interval'] = config['interval']
    
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
    config = get_fetch_config()
    log.info(f"[{config['mode']}] {source}: +{added} new | Avail: {pool.count()}")
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
    return jsonify({
        'status': 'ok',
        'available': pool.count(),
        'mode': config['mode'],
        'is_peak': is_peak_hours(),
        'local_hour': tz_detector.get_local_hour()
    })

@app.route('/ips', methods=['GET'])
def get_ip_status():
    """View detailed IP status"""
    return jsonify(ip_rotator.get_stats())

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    
    log.info(f"[STARTUP] Main API on port {port}")
    log.info(f"[CONFIG] TTL={SERVER_TTL}s | Peak={PEAK_START_HOUR}-{PEAK_END_HOUR}")
    log.info(f"[CONFIG] 4 Static IPs + Residential fallback")
    log.info(f"[TIMEZONE] {tz_detector.timezone} (UTC{tz_detector.utc_offset:+d})")
    
    fetcher.start()
    time.sleep(2)
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
