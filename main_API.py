#!/usr/bin/env python3
"""
MAIN API - Simple Server Pool with TTL
- Receives servers from Mini API via /add-pool
- Distributes to bots via /get-server and /get-batch
- Servers expire after 30 seconds to prevent stale servers
"""

import os
import time
import logging
import threading
from collections import deque
from flask import Flask, jsonify, request

# ==================== CONFIG ====================
SERVER_TTL = 300  # Servers expire after 5 minutes

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

app = Flask(__name__)

# ==================== SERVER POOL ====================
class ServerPool:
    def __init__(self):
        self._lock = threading.Lock()
        self._servers = deque()  # (job_id, players, timestamp)
        self._stats = {'total_given': 0, 'total_received': 0, 'expired': 0}
    
    def _clean_expired(self):
        """Remove expired servers (call while holding lock)"""
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
        return expired
    
    def add_servers(self, servers):
        """Add servers from Mini API"""
        added = 0
        now = time.time()
        with self._lock:
            # Clean old servers first
            self._clean_expired()
            
            # Get existing job IDs to avoid duplicates
            existing = {s[0] for s in self._servers}
            
            for item in servers:
                # Handle both string job_ids and dict formats
                if isinstance(item, str):
                    job_id = item
                    players = 0
                elif isinstance(item, dict):
                    job_id = item.get('id') or item.get('job_id')
                    players = item.get('players', 0)
                else:
                    continue
                
                if job_id and job_id not in existing:
                    self._servers.append((job_id, players, now))
                    existing.add(job_id)
                    added += 1
            
            self._stats['total_received'] += added
        
        return added
    
    def get_server(self):
        """Get one fresh server for bot"""
        now = time.time()
        with self._lock:
            # Clean expired and find a valid server
            while self._servers:
                job_id, players, added_time = self._servers.popleft()
                
                # Skip if expired
                if now - added_time > SERVER_TTL:
                    self._stats['expired'] += 1
                    continue
                
                self._stats['total_given'] += 1
                return {'job_id': job_id, 'players': players, 'age': int(now - added_time)}
            
            return None
    
    def get_batch(self, count):
        """Get multiple fresh servers for bot"""
        results = []
        now = time.time()
        with self._lock:
            while self._servers and len(results) < count:
                job_id, players, added_time = self._servers.popleft()
                
                # Skip if expired
                if now - added_time > SERVER_TTL:
                    self._stats['expired'] += 1
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
    
    def get_stats(self):
        with self._lock:
            self._clean_expired()
            return {
                'available': len(self._servers),
                'total_given': self._stats['total_given'],
                'total_received': self._stats['total_received'],
                'expired': self._stats['expired'],
                'ttl_seconds': SERVER_TTL
            }

pool = ServerPool()

# ==================== API ENDPOINTS ====================

@app.route('/status', methods=['GET'])
def status():
    """Get pool statistics"""
    return jsonify(pool.get_stats())

@app.route('/get-server', methods=['GET'])
def get_server():
    """Get single server for bot"""
    server = pool.get_server()
    
    if not server:
        return jsonify({'error': 'No servers available'}), 404
    
    log.info(f"[GIVE] {server['job_id'][:8]}... age={server['age']}s ({pool.count()} left)")
    return jsonify(server)

@app.route('/get-batch', methods=['GET'])
def get_batch():
    """Get multiple servers for bot"""
    count = request.args.get('count', default=10, type=int)
    count = min(count, 100)
    
    servers = pool.get_batch(count)
    
    if not servers:
        return jsonify({'servers': [], 'count': 0}), 200
    
    log.info(f"[BATCH] Gave {len(servers)} ({pool.count()} left)")
    return jsonify({'servers': servers, 'count': len(servers)})

@app.route('/add-pool', methods=['POST'])
def add_pool():
    """Receive servers from Mini API"""
    data = request.get_json() or {}
    servers = data.get('servers', [])
    
    if not servers:
        return jsonify({'added': 0, 'error': 'No servers provided'}), 400
    
    added = pool.add_servers(servers)
    log.info(f"[ADD] Received {len(servers)}, added {added}, pool now {pool.count()}")
    
    return jsonify({'added': added, 'pool_size': pool.count()})

@app.route('/report-dead', methods=['POST'])
def report_dead():
    """Bots can report dead servers (optional endpoint for future use)"""
    data = request.get_json() or {}
    job_id = data.get('job_id')
    log.info(f"[DEAD] Reported: {job_id[:8] if job_id else 'unknown'}...")
    return jsonify({'status': 'noted'})

@app.route('/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({'status': 'ok', 'servers': pool.count()})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    
    log.info(f"🚀 Main API starting on port {port}")
    log.info(f"⏱️ Server TTL: {SERVER_TTL} seconds")
    log.info("Waiting for servers from Mini API...")
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
