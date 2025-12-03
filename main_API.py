#!/usr/bin/env python3
"""
MAIN API - Simple Server Pool
- Receives servers from Mini API via /add-pool
- Distributes to bots via /get-server and /get-batch
- No internal Roblox fetching (Mini API handles that)
"""

import os
import time
import logging
import threading
from collections import deque
from flask import Flask, jsonify, request

# ==================== CONFIG ====================
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
        self._servers = deque()
        self._stats = {'total_given': 0, 'total_received': 0}
    
    def add_servers(self, servers):
        """Add servers from Mini API"""
        added = 0
        with self._lock:
            # Get existing job IDs to avoid duplicates in current pool only
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
                    self._servers.append((job_id, players))
                    existing.add(job_id)
                    added += 1
            
            self._stats['total_received'] += added
        
        return added
    
    def get_server(self):
        """Get one server for bot"""
        with self._lock:
            if not self._servers:
                return None
            job_id, players = self._servers.popleft()
            self._stats['total_given'] += 1
            return {'job_id': job_id, 'players': players}
    
    def get_batch(self, count):
        """Get multiple servers for bot"""
        results = []
        with self._lock:
            for _ in range(min(count, len(self._servers))):
                job_id, players = self._servers.popleft()
                results.append({'job_id': job_id, 'players': players})
                self._stats['total_given'] += 1
        return results
    
    def count(self):
        with self._lock:
            return len(self._servers)
    
    def get_stats(self):
        with self._lock:
            return {
                'available': len(self._servers),
                'total_given': self._stats['total_given'],
                'total_received': self._stats['total_received']
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
    
    log.info(f"[GIVE] {server['job_id'][:8]}... ({pool.count()} left)")
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

@app.route('/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({'status': 'ok', 'servers': pool.count()})

# ==================== MAIN ====================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    
    log.info(f"🚀 Main API starting on port {port}")
    log.info("Waiting for servers from Mini API...")
    
    app.run(host='0.0.0.0', port=port, threaded=True, debug=False)
