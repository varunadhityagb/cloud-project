#!/usr/bin/env python3
from flask import Flask, request, jsonify
from datetime import datetime, timezone
import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import time

app = Flask(__name__)

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'postgres-service'),
    'port': os.environ.get('DB_PORT', '5432'),
    'database': os.environ.get('DB_NAME', 'carbon_metrics'),
    'user': os.environ.get('DB_USER', 'carbon_user'),
    'password': os.environ.get('DB_PASSWORD', 'carbon_pass_123')
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def init_database():
    max_retries = 10
    for attempt in range(max_retries):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS device_metrics (
                    id SERIAL PRIMARY KEY,
                    device_id VARCHAR(100) NOT NULL,
                    device_type VARCHAR(50),
                    timestamp TIMESTAMP NOT NULL,
                    cpu_percent FLOAT,
                    memory_percent FLOAT,
                    total_power_watts FLOAT,
                    cpu_count INTEGER,
                    applications JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_device_id ON device_metrics(device_id)")
            conn.commit()
            cur.close()
            conn.close()
            print("✅ Database initialized successfully")
            return
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⏳ Waiting for database... ({attempt + 1}/{max_retries})")
                time.sleep(3)
            else:
                print(f"❌ Failed to connect: {e}")
                raise

init_database()

@app.route('/health', methods=['GET'])
def health_check():
    try:
        conn = get_db_connection()
        conn.close()
        db_status = "healthy"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return jsonify({
        "status": "healthy" if db_status == "healthy" else "degraded",
        "database": db_status,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }), 200

@app.route('/api/v1/metrics/ingest', methods=['POST'])
def ingest_metrics():
    try:
        data = request.get_json()
        device_id = data['device_id']
        system_metrics = data['system_metrics']
        applications = data.get('applications', [])
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO device_metrics 
            (device_id, device_type, timestamp, cpu_percent, memory_percent, 
             total_power_watts, cpu_count, applications)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            device_id,
            data.get('device_type', 'laptop'),
            data['timestamp'],
            system_metrics.get('cpu_percent'),
            system_metrics.get('memory_percent'),
            system_metrics.get('total_power_watts'),
            system_metrics.get('cpu_count'),
            json.dumps(applications)
        ))
        record_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({"status": "accepted", "record_id": record_id}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/stats', methods=['GET'])
def get_stats():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT device_id) as unique_devices,
                COALESCE(AVG(total_power_watts), 0) as avg_power
            FROM device_metrics
        """)
        stats = cur.fetchone()
        cur.close()
        conn.close()
        
        return jsonify({
            "total_records": stats['total_records'],
            "unique_devices": stats['unique_devices'],
            "average_power_watts": round(float(stats['avg_power']), 2)
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/metrics/devices', methods=['GET'])
def list_devices():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT 
                device_id,
                COUNT(*) as record_count,
                MAX(timestamp) as last_seen
            FROM device_metrics
            GROUP BY device_id
        """)
        devices = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"devices": devices, "total": len(devices)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
