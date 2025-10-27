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

@app.route('/api/v1/carbon/summary', methods=['GET'])
def carbon_summary():
    """Get overall carbon footprint summary."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT 
                COUNT(*) as total_measurements,
                SUM(carbon_gco2) as total_carbon_g,
                AVG(carbon_gco2) as avg_carbon_per_measurement,
                SUM(power_kwh) as total_energy_kwh,
                COUNT(DISTINCT device_id) as unique_devices
            FROM carbon_footprints
        """)
        
        summary = cur.fetchone()
        cur.close()
        conn.close()
        
        return jsonify({
            "total_measurements": summary['total_measurements'],
            "total_carbon_grams": round(float(summary['total_carbon_g'] or 0), 4),
            "total_carbon_kg": round(float(summary['total_carbon_g'] or 0) / 1000, 6),
            "avg_carbon_per_measurement_g": round(float(summary['avg_carbon_per_measurement'] or 0), 6),
            "total_energy_kwh": round(float(summary['total_energy_kwh'] or 0), 6),
            "unique_devices": summary['unique_devices']
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/carbon/by-device', methods=['GET'])
def carbon_by_device():
    """Get carbon footprint breakdown by device."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT 
                device_id,
                COUNT(*) as measurement_count,
                SUM(carbon_gco2) as total_carbon_g,
                AVG(carbon_gco2) as avg_carbon_g,
                MIN(timestamp) as first_seen,
                MAX(timestamp) as last_seen
            FROM carbon_footprints
            GROUP BY device_id
            ORDER BY total_carbon_g DESC
        """)
        
        devices = cur.fetchall()
        cur.close()
        conn.close()
        
        result = []
        for device in devices:
            result.append({
                "device_id": device['device_id'],
                "measurement_count": device['measurement_count'],
                "total_carbon_grams": round(float(device['total_carbon_g']), 4),
                "total_carbon_kg": round(float(device['total_carbon_g']) / 1000, 6),
                "avg_carbon_per_measurement_g": round(float(device['avg_carbon_g']), 6),
                "first_seen": device['first_seen'].isoformat(),
                "last_seen": device['last_seen'].isoformat()
            })
        
        return jsonify({"devices": result, "total_devices": len(result)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/carbon/by-hour', methods=['GET'])
def carbon_by_hour():
    """Get carbon footprint by hour of day (shows grid intensity impact)."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT 
                EXTRACT(HOUR FROM timestamp) as hour,
                COUNT(*) as measurement_count,
                AVG(grid_intensity_gco2_per_kwh) as avg_grid_intensity,
                SUM(carbon_gco2) as total_carbon_g,
                AVG(carbon_gco2) as avg_carbon_g
            FROM carbon_footprints
            GROUP BY EXTRACT(HOUR FROM timestamp)
            ORDER BY hour
        """)
        
        hours = cur.fetchall()
        cur.close()
        conn.close()
        
        result = []
        for hour_data in hours:
            result.append({
                "hour": int(hour_data['hour']),
                "measurement_count": hour_data['measurement_count'],
                "avg_grid_intensity_gco2_kwh": round(float(hour_data['avg_grid_intensity']), 2),
                "total_carbon_grams": round(float(hour_data['total_carbon_g']), 4),
                "avg_carbon_per_measurement_g": round(float(hour_data['avg_carbon_g']), 6)
            })
        
        return jsonify({"hourly_breakdown": result}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/carbon/device/<device_id>', methods=['GET'])
def carbon_device_detail(device_id: str):
    """Get detailed carbon footprint for a specific device."""
    try:
        limit = request.args.get('limit', type=int, default=50)
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT 
                timestamp,
                power_kwh,
                grid_intensity_gco2_per_kwh,
                carbon_gco2,
                calculated_at
            FROM carbon_footprints
            WHERE device_id = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (device_id, limit))
        
        records = cur.fetchall()
        cur.close()
        conn.close()
        
        if not records:
            return jsonify({"error": f"No carbon data for device {device_id}"}), 404
        
        result = []
        for record in records:
            result.append({
                "timestamp": record['timestamp'].isoformat(),
                "power_kwh": float(record['power_kwh']),
                "grid_intensity_gco2_kwh": float(record['grid_intensity_gco2_per_kwh']),
                "carbon_gco2": float(record['carbon_gco2']),
                "calculated_at": record['calculated_at'].isoformat()
            })
        
        return jsonify({
            "device_id": device_id,
            "record_count": len(result),
            "measurements": result
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
