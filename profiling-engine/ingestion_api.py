from flask import Flask, request, jsonify
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import time
from prophet import Prophet
import pickle
from pathlib import Path
import pandas as pd
import numpy as np
import sys
import warnings
warnings.filterwarnings('ignore')


app = Flask(__name__)

# Indian Standard Time
IST = ZoneInfo("Asia/Kolkata")

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'postgres-service'),
    'port': os.environ.get('DB_PORT', '5432'),
    'database': os.environ.get('DB_NAME', 'carbon_metrics'),
    'user': os.environ.get('DB_USER', 'carbon_user'),
    'password': os.environ.get('DB_PASSWORD', 'carbon_pass_123')
}

ML_MODEL_PATH = Path("./models/grid_prophet.pkl")
ml_model = None
ml_model_error = None

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def init_database():
    max_retries = 10
    for attempt in range(max_retries):
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            # Set timezone to IST for this connection
            cur.execute("SET timezone = 'Asia/Kolkata'")

            # Main metrics table with location
            cur.execute("""
                CREATE TABLE IF NOT EXISTS device_metrics (
                    id SERIAL PRIMARY KEY,
                    device_id VARCHAR(100) NOT NULL,
                    device_type VARCHAR(50),
                    timestamp TIMESTAMPTZ NOT NULL,

                    -- Location data
                    latitude FLOAT,
                    longitude FLOAT,
                    city VARCHAR(100),
                    region VARCHAR(100),
                    country VARCHAR(100),
                    country_code VARCHAR(10),

                    -- System metrics
                    cpu_percent FLOAT,
                    memory_percent FLOAT,
                    total_power_watts FLOAT,
                    cpu_count INTEGER,
                    applications JSONB,

                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            cur.execute("CREATE INDEX IF NOT EXISTS idx_device_id ON device_metrics(device_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON device_metrics(timestamp)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_location ON device_metrics(latitude, longitude)")

            conn.commit()
            cur.close()
            conn.close()
            print("Database initialized successfully (Timezone: Asia/Kolkata)")
            return
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⏳ Waiting for database... ({attempt + 1}/{max_retries})")
                time.sleep(3)
            else:
                print(f"Failed to connect: {e}")
                raise

init_database()


def load_ml_model():
    """Load Prophet model if available."""
    global ml_model, ml_model_error

    if ml_model is not None:
        return True  # Already loaded

    print(f"🔍 Attempting to load ML model...", file=sys.stderr)
    print(f"   Path: {ML_MODEL_PATH}", file=sys.stderr)
    print(f"   Exists: {ML_MODEL_PATH.exists()}", file=sys.stderr)
    print(f"   Current dir: {Path.cwd()}", file=sys.stderr)

    if ML_MODEL_PATH.exists():
        try:
            print("📥 Loading Prophet model...", file=sys.stderr)
            with open(ML_MODEL_PATH, 'rb') as f:
                ml_model = pickle.load(f)
            print("✅ ML prediction model loaded successfully", file=sys.stderr)
            return True
        except Exception as e:
            ml_model_error = str(e)
            print(f"❌ Failed to load ML model: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            return False
    else:
        ml_model_error = f"Model file not found at {ML_MODEL_PATH}"
        print(f"❌ {ml_model_error}", file=sys.stderr)
        # List what files ARE there
        models_dir = Path("/app/models")
        if models_dir.exists():
            print(f"   Files in models dir: {list(models_dir.glob('*'))}", file=sys.stderr)
        return False

def predict_next_24h():
    """Predict grid intensity for next 24 hours."""
    global ml_model

    # Lazy load model if not loaded
    if ml_model is None:
        load_ml_model()

    if ml_model is None:
        return None

    try:
        now = datetime.now(IST).replace(tzinfo=None)
        future_dates = pd.date_range(start=now, end=now + timedelta(hours=24), freq='H')
        future = pd.DataFrame({'ds': future_dates})

        forecast = ml_model.predict(future)

        result = []
        for _, row in forecast.iterrows():
            result.append({
                'timestamp': pd.to_datetime(row['ds']).tz_localize(IST).isoformat(),
                'hour': pd.to_datetime(row['ds']).hour,
                'predicted_intensity': round(float(row['yhat']), 2),
                'lower_bound': round(float(row['yhat_lower']), 2),
                'upper_bound': round(float(row['yhat_upper']), 2)
            })

        return result
    except Exception as e:
        print(f"Prediction error: {e}")
        import traceback
        traceback.print_exc()
        return None

@app.route('/health', methods=['GET'])
def health_check():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SET timezone = 'Asia/Kolkata'")
        cur.execute("SHOW timezone")
        db_timezone = cur.fetchone()[0]
        cur.close()
        conn.close()
        db_status = "healthy"
    except Exception as e:
        db_status = f"error: {str(e)}"
        db_timezone = "unknown"

    return jsonify({
        "status": "healthy" if db_status == "healthy" else "degraded",
        "database": db_status,
        "timezone": db_timezone,
        "timestamp": datetime.now(IST).isoformat()
    }), 200

@app.route('/api/v1/metrics/ingest', methods=['POST'])
def ingest_metrics():
    try:
        data = request.get_json()
        device_id = data['device_id']
        system_metrics = data['system_metrics']
        applications = data.get('applications', [])
        location = data.get('location', {})

        # Parse timestamp - handle both with/without timezone
        timestamp_str = data['timestamp']
        if '+' in timestamp_str or 'Z' in timestamp_str:
            # Already has timezone
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        else:
            # Assume IST
            timestamp = datetime.fromisoformat(timestamp_str).replace(tzinfo=IST)

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SET timezone = 'Asia/Kolkata'")

        cur.execute("""
            INSERT INTO device_metrics
            (device_id, device_type, timestamp,
             latitude, longitude, city, region, country, country_code,
             cpu_percent, memory_percent, total_power_watts, cpu_count, applications)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            device_id,
            data.get('device_type', 'laptop'),
            timestamp,
            location.get('latitude'),
            location.get('longitude'),
            location.get('city'),
            location.get('region'),
            location.get('country'),
            location.get('country_code'),
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
        cur.execute("SET timezone = 'Asia/Kolkata'")

        cur.execute("""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT device_id) as unique_devices,
                COALESCE(AVG(total_power_watts), 0) as avg_power,
                COUNT(DISTINCT city) as unique_cities
            FROM device_metrics
        """)
        stats = cur.fetchone()
        cur.close()
        conn.close()

        return jsonify({
            "total_records": stats['total_records'],
            "unique_devices": stats['unique_devices'],
            "unique_cities": stats['unique_cities'],
            "average_power_watts": round(float(stats['avg_power']), 2),
            "timezone": "Asia/Kolkata (IST)"
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/carbon/summary', methods=['GET'])
def carbon_summary():
    """Get overall carbon footprint summary with embodied carbon breakdown."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SET timezone = 'Asia/Kolkata'")

        cur.execute("""
            SELECT
                COUNT(*) as total_measurements,
                SUM(operational_carbon_gco2) as total_operational_g,
                SUM(embodied_carbon_gco2) as total_embodied_g,
                SUM(total_carbon_gco2) as total_carbon_g,
                AVG(total_carbon_gco2) as avg_carbon_per_measurement,
                SUM(power_kwh) as total_energy_kwh,
                COUNT(DISTINCT device_id) as unique_devices
            FROM carbon_footprints
        """)

        summary = cur.fetchone()
        cur.close()
        conn.close()

        total_operational = float(summary['total_operational_g'] or 0)
        total_embodied = float(summary['total_embodied_g'] or 0)
        total_carbon = float(summary['total_carbon_g'] or 0)

        operational_pct = (total_operational / total_carbon * 100) if total_carbon > 0 else 0
        embodied_pct = (total_embodied / total_carbon * 100) if total_carbon > 0 else 0

        return jsonify({
            "total_measurements": summary['total_measurements'],
            "operational_carbon_grams": round(total_operational, 4),
            "embodied_carbon_grams": round(total_embodied, 4),
            "total_carbon_grams": round(total_carbon, 4),
            "total_carbon_kg": round(total_carbon / 1000, 6),
            "operational_percentage": round(operational_pct, 2),
            "embodied_percentage": round(embodied_pct, 2),
            "avg_carbon_per_measurement_g": round(float(summary['avg_carbon_per_measurement'] or 0), 6),
            "total_energy_kwh": round(float(summary['total_energy_kwh'] or 0), 6),
            "unique_devices": summary['unique_devices'],
            "timezone": "Asia/Kolkata (IST)"
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/carbon/by-device', methods=['GET'])
def carbon_by_device():
    """Get carbon footprint breakdown by device with embodied carbon."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SET timezone = 'Asia/Kolkata'")

        cur.execute("""
            SELECT
                device_id,
                device_type,
                COUNT(*) as measurement_count,
                SUM(operational_carbon_gco2) as total_operational_g,
                SUM(embodied_carbon_gco2) as total_embodied_g,
                SUM(total_carbon_gco2) as total_carbon_g,
                AVG(embodied_total_kgco2e) as avg_embodied_total_kg,
                MIN(timestamp) as first_seen,
                MAX(timestamp) as last_seen
            FROM carbon_footprints
            GROUP BY device_id, device_type
            ORDER BY total_carbon_g DESC
        """)

        devices = cur.fetchall()
        cur.close()
        conn.close()

        result = []
        for device in devices:
            total_carbon = float(device['total_carbon_g'])
            total_operational = float(device['total_operational_g'])
            total_embodied = float(device['total_embodied_g'])

            result.append({
                "device_id": device['device_id'],
                "device_type": device['device_type'],
                "measurement_count": device['measurement_count'],
                "operational_carbon_grams": round(total_operational, 4),
                "embodied_carbon_grams": round(total_embodied, 4),
                "total_carbon_grams": round(total_carbon, 4),
                "total_carbon_kg": round(total_carbon / 1000, 6),
                "embodied_total_device_kg": round(float(device['avg_embodied_total_kg'] or 0), 2),
                "first_seen": device['first_seen'].isoformat(),
                "last_seen": device['last_seen'].isoformat()
            })

        return jsonify({
            "devices": result,
            "total_devices": len(result),
            "timezone": "Asia/Kolkata (IST)"
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/carbon/by-hour', methods=['GET'])
def carbon_by_hour():
    """Get carbon footprint by hour with embodied carbon breakdown (IST timezone)."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SET timezone = 'Asia/Kolkata'")

        cur.execute("""
            SELECT
                EXTRACT(HOUR FROM timestamp AT TIME ZONE 'Asia/Kolkata') as hour,
                COUNT(*) as measurement_count,
                AVG(grid_intensity_gco2_per_kwh) as avg_grid_intensity,
                SUM(operational_carbon_gco2) as total_operational_g,
                SUM(embodied_carbon_gco2) as total_embodied_g,
                SUM(total_carbon_gco2) as total_carbon_g
            FROM carbon_footprints
            GROUP BY EXTRACT(HOUR FROM timestamp AT TIME ZONE 'Asia/Kolkata')
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
                "operational_carbon_grams": round(float(hour_data['total_operational_g']), 4),
                "embodied_carbon_grams": round(float(hour_data['total_embodied_g']), 4),
                "total_carbon_grams": round(float(hour_data['total_carbon_g']), 4)
            })

        return jsonify({
            "hourly_breakdown": result,
            "timezone": "Asia/Kolkata (IST)"
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/carbon/device/<device_id>', methods=['GET'])
def carbon_device_detail(device_id: str):
    """Get detailed carbon footprint for a specific device."""
    try:
        limit = request.args.get('limit', type=int, default=50)

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SET timezone = 'Asia/Kolkata'")

        cur.execute("""
            SELECT
                timestamp,
                power_kwh,
                grid_intensity_gco2_per_kwh,
                operational_carbon_gco2,
                embodied_carbon_gco2,
                total_carbon_gco2,
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
                "operational_carbon_gco2": float(record['operational_carbon_gco2']),
                "embodied_carbon_gco2": float(record['embodied_carbon_gco2']),
                "total_carbon_gco2": float(record['total_carbon_gco2']),
                "calculated_at": record['calculated_at'].isoformat()
            })

        return jsonify({
            "device_id": device_id,
            "record_count": len(result),
            "measurements": result,
            "timezone": "Asia/Kolkata (IST)"
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/metrics/devices', methods=['GET'])
def list_devices():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SET timezone = 'Asia/Kolkata'")

        cur.execute("""
            SELECT
                device_id,
                city,
                country,
                COUNT(*) as record_count,
                MAX(timestamp) as last_seen
            FROM device_metrics
            GROUP BY device_id, city, country
        """)
        devices = cur.fetchall()
        cur.close()
        conn.close()

        # Convert timestamps to IST
        for device in devices:
            if device['last_seen']:
                device['last_seen'] = device['last_seen'].isoformat()

        return jsonify({
            "devices": devices,
            "total": len(devices),
            "timezone": "Asia/Kolkata (IST)"
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/v1/ml/predict-24h', methods=['GET'])
def ml_predict_24h():
    """Get ML predictions for next 24 hours."""
    # Try to load model
    load_ml_model()

    if ml_model is None:
        error_msg = ml_model_error or 'Model not available'
        print(f"❌ ML endpoint called but model unavailable: {error_msg}", file=sys.stderr)
        return jsonify({
            'error': 'ML model not available',
            'details': error_msg
        }), 503

    predictions = predict_next_24h()
    if predictions is None:
        return jsonify({'error': 'Prediction failed'}), 500

    return jsonify({
        'predictions': predictions,
        'model_available': True,
        'timezone': 'Asia/Kolkata (IST)',
        'generated_at': datetime.now(IST).isoformat()
    }), 200

@app.route('/api/v1/ml/greenest-hours', methods=['GET'])
def ml_greenest_hours():
    """Get greenest hours from ML predictions."""
    load_ml_model()
    if ml_model is None:
        return jsonify({'error': 'ML model not available'}), 503

    predictions = predict_next_24h()
    if predictions is None:
        return jsonify({'error': 'Prediction failed'}), 500

    # Sort by intensity and get top 5
    sorted_predictions = sorted(predictions, key=lambda x: x['predicted_intensity'])
    greenest = sorted_predictions[:5]

    return jsonify({
        'greenest_hours': greenest,
        'timezone': 'Asia/Kolkata (IST)',
        'generated_at': datetime.now(IST).isoformat()
    }), 200

@app.route('/api/v1/ml/recommendation', methods=['GET'])
def ml_recommendation():
    """Get smart scheduling recommendation based on predictions."""

    predictions = predict_next_24h()
    if predictions is None:
        return jsonify({'error': 'Prediction failed'}), 500

    current_hour = datetime.now(IST).hour
    current_pred = next((p for p in predictions if p['hour'] == current_hour), predictions[0])

    avg_intensity = sum(p['predicted_intensity'] for p in predictions) / len(predictions)
    min_intensity = min(p['predicted_intensity'] for p in predictions)

    current_intensity = current_pred['predicted_intensity']
    percent_vs_avg = ((current_intensity - avg_intensity) / avg_intensity) * 100
    percent_vs_best = ((current_intensity - min_intensity) / min_intensity) * 100

    # Find greenest hour
    greenest = min(predictions, key=lambda x: x['predicted_intensity'])
    hours_until_greenest = (greenest['hour'] - current_hour) % 24

    # Generate recommendation
    if percent_vs_avg < -15:
        status = "excellent"
        message = "Grid is predicted to be much cleaner than average. Excellent time for intensive workloads."
        action = "Run batch jobs, ML training, large builds now"
    elif percent_vs_avg < 0:
        status = "good"
        message = "Grid is cleaner than average. Good time for most workloads."
        action = "Normal operations recommended"
    elif percent_vs_avg < 15:
        status = "moderate"
        message = "Grid is slightly dirtier than average. Consider deferring non-urgent tasks."
        action = f"Wait {hours_until_greenest}h for greenest window"
    else:
        status = "poor"
        message = "Grid is predicted to be much dirtier than average. Defer intensive workloads."
        action = f"Wait {hours_until_greenest}h for cleanest grid (save {abs(percent_vs_best):.0f}% emissions)"

    return jsonify({
        'status': status,
        'current_hour': current_hour,
        'current_intensity': round(current_intensity, 2),
        'average_intensity': round(avg_intensity, 2),
        'best_intensity': round(min_intensity, 2),
        'percent_vs_average': round(percent_vs_avg, 2),
        'percent_vs_best': round(percent_vs_best, 2),
        'message': message,
        'action': action,
        'greenest_hour': greenest['hour'],
        'hours_until_greenest': hours_until_greenest,
        'timezone': 'Asia/Kolkata (IST)',
        'generated_at': datetime.now(IST).isoformat()
    }), 200

@app.route('/api/v1/insights/missed-opportunities', methods=['GET'])
def missed_opportunities():
    """Calculate missed carbon savings opportunities."""

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SET timezone = 'Asia/Kolkata'")

        # Get last 24 hours of actual usage
        cur.execute("""
            SELECT
                EXTRACT(HOUR FROM timestamp) as hour,
                SUM(total_carbon_gco2) as actual_carbon,
                AVG(grid_intensity_gco2_per_kwh) as actual_intensity,
                SUM(power_kwh) as total_energy
            FROM carbon_footprints
            WHERE timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY EXTRACT(HOUR FROM timestamp)
            ORDER BY hour
        """)

        actual_usage = cur.fetchall()
        cur.close()
        conn.close()

        if not actual_usage:
            return jsonify({'message': 'Not enough data yet'}), 200

        # Get today's predictions for comparison
        predictions = predict_next_24h()
        if not predictions:
            return jsonify({'error': 'Could not generate predictions'}), 500

        # Calculate missed opportunities
        opportunities = []
        total_missed_savings = 0

        for usage in actual_usage:
            hour = int(usage['hour'])
            actual_carbon = float(usage['actual_carbon'])
            actual_intensity = float(usage['actual_intensity'])
            energy = float(usage['total_energy'])

            # Find greenest hour in predictions
            greenest = min(predictions, key=lambda x: x['predicted_intensity'])
            optimal_intensity = greenest['predicted_intensity']
            optimal_carbon = energy * optimal_intensity

            if actual_intensity > optimal_intensity * 1.15:  # 15% threshold
                savings = actual_carbon - optimal_carbon
                total_missed_savings += savings

                opportunities.append({
                    'hour': hour,
                    'actual_intensity': round(actual_intensity, 2),
                    'optimal_intensity': round(optimal_intensity, 2),
                    'optimal_hour': greenest['hour'],
                    'carbon_emitted': round(actual_carbon, 4),
                    'carbon_could_have_been': round(optimal_carbon, 4),
                    'missed_savings_gco2': round(savings, 4),
                    'percent_savings_missed': round((savings / actual_carbon) * 100, 1)
                })

        return jsonify({
            'opportunities': opportunities,
            'total_missed_savings_gco2': round(total_missed_savings, 4),
            'total_missed_savings_kg': round(total_missed_savings / 1000, 6),
            'opportunity_count': len(opportunities),
            'timezone': 'Asia/Kolkata (IST)'
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    load_ml_model()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
