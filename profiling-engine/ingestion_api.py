"""
Carbon Data Ingestion API
Receives device metrics from personal computing devices
and stores them for processing.
"""

from flask import Flask, request, jsonify
from datetime import datetime
import json
import os
from typing import Dict, List

app = Flask(__name__)

# In-memory storage for now (we'll add a database later)
# Structure: {device_id: [metrics_list]}
metrics_store: Dict[str, List[Dict]] = {}

# Statistics
stats = {
    "total_records": 0,
    "unique_devices": 0,
    "api_version": "v1.0",
    "started_at": datetime.utcnow().isoformat()
}


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for Kubernetes liveness/readiness probes."""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "carbon-ingestion-api"
    }), 200


@app.route('/api/v1/metrics/ingest', methods=['POST'])
def ingest_metrics():
    """
    Ingest device metrics.
    
    Expected payload:
    {
        "device_id": "device_5613",
        "device_type": "laptop",
        "timestamp": "2025-10-26T18:36:54.790884Z",
        "system_metrics": {...},
        "applications": [...]
    }
    """
    try:
        # Parse incoming JSON
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['device_id', 'timestamp', 'system_metrics']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    "error": f"Missing required field: {field}",
                    "status": "rejected"
                }), 400
        
        device_id = data['device_id']
        
        # Initialize device storage if new
        if device_id not in metrics_store:
            metrics_store[device_id] = []
            stats["unique_devices"] = len(metrics_store)
        
        # Add server-side timestamp
        data['received_at'] = datetime.utcnow().isoformat()
        
        # Store the metrics
        metrics_store[device_id].append(data)
        stats["total_records"] += 1
        
        # Log the ingestion
        app.logger.info(f"Ingested metrics from {device_id} | "
                       f"Power: {data['system_metrics']['total_power_watts']}W | "
                       f"Apps: {len(data.get('applications', []))}")
        
        return jsonify({
            "status": "accepted",
            "device_id": device_id,
            "record_count": len(metrics_store[device_id]),
            "received_at": data['received_at']
        }), 201
        
    except json.JSONDecodeError:
        return jsonify({
            "error": "Invalid JSON payload",
            "status": "rejected"
        }), 400
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        app.logger.error(f"Error ingesting metrics: {str(e)}\n{error_details}")
        return jsonify({
            "error": "Internal server error",
            "details": str(e),
            "status": "error"
        }), 500


@app.route('/api/v1/metrics/devices', methods=['GET'])
def list_devices():
    """List all devices currently sending data."""
    devices = []
    
    for device_id, records in metrics_store.items():
        if records:
            latest = records[-1]
            devices.append({
                "device_id": device_id,
                "device_type": latest.get("device_type", "unknown"),
                "record_count": len(records),
                "last_seen": latest.get("timestamp"),
                "latest_power_watts": latest.get("system_metrics", {}).get("total_power_watts")
            })
    
    return jsonify({
        "devices": devices,
        "total_devices": len(devices)
    }), 200


@app.route('/api/v1/metrics/device/<device_id>', methods=['GET'])
def get_device_metrics(device_id: str):
    """Get all metrics for a specific device."""
    if device_id not in metrics_store:
        return jsonify({
            "error": f"Device {device_id} not found",
            "status": "not_found"
        }), 404
    
    records = metrics_store[device_id]
    
    # Optional: limit results
    limit = request.args.get('limit', type=int, default=100)
    records = records[-limit:]
    
    return jsonify({
        "device_id": device_id,
        "record_count": len(records),
        "metrics": records
    }), 200


@app.route('/api/v1/stats', methods=['GET'])
def get_stats():
    """Get API statistics."""
    # Calculate aggregate statistics
    total_power = 0
    total_apps = 0
    record_count = 0
    
    for records in metrics_store.values():
        for record in records:
            total_power += record.get('system_metrics', {}).get('total_power_watts', 0)
            total_apps += len(record.get('applications', []))
            record_count += 1
    
    avg_power = total_power / record_count if record_count > 0 else 0
    
    return jsonify({
        "api_stats": stats,
        "aggregate_metrics": {
            "total_records": record_count,
            "unique_devices": len(metrics_store),
            "average_power_watts": round(avg_power, 2),
            "total_applications_tracked": total_apps
        }
    }), 200


@app.route('/api/v1/metrics/reset', methods=['POST'])
def reset_metrics():
    """Reset all stored metrics (for testing/debugging)."""
    global metrics_store, stats
    
    metrics_store.clear()
    stats["total_records"] = 0
    stats["unique_devices"] = 0
    
    return jsonify({
        "status": "reset_complete",
        "message": "All metrics cleared"
    }), 200


@app.route('/', methods=['GET'])
def index():
    """API information endpoint."""
    return jsonify({
        "service": "Carbon Profiling Ingestion API",
        "version": stats["api_version"],
        "endpoints": {
            "health": "/health",
            "ingest": "/api/v1/metrics/ingest [POST]",
            "devices": "/api/v1/metrics/devices [GET]",
            "device_metrics": "/api/v1/metrics/device/<device_id> [GET]",
            "stats": "/api/v1/stats [GET]",
            "reset": "/api/v1/metrics/reset [POST]"
        },
        "documentation": "https://github.com/yourproject/carbon-profiling"
    }), 200


if __name__ == '__main__':
    # Get port from environment variable (Kubernetes will set this)
    port = int(os.environ.get('PORT', 5000))
    
    # Run the Flask app
    app.run(
        host='0.0.0.0',  # Listen on all interfaces
        port=port,
        debug=os.environ.get('FLASK_ENV') == 'development'
    )
