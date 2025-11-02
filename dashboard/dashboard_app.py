from flask import Flask, render_template, jsonify
import requests
import os

app = Flask(__name__)

API_BASE_URL = os.environ.get('API_URL', 'http://ingestion-api-service:5000')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/dashboard/summary')
def dashboard_summary():
    """Aggregate data for dashboard."""
    try:
        # Fetch data from ingestion API
        carbon_summary = requests.get(f'{API_BASE_URL}/api/v1/carbon/summary').json()
        by_device = requests.get(f'{API_BASE_URL}/api/v1/carbon/by-device').json()
        by_hour = requests.get(f'{API_BASE_URL}/api/v1/carbon/by-hour').json()
        stats = requests.get(f'{API_BASE_URL}/api/v1/stats').json()
        
        return jsonify({
            'carbon_summary': carbon_summary,
            'by_device': by_device,
            'by_hour': by_hour,
            'system_stats': stats
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
