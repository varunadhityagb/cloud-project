from flask import Flask, render_template, jsonify
import requests
import os

app = Flask(__name__)

API_BASE_URL = os.environ.get('API_URL', 'http://ingestion-api-service:5000')

@app.route('/')
def index():
    """Serve the carbon-aware scheduling dashboard."""
    return render_template('index.html')

@app.route('/api/dashboard/summary')
def dashboard_summary():
    """Aggregate data for carbon-aware dashboard."""
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

@app.route('/api/carbon-aware/recommendations')
def carbon_recommendations():
    """Get carbon-aware scheduling recommendations."""
    try:
        by_hour = requests.get(f'{API_BASE_URL}/api/v1/carbon/by-hour').json()

        if 'hourly_breakdown' not in by_hour:
            return jsonify({'error': 'No hourly data available'}), 404

        hourly_data = by_hour['hourly_breakdown']

        # Calculate statistics
        intensities = [h['avg_grid_intensity_gco2_kwh'] for h in hourly_data]
        avg_intensity = sum(intensities) / len(intensities)
        min_intensity = min(intensities)
        max_intensity = max(intensities)

        # Find best and worst hours
        sorted_hours = sorted(hourly_data, key=lambda x: x['avg_grid_intensity_gco2_kwh'])
        greenest_hours = sorted_hours[:3]
        dirtiest_hours = sorted_hours[-3:]

        # Current hour analysis
        from datetime import datetime
        current_hour = datetime.now().hour
        current_hour_data = next((h for h in hourly_data if h['hour'] == current_hour), None)

        if current_hour_data:
            current_intensity = current_hour_data['avg_grid_intensity_gco2_kwh']
            percent_diff = ((avg_intensity - current_intensity) / avg_intensity) * 100
        else:
            current_intensity = avg_intensity
            percent_diff = 0

        # Calculate potential savings
        avg_carbon = sum(h['total_carbon_grams'] for h in hourly_data) / len(hourly_data)
        green_avg_carbon = sum(h['total_carbon_grams'] for h in greenest_hours) / len(greenest_hours)
        potential_savings = ((avg_carbon - green_avg_carbon) / avg_carbon) * 100

        return jsonify({
            'current_hour': current_hour,
            'current_intensity': current_intensity,
            'average_intensity': avg_intensity,
            'min_intensity': min_intensity,
            'max_intensity': max_intensity,
            'percent_difference': percent_diff,
            'greenest_hours': [
                {
                    'hour': h['hour'],
                    'intensity': h['avg_grid_intensity_gco2_kwh'],
                    'carbon': h['total_carbon_grams']
                } for h in greenest_hours
            ],
            'dirtiest_hours': [
                {
                    'hour': h['hour'],
                    'intensity': h['avg_grid_intensity_gco2_kwh'],
                    'carbon': h['total_carbon_grams']
                } for h in dirtiest_hours
            ],
            'potential_savings_percent': potential_savings,
            'recommendation': get_recommendation(percent_diff, greenest_hours[0]['hour'], current_hour)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_recommendation(percent_diff, greenest_hour, current_hour):
    """Generate recommendation based on current grid status."""
    if percent_diff > 15:
        return {
            'status': 'excellent',
            'title': 'Excellent Time to Compute',
            'message': 'Grid is significantly cleaner than average. Perfect for intensive workloads.',
            'action': 'Run batch jobs, updates, or data processing now'
        }
    elif percent_diff > 0:
        return {
            'status': 'good',
            'title': 'Good Time to Compute',
            'message': 'Grid is cleaner than average. Suitable for most workloads.',
            'action': 'Normal operations are fine'
        }
    elif percent_diff > -15:
        return {
            'status': 'moderate',
            'title': 'Moderate Carbon Intensity',
            'message': 'Grid is slightly dirtier than average. Consider deferring non-urgent tasks.',
            'action': 'Delay intensive workloads if possible'
        }
    else:
        wait_hours = (greenest_hour - current_hour) % 24
        return {
            'status': 'poor',
            'title': 'High Carbon Intensity',
            'message': 'Grid is significantly dirtier than average. Avoid intensive workloads.',
            'action': f'Wait {wait_hours}h for cleaner grid'
        }

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
