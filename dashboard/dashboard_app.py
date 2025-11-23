from flask import Flask, render_template, jsonify
import requests
import os
from datetime import datetime
from zoneinfo import ZoneInfo

app = Flask(__name__)

# Indian Standard Time
IST = ZoneInfo("Asia/Kolkata")

API_BASE_URL = os.environ.get('API_URL', 'http://ingestion-api-service:5000')
# API_BASE_URL = os.environ.get('API_URL', 'http://localhost:5000')

@app.route('/')
def index():
    """Serve the carbon-aware scheduling dashboard."""
    return render_template('index.html')

@app.route('/api/dashboard/summary')
def dashboard_summary():
    """Aggregate data for carbon-aware dashboard with IST timezone."""
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
            'system_stats': stats,
            'timezone': 'Asia/Kolkata (IST)',
            'current_time': datetime.now(IST).strftime('%H:%M:%S IST')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/carbon-aware/recommendations')
def carbon_recommendations():
    """Get carbon-aware scheduling recommendations (IST timezone)."""
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

        # Current hour analysis (IST)
        current_hour = datetime.now(IST).hour
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
            'current_time_ist': datetime.now(IST).strftime('%H:%M:%S IST'),
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
            'recommendation': get_recommendation(percent_diff, greenest_hours[0]['hour'], current_hour),
            'timezone': 'Asia/Kolkata (IST)'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/dashboard/ml-insights')
def ml_insights():
    """Get ML-powered insights for dashboard."""
    try:
        # Fetch ML predictions
        ml_available = True
        ml_error = None

        try:
            recommendation = requests.get(f'{API_BASE_URL}/api/v1/ml/recommendation', timeout=5).json()
            predictions = requests.get(f'{API_BASE_URL}/api/v1/ml/predict-24h', timeout=5).json()
            greenest = requests.get(f'{API_BASE_URL}/api/v1/ml/greenest-hours', timeout=5).json()
            missed_opps = requests.get(f'{API_BASE_URL}/api/v1/insights/missed-opportunities', timeout=5).json()
        except Exception as e:
            ml_available = False
            ml_error = str(e)
            recommendation = {'error': 'ML model not available'}
            predictions = {'predictions': []}
            greenest = {'greenest_hours': []}
            missed_opps = {'opportunities': [], 'total_missed_savings_gco2': 0}

        # Get historical carbon data
        carbon_summary = requests.get(f'{API_BASE_URL}/api/v1/carbon/summary', timeout=5).json()
        by_hour = requests.get(f'{API_BASE_URL}/api/v1/carbon/by-hour', timeout=5).json()

        # Calculate impact metrics
        total_carbon_kg = carbon_summary.get('total_carbon_kg', 0)
        missed_savings_kg = missed_opps.get('total_missed_savings_kg', 0)

        # Calculate what you COULD have saved
        potential_savings_percent = 0
        if total_carbon_kg > 0 and missed_savings_kg > 0:
            potential_savings_percent = (missed_savings_kg / (total_carbon_kg + missed_savings_kg)) * 100

        # Calculate trees equivalent (1 tree absorbs ~21.77 kg CO2/year)
        trees_equivalent = missed_savings_kg * 365 / 21.77 if missed_savings_kg > 0 else 0

        # Calculate km driven equivalent (1 km = ~0.12 kg CO2 for avg car)
        km_equivalent = missed_savings_kg / 0.12 if missed_savings_kg > 0 else 0

        return jsonify({
            'ml_available': ml_available,
            'ml_error': ml_error,
            'recommendation': recommendation,
            'predictions': predictions.get('predictions', []),
            'greenest_hours': greenest.get('greenest_hours', []),
            'missed_opportunities': {
                'count': len(missed_opps.get('opportunities', [])),
                'opportunities': missed_opps.get('opportunities', [])[:10],  # Top 10
                'total_missed_kg': missed_savings_kg,
                'potential_savings_percent': round(potential_savings_percent, 1),
                'trees_equivalent': round(trees_equivalent, 2),
                'km_equivalent': round(km_equivalent, 1)
            },
            'carbon_stats': {
                'total_collected_kg': total_carbon_kg,
                'operational_kg': carbon_summary.get('operational_carbon_grams', 0) / 1000,
                'embodied_kg': carbon_summary.get('embodied_carbon_grams', 0) / 1000,
                'measurements': carbon_summary.get('total_measurements', 0)
            },
            'hourly_pattern': by_hour.get('hourly_breakdown', []),
            'timezone': 'Asia/Kolkata (IST)',
            'timestamp': datetime.now(IST).strftime('%H:%M:%S IST')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/impact-score')
def impact_score():
    """Calculate user's green computing impact score (0-100)."""
    try:
        missed_opps = requests.get(f'{API_BASE_URL}/api/v1/insights/missed-opportunities', timeout=5).json()
        carbon_summary = requests.get(f'{API_BASE_URL}/api/v1/carbon/summary', timeout=5).json()
        by_hour = requests.get(f'{API_BASE_URL}/api/v1/carbon/by-hour', timeout=5).json()

        # Calculate score based on multiple factors
        score = 50  # Base score

        # Factor 1: Missed opportunities (lower is better)
        missed_kg = missed_opps.get('total_missed_savings_kg', 0)
        total_kg = carbon_summary.get('total_carbon_kg', 0)
        if total_kg > 0:
            miss_rate = missed_kg / (total_kg + missed_kg)
            score += (1 - miss_rate) * 30  # Up to 30 points

        # Factor 2: Time of day patterns (using greenest hours)
        # This requires more complex logic - simplified here
        score += 10  # Placeholder

        # Factor 3: Device efficiency
        measurements = carbon_summary.get('total_measurements', 1)
        avg_carbon_per_measurement = total_kg * 1000 / measurements if measurements > 0 else 0
        if avg_carbon_per_measurement < 0.5:  # Good efficiency
            score += 10

        score = min(100, max(0, score))  # Clamp 0-100

        # Determine grade
        if score >= 90:
            grade = 'A'
            message = 'Excellent! You\'re a green computing champion!'
        elif score >= 80:
            grade = 'B'
            message = 'Great job! Minor improvements possible.'
        elif score >= 70:
            grade = 'C'
            message = 'Good effort. Room for optimization.'
        elif score >= 60:
            grade = 'D'
            message = 'Need improvement. Follow more recommendations.'
        else:
            grade = 'F'
            message = 'Significant changes needed. Start with green hours.'

        return jsonify({
            'score': round(score, 1),
            'grade': grade,
            'message': message,
            'breakdown': {
                'optimization': round((1 - (missed_kg / (total_kg + missed_kg))) * 100 if total_kg > 0 else 50, 1),
                'efficiency': round(50 if avg_carbon_per_measurement < 0.5 else 30, 1),
                'consistency': 50  # Placeholder
            }
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
