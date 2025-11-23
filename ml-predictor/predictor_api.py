"""
ML Prediction API - REST endpoints for carbon intensity predictions
Integrates with existing profiling engine
"""

from flask import Flask, jsonify, request
from grid_predictor import GridCarbonPredictor
from datetime import datetime
from zoneinfo import ZoneInfo
import os

app = Flask(__name__)
IST = ZoneInfo("Asia/Kolkata")

# Initialize predictor (load model on startup)
predictor = GridCarbonPredictor(
    model_path=os.environ.get('MODEL_PATH', 'models/grid_prophet.pkl')
)

try:
    predictor.load_model()
    print("✅ ML model loaded successfully")
except FileNotFoundError:
    print("⚠️  No trained model found. Please train first using train_model.py")

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    model_status = "ready" if predictor.model else "not_trained"
    
    return jsonify({
        'status': 'healthy',
        'model_status': model_status,
        'last_trained': predictor.last_trained.isoformat() if predictor.last_trained else None,
        'timestamp': datetime.now(IST).isoformat()
    })

@app.route('/api/v1/predict/next-24h', methods=['GET'])
def predict_next_24h():
    """Get predictions for next 24 hours."""
    if not predictor.model:
        return jsonify({'error': 'Model not trained'}), 503
    
    try:
        predictions = predictor.predict_next_24h()
        
        result = []
        for _, row in predictions.iterrows():
            result.append({
                'timestamp': row['timestamp'].isoformat(),
                'hour': int(row['hour']),
                'predicted_intensity': round(float(row['predicted_intensity']), 2),
                'confidence_range': [
                    round(float(row['lower_bound']), 2),
                    round(float(row['upper_bound']), 2)
                ]
            })
        
        return jsonify({
            'predictions': result,
            'timezone': 'Asia/Kolkata (IST)',
            'generated_at': datetime.now(IST).isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/predict/greenest-hours', methods=['GET'])
def greenest_hours():
    """Get top N greenest hours in next 24h."""
    if not predictor.model:
        return jsonify({'error': 'Model not trained'}), 503
    
    try:
        top_n = request.args.get('top', type=int, default=5)
        greenest = predictor.get_greenest_hours(top_n)
        
        return jsonify({
            'greenest_hours': greenest,
            'count': len(greenest),
            'timezone': 'Asia/Kolkata (IST)',
            'generated_at': datetime.now(IST).isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/predict/recommendation', methods=['GET'])
def recommendation():
    """Get scheduling recommendation for current time."""
    if not predictor.model:
        return jsonify({'error': 'Model not trained'}), 503
    
    try:
        rec = predictor.get_recommendation()
        rec['timezone'] = 'Asia/Kolkata (IST)'
        rec['generated_at'] = datetime.now(IST).isoformat()
        
        return jsonify(rec)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/v1/train', methods=['POST'])
def train_model():
    """Trigger model retraining (admin endpoint)."""
    try:
        csv_path = request.json.get('csv_path', 'IN_2024_hourly.csv')
        
        if not os.path.exists(csv_path):
            return jsonify({'error': f'CSV file not found: {csv_path}'}), 400
        
        # Train in background (for production, use Celery/background job)
        predictor.train(csv_path)
        
        return jsonify({
            'status': 'success',
            'message': 'Model trained successfully',
            'trained_at': predictor.last_trained.isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(host='0.0.0.0', port=port)

