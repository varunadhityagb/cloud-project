"""
Grid Carbon Intensity Predictor using Prophet
Predicts next 24 hours of carbon intensity for carbon-aware scheduling
"""

import pandas as pd
from prophet import Prophet
import pickle
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import numpy as np

IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")

class GridCarbonPredictor:
    """Predict grid carbon intensity for next 24 hours."""
    
    def __init__(self, model_path: str = "models/grid_prophet.pkl"):
        self.model_path = Path(model_path)
        self.model = None
        self.last_trained = None
        
    def load_historical_data(self, csv_path: str) -> pd.DataFrame:
        """Load and prepare historical CSV data."""
        print(f"📂 Loading data from {csv_path}")
        
        # Read CSV
        df = pd.read_csv(csv_path)
        
        # Parse datetime (UTC) and convert to IST, then REMOVE timezone for Prophet
        df['ds'] = pd.to_datetime(df['Datetime (UTC)'])
        df['ds'] = df['ds'].dt.tz_localize('UTC').dt.tz_convert(IST).dt.tz_localize(None)
        
        # Use lifecycle carbon intensity as target
        df['y'] = df['Carbon intensity gCO₂eq/kWh (Life cycle)']
        
        # Keep only required columns
        df = df[['ds', 'y']].dropna()
        
        print(f"✅ Loaded {len(df)} records")
        print(f"   Date range: {df['ds'].min()} to {df['ds'].max()}")
        print(f"   Avg intensity: {df['y'].mean():.2f} gCO₂/kWh")
        
        return df
    
    def train(self, csv_path: str):
        """Train Prophet model on historical data."""
        print("\n" + "="*60)
        print("🧠 Training Carbon Intensity Prediction Model")
        print("="*60)
        
        # Load data
        df = self.load_historical_data(csv_path)
        
        # Configure Prophet
        print("\n⚙️  Configuring Prophet model...")
        self.model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=True,
            seasonality_mode='multiplicative',
            changepoint_prior_scale=0.05,
            interval_width=0.95
        )
        
        # Add custom seasonalities
        self.model.add_seasonality(
            name='hourly',
            period=1,
            fourier_order=8
        )
        
        # Train
        print("🔄 Training model (this may take a minute)...")
        self.model.fit(df)
        
        # Save model
        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.model_path, 'wb') as f:
            pickle.dump(self.model, f)
        
        self.last_trained = datetime.now(IST)
        
        print(f"✅ Model trained and saved to {self.model_path}")
        print(f"   Training timestamp: {self.last_trained.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print("="*60)
    
    def load_model(self):
        """Load trained model from disk."""
        if not self.model_path.exists():
            raise FileNotFoundError(
                f"Model not found at {self.model_path}. "
                "Please train the model first using train() method."
            )
        
        with open(self.model_path, 'rb') as f:
            self.model = pickle.load(f)
        
        print(f"✅ Model loaded from {self.model_path}")
    
    def predict_next_24h(self) -> pd.DataFrame:
        """Predict carbon intensity for next 24 hours."""
        if self.model is None:
            self.load_model()
        
        # Create future dataframe (next 24 hours) - NO timezone for Prophet
        now = datetime.now(IST).replace(tzinfo=None)
        future_dates = pd.date_range(
            start=now,
            end=now + timedelta(hours=24),
            freq='H'
        )
        
        future = pd.DataFrame({'ds': future_dates})
        
        # Predict
        forecast = self.model.predict(future)
        
        # Extract predictions and add timezone back for output
        result = pd.DataFrame({
            'timestamp': pd.to_datetime(forecast['ds']).dt.tz_localize(IST),
            'predicted_intensity': forecast['yhat'],
            'lower_bound': forecast['yhat_lower'],
            'upper_bound': forecast['yhat_upper']
        })
        
        # Add hour for easier filtering
        result['hour'] = result['timestamp'].dt.hour
        
        return result
    
    def get_greenest_hours(self, top_n: int = 5) -> list:
        """Get the greenest hours in next 24h."""
        predictions = self.predict_next_24h()
        
        # Sort by predicted intensity (lowest first)
        greenest = predictions.nsmallest(top_n, 'predicted_intensity')
        
        results = []
        for _, row in greenest.iterrows():
            results.append({
                'timestamp': row['timestamp'].isoformat(),
                'hour': int(row['hour']),
                'predicted_intensity': round(float(row['predicted_intensity']), 2),
                'confidence_range': [
                    round(float(row['lower_bound']), 2),
                    round(float(row['upper_bound']), 2)
                ]
            })
        
        return results
    
    def get_recommendation(self) -> dict:
        """Get scheduling recommendation for current time."""
        predictions = self.predict_next_24h()
        
        current_hour = datetime.now(IST).hour
        current_pred = predictions[predictions['hour'] == current_hour].iloc[0]
        
        avg_intensity = predictions['predicted_intensity'].mean()
        min_intensity = predictions['predicted_intensity'].min()
        
        current_intensity = float(current_pred['predicted_intensity'])
        percent_vs_avg = ((current_intensity - avg_intensity) / avg_intensity) * 100
        percent_vs_best = ((current_intensity - min_intensity) / min_intensity) * 100
        
        # Find greenest hour
        greenest = predictions.loc[predictions['predicted_intensity'].idxmin()]
        greenest_hour = int(greenest['hour'])
        hours_until_greenest = (greenest_hour - current_hour) % 24
        
        # Determine recommendation
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
        
        return {
            'status': status,
            'current_hour': current_hour,
            'current_intensity': round(current_intensity, 2),
            'average_intensity': round(avg_intensity, 2),
            'best_intensity': round(min_intensity, 2),
            'percent_vs_average': round(percent_vs_avg, 2),
            'percent_vs_best': round(percent_vs_best, 2),
            'message': message,
            'action': action,
            'greenest_hour': greenest_hour,
            'hours_until_greenest': hours_until_greenest
        }


if __name__ == "__main__":
    # Example usage
    predictor = GridCarbonPredictor()
    
    # Train model (run this once)
    predictor.train("IN_2024_hourly.csv")
    
    # Get predictions
    print("\n" + "="*60)
    print("🔮 Predictions for Next 24 Hours")
    print("="*60)
    
    predictions = predictor.predict_next_24h()
    print(predictions[['timestamp', 'hour', 'predicted_intensity']].head(10))
    
    # Get greenest hours
    print("\n🌱 Top 5 Greenest Hours:")
    greenest = predictor.get_greenest_hours(5)
    for i, hour in enumerate(greenest, 1):
        print(f"  {i}. Hour {hour['hour']:02d}:00 - {hour['predicted_intensity']} gCO₂/kWh")
    
    # Get recommendation
    print("\n💡 Current Recommendation:")
    rec = predictor.get_recommendation()
    print(f"  Status: {rec['status'].upper()}")
    print(f"  Current: {rec['current_intensity']} gCO₂/kWh")
    print(f"  Message: {rec['message']}")
    print(f"  Action: {rec['action']}")
