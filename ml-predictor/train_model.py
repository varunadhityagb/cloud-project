#!/usr/bin/env python3
"""
Train the Prophet model on historical grid data
Run this once before deploying the ML service
"""

import sys
from pathlib import Path
from grid_predictor import GridCarbonPredictor

def main():
    csv_path = "IN_2024_hourly.csv"
    
    # Check if CSV exists
    if not Path(csv_path).exists():
        print(f"❌ Error: {csv_path} not found")
        print("   Please place the CSV file in the ml-predictor directory")
        sys.exit(1)
    
    print("="*60)
    print("🚀 Training Carbon Intensity Prediction Model")
    print("="*60)
    print(f"📁 Using data from: {csv_path}")
    print()
    
    # Initialize predictor
    predictor = GridCarbonPredictor(model_path="models/grid_prophet.pkl")
    
    # Train model
    predictor.train(csv_path)
    
    print("\n" + "="*60)
    print("✅ Training Complete!")
    print("="*60)
    print()
    print("Next steps:")
    print("  1. Test predictions: python grid_predictor.py")
    print("  2. Start API server: python predictor_api.py")
    print("  3. Deploy to Kubernetes with updated manifests")
    print()
    print("="*60)

if __name__ == "__main__":
    main()

