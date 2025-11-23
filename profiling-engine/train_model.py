#!/usr/bin/env python3
"""
Train Prophet model for grid carbon intensity prediction
Place in profiling-engine/ directory and run once
"""

import pandas as pd
import os
os.environ["CMDSTAN"] = "/home/varunadhityagb/.cmdstan/cmdstan-2.37.0"

from prophet import Prophet
import pickle
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import glob

IST = ZoneInfo("Asia/Kolkata")

def load_multiple_csv_files(pattern: str = "IN-SO_*_hourly.csv"):
    """Load and combine multiple years of data."""
    print(f"\n📂 Looking for files matching: {pattern}")

    csv_files = sorted(glob.glob(pattern))

    if not csv_files:
        # Fallback to single file
        print("   No multi-year files found, trying IN_2024_hourly.csv")
        csv_files = ["IN_2024_hourly.csv"]

    print(f"   Found {len(csv_files)} file(s): {csv_files}")

    all_dfs = []

    for csv_path in csv_files:
        if not Path(csv_path).exists():
            print(f"   ⚠️  Skipping {csv_path} (not found)")
            continue

        print(f"   Loading {csv_path}...")
        df = pd.read_csv(csv_path)

        # Parse datetime
        df['ds'] = pd.to_datetime(df['Datetime (UTC)'])
        df['ds'] = df['ds'].dt.tz_localize('UTC').dt.tz_convert(IST).dt.tz_localize(None)
        df['y'] = df['Carbon intensity gCO₂eq/kWh (Life cycle)']
        df = df[['ds', 'y']].dropna()

        all_dfs.append(df)
        print(f"      {len(df)} records ({df['ds'].min().year} to {df['ds'].max().year})")

    if not all_dfs:
        raise FileNotFoundError("No valid CSV files found")

    # Combine all dataframes
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df = combined_df.sort_values('ds').drop_duplicates(subset=['ds'])

    return combined_df

def train_model(csv_pattern: str = "IN-SO_*_hourly.csv", output_path: str = "models/grid_prophet.pkl"):
    """Train Prophet model on India grid data."""

    print("=" * 60)
    print("🧠 Training Grid Carbon Intensity Model")
    print("=" * 60)

    # Load data (single or multiple files)
    df = load_multiple_csv_files(csv_pattern)

    print(f"\n✅ Combined dataset: {len(df)} records")
    print(f"   Date range: {df['ds'].min()} to {df['ds'].max()}")
    print(f"   Avg intensity: {df['y'].mean():.2f} gCO₂/kWh")
    print(f"   Min: {df['y'].min():.2f}, Max: {df['y'].max():.2f}")

    # Configure Prophet with cmdstanpy backend (more stable)
    print("\n⚙️  Configuring Prophet model...")


    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=True,
        seasonality_mode='multiplicative',
        changepoint_prior_scale=0.05,
        interval_width=0.95,
        stan_backend="CMDSTANPY"
    )

    model.add_seasonality(name='hourly', period=1, fourier_order=8)

    # Train
    print("🔄 Training model (this may take 1-2 minutes)...")
    model.fit(df)

    # Save model
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'wb') as f:
        pickle.dump(model, f)

    print(f"\n✅ Model trained and saved to {output_path}")
    print(f"   Training timestamp: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 60)

    return True

if __name__ == "__main__":
    import sys

    # Use pattern to load all years of data
    csv_pattern = sys.argv[1] if len(sys.argv) > 1 else "IN-SO_*_hourly.csv"
    success = train_model(csv_pattern)

    if success:
        print("\n✅ Next steps:")
        print("   1. Copy model to profiling-engine: cp models/grid_prophet.pkl profiling-engine/models/")
        print("   2. Rebuild ingestion-api container: ./rebuild-minikube-keep-db")
        print("   3. Model will be automatically loaded on startup")
        print("   4. Access predictions at /api/v1/ml/predict-24h")
    else:
        print("\n❌ Training failed")
        sys.exit(1)
