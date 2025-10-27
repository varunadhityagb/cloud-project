#!/usr/bin/env python3
"""
Carbon Profiling Worker
Processes device metrics and calculates carbon footprint.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import time
import os
from datetime import datetime, timezone

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'postgres-service'),
    'port': os.environ.get('DB_PORT', '5432'),
    'database': os.environ.get('DB_NAME', 'carbon_metrics'),
    'user': os.environ.get('DB_USER', 'carbon_user'),
    'password': os.environ.get('DB_PASSWORD', 'carbon_pass_123')
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def init_carbon_table():
    """Create table for carbon footprint results."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS carbon_footprints (
            id SERIAL PRIMARY KEY,
            device_id VARCHAR(100) NOT NULL,
            metric_id INTEGER REFERENCES device_metrics(id),
            timestamp TIMESTAMP NOT NULL,
            power_kwh FLOAT NOT NULL,
            grid_intensity_gco2_per_kwh FLOAT NOT NULL,
            carbon_gco2 FLOAT NOT NULL,
            calculated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_carbon_device 
        ON carbon_footprints(device_id)
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_carbon_timestamp 
        ON carbon_footprints(timestamp)
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Carbon footprint table initialized")

def get_grid_carbon_intensity(hour: int) -> float:
    """
    Simulate grid carbon intensity based on time of day.
    In production, this would call a real API like electricityMap or WattTime.
    
    Returns: gCO2eq/kWh
    """
    # Simulated values: Higher during peak hours, lower at night
    intensity_map = {
        0: 150, 1: 140, 2: 130, 3: 120, 4: 120, 5: 130,
        6: 200, 7: 250, 8: 280, 9: 300, 10: 290, 11: 280,
        12: 270, 13: 260, 14: 270, 15: 280, 16: 300, 17: 320,
        18: 340, 19: 350, 20: 330, 21: 300, 22: 250, 23: 180
    }
    return intensity_map.get(hour, 250)

def process_unprocessed_metrics():
    """Find and process metrics that haven't been profiled yet."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Find metrics not yet processed
    cur.execute("""
        SELECT m.id, m.device_id, m.timestamp, m.total_power_watts
        FROM device_metrics m
        LEFT JOIN carbon_footprints c ON m.id = c.metric_id
        WHERE c.id IS NULL
        ORDER BY m.timestamp
        LIMIT 100
    """)
    
    unprocessed = cur.fetchall()
    
    if not unprocessed:
        print("📊 No new metrics to process")
        cur.close()
        conn.close()
        return 0
    
    processed_count = 0
    
    for metric in unprocessed:
        # Extract hour from timestamp
        hour = metric['timestamp'].hour
        
        # Get grid intensity for that hour
        grid_intensity = get_grid_carbon_intensity(hour)
        
        # Convert power (Watts) to energy (kWh)
        # Assuming 5-second measurement interval
        power_kwh = (metric['total_power_watts'] * 5) / (1000 * 3600)
        
        # Calculate carbon footprint: Energy (kWh) × Grid Intensity (gCO2/kWh)
        carbon_gco2 = power_kwh * grid_intensity
        
        # Store the result
        cur.execute("""
            INSERT INTO carbon_footprints 
            (device_id, metric_id, timestamp, power_kwh, 
             grid_intensity_gco2_per_kwh, carbon_gco2)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            metric['device_id'],
            metric['id'],
            metric['timestamp'],
            power_kwh,
            grid_intensity,
            carbon_gco2
        ))
        
        processed_count += 1
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"✅ Processed {processed_count} metrics")
    return processed_count

def main():
    """Main worker loop."""
    print("🔬 Carbon Profiling Worker Starting...")
    
    # Wait for database
    time.sleep(5)
    
    # Initialize carbon table
    try:
        init_carbon_table()
    except Exception as e:
        print(f"❌ Error initializing: {e}")
        time.sleep(10)
        return
    
    print("🔄 Starting processing loop...")
    
    # Process loop
    while True:
        try:
            process_unprocessed_metrics()
            time.sleep(10)  # Check every 10 seconds
        except KeyboardInterrupt:
            print("\n⚠️  Worker stopped")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
