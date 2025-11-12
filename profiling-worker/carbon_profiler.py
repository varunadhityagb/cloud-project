#!/usr/bin/env python3
"""
Carbon Profiling Worker - Research-Backed Version
Processes device metrics and calculates carbon footprint including embodied emissions.

References:
- Embodied carbon values: Environmental Science & Technology (2013) 
  "Comparing Embodied GHG Emissions of Modern Computing Products"
- Updated values: Renewable and Sustainable Energy Reviews (2023)
  "Assessing embodied carbon emissions of communication user devices"
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

# Source: Malmodin & Lundén (2023) - Renewable & Sustainable Energy Reviews
EMBODIED_CARBON_KG = {
    "smartphone": 50,      # kg CO2e
    "tablet": 100,         # kg CO2e
    "laptop": 200,         # kg CO2e
    "desktop": 300,        # kg CO2e (conservative estimate)
    "workstation": 400,    # kg CO2e (conservative estimate)
    "server": 1500         # kg CO2e (data center literature)
}

# Expected device lifetimes (years) - industry standard values
EXPECTED_LIFETIME_YEARS = {
    "smartphone": 2.5,
    "tablet": 3.0,
    "laptop": 4.0,
    "desktop": 5.0,
    "workstation": 5.0,
    "server": 5.0
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def init_carbon_table():
    """Create table for carbon footprint results with embodied carbon tracking."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS carbon_footprints (
            id SERIAL PRIMARY KEY,
            device_id VARCHAR(100) NOT NULL,
            device_type VARCHAR(50),
            metric_id INTEGER REFERENCES device_metrics(id),
            timestamp TIMESTAMP NOT NULL,
            
            -- Operational emissions
            power_kwh FLOAT NOT NULL,
            grid_intensity_gco2_per_kwh FLOAT NOT NULL,
            operational_carbon_gco2 FLOAT NOT NULL,
            
            -- Embodied emissions (amortized)
            embodied_carbon_gco2 FLOAT NOT NULL,
            embodied_total_kgco2e FLOAT,
            device_lifetime_years FLOAT,
            
            -- Total emissions
            total_carbon_gco2 FLOAT NOT NULL,
            
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
    print("Carbon footprint table initialized (with embodied carbon support)")

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

def calculate_embodied_carbon_per_measurement(device_type: str, measurement_interval_seconds: int = 5) -> float:
    """
    Calculate amortized embodied carbon per measurement period.
    
    Formula: E_embodied_amortized = E_total / (lifetime_years × 365 × 24 × 3600 / interval_seconds)
    
    Args:
        device_type: Type of device (laptop, desktop, etc.)
        measurement_interval_seconds: Time between measurements (default: 5 seconds)
    
    Returns:
        Embodied carbon in grams CO2e for this measurement period
    """
    embodied_total_kg = EMBODIED_CARBON_KG.get(device_type, EMBODIED_CARBON_KG["laptop"])
    lifetime_years = EXPECTED_LIFETIME_YEARS.get(device_type, EXPECTED_LIFETIME_YEARS["laptop"])
    
    # Total seconds in device lifetime
    total_lifetime_seconds = lifetime_years * 365 * 24 * 3600
    
    # Number of measurement periods in lifetime
    total_measurements = total_lifetime_seconds / measurement_interval_seconds
    
    # Embodied carbon per measurement (in grams)
    embodied_per_measurement_g = (embodied_total_kg * 1000) / total_measurements
    
    return embodied_per_measurement_g

def process_unprocessed_metrics():
    """Find and process metrics that haven't been profiled yet."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Find metrics not yet processed
    cur.execute("""
        SELECT m.id, m.device_id, m.device_type, m.timestamp, m.total_power_watts
        FROM device_metrics m
        LEFT JOIN carbon_footprints c ON m.id = c.metric_id
        WHERE c.id IS NULL
        ORDER BY m.timestamp
        LIMIT 100
    """)
    
    unprocessed = cur.fetchall()
    
    if not unprocessed:
        print("No new metrics to process")
        cur.close()
        conn.close()
        return 0
    
    processed_count = 0
    
    for metric in unprocessed:
        # Extract hour from timestamp
        hour = metric['timestamp'].hour
        device_type = metric['device_type'] or 'laptop'
        
        # Get grid intensity for that hour
        grid_intensity = get_grid_carbon_intensity(hour)
        
        # === OPERATIONAL CARBON ===
        # Convert power (Watts) to energy (kWh)
        # Assuming 5-second measurement interval
        power_kwh = (metric['total_power_watts'] * 5) / (1000 * 3600)
        
        # Calculate operational carbon: Energy (kWh) × Grid Intensity (gCO2/kWh)
        operational_carbon_gco2 = power_kwh * grid_intensity
        
        # === EMBODIED CARBON (Amortized) ===
        embodied_carbon_gco2 = calculate_embodied_carbon_per_measurement(
            device_type=device_type,
            measurement_interval_seconds=5
        )
        
        # Get total embodied carbon for reference
        embodied_total_kg = EMBODIED_CARBON_KG.get(device_type, EMBODIED_CARBON_KG["laptop"])
        device_lifetime = EXPECTED_LIFETIME_YEARS.get(device_type, EXPECTED_LIFETIME_YEARS["laptop"])
        
        # === TOTAL CARBON ===
        total_carbon_gco2 = operational_carbon_gco2 + embodied_carbon_gco2
        
        # Store the result
        cur.execute("""
            INSERT INTO carbon_footprints 
            (device_id, device_type, metric_id, timestamp, 
             power_kwh, grid_intensity_gco2_per_kwh, 
             operational_carbon_gco2, embodied_carbon_gco2,
             embodied_total_kgco2e, device_lifetime_years,
             total_carbon_gco2)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            metric['device_id'],
            device_type,
            metric['id'],
            metric['timestamp'],
            power_kwh,
            grid_intensity,
            operational_carbon_gco2,
            embodied_carbon_gco2,
            embodied_total_kg,
            device_lifetime,
            total_carbon_gco2
        ))
        
        processed_count += 1
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Processed {processed_count} metrics (with embodied carbon)")
    return processed_count

def main():
    """Main worker loop."""
    print("Carbon Profiling Worker Starting")
    
    # Wait for database
    time.sleep(5)
    
    # Initialize carbon table
    try:
        init_carbon_table()
    except Exception as e:
        print(f"Error initializing: {e}")
        time.sleep(10)
        return
    
    print("Starting processing loop...")
    print(f"Embodied carbon values (kg CO2e): {EMBODIED_CARBON_KG}")
    print(f"Expected lifetimes (years): {EXPECTED_LIFETIME_YEARS}")
    
    # Process loop
    while True:
        try:
            process_unprocessed_metrics()
            time.sleep(10)  # Check every 10 seconds
        except KeyboardInterrupt:
            print("\n Worker stopped")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
