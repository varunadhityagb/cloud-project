import psycopg2
from psycopg2.extras import RealDictCursor
import time
import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

# Indian Standard Time
IST = ZoneInfo("Asia/Kolkata")

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'postgres-service'),
    'port': os.environ.get('DB_PORT', '5432'),
    'database': os.environ.get('DB_NAME', 'carbon_metrics'),
    'user': os.environ.get('DB_USER', 'carbon_user'),
    'password': os.environ.get('DB_PASSWORD', 'carbon_pass_123')
}

# Electricity Maps Configuration
ELECTRICITY_MAPS_TOKEN = os.environ.get('ELECTRICITY_MAPS_TOKEN', '')
ELECTRICITY_MAPS_API = "https://api.electricitymaps.com/v3/carbon-intensity/latest"

# Fallback values by region (gCO2eq/kWh)
FALLBACK_GRID_INTENSITY = {
    'IN': 632,    # India average
    'US': 417,    # USA average
    'EU': 295,    # EU average
    'CN': 555,    # China average
    'default': 475  # Global average
}

# Grid intensity cache (location-based)
grid_intensity_cache = {}
CACHE_TTL_SECONDS = 300  # 5 minutes

# Embodied carbon values
EMBODIED_CARBON_KG = {
    "smartphone": 50,
    "tablet": 100,
    "laptop": 200,
    "desktop": 300,
    "workstation": 400,
    "server": 1500
}

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
    """Create table for carbon footprint results with IST timezone."""
    conn = get_db_connection()
    cur = conn.cursor()

    # Set timezone to IST
    cur.execute("SET timezone = 'Asia/Kolkata'")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS carbon_footprints (
            id SERIAL PRIMARY KEY,
            device_id VARCHAR(100) NOT NULL,
            device_type VARCHAR(50),
            metric_id INTEGER REFERENCES device_metrics(id),
            timestamp TIMESTAMPTZ NOT NULL,

            -- Location (for reference)
            latitude FLOAT,
            longitude FLOAT,

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

            calculated_at TIMESTAMPTZ DEFAULT NOW()
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
    print("Carbon footprint table initialized (Timezone: Asia/Kolkata)")

def fetch_grid_intensity_by_location(lat: float, lon: float) -> Optional[float]:
    """
    Fetch real-time grid carbon intensity using lat/lon.

    Args:
        lat: Latitude
        lon: Longitude

    Returns:
        Carbon intensity in gCO2eq/kWh, or None if request fails
    """
    if not ELECTRICITY_MAPS_TOKEN:
        return None

    try:
        headers = {'auth-token': ELECTRICITY_MAPS_TOKEN}
        params = {'lat': lat, 'lon': lon, 'temporalGranularity':'5_minutes'}

        response = requests.get(
            ELECTRICITY_MAPS_API,
            headers=headers,
            params=params,
            timeout=5
        )

        if response.status_code == 200:
            data = response.json()
            intensity = data.get('carbonIntensity')

            if intensity is not None:
                print(f"Grid intensity: {intensity} gCO2/kWh (lat={lat}, lon={lon})")
                return float(intensity)
            else:
                print(f"No carbonIntensity in response")
                return None
        else:
            print(f"Electricity Maps API error: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch grid intensity: {e}")
        return None
    except (KeyError, ValueError) as e:
        print(f"Error parsing API response: {e}")
        return None

def get_grid_intensity_with_cache(lat: float, lon: float, country_code: str = None) -> float:
    """
    Get grid intensity with location-based caching.

    Args:
        lat: Latitude
        lon: Longitude
        country_code: ISO country code for fallback

    Returns:
        Carbon intensity in gCO2eq/kWh
    """
    # Create cache key (rounded to 1 decimal for nearby locations)
    cache_key = f"{round(lat, 1)}_{round(lon, 1)}"
    now = time.time()

    # Check cache
    if cache_key in grid_intensity_cache:
        cached_data = grid_intensity_cache[cache_key]
        if now - cached_data['timestamp'] < CACHE_TTL_SECONDS:
            return cached_data['value']

    # Try to fetch fresh data
    intensity = fetch_grid_intensity_by_location(lat, lon)

    if intensity is not None:
        # Update cache
        grid_intensity_cache[cache_key] = {
            'value': intensity,
            'timestamp': now
        }
        return intensity
    else:
        # Use regional fallback
        fallback = FALLBACK_GRID_INTENSITY.get(
            country_code,
            FALLBACK_GRID_INTENSITY['default']
        )
        print(f"Using fallback intensity: {fallback} gCO2/kWh ({country_code or 'default'})")
        return fallback

def calculate_embodied_carbon_per_measurement(device_type: str, measurement_interval_seconds: int = 5) -> float:
    """Calculate amortized embodied carbon per measurement period."""
    embodied_total_kg = EMBODIED_CARBON_KG.get(device_type, EMBODIED_CARBON_KG["laptop"])
    lifetime_years = EXPECTED_LIFETIME_YEARS.get(device_type, EXPECTED_LIFETIME_YEARS["laptop"])

    total_lifetime_seconds = lifetime_years * 365 * 24 * 3600
    total_measurements = total_lifetime_seconds / measurement_interval_seconds
    embodied_per_measurement_g = (embodied_total_kg * 1000) / total_measurements

    return embodied_per_measurement_g

def process_unprocessed_metrics():
    """Find and process metrics that haven't been profiled yet (IST timezone)."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Set timezone to IST
    cur.execute("SET timezone = 'Asia/Kolkata'")

    # Find unprocessed metrics WITH location data
    cur.execute("""
        SELECT
            m.id, m.device_id, m.device_type, m.timestamp,
            m.total_power_watts, m.latitude, m.longitude,
            m.country_code
        FROM device_metrics m
        LEFT JOIN carbon_footprints c ON m.id = c.metric_id
        WHERE c.id IS NULL
        ORDER BY m.timestamp
        LIMIT 100
    """)

    unprocessed = cur.fetchall()

    if not unprocessed:
        cur.close()
        conn.close()
        return 0

    processed_count = 0

    for metric in unprocessed:
        device_type = metric['device_type'] or 'laptop'
        lat = metric.get('latitude')
        lon = metric.get('longitude')
        country_code = metric.get('country_code')

        # Get location-aware grid intensity
        if lat is not None and lon is not None:
            grid_intensity = get_grid_intensity_with_cache(lat, lon, country_code)
        else:
            # No location data - use country fallback
            grid_intensity = FALLBACK_GRID_INTENSITY.get(
                country_code,
                FALLBACK_GRID_INTENSITY['default']
            )
            print(f"No location for metric {metric['id']}, using fallback: {grid_intensity}")

        # === OPERATIONAL CARBON ===
        power_kwh = (metric['total_power_watts'] * 5) / (1000 * 3600)
        operational_carbon_gco2 = power_kwh * grid_intensity

        # === EMBODIED CARBON ===
        embodied_carbon_gco2 = calculate_embodied_carbon_per_measurement(
            device_type=device_type,
            measurement_interval_seconds=5
        )

        embodied_total_kg = EMBODIED_CARBON_KG.get(device_type, EMBODIED_CARBON_KG["laptop"])
        device_lifetime = EXPECTED_LIFETIME_YEARS.get(device_type, EXPECTED_LIFETIME_YEARS["laptop"])

        # === TOTAL CARBON ===
        total_carbon_gco2 = operational_carbon_gco2 + embodied_carbon_gco2

        # Store the result (timestamp will be stored in IST)
        cur.execute("""
            INSERT INTO carbon_footprints
            (device_id, device_type, metric_id, timestamp,
             latitude, longitude,
             power_kwh, grid_intensity_gco2_per_kwh,
             operational_carbon_gco2, embodied_carbon_gco2,
             embodied_total_kgco2e, device_lifetime_years,
             total_carbon_gco2)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            metric['device_id'],
            device_type,
            metric['id'],
            metric['timestamp'],
            lat,
            lon,
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

    ist_time = datetime.now(IST).strftime('%H:%M:%S IST')
    print(f"Processed {processed_count} metrics at {ist_time}")
    return processed_count

def main():
    """Main worker loop."""
    print("=" * 60)
    print("🌱 Carbon Profiling Worker Starting (IST Timezone)")
    print("=" * 60)

    # Configuration status
    if ELECTRICITY_MAPS_TOKEN:
        print("Electricity Maps API: ENABLED (location-based)")
    else:
        print("Electricity Maps API: DISABLED (using regional fallbacks)")

    print(f"Embodied carbon values: {EMBODIED_CARBON_KG}")
    print(f"Expected lifetimes: {EXPECTED_LIFETIME_YEARS}")
    print(f"Fallback intensities: {FALLBACK_GRID_INTENSITY}")
    print(f"Timezone: Asia/Kolkata (IST)")
    print("=" * 60)

    # Wait for database
    time.sleep(5)

    # Initialize carbon table
    try:
        init_carbon_table()
    except Exception as e:
        print(f"Error initializing: {e}")
        time.sleep(10)
        return

    print(f"\n🚀 Starting processing loop at {datetime.now(IST).strftime('%H:%M:%S IST')}...\n")

    # Process loop
    while True:
        try:
            process_unprocessed_metrics()
            time.sleep(10)
        except KeyboardInterrupt:
            print("\n Worker stopped by user")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
