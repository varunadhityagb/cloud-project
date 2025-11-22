import requests
import os
from typing import Optional, Dict
import json

class GeolocationService:
    """Service for detecting device location via IP geolocation."""

    def __init__(self):
        self.ip_geolocation_key = os.environ.get('IP_GEOLOCATION_KEY', '')
        self.ip_geolocation_api = "https://api.ip2location.io/"
        self.ipify_api = "https://api.ipify.org"

        # Cache location to avoid repeated API calls
        self.cached_location = None

    def get_public_ip(self) -> Optional[str]:
        """
        Fetch the public IP address of this device.

        Returns:
            Public IP address as string, or None if request fails
        """
        try:
            response = requests.get(self.ipify_api, timeout=5)
            if response.status_code == 200:
                ip = response.text.strip()
                print(f"✅ Detected public IP: {ip}")
                return ip
            else:
                print(f"⚠️  Failed to get public IP: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Error fetching public IP: {e}")
            return None

    def get_location_from_ip(self, ip_address: str) -> Optional[Dict]:
        """
        Get geolocation data from IP address using IP2Location.

        Args:
            ip_address: Public IP address

        Returns:
            Dict with location data or None if request fails
        """
        if not self.ip_geolocation_key:
            print("⚠️  No IP_GEOLOCATION_KEY configured")
            return None

        try:
            params = {
                'key': self.ip_geolocation_key,
                'ip': ip_address,
                'format': 'json'
            }

            response = requests.get(
                self.ip_geolocation_api,
                params=params,
                timeout=5
            )

            if response.status_code == 200:
                data = response.json()

                # Check for API errors
                if 'error' in data:
                    print(f"⚠️  IP2Location API error: {data['error']}")
                    return None

                location = {
                    'ip': data.get('ip'),
                    'country_code': data.get('country_code'),
                    'country_name': data.get('country_name'),
                    'region_name': data.get('region_name'),
                    'city_name': data.get('city_name'),
                    'latitude': float(data.get('latitude', 0)),
                    'longitude': float(data.get('longitude', 0)),
                    'zip_code': data.get('zip_code'),
                    'time_zone': data.get('time_zone')
                }

                print(f"✅ Location detected: {location['city_name']}, {location['region_name']}, {location['country_name']}")
                print(f"   Coordinates: {location['latitude']}, {location['longitude']}")

                return location
            else:
                print(f"⚠️  IP2Location API error: {response.status_code}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"⚠️  Error fetching location: {e}")
            return None
        except (KeyError, ValueError, TypeError) as e:
            print(f"⚠️  Error parsing location data: {e}")
            return None

    def detect_device_location(self) -> Optional[Dict]:
        """
        Detect the current device's location.
        Uses caching to avoid repeated API calls.

        Returns:
            Location dict or None if detection fails
        """
        # Return cached location if available
        if self.cached_location:
            print("📍 Using cached location")
            return self.cached_location

        # Step 1: Get public IP
        print("🌐 Detecting device location...")
        public_ip = self.get_public_ip()

        if not public_ip:
            print("⚠️  Could not detect public IP")
            return None

        # Step 2: Get location from IP
        location = self.get_location_from_ip(public_ip)

        if location:
            # Cache the result
            self.cached_location = location
            return location
        else:
            print("⚠️  Could not detect location from IP")
            return None

    def get_fallback_location(self) -> Dict:
        """
        Get fallback location (Bengaluru, India) if detection fails.

        Returns:
            Default location dict
        """
        return {
            'ip': 'unknown',
            'country_code': 'IN',
            'country_name': 'India',
            'region_name': 'Karnataka',
            'city_name': 'Bengaluru',
            'latitude': 12.9716,
            'longitude': 77.5946,
            'zip_code': '560001',
            'time_zone': '+05:30'
        }


# Convenience function for quick use
def get_device_location() -> Dict:
    """
    Quick function to get device location with fallback.

    Returns:
        Location dict (detected or fallback)
    """
    service = GeolocationService()
    location = service.detect_device_location()

    if location:
        return location
    else:
        print("⚠️  Using fallback location (Bengaluru, India)")
        return service.get_fallback_location()


if __name__ == "__main__":
    # Test the geolocation service
    print("=" * 60)
    print("🌍 Testing Geolocation Service")
    print("=" * 60)

    location = get_device_location()
    print("\n📍 Final Location Data:")
    print(json.dumps(location, indent=2))
