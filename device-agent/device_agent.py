import psutil
import time
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional
import random
import requests
import os
import subprocess
import sys
from pathlib import Path
from geolocation_utils import get_device_location

class DeviceConfig:
    """Manages persistent device configuration."""
    
    def __init__(self, config_path: str = ".device_config.json"):
        self.config_path = Path(config_path)
        self.config = self._load_or_create_config()
    
    def _generate_device_id(self) -> str:
        """Generate a unique device ID based on system info."""
        try:
            # Try to get a stable hardware identifier
            import platform
            import hashlib
            
            # Combine multiple system identifiers for uniqueness
            system_info = f"{platform.node()}-{platform.machine()}-{platform.system()}"
            
            # Try to get MAC address for additional uniqueness
            try:
                import uuid
                mac = uuid.getnode()
                system_info += f"-{mac}"
            except:
                pass
            
            # Create a short hash of the system info
            hash_obj = hashlib.md5(system_info.encode())
            device_hash = hash_obj.hexdigest()[:8]
            
            return f"device_{device_hash}"
        except:
            # Fallback to random if system info fails
            return f"device_{random.randint(10000, 99999)}"
    
    def _load_or_create_config(self) -> Dict:
        """Load existing config or create new one."""
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                    print(f"✅ Loaded existing device config: {config['device_id']}")
                    return config
            except Exception as e:
                print(f"⚠️  Error loading config: {e}")
        
        # Create new config
        config = {
            "device_id": self._generate_device_id(),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "device_type": "laptop"
        }
        
        self._save_config(config)
        print(f"✨ Created new device config: {config['device_id']}")
        return config
    
    def _save_config(self, config: Dict):
        """Save config to file."""
        try:
            with open(self.config_path, 'w') as f:
                json.dump(config, f, indent=2)
        except Exception as e:
            print(f"⚠️  Error saving config: {e}")
    
    def get_device_id(self) -> str:
        return self.config['device_id']
    
    def get_device_type(self) -> str:
        return self.config.get('device_type', 'laptop')
    
    def set_device_type(self, device_type: str):
        self.config['device_type'] = device_type
        self._save_config(self.config)


class APIEndpointDetector:
    """Automatically detects the API endpoint from minikube."""
    
    @staticmethod
    def detect_minikube_endpoint() -> Optional[str]:
        """Try to auto-detect the minikube service URL."""
        try:
            result = subprocess.run(
                ['minikube', 'service', 'ingestion-api-service', '-n', 'carbon-profiling', '--url'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                url = result.stdout.strip().split('\n')[0]
                if url.startswith('http'):
                    print(f"✅ Auto-detected API endpoint: {url}")
                    return url
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
            print(f"⚠️  Could not auto-detect minikube endpoint: {e}")
        
        return None
    
    @staticmethod
    def get_endpoint() -> str:
        """Get API endpoint with fallback strategy."""
        # 1. Check environment variable
        env_endpoint = os.environ.get('API_ENDPOINT')
        if env_endpoint:
            print(f"✅ Using API endpoint from environment: {env_endpoint}")
            return env_endpoint
        
        # 2. Try to auto-detect from minikube
        print("🔍 No API_ENDPOINT set, attempting auto-detection...")
        minikube_endpoint = APIEndpointDetector.detect_minikube_endpoint()
        if minikube_endpoint:
            return minikube_endpoint
        
        # 3. Use localhost fallback
        fallback = "http://localhost:5000"
        print(f"⚠️  Using fallback endpoint: {fallback}")
        print(f"💡 Tip: Set API_ENDPOINT environment variable or ensure minikube is running")
        return fallback


class DeviceAgent:
    def __init__(self, device_id: str = None, api_endpoint: str = None):
        """Initialize the device monitoring agent with persistent config."""
        # Load or create device config
        self.config = DeviceConfig()
        self.device_id = device_id or self.config.get_device_id()
        self.device_type = self.config.get_device_type()
        
        # Detect API endpoint
        self.api_endpoint = api_endpoint or APIEndpointDetector.get_endpoint()
        
        # Device profiles
        self.device_profiles = {
            "laptop": {"tdp": 45, "idle": 8},
            "desktop": {"tdp": 95, "idle": 25},
            "workstation": {"tdp": 150, "idle": 40}
        }
        
        # Detect location on initialization
        print("\n🌍 Detecting device location...")
        self.location = get_device_location()
        print(f"📍 Location: {self.location['city_name']}, {self.location['country_name']}")
        print(f"   Coordinates: ({self.location['latitude']}, {self.location['longitude']})\n")

    def get_cpu_power_draw(self) -> tuple:
        """Estimate CPU power draw based on utilization."""
        cpu_percent = psutil.cpu_percent(interval=1)
        profile = self.device_profiles[self.device_type]
        power_draw = profile["idle"] + (cpu_percent / 100.0) * (profile["tdp"] - profile["idle"])
        return round(power_draw, 2), round(cpu_percent, 2)

    def get_top_processes(self, limit: int = 5) -> List[Dict]:
        """Get top CPU-consuming processes."""
        processes = []
        cpu_count = psutil.cpu_count()

        for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
            try:
                pinfo = proc.info
                if pinfo['cpu_percent'] > 0:
                    pinfo['cpu_percent'] = pinfo['cpu_percent'] / cpu_count
                    processes.append(pinfo)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
        return processes[:limit]

    def estimate_app_power_distribution(self, total_power: float, processes: List[Dict]) -> List[Dict]:
        """Distribute power consumption across applications."""
        total_cpu = sum(p['cpu_percent'] for p in processes) or 1

        app_metrics = []
        for proc in processes:
            cpu_share = proc['cpu_percent'] / total_cpu
            app_power = total_power * cpu_share

            app_metrics.append({
                "app_name": proc['name'],
                "pid": proc['pid'],
                "cpu_percent": round(proc['cpu_percent'], 2),
                "memory_percent": round(proc['memory_percent'], 2),
                "estimated_power_watts": round(app_power, 2)
            })

        return app_metrics

    def collect_metrics(self) -> Dict:
        """Collect comprehensive device metrics with location."""
        timestamp = datetime.now(timezone.utc).isoformat()

        # System-wide metrics
        total_power, cpu_percent = self.get_cpu_power_draw()
        memory = psutil.virtual_memory()

        # Per-application metrics
        top_processes = self.get_top_processes(limit=5)
        app_metrics = self.estimate_app_power_distribution(total_power, top_processes)

        payload = {
            "device_id": self.device_id,
            "device_type": self.device_type,
            "timestamp": timestamp,
            "location": {
                "latitude": self.location['latitude'],
                "longitude": self.location['longitude'],
                "city": self.location['city_name'],
                "region": self.location['region_name'],
                "country": self.location['country_name'],
                "country_code": self.location['country_code']
            },
            "system_metrics": {
                "cpu_percent": cpu_percent,
                "memory_percent": round(memory.percent, 2),
                "total_power_watts": total_power,
                "cpu_count": psutil.cpu_count()
            },
            "applications": app_metrics
        }

        return payload

    def send_to_api(self, metrics: Dict) -> bool:
        """Send metrics to the ingestion API."""
        try:
            url = f"{self.api_endpoint}/api/v1/metrics/ingest"
            response = requests.post(url, json=metrics, timeout=5)

            if response.status_code == 201:
                return True
            else:
                print(f"❌ API Error: {response.status_code} - {response.text}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"❌ Connection Error: {str(e)}")
            return False

    def test_api_connection(self) -> bool:
        """Test if API is reachable."""
        try:
            response = requests.get(f"{self.api_endpoint}/health", timeout=5)
            if response.status_code == 200:
                print(f"✅ API connection successful")
                return True
            else:
                print(f"⚠️  API returned status {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"❌ Cannot reach API: {e}")
            return False

    def run_continuous(self, interval: int = 5, send_to_api: bool = True):
        """
        Run continuous monitoring loop.
        
        Args:
            interval: Seconds between collections
            send_to_api: Whether to send data to the ingestion API
        """
        print(f"\n{'='*80}")
        print(f"🖥️  Device Agent Started")
        print(f"{'='*80}")
        print(f"📱 Device ID: {self.device_id}")
        print(f"🔧 Device Type: {self.device_type}")
        print(f"📍 Location: {self.location['city_name']}, {self.location['country_name']}")
        print(f"📊 Collection Interval: {interval} seconds")
        print(f"🌐 API Endpoint: {self.api_endpoint}")
        print(f"📤 API Sending: {'ENABLED' if send_to_api else 'DISABLED'}")
        print(f"{'='*80}\n")

        # Test API connection if enabled
        if send_to_api:
            if not self.test_api_connection():
                print("\n⚠️  WARNING: API is not reachable!")
                print("💡 The agent will continue collecting data, but won't send to API")
                response = input("Continue anyway? (y/n): ")
                if response.lower() != 'y':
                    print("Exiting...")
                    return
                send_to_api = False

        print("\n🚀 Starting continuous monitoring... (Press Ctrl+C to stop)\n")
        
        collection_count = 0
        success_count = 0
        fail_count = 0

        try:
            while True:
                collection_count += 1
                
                # Collect metrics
                metrics = self.collect_metrics()

                # Display summary
                print(f"\n[Collection #{collection_count}] {datetime.now().strftime('%H:%M:%S')}")
                print(f"💻 CPU: {metrics['system_metrics']['cpu_percent']:>5}% | "
                      f"RAM: {metrics['system_metrics']['memory_percent']:>5}% | "
                      f"Power: {metrics['system_metrics']['total_power_watts']:>6}W")

                # Send to API if enabled
                if send_to_api:
                    success = self.send_to_api(metrics)
                    if success:
                        success_count += 1
                        print(f"📤 API: ✅ Sent (Success: {success_count}, Failed: {fail_count})")
                    else:
                        fail_count += 1
                        print(f"📤 API: ❌ Failed (Success: {success_count}, Failed: {fail_count})")
                else:
                    print(f"📤 API: ⏭️  Skipped (local mode)")

                print("-" * 80)

                # Wait for next interval
                time.sleep(interval)

        except KeyboardInterrupt:
            print(f"\n\n{'='*80}")
            print(f"⚠️  Monitoring stopped by user")
            print(f"{'='*80}")
            print(f"📊 Total Collections: {collection_count}")
            if send_to_api:
                print(f"✅ Successful Sends: {success_count}")
                print(f"❌ Failed Sends: {fail_count}")
                success_rate = (success_count / collection_count * 100) if collection_count > 0 else 0
                print(f"📈 Success Rate: {success_rate:.1f}%")
            print(f"{'='*80}\n")

    def export_sample_json(self, filename: str = "sample_metrics.json"):
        """Export a single metrics collection as JSON for testing."""
        metrics = self.collect_metrics()
        with open(filename, 'w') as f:
            json.dump(metrics, f, indent=2)
        print(f"📝 Sample metrics exported to {filename}")


def main():
    """Main entry point for the device agent."""
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║     Dynamic Carbon Profiling - Device Agent v3.0         ║
    ║     Persistent ID & Auto-Config Support                  ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    # Create agent instance (auto-detects everything)
    agent = DeviceAgent()

    # Export a sample for inspection
    print("\n📝 Exporting sample metrics...")
    agent.export_sample_json()
    print("✅ Sample exported to sample_metrics.json\n")

    # Check if API mode should be enabled
    # Default to TRUE now - user doesn't need to set environment variable
    send_to_api_env = os.environ.get('SEND_TO_API', 'true').lower()
    send_to_api = send_to_api_env in ('true', '1', 'yes', 'y')

    if not send_to_api:
        print("ℹ️  Running in LOCAL MODE (set SEND_TO_API=true to enable API)")

    # Run continuous monitoring
    try:
        agent.run_continuous(interval=5, send_to_api=send_to_api)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
