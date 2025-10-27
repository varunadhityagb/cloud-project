"""
Device Carbon Monitoring Agent
Simulates a personal device monitoring system that tracks
application-level power consumption and carbon footprint.
"""

import psutil
import time
import json
from datetime import datetime, timezone
from typing import Dict, List
import random
import requests
import os

class DeviceAgent:
    def __init__(self, device_id: str = None, api_endpoint: str = None):
        """Initialize the device monitoring agent."""
        self.device_id = device_id or f"device_{random.randint(1000, 9999)}"
        self.api_endpoint = api_endpoint or os.environ.get('API_ENDPOINT', 'http://localhost:5000')
        
        # TDP values for common device types (Watts)
        self.device_profiles = {
            "laptop": {"tdp": 45, "idle": 8},
            "desktop": {"tdp": 95, "idle": 25},
            "workstation": {"tdp": 150, "idle": 40}
        }
        self.device_type = "laptop"  # Default
        
    def get_cpu_power_draw(self) -> tuple:
        """
        Estimate CPU power draw based on utilization.
        Uses a simplified linear model.
        Returns: (power_draw, cpu_percent)
        """
        cpu_percent = psutil.cpu_percent(interval=1)
        profile = self.device_profiles[self.device_type]
        
        # Linear interpolation between idle and TDP
        power_draw = profile["idle"] + (cpu_percent / 100.0) * (profile["tdp"] - profile["idle"])
        return round(power_draw, 2), round(cpu_percent, 2)
    
    def get_top_processes(self, limit: int = 5) -> List[Dict]:
        """Get top CPU-consuming processes."""
        processes = []
        cpu_count = psutil.cpu_count()  # Get number of CPU cores
        
        for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
            try:
                pinfo = proc.info
                if pinfo['cpu_percent'] > 0:
                    # Normalize CPU percent to 0-100 scale (divide by core count)
                    pinfo['cpu_percent'] = pinfo['cpu_percent'] / cpu_count
                    processes.append(pinfo)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        # Sort by CPU usage
        processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
        return processes[:limit]
    
    def estimate_app_power_distribution(self, total_power: float, processes: List[Dict]) -> List[Dict]:
        """
        Distribute power consumption across applications based on their CPU usage.
        """
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
        """Collect comprehensive device metrics."""
        timestamp = datetime.now(timezone.utc).isoformat()
        
        # System-wide metrics (this call blocks for 1 second to get accurate reading)
        total_power, cpu_percent = self.get_cpu_power_draw()
        memory = psutil.virtual_memory()
        
        # Per-application metrics
        top_processes = self.get_top_processes(limit=5)
        app_metrics = self.estimate_app_power_distribution(total_power, top_processes)
        
        payload = {
            "device_id": self.device_id,
            "device_type": self.device_type,
            "timestamp": timestamp,
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
    
    def simulate_and_print(self, interval: int = 5, duration: int = 60, send_to_api: bool = False):
        """
        Simulate data collection and print to console.
        
        Args:
            interval: Seconds between collections
            duration: Total simulation duration in seconds
            send_to_api: Whether to send data to the ingestion API
        """
        print(f"🖥️  Device Agent Started: {self.device_id}")
        print(f"📊 Collecting metrics every {interval} seconds")
        print(f"⏱️  Duration: {duration} seconds")
        if send_to_api:
            print(f"🌐 API Endpoint: {self.api_endpoint}")
        print("=" * 80)
        
        iterations = duration // interval
        
        for i in range(iterations):
            metrics = self.collect_metrics()
            
            print(f"\n[Collection #{i+1}] {metrics['timestamp']}")
            print(f"💻 System: CPU {metrics['system_metrics']['cpu_percent']}% | "
                  f"RAM {metrics['system_metrics']['memory_percent']}% | "
                  f"Power {metrics['system_metrics']['total_power_watts']}W")
            
            print(f"\n🔝 Top Applications:")
            for app in metrics['applications']:
                print(f"   • {app['app_name']:<20} "
                      f"CPU: {app['cpu_percent']:>6}% | "
                      f"Power: {app['estimated_power_watts']:>6}W")
            
            print("-" * 80)
            
            # Send to API if enabled
            if send_to_api:
                success = self.send_to_api(metrics)
                status = "✅ Sent" if success else "❌ Failed"
                print(f"📤 API: {status}")
            
            if i < iterations - 1:
                time.sleep(interval)
        
        print(f"\n✅ Simulation complete! Collected {iterations} data points.")

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
    ║     Dynamic Carbon Profiling - Device Agent v1.0         ║
    ║     Monitoring system for personal computing devices      ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    # Create agent instance
    agent = DeviceAgent()
    
    # Export a sample for inspection
    agent.export_sample_json()
    
    print("\nStarting live monitoring...\n")
    
    # Check if API mode is enabled
    send_to_api = os.environ.get('SEND_TO_API', 'false').lower() == 'true'
    
    # Run simulation (collect for 30 seconds, every 5 seconds)
    try:
        agent.simulate_and_print(interval=5, duration=30, send_to_api=send_to_api)
    except KeyboardInterrupt:
        print("\n\n⚠️  Monitoring stopped by user")


if __name__ == "__main__":
    main()
