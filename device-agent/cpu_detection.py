"""
CPU TDP Data Manager - Retrieve and cache TDP data from various sources
Supports multiple data sources with fallback mechanisms and local caching
"""

import json
import re
import platform
import subprocess
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime, timedelta
import urllib.request
import urllib.error


class CPUDataManager:
    """Manages CPU TDP data from multiple sources with intelligent caching."""

    CACHE_FILE = Path.home() / ".cache" / "cpu_tdp_cache.json"
    CACHE_DURATION_DAYS = 30

    # Data source URLs
    DATA_SOURCES = {
        "boavizta_csv": "https://raw.githubusercontent.com/Boavizta/boaviztapi/main/boaviztapi/data/crowdsourcing/cpu_specs.csv",
        "intel_json": "https://raw.githubusercontent.com/divinity76/intel-cpu-database/master/databases/intel_cpu_database.json",
    }

    def __init__(self, auto_update: bool = True):
        """Initialize CPU data manager with optional auto-update."""
        self.cache = self._load_cache()

        if auto_update and self._should_update_cache():
            print("📡 Updating CPU database from online sources...")
            self._update_database()

    def _load_cache(self) -> Dict:
        """Load cached CPU data from disk."""
        if self.CACHE_FILE.exists():
            try:
                with open(self.CACHE_FILE, 'r') as f:
                    cache = json.load(f)
                    print(f"✅ Loaded {len(cache.get('cpus', {}))} CPUs from cache")
                    return cache
            except Exception as e:
                print(f"⚠️  Cache load failed: {e}")

        return {"cpus": {}, "last_updated": None, "sources": []}

    def _save_cache(self):
        """Save CPU data cache to disk."""
        try:
            self.CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(self.CACHE_FILE, 'w') as f:
                json.dump(self.cache, f, indent=2)
            print(f"💾 Saved cache with {len(self.cache['cpus'])} CPUs")
        except Exception as e:
            print(f"⚠️  Cache save failed: {e}")

    def _should_update_cache(self) -> bool:
        """Check if cache needs updating."""
        if not self.cache.get("last_updated"):
            return True

        last_updated = datetime.fromisoformat(self.cache["last_updated"])
        age = datetime.now() - last_updated

        return age > timedelta(days=self.CACHE_DURATION_DAYS)

    def _fetch_url(self, url: str, timeout: int = 10) -> Optional[str]:
        """Fetch data from URL with error handling."""
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                return response.read().decode('utf-8')
        except Exception as e:
            print(f"⚠️  Failed to fetch {url}: {e}")
            return None

    def _parse_boavizta_csv(self, csv_data: str) -> Dict[str, Dict]:
        """Parse Boavizta CSV data into structured format."""
        cpus = {}
        lines = csv_data.strip().split('\n')

        if len(lines) < 2:
            return cpus

        headers = [h.strip() for h in lines[0].split(',')]

        for line in lines[1:]:
            try:
                values = [v.strip() for v in line.split(',')]
                if len(values) < len(headers):
                    continue

                row = dict(zip(headers, values))
                name = row.get('name', '').strip()

                if not name:
                    continue

                # Parse TDP
                tdp_str = row.get('tdp', '').strip()
                try:
                    tdp = float(tdp_str) if tdp_str else None
                except:
                    tdp = None

                if tdp and tdp > 0:
                    # Estimate idle power (rough heuristic)
                    idle = self._estimate_idle_from_tdp(tdp, name)

                    cpus[name.lower()] = {
                        "name": name,
                        "tdp": tdp,
                        "idle": idle,
                        "cores": self._parse_number(row.get('cores')),
                        "threads": self._parse_number(row.get('threads')),
                        "manufacturer": row.get('manufacturer', '').strip(),
                        "source": "boavizta"
                    }
            except Exception as e:
                continue

        return cpus

    def _parse_intel_json(self, json_data: str) -> Dict[str, Dict]:
        """Parse Intel JSON database."""
        cpus = {}

        try:
            data = json.loads(json_data)

            for cpu_id, cpu_info in data.items():
                name = cpu_info.get('name', '').strip()
                if not name:
                    continue

                # Extract TDP from various possible fields
                tdp = None
                for field in ['TDP', 'tdp', 'Thermal Design Power', 'thermal_design_power']:
                    if field in cpu_info:
                        tdp_str = str(cpu_info[field]).replace('W', '').strip()
                        try:
                            tdp = float(tdp_str)
                            break
                        except:
                            continue

                if tdp and tdp > 0:
                    idle = self._estimate_idle_from_tdp(tdp, name)

                    cpus[name.lower()] = {
                        "name": name,
                        "tdp": tdp,
                        "idle": idle,
                        "manufacturer": "Intel",
                        "source": "intel_db"
                    }
        except Exception as e:
            print(f"⚠️  Intel JSON parse error: {e}")

        return cpus

    def _parse_number(self, value: str) -> Optional[float]:
        """Parse numeric value from string."""
        if not value:
            return None
        try:
            return float(value)
        except:
            return None

    def _extract_model_token(self, name: str) -> Optional[str]:
        """
        Extract key CPU model identifier such as i7-8650U, 1145G7, 7800X3D, etc.
        """
        if not name:
            return None

        name = name.upper().replace("INTEL", "").replace("AMD", "")

        # Common CPU model patterns
        patterns = [
            r"[A-Z]?\d{4}[A-Z]{0,3}\d?",      # 1145G7, 1195G7, 5600X, 7800X3D
            r"[iI][3579]-\d{3,4}[A-Z]?\d?",   # i5-1145G7
            r"RYZEN\s*\d+\s*\d{4}[A-Z]?",     # Ryzen 7 5800X
        ]

        for p in patterns:
            m = re.search(p, name)
            if m:
                return m.group(0)

        return None


    def _estimate_idle_from_tdp(self, tdp: float, cpu_name: str) -> float:
        """Estimate idle power from TDP based on CPU characteristics."""
        name_upper = cpu_name.upper()

        # Mobile/Laptop processors
        if any(x in name_upper for x in ['U', 'Y', 'MOBILE', 'M1', 'M2', 'M3', 'M4']):
            return max(2.0, tdp * 0.15)

        # High-end workstation
        elif any(x in name_upper for x in ['THREADRIPPER', 'XEON', 'EPYC']):
            return max(20.0, tdp * 0.18)

        # Desktop - standard
        else:
            return max(8.0, tdp * 0.16)

    def _update_database(self):
        """Update database from online sources."""
        all_cpus = {}
        sources_used = []

        # Fetch Boavizta CSV
        print("📥 Fetching Boavizta database...")
        csv_data = self._fetch_url(self.DATA_SOURCES["boavizta_csv"])
        if csv_data:
            cpus = self._parse_boavizta_csv(csv_data)
            all_cpus.update(cpus)
            sources_used.append("boavizta")
            print(f"   ✅ Added {len(cpus)} CPUs from Boavizta")

        # Fetch Intel JSON (note: this might be large)
        print("📥 Fetching Intel database...")
        json_data = self._fetch_url(self.DATA_SOURCES["intel_json"])
        if json_data:
            cpus = self._parse_intel_json(json_data)
            # Merge, preferring existing data
            for key, value in cpus.items():
                if key not in all_cpus:
                    all_cpus[key] = value
            sources_used.append("intel")
            print(f"   ✅ Added Intel CPUs")

        # Update cache
        if all_cpus:
            self.cache = {
                "cpus": all_cpus,
                "last_updated": datetime.now().isoformat(),
                "sources": sources_used,
                "total_cpus": len(all_cpus)
            }
            self._save_cache()
            print(f"🎉 Database updated with {len(all_cpus)} total CPUs")
        else:
            print("⚠️  No data fetched, keeping existing cache")

    def lookup_cpu(self, cpu_name: str) -> Optional[Dict]:
        """Look up CPU by name with fuzzy matching."""
        if not cpu_name:
            return None

        cpu_lower = cpu_name.lower().strip()
        cpus = self.cache.get("cpus", {})

        # Direct match
        if cpu_lower in cpus:
            return cpus[cpu_lower]

        # Fuzzy match - find best match
        best_match = None
        best_score = 0

        for stored_name, cpu_data in cpus.items():
            score = self._match_score(cpu_lower, stored_name)
            if score > best_score and score > 0.6:  # Threshold
                best_score = score
                best_match = cpu_data

        return best_match

    def _match_score(self, query: str, candidate: str) -> float:
        q_token = self._extract_model_token(query)
        c_token = self._extract_model_token(candidate)

        # Exact model token match → almost perfect match
        if q_token and c_token and q_token == c_token:
            return 0.99

        # Fallback: simple word intersection
        query_words = set(re.findall(r'\w+', query.lower()))
        candidate_words = set(re.findall(r'\w+', candidate.lower()))

        if not query_words:
            return 0.0

        matches = len(query_words & candidate_words)
        return matches / len(query_words)

    def get_cpu_stats(self) -> Dict:
        """Get statistics about cached database."""
        cpus = self.cache.get("cpus", {})

        manufacturers = {}
        for cpu_data in cpus.values():
            mfr = cpu_data.get("manufacturer", "Unknown")
            manufacturers[mfr] = manufacturers.get(mfr, 0) + 1

        return {
            "total_cpus": len(cpus),
            "last_updated": self.cache.get("last_updated"),
            "sources": self.cache.get("sources", []),
            "manufacturers": manufacturers
        }


class CPUDetector:
    """Enhanced CPU detector using online database."""

    def __init__(self, data_manager: Optional[CPUDataManager] = None):
        self.data_manager = data_manager or CPUDataManager()
        self.cpu_model = self._detect_cpu_model()
        self.cpu_count = self._get_cpu_count()
        self.tdp_info = self._lookup_tdp()

    def _detect_cpu_model(self) -> str:
        """Detect CPU model name."""
        try:
            system = platform.system()

            if system == "Linux":
                with open('/proc/cpuinfo', 'r') as f:
                    for line in f:
                        if line.startswith('model name'):
                            return line.split(':')[1].strip()

            elif system == "Darwin":
                result = subprocess.run(
                    ['sysctl', '-n', 'machdep.cpu.brand_string'],
                    capture_output=True, text=True, timeout=2
                )
                if result.returncode == 0:
                    return result.stdout.strip()

            elif system == "Windows":
                result = subprocess.run(
                    ['wmic', 'cpu', 'get', 'name'],
                    capture_output=True, text=True, timeout=2
                )
                if result.returncode == 0:
                    lines = result.stdout.strip().split('\n')
                    if len(lines) > 1:
                        return lines[1].strip()
        except:
            pass

        return platform.processor() or "Unknown CPU"

    def _get_cpu_count(self) -> int:
        """Get CPU core count."""
        try:
            import psutil
            return psutil.cpu_count(logical=False) or psutil.cpu_count(logical=True)
        except:
            import os
            return os.cpu_count() or 4

    def _lookup_tdp(self) -> Dict:
        """Look up TDP from database."""
        cpu_data = self.data_manager.lookup_cpu(self.cpu_model)

        if cpu_data:
            return {
                "tdp": cpu_data["tdp"],
                "idle": cpu_data["idle"],
                "category": self._guess_category(),
                "detected": True,
                "source": cpu_data.get("source", "unknown")
            }

        # Fallback
        print(f"⚠️  CPU '{self.cpu_model}' not found in database")
        category = self._guess_category()
        fallback_tdp = {"laptop": 45, "desktop": 95, "workstation": 165}
        fallback_idle = {"laptop": 8, "desktop": 15, "workstation": 30}

        return {
            "tdp": fallback_tdp[category],
            "idle": fallback_idle[category],
            "category": category,
            "detected": False,
            "source": "fallback"
        }

    def _guess_category(self) -> str:
        """Guess device category."""
        cpu_upper = self.cpu_model.upper()

        if any(x in cpu_upper for x in ["XEON", "THREADRIPPER", "EPYC"]):
            return "workstation"
        elif any(x in cpu_upper for x in ["U", "H", "MOBILE", "M1", "M2", "M3", "M4"]):
            return "laptop"
        else:
            return "desktop"

    def get_power_profile(self) -> Dict:
        """Get complete power profile."""
        return {
            "cpu_model": self.cpu_model,
            "cpu_cores": self.cpu_count,
            "tdp_watts": self.tdp_info["tdp"],
            "idle_watts": self.tdp_info["idle"],
            "category": self.tdp_info["category"],
            "auto_detected": self.tdp_info["detected"],
            "data_source": self.tdp_info["source"]
        }

    def calculate_power(self, cpu_percent: float) -> float:
        """Calculate power draw from CPU utilization."""
        idle = self.tdp_info["idle"]
        tdp = self.tdp_info["tdp"]
        power = idle + (cpu_percent / 100.0) * (tdp - idle)
        return round(power, 2)


# Example usage
if __name__ == "__main__":
    print("=" * 70)
    print("CPU TDP Database Manager - Enhanced Version")
    print("=" * 70)

    # Initialize data manager (will auto-update if cache is old)
    data_mgr = CPUDataManager(auto_update=True)

    # Show database stats
    print("\n📊 Database Statistics:")
    stats = data_mgr.get_cpu_stats()
    print(f"   Total CPUs: {stats['total_cpus']}")
    print(f"   Last Updated: {stats['last_updated']}")
    print(f"   Sources: {', '.join(stats['sources'])}")
    print(f"   Manufacturers: {dict(list(stats['manufacturers'].items())[:5])}")

    # Detect current CPU
    print("\n" + "=" * 70)
    print("Detecting Current CPU")
    print("=" * 70)

    detector = CPUDetector(data_mgr)
    profile = detector.get_power_profile()

    print(f"\n🔍 CPU: {profile['cpu_model']}")
    print(f"   Category: {profile['category']}")
    print(f"   Cores: {profile['cpu_cores']}")
    print(f"   TDP: {profile['tdp_watts']}W")
    print(f"   Idle: {profile['idle_watts']}W")
    print(f"   Source: {profile['data_source']}")

    if profile['auto_detected']:
        print(f"   ✅ Found in database")
    else:
        print(f"   ⚠️  Using fallback estimates")

    # Power examples
    print("\n" + "=" * 70)
    print("Power Consumption Examples")
    print("=" * 70)

    for util in [0, 25, 50, 75, 100]:
        power = detector.calculate_power(util)
        print(f"   CPU @ {util:3d}% → {power:6.2f}W")

    print("\n" + "=" * 70)
