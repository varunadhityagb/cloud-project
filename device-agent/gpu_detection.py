"""
GPU TDP Data Manager - Retrieve and cache GPU TDP data
Supports NVIDIA, AMD, and Intel GPUs with fallback mechanisms
"""

import json
import re
import platform
import subprocess
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime, timedelta


class GPUDataManager:
    """Manages GPU TDP data with intelligent caching."""

    CACHE_FILE = Path.home() / ".cache" / "gpu_tdp_cache.json"
    CACHE_DURATION_DAYS = 30

    # Common GPU TDP values (Watts)
    GPU_TDP_DATABASE = {
        # NVIDIA RTX 40 Series
        "RTX 4090": {"tdp": 450, "idle": 25, "category": "high_end"},
        "RTX 4080": {"tdp": 320, "idle": 20, "category": "high_end"},
        "RTX 4070 TI": {"tdp": 285, "idle": 18, "category": "high_end"},
        "RTX 4070": {"tdp": 200, "idle": 15, "category": "mid_range"},
        "RTX 4060 TI": {"tdp": 160, "idle": 12, "category": "mid_range"},
        "RTX 4060": {"tdp": 115, "idle": 10, "category": "mid_range"},
        
        # NVIDIA RTX 30 Series
        "RTX 3090 TI": {"tdp": 450, "idle": 25, "category": "high_end"},
        "RTX 3090": {"tdp": 350, "idle": 22, "category": "high_end"},
        "RTX 3080 TI": {"tdp": 350, "idle": 22, "category": "high_end"},
        "RTX 3080": {"tdp": 320, "idle": 20, "category": "high_end"},
        "RTX 3070 TI": {"tdp": 290, "idle": 18, "category": "high_end"},
        "RTX 3070": {"tdp": 220, "idle": 15, "category": "mid_range"},
        "RTX 3060 TI": {"tdp": 200, "idle": 15, "category": "mid_range"},
        "RTX 3060": {"tdp": 170, "idle": 12, "category": "mid_range"},
        "RTX 3050": {"tdp": 130, "idle": 10, "category": "entry"},
        
        # NVIDIA RTX 20 Series
        "RTX 2080 TI": {"tdp": 250, "idle": 18, "category": "high_end"},
        "RTX 2080": {"tdp": 215, "idle": 15, "category": "mid_range"},
        "RTX 2070": {"tdp": 175, "idle": 12, "category": "mid_range"},
        "RTX 2060": {"tdp": 160, "idle": 12, "category": "mid_range"},
        
        # NVIDIA GTX 16 Series
        "GTX 1660 TI": {"tdp": 120, "idle": 10, "category": "entry"},
        "GTX 1660": {"tdp": 120, "idle": 10, "category": "entry"},
        "GTX 1650": {"tdp": 75, "idle": 8, "category": "entry"},
        
        # NVIDIA Mobile GPUs
        "RTX 4090 MOBILE": {"tdp": 175, "idle": 10, "category": "mobile_high"},
        "RTX 4080 MOBILE": {"tdp": 150, "idle": 8, "category": "mobile_high"},
        "RTX 4070 MOBILE": {"tdp": 140, "idle": 8, "category": "mobile_mid"},
        "RTX 4060 MOBILE": {"tdp": 115, "idle": 6, "category": "mobile_mid"},
        "RTX 3080 MOBILE": {"tdp": 165, "idle": 10, "category": "mobile_high"},
        "RTX 3070 MOBILE": {"tdp": 140, "idle": 8, "category": "mobile_mid"},
        "RTX 3060 MOBILE": {"tdp": 115, "idle": 6, "category": "mobile_mid"},
        "RTX 3050 MOBILE": {"tdp": 95, "idle": 5, "category": "mobile_entry"},
        
        # AMD RX 7000 Series
        "RX 7900 XTX": {"tdp": 355, "idle": 20, "category": "high_end"},
        "RX 7900 XT": {"tdp": 315, "idle": 18, "category": "high_end"},
        "RX 7800 XT": {"tdp": 263, "idle": 16, "category": "mid_range"},
        "RX 7700 XT": {"tdp": 245, "idle": 15, "category": "mid_range"},
        "RX 7600": {"tdp": 165, "idle": 12, "category": "mid_range"},
        
        # AMD RX 6000 Series
        "RX 6950 XT": {"tdp": 335, "idle": 20, "category": "high_end"},
        "RX 6900 XT": {"tdp": 300, "idle": 18, "category": "high_end"},
        "RX 6800 XT": {"tdp": 300, "idle": 18, "category": "high_end"},
        "RX 6800": {"tdp": 250, "idle": 16, "category": "mid_range"},
        "RX 6700 XT": {"tdp": 230, "idle": 15, "category": "mid_range"},
        "RX 6600 XT": {"tdp": 160, "idle": 12, "category": "mid_range"},
        "RX 6600": {"tdp": 132, "idle": 10, "category": "entry"},
        "RX 6500 XT": {"tdp": 107, "idle": 8, "category": "entry"},
        
        # Intel Arc
        "ARC A770": {"tdp": 225, "idle": 15, "category": "mid_range"},
        "ARC A750": {"tdp": 225, "idle": 15, "category": "mid_range"},
        "ARC A580": {"tdp": 185, "idle": 12, "category": "mid_range"},
        "ARC A380": {"tdp": 75, "idle": 8, "category": "entry"},
        
        # Integrated GPUs
        "INTEL UHD": {"tdp": 15, "idle": 2, "category": "integrated"},
        "INTEL IRIS XE": {"tdp": 28, "idle": 3, "category": "integrated"},
        "AMD RADEON VEGA": {"tdp": 25, "idle": 3, "category": "integrated"},
        "AMD RADEON 680M": {"tdp": 35, "idle": 4, "category": "integrated"},
        "AMD RADEON 780M": {"tdp": 45, "idle": 5, "category": "integrated"},
        
        # Apple Silicon (integrated GPU)
        "M1": {"tdp": 20, "idle": 2, "category": "integrated"},
        "M1 PRO": {"tdp": 30, "idle": 3, "category": "integrated"},
        "M1 MAX": {"tdp": 60, "idle": 5, "category": "integrated"},
        "M2": {"tdp": 25, "idle": 2, "category": "integrated"},
        "M2 PRO": {"tdp": 35, "idle": 3, "category": "integrated"},
        "M2 MAX": {"tdp": 70, "idle": 5, "category": "integrated"},
        "M3": {"tdp": 30, "idle": 2, "category": "integrated"},
        "M3 PRO": {"tdp": 40, "idle": 3, "category": "integrated"},
        "M3 MAX": {"tdp": 80, "idle": 5, "category": "integrated"},
        "M4": {"tdp": 35, "idle": 2, "category": "integrated"},
    }

    def __init__(self):
        """Initialize GPU data manager."""
        self.cache = self._load_cache()
        
        # Save cache if it was just created
        if not self.CACHE_FILE.exists() or self.cache.get('source') == 'built-in':
            self._save_cache()

    def _load_cache(self) -> Dict:
        """Load cached GPU data from disk."""
        if self.CACHE_FILE.exists():
            try:
                with open(self.CACHE_FILE, 'r') as f:
                    cache = json.load(f)
                    print(f"✅ Loaded GPU database with {len(cache.get('gpus', {}))} entries")
                    return cache
            except Exception as e:
                print(f"⚠️  GPU cache load failed: {e}")

        # Initialize with built-in database
        return {
            "gpus": self.GPU_TDP_DATABASE,
            "last_updated": datetime.now().isoformat(),
            "source": "built-in"
        }

    def _save_cache(self):
        """Save GPU data cache to disk."""
        try:
            self.CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(self.CACHE_FILE, 'w') as f:
                json.dump(self.cache, f, indent=2)
            print(f"💾 Saved GPU cache with {len(self.cache.get('gpus', {}))} entries")
        except Exception as e:
            print(f"⚠️  GPU cache save failed: {e}")

    def _extract_gpu_model(self, gpu_name: str) -> Optional[str]:
        """Extract key GPU model identifier."""
        if not gpu_name:
            return None

        gpu_upper = gpu_name.upper()

        # Remove common prefixes
        for prefix in ["NVIDIA", "AMD", "INTEL", "GEFORCE", "RADEON"]:
            gpu_upper = gpu_upper.replace(prefix, "").strip()

        # Match patterns
        patterns = [
            r"RTX\s*\d{4}\s*(?:TI|SUPER)?(?:\s*MOBILE)?",  # RTX 4090, RTX 3080 TI
            r"GTX\s*\d{4}\s*(?:TI|SUPER)?",                # GTX 1660 TI
            r"RX\s*\d{4}\s*(?:XT|XTX)?",                   # RX 7900 XTX
            r"ARC\s*A\d{3}",                               # Arc A770
            r"M\d+\s*(?:PRO|MAX|ULTRA)?",                  # M1, M2 Pro, M3 Max
            r"(?:UHD|IRIS\s*XE|VEGA|\d{3,4}M)",           # Integrated GPUs
        ]

        for pattern in patterns:
            match = re.search(pattern, gpu_upper)
            if match:
                return match.group(0).strip()

        return None

    def lookup_gpu(self, gpu_name: str) -> Optional[Dict]:
        """Look up GPU by name with fuzzy matching."""
        if not gpu_name:
            return None

        gpu_upper = gpu_name.upper().strip()
        gpus = self.cache.get("gpus", {})

        # Extract model token
        model_token = self._extract_gpu_model(gpu_name)
        
        # Direct match
        if gpu_upper in gpus:
            return gpus[gpu_upper]

        # Match by model token
        if model_token:
            for stored_name, gpu_data in gpus.items():
                if model_token in stored_name:
                    return gpu_data

        # Fuzzy match
        best_match = None
        best_score = 0

        for stored_name, gpu_data in gpus.items():
            score = self._match_score(gpu_upper, stored_name)
            if score > best_score and score > 0.6:
                best_score = score
                best_match = gpu_data

        return best_match

    def _match_score(self, query: str, candidate: str) -> float:
        """Calculate match score between query and candidate."""
        query_words = set(re.findall(r'\w+', query.lower()))
        candidate_words = set(re.findall(r'\w+', candidate.lower()))

        if not query_words:
            return 0.0

        matches = len(query_words & candidate_words)
        return matches / len(query_words)


class GPUDetector:
    """Enhanced GPU detector with power profiling."""

    def __init__(self, data_manager: Optional[GPUDataManager] = None):
        self.data_manager = data_manager or GPUDataManager()
        self.gpu_info = self._detect_gpu()
        self.gpu_support = self._check_monitoring_support()

    def _detect_gpu(self) -> List[Dict]:
        """Detect all available GPUs."""
        gpus = []
        
        # Try nvidia-smi for NVIDIA GPUs
        nvidia_gpus = self._detect_nvidia_gpu()
        if nvidia_gpus:
            gpus.extend(nvidia_gpus)
        
        # Try for AMD GPUs
        amd_gpus = self._detect_amd_gpu()
        if amd_gpus:
            gpus.extend(amd_gpus)
        
        # Try for Intel GPUs
        intel_gpus = self._detect_intel_gpu()
        if intel_gpus:
            gpus.extend(intel_gpus)
        
        # Fallback to system detection
        if not gpus:
            gpus = self._detect_fallback_gpu()
        
        return gpus

    def _detect_nvidia_gpu(self) -> List[Dict]:
        """Detect NVIDIA GPUs using nvidia-smi."""
        try:
            result = subprocess.run(
                ['nvidia-smi', '--query-gpu=name,memory.total', '--format=csv,noheader'],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                gpus = []
                for line in result.stdout.strip().split('\n'):
                    if line:
                        parts = line.split(',')
                        name = parts[0].strip()
                        
                        gpu_data = self.data_manager.lookup_gpu(name)
                        gpus.append({
                            'name': name,
                            'vendor': 'NVIDIA',
                            'tdp': gpu_data['tdp'] if gpu_data else 150,
                            'idle': gpu_data['idle'] if gpu_data else 10,
                            'category': gpu_data['category'] if gpu_data else 'mid_range',
                            'detected': bool(gpu_data),
                            'monitoring': 'nvidia-smi'
                        })
                
                return gpus
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        
        return []

    def _detect_amd_gpu(self) -> List[Dict]:
        """Detect AMD GPUs using rocm-smi or lspci."""
        try:
            # Try rocm-smi first
            result = subprocess.run(
                ['rocm-smi', '--showproductname'],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                gpus = []
                for line in result.stdout.split('\n'):
                    if 'GPU' in line or 'Radeon' in line:
                        name = line.strip()
                        gpu_data = self.data_manager.lookup_gpu(name)
                        
                        gpus.append({
                            'name': name,
                            'vendor': 'AMD',
                            'tdp': gpu_data['tdp'] if gpu_data else 180,
                            'idle': gpu_data['idle'] if gpu_data else 12,
                            'category': gpu_data['category'] if gpu_data else 'mid_range',
                            'detected': bool(gpu_data),
                            'monitoring': 'rocm-smi'
                        })
                
                if gpus:
                    return gpus
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        
        # Fallback to lspci for AMD
        try:
            result = subprocess.run(
                ['lspci'], capture_output=True, text=True, timeout=5
            )
            
            if result.returncode == 0:
                for line in result.stdout.split('\n'):
                    if 'VGA' in line and ('AMD' in line or 'Radeon' in line):
                        name = line.split(':')[-1].strip()
                        gpu_data = self.data_manager.lookup_gpu(name)
                        
                        return [{
                            'name': name,
                            'vendor': 'AMD',
                            'tdp': gpu_data['tdp'] if gpu_data else 180,
                            'idle': gpu_data['idle'] if gpu_data else 12,
                            'category': gpu_data['category'] if gpu_data else 'mid_range',
                            'detected': bool(gpu_data),
                            'monitoring': 'lspci'
                        }]
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        
        return []

    def _detect_intel_gpu(self) -> List[Dict]:
        """Detect Intel GPUs."""
        try:
            result = subprocess.run(
                ['lspci'], capture_output=True, text=True, timeout=5
            )
            
            if result.returncode == 0:
                for line in result.stdout.split('\n'):
                    if 'VGA' in line and 'Intel' in line:
                        name = line.split(':')[-1].strip()
                        gpu_data = self.data_manager.lookup_gpu(name)
                        
                        return [{
                            'name': name,
                            'vendor': 'Intel',
                            'tdp': gpu_data['tdp'] if gpu_data else 25,
                            'idle': gpu_data['idle'] if gpu_data else 3,
                            'category': gpu_data['category'] if gpu_data else 'integrated',
                            'detected': bool(gpu_data),
                            'monitoring': 'lspci'
                        }]
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        
        return []

    def _detect_fallback_gpu(self) -> List[Dict]:
        """Fallback GPU detection using system info."""
        system = platform.system()
        
        # macOS - assume Apple Silicon
        if system == "Darwin":
            try:
                result = subprocess.run(
                    ['system_profiler', 'SPDisplaysDataType'],
                    capture_output=True, text=True, timeout=5
                )
                
                if 'Apple' in result.stdout:
                    # Try to detect M-series chip
                    for model in ['M4', 'M3', 'M2', 'M1']:
                        if model in result.stdout.upper():
                            gpu_data = self.data_manager.lookup_gpu(model)
                            return [{
                                'name': f'Apple {model}',
                                'vendor': 'Apple',
                                'tdp': gpu_data['tdp'] if gpu_data else 30,
                                'idle': gpu_data['idle'] if gpu_data else 3,
                                'category': 'integrated',
                                'detected': bool(gpu_data),
                                'monitoring': 'system_profiler'
                            }]
            except:
                pass
        
        # Generic fallback
        return [{
            'name': 'Unknown GPU',
            'vendor': 'Unknown',
            'tdp': 100,
            'idle': 8,
            'category': 'mid_range',
            'detected': False,
            'monitoring': 'none'
        }]

    def _check_monitoring_support(self) -> Dict:
        """Check which GPU monitoring tools are available."""
        support = {
            'nvidia_smi': False,
            'rocm_smi': False,
            'intel_gpu_top': False
        }
        
        # Check nvidia-smi
        try:
            subprocess.run(['nvidia-smi'], capture_output=True, timeout=2)
            support['nvidia_smi'] = True
        except:
            pass
        
        # Check rocm-smi
        try:
            subprocess.run(['rocm-smi'], capture_output=True, timeout=2)
            support['rocm_smi'] = True
        except:
            pass
        
        # Check intel_gpu_top
        try:
            subprocess.run(['intel_gpu_top', '-h'], capture_output=True, timeout=2)
            support['intel_gpu_top'] = True
        except:
            pass
        
        return support

    def get_gpu_utilization(self, gpu_index: int = 0) -> Optional[float]:
        """Get GPU utilization percentage."""
        if gpu_index >= len(self.gpu_info):
            return None
        
        gpu = self.gpu_info[gpu_index]
        
        # NVIDIA GPU
        if gpu['vendor'] == 'NVIDIA' and self.gpu_support['nvidia_smi']:
            try:
                result = subprocess.run(
                    ['nvidia-smi', '--query-gpu=utilization.gpu',
                     '--format=csv,noheader,nounits', f'--id={gpu_index}'],
                    capture_output=True, text=True, timeout=2
                )
                
                if result.returncode == 0:
                    return float(result.stdout.strip())
            except:
                pass
        
        # AMD GPU
        elif gpu['vendor'] == 'AMD' and self.gpu_support['rocm_smi']:
            try:
                result = subprocess.run(
                    ['rocm-smi', '--showuse'],
                    capture_output=True, text=True, timeout=2
                )
                
                if result.returncode == 0:
                    # Parse rocm-smi output for utilization
                    for line in result.stdout.split('\n'):
                        if 'GPU use' in line or '%' in line:
                            match = re.search(r'(\d+(?:\.\d+)?)\s*%', line)
                            if match:
                                return float(match.group(1))
            except:
                pass
        
        # Fallback - assume low utilization
        return 5.0

    def calculate_gpu_power(self, gpu_index: int = 0, utilization: Optional[float] = None) -> float:
        """Calculate GPU power draw from utilization."""
        if gpu_index >= len(self.gpu_info):
            return 0.0
        
        gpu = self.gpu_info[gpu_index]
        
        if utilization is None:
            utilization = self.get_gpu_utilization(gpu_index) or 5.0
        
        idle = gpu['idle']
        tdp = gpu['tdp']
        
        power = idle + (utilization / 100.0) * (tdp - idle)
        return round(power, 2)

    def get_all_gpus_power(self) -> Dict:
        """Get power consumption for all GPUs."""
        total_power = 0.0
        gpu_details = []
        
        for i, gpu in enumerate(self.gpu_info):
            utilization = self.get_gpu_utilization(i)
            power = self.calculate_gpu_power(i, utilization)
            
            total_power += power
            gpu_details.append({
                'index': i,
                'name': gpu['name'],
                'vendor': gpu['vendor'],
                'utilization': utilization,
                'power_watts': power,
                'tdp': gpu['tdp'],
                'idle': gpu['idle']
            })
        
        return {
            'total_power_watts': round(total_power, 2),
            'gpu_count': len(self.gpu_info),
            'gpus': gpu_details
        }


# Example usage
if __name__ == "__main__":
    print("=" * 70)
    print("GPU Power Detection System")
    print("=" * 70)

    detector = GPUDetector()
    
    print("\n🎮 Detected GPUs:")
    print("=" * 70)
    
    for i, gpu in enumerate(detector.gpu_info):
        print(f"\nGPU {i}: {gpu['name']}")
        print(f"  Vendor: {gpu['vendor']}")
        print(f"  Category: {gpu['category']}")
        print(f"  TDP: {gpu['tdp']}W | Idle: {gpu['idle']}W")
        print(f"  Monitoring: {gpu['monitoring']}")
        print(f"  Auto-detected: {'✅' if gpu['detected'] else '⚠️  (using fallback)'}")
    
    print("\n" + "=" * 70)
    print("Current GPU Power Consumption")
    print("=" * 70)
    
    gpu_power = detector.get_all_gpus_power()
    
    for gpu_detail in gpu_power['gpus']:
        print(f"\n{gpu_detail['name']}:")
        print(f"  Utilization: {gpu_detail['utilization']:.1f}%")
        print(f"  Power Draw: {gpu_detail['power_watts']}W")
    
    print(f"\n{'=' * 70}")
    print(f"Total GPU Power: {gpu_power['total_power_watts']}W")
    print("=" * 70)

