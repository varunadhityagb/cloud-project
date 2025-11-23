#!/usr/bin/env python3
"""
Cross-Platform GUI for Device Carbon Profiling Agent
Features: Settings, Real-time Logs, System Tray Support
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import queue
import json
import time
from datetime import datetime
from pathlib import Path
import sys

# Import device agent components
from device_agent import DeviceAgent, DeviceConfig, APIEndpointDetector
from cpu_detection import CPUDataManager
from gpu_detection import GPUDataManager
from timezone_utils import now_local, get_timezone_display_name

# System tray support
try:
    import pystray
    from pystray import MenuItem as item
    from PIL import Image, ImageDraw
    TRAY_AVAILABLE = True
except ImportError:
    TRAY_AVAILABLE = False
    print("⚠️  pystray not available. Install with: pip install pystray pillow")


class DeviceAgentGUI:
    """Main GUI Application for Device Agent."""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Carbon Profiling Device Agent")
        self.root.geometry("900x700")
        
        # State variables
        self.is_running = False
        self.agent = None
        self.monitor_thread = None
        self.log_queue = queue.Queue()
        self.tray_icon = None
        self.is_visible = True
        
        # Load configuration
        self.config = DeviceConfig()
        self.settings = self._load_settings()
        
        # Setup GUI
        self._setup_styles()
        self._create_widgets()
        self._populate_current_settings()
        
        # Start log processor
        self._process_log_queue()
        
        # Bind close event
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        
        # Setup system tray if available
        if TRAY_AVAILABLE and self.settings.get('enable_tray', True):
            self._setup_tray()
        
    def _setup_styles(self):
        """Configure ttk styles."""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Configure colors
        style.configure('Title.TLabel', font=('Arial', 14, 'bold'))
        style.configure('Header.TLabel', font=('Arial', 10, 'bold'))
        style.configure('Success.TButton', foreground='green')
        style.configure('Danger.TButton', foreground='red')
        
    def _create_widgets(self):
        """Create all GUI widgets."""
        
        # Main container with notebook (tabs)
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Create tabs
        self.monitoring_frame = ttk.Frame(self.notebook)
        self.settings_frame = ttk.Frame(self.notebook)
        self.hardware_frame = ttk.Frame(self.notebook)
        
        self.notebook.add(self.monitoring_frame, text='📊 Monitoring')
        self.notebook.add(self.settings_frame, text='⚙️ Settings')
        self.notebook.add(self.hardware_frame, text='💻 Hardware Info')
        
        # Build each tab
        self._create_monitoring_tab()
        self._create_settings_tab()
        self._create_hardware_tab()
        
    def _create_monitoring_tab(self):
        """Create monitoring tab with status and logs."""
        
        # Status section
        status_frame = ttk.LabelFrame(self.monitoring_frame, text="Status", padding=10)
        status_frame.pack(fill='x', padx=10, pady=10)
        
        status_grid = ttk.Frame(status_frame)
        status_grid.pack(fill='x')
        
        # Status indicators
        ttk.Label(status_grid, text="Device ID:").grid(row=0, column=0, sticky='w', pady=2)
        self.device_id_label = ttk.Label(status_grid, text="Not initialized")
        self.device_id_label.grid(row=0, column=1, sticky='w', padx=10, pady=2)
        
        ttk.Label(status_grid, text="Status:").grid(row=1, column=0, sticky='w', pady=2)
        self.status_label = ttk.Label(status_grid, text="● Stopped", foreground='red')
        self.status_label.grid(row=1, column=1, sticky='w', padx=10, pady=2)
        
        ttk.Label(status_grid, text="Collections:").grid(row=2, column=0, sticky='w', pady=2)
        self.collection_label = ttk.Label(status_grid, text="0")
        self.collection_label.grid(row=2, column=1, sticky='w', padx=10, pady=2)
        
        ttk.Label(status_grid, text="Success Rate:").grid(row=3, column=0, sticky='w', pady=2)
        self.success_rate_label = ttk.Label(status_grid, text="N/A")
        self.success_rate_label.grid(row=3, column=1, sticky='w', padx=10, pady=2)
        
        # Control buttons
        button_frame = ttk.Frame(self.monitoring_frame)
        button_frame.pack(fill='x', padx=10, pady=5)
        
        self.start_button = ttk.Button(
            button_frame, 
            text="▶ Start Monitoring", 
            command=self._start_monitoring,
            style='Success.TButton'
        )
        self.start_button.pack(side='left', padx=5)
        
        self.stop_button = ttk.Button(
            button_frame, 
            text="⏹ Stop Monitoring", 
            command=self._stop_monitoring,
            state='disabled',
            style='Danger.TButton'
        )
        self.stop_button.pack(side='left', padx=5)
        
        ttk.Button(
            button_frame,
            text="🔄 Test API Connection",
            command=self._test_api
        ).pack(side='left', padx=5)
        
        ttk.Button(
            button_frame,
            text="🗑️ Clear Logs",
            command=self._clear_logs
        ).pack(side='left', padx=5)
        
        if TRAY_AVAILABLE:
            self.minimize_to_tray_button = ttk.Button(
                button_frame,
                text="📥 Minimize to Tray",
                command=self._minimize_to_tray
            )
            self.minimize_to_tray_button.pack(side='left', padx=5)
        
        # Log display
        log_frame = ttk.LabelFrame(self.monitoring_frame, text="Activity Log", padding=10)
        log_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        self.log_text = scrolledtext.ScrolledText(
            log_frame, 
            height=20, 
            wrap=tk.WORD,
            font=('Consolas', 9)
        )
        self.log_text.pack(fill='both', expand=True)
        
        # Configure log text tags for colors
        self.log_text.tag_config('info', foreground='blue')
        self.log_text.tag_config('success', foreground='green')
        self.log_text.tag_config('warning', foreground='orange')
        self.log_text.tag_config('error', foreground='red')
        
    def _create_settings_tab(self):
        """Create settings configuration tab."""
        
        # Create scrollable frame
        canvas = tk.Canvas(self.settings_frame)
        scrollbar = ttk.Scrollbar(self.settings_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # API Settings
        api_frame = ttk.LabelFrame(scrollable_frame, text="API Configuration", padding=10)
        api_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Label(api_frame, text="API Endpoint:").grid(row=0, column=0, sticky='w', pady=5)
        self.api_endpoint_var = tk.StringVar(value=self.settings.get('api_endpoint', ''))
        ttk.Entry(api_frame, textvariable=self.api_endpoint_var, width=50).grid(row=0, column=1, pady=5, padx=5)
        
        ttk.Button(api_frame, text="Auto-Detect", command=self._auto_detect_api).grid(row=0, column=2, padx=5)
        
        self.send_to_api_var = tk.BooleanVar(value=self.settings.get('send_to_api', True))
        ttk.Checkbutton(
            api_frame, 
            text="Enable API Sending", 
            variable=self.send_to_api_var
        ).grid(row=1, column=0, columnspan=2, sticky='w', pady=5)
        
        # Collection Settings
        collection_frame = ttk.LabelFrame(scrollable_frame, text="Collection Settings", padding=10)
        collection_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Label(collection_frame, text="Collection Interval (seconds):").grid(row=0, column=0, sticky='w', pady=5)
        self.interval_var = tk.IntVar(value=self.settings.get('interval', 5))
        ttk.Spinbox(
            collection_frame, 
            from_=1, 
            to=300, 
            textvariable=self.interval_var,
            width=10
        ).grid(row=0, column=1, sticky='w', pady=5, padx=5)
        
        ttk.Label(collection_frame, text="Top Processes to Track:").grid(row=1, column=0, sticky='w', pady=5)
        self.top_processes_var = tk.IntVar(value=self.settings.get('top_processes', 5))
        ttk.Spinbox(
            collection_frame,
            from_=1,
            to=20,
            textvariable=self.top_processes_var,
            width=10
        ).grid(row=1, column=1, sticky='w', pady=5, padx=5)
        
        # Device Settings
        device_frame = ttk.LabelFrame(scrollable_frame, text="Device Configuration", padding=10)
        device_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Label(device_frame, text="Device Type:").grid(row=0, column=0, sticky='w', pady=5)
        self.device_type_var = tk.StringVar(value=self.settings.get('device_type', 'auto'))
        device_combo = ttk.Combobox(
            device_frame,
            textvariable=self.device_type_var,
            values=['auto', 'laptop', 'desktop', 'workstation'],
            state='readonly',
            width=15
        )
        device_combo.grid(row=0, column=1, sticky='w', pady=5, padx=5)
        
        ttk.Button(
            device_frame,
            text="Reset Device ID",
            command=self._reset_device_id
        ).grid(row=1, column=0, columnspan=2, sticky='w', pady=5)
        
        # Database Update Settings
        db_frame = ttk.LabelFrame(scrollable_frame, text="Hardware Database", padding=10)
        db_frame.pack(fill='x', padx=10, pady=10)
        
        self.auto_update_db_var = tk.BooleanVar(value=self.settings.get('auto_update_db', True))
        ttk.Checkbutton(
            db_frame,
            text="Auto-update CPU/GPU database on startup",
            variable=self.auto_update_db_var
        ).grid(row=0, column=0, sticky='w', pady=5)
        
        ttk.Button(
            db_frame,
            text="Update Databases Now",
            command=self._update_databases
        ).grid(row=1, column=0, sticky='w', pady=5)
        
        # System Tray Settings
        if TRAY_AVAILABLE:
            tray_frame = ttk.LabelFrame(scrollable_frame, text="System Tray", padding=10)
            tray_frame.pack(fill='x', padx=10, pady=10)
            
            self.enable_tray_var = tk.BooleanVar(value=self.settings.get('enable_tray', True))
            ttk.Checkbutton(
                tray_frame,
                text="Enable system tray icon",
                variable=self.enable_tray_var
            ).grid(row=0, column=0, sticky='w', pady=5)
            
            self.minimize_on_close_var = tk.BooleanVar(value=self.settings.get('minimize_on_close', False))
            ttk.Checkbutton(
                tray_frame,
                text="Minimize to tray on close (instead of exit)",
                variable=self.minimize_on_close_var
            ).grid(row=1, column=0, sticky='w', pady=5)
            
            self.start_minimized_var = tk.BooleanVar(value=self.settings.get('start_minimized', False))
            ttk.Checkbutton(
                tray_frame,
                text="Start minimized to tray",
                variable=self.start_minimized_var
            ).grid(row=2, column=0, sticky='w', pady=5)
        
        # Save button
        ttk.Button(
            scrollable_frame,
            text="💾 Save Settings",
            command=self._save_settings
        ).pack(pady=20)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
    def _create_hardware_tab(self):
        """Create hardware information display tab."""
        
        # Create scrollable text widget
        text_frame = ttk.Frame(self.hardware_frame)
        text_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        self.hardware_text = scrolledtext.ScrolledText(
            text_frame,
            height=30,
            wrap=tk.WORD,
            font=('Consolas', 9)
        )
        self.hardware_text.pack(fill='both', expand=True)
        
        # Refresh button
        ttk.Button(
            self.hardware_frame,
            text="🔄 Refresh Hardware Info",
            command=self._refresh_hardware_info
        ).pack(pady=10)
        
        # Initial load
        self._refresh_hardware_info()
        
    def _setup_tray(self):
        """Setup system tray icon."""
        if not TRAY_AVAILABLE:
            return
        
        # Create icon image
        def create_icon_image():
            # Create a simple icon
            width = 64
            height = 64
            color1 = (76, 175, 80)  # Green
            color2 = (33, 150, 243)  # Blue
            
            image = Image.new('RGB', (width, height), color1)
            draw = ImageDraw.Draw(image)
            
            # Draw a leaf-like shape for carbon/eco theme
            draw.ellipse([10, 10, 54, 54], fill=color2)
            draw.rectangle([20, 25, 44, 45], fill=color1)
            
            return image
        
        icon_image = create_icon_image()
        
        # Create menu
        menu = pystray.Menu(
            item('Show Window', self._show_window, default=True),
            item('Start Monitoring', self._start_monitoring_tray, enabled=lambda item: not self.is_running),
            item('Stop Monitoring', self._stop_monitoring_tray, enabled=lambda item: self.is_running),
            pystray.Menu.SEPARATOR,
            item('Exit', self._quit_from_tray)
        )
        
        # Create tray icon
        self.tray_icon = pystray.Icon(
            "carbon_agent",
            icon_image,
            "Carbon Profiling Agent",
            menu
        )
        
        # Run tray icon in separate thread
        tray_thread = threading.Thread(target=self.tray_icon.run, daemon=True)
        tray_thread.start()
        
        self._log("System tray icon enabled", 'success')
        
        # Start minimized if configured
        if self.settings.get('start_minimized', False):
            self.root.after(500, self._minimize_to_tray)
    
    def _show_window(self, icon=None, item=None):
        """Show the main window."""
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()
        self.is_visible = True
    
    def _minimize_to_tray(self):
        """Minimize window to system tray."""
        if TRAY_AVAILABLE and self.tray_icon:
            self.root.withdraw()
            self.is_visible = False
            self._log("Minimized to system tray", 'info')
        else:
            messagebox.showinfo("Not Available", "System tray is not available")
    
    def _start_monitoring_tray(self, icon=None, item=None):
        """Start monitoring from tray."""
        self.root.after(0, self._start_monitoring)
    
    def _stop_monitoring_tray(self, icon=None, item=None):
        """Stop monitoring from tray."""
        self.root.after(0, self._stop_monitoring)
    
    def _quit_from_tray(self, icon=None, item=None):
        """Quit application from tray."""
        if self.tray_icon:
            self.tray_icon.stop()
        self.root.after(0, self._force_quit)
    
    def _force_quit(self):
        """Force quit the application."""
        if self.is_running:
            self._stop_monitoring()
            time.sleep(0.5)
        self.root.quit()
        self.root.destroy()
    
    def _load_settings(self) -> dict:
        """Load settings from file."""
        settings_file = Path("agent_gui_settings.json")
        
        if settings_file.exists():
            try:
                with open(settings_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        
        # Default settings
        return {
            'api_endpoint': APIEndpointDetector.get_endpoint(),
            'send_to_api': True,
            'interval': 5,
            'top_processes': 5,
            'device_type': 'auto',
            'auto_update_db': True,
            'enable_tray': True,
            'minimize_on_close': False,
            'start_minimized': False
        }
    
    def _save_settings(self):
        """Save current settings to file."""
        self.settings = {
            'api_endpoint': self.api_endpoint_var.get(),
            'send_to_api': self.send_to_api_var.get(),
            'interval': self.interval_var.get(),
            'top_processes': self.top_processes_var.get(),
            'device_type': self.device_type_var.get(),
            'auto_update_db': self.auto_update_db_var.get()
        }
        
        # Add tray settings if available
        if TRAY_AVAILABLE:
            self.settings['enable_tray'] = self.enable_tray_var.get()
            self.settings['minimize_on_close'] = self.minimize_on_close_var.get()
            self.settings['start_minimized'] = self.start_minimized_var.get()
        
        try:
            with open("agent_gui_settings.json", 'w') as f:
                json.dump(self.settings, f, indent=2)
            
            self._log("Settings saved successfully", 'success')
            messagebox.showinfo("Success", "Settings saved successfully!\nRestart the app for tray settings to take effect.")
        except Exception as e:
            self._log(f"Error saving settings: {e}", 'error')
            messagebox.showerror("Error", f"Failed to save settings: {e}")
    
    def _populate_current_settings(self):
        """Populate GUI with current settings."""
        self.device_id_label.config(text=self.config.get_device_id())
    
    def _auto_detect_api(self):
        """Auto-detect API endpoint."""
        self._log("Auto-detecting API endpoint...", 'info')
        endpoint = APIEndpointDetector.detect_minikube_endpoint()
        
        if endpoint:
            self.api_endpoint_var.set(endpoint)
            self._log(f"Detected endpoint: {endpoint}", 'success')
        else:
            self._log("Could not auto-detect endpoint", 'warning')
            messagebox.showwarning("Not Found", "Could not auto-detect API endpoint. Please enter manually.")
    
    def _test_api(self):
        """Test API connection."""
        endpoint = self.api_endpoint_var.get()
        
        if not endpoint:
            messagebox.showerror("Error", "Please set API endpoint first")
            return
        
        self._log(f"Testing connection to {endpoint}...", 'info')
        
        # Test in separate thread
        def test():
            try:
                import requests
                response = requests.get(f"{endpoint}/health", timeout=5)
                
                if response.status_code == 200:
                    self.log_queue.put(("API connection successful! ✓", 'success'))
                    self.root.after(0, lambda: messagebox.showinfo("Success", "API connection successful!"))
                else:
                    self.log_queue.put((f"API returned status {response.status_code}", 'warning'))
                    self.root.after(0, lambda: messagebox.showwarning("Warning", f"API returned status {response.status_code}"))
            except Exception as e:
                self.log_queue.put((f"Connection failed: {e}", 'error'))
                self.root.after(0, lambda: messagebox.showerror("Error", f"Connection failed: {e}"))
        
        threading.Thread(target=test, daemon=True).start()
    
    def _reset_device_id(self):
        """Reset device ID."""
        result = messagebox.askyesno(
            "Confirm Reset",
            "This will create a new device ID. The old device will remain in the database but won't receive new metrics.\n\nContinue?"
        )
        
        if result:
            # Delete config file
            config_file = Path(".device_config.json")
            if config_file.exists():
                config_file.unlink()
            
            # Reload config
            self.config = DeviceConfig()
            self.device_id_label.config(text=self.config.get_device_id())
            
            self._log(f"New device ID: {self.config.get_device_id()}", 'success')
            messagebox.showinfo("Success", f"New device ID created: {self.config.get_device_id()}")
    
    def _update_databases(self):
        """Update CPU and GPU databases."""
        self._log("Updating hardware databases...", 'info')
        
        def update():
            try:
                # Update CPU database
                self.log_queue.put(("Updating CPU database...", 'info'))
                cpu_mgr = CPUDataManager(auto_update=True)
                self.log_queue.put(("CPU database updated", 'success'))
                
                # Update GPU database
                self.log_queue.put(("Updating GPU database...", 'info'))
                gpu_mgr = GPUDataManager(auto_update=True)
                self.log_queue.put(("GPU database updated", 'success'))
                
                self.log_queue.put(("All databases updated successfully!", 'success'))
            except Exception as e:
                self.log_queue.put((f"Database update failed: {e}", 'error'))
        
        threading.Thread(target=update, daemon=True).start()
    
    def _refresh_hardware_info(self):
        """Refresh hardware information display."""
        self.hardware_text.delete(1.0, tk.END)
        
        def load_hardware():
            try:
                from cpu_detection import CPUDetector
                from gpu_detection import GPUDetector
                
                info = []
                info.append("=" * 70)
                info.append("HARDWARE INFORMATION")
                info.append("=" * 70)
                info.append("")
                
                # Device info
                info.append(f"Device ID: {self.config.get_device_id()}")
                info.append(f"Device Type: {self.config.get_device_type() or 'Auto-detected'}")
                info.append(f"Timezone: {get_timezone_display_name()}")
                info.append("")
                
                # CPU info
                info.append("CPU INFORMATION")
                info.append("-" * 70)
                cpu_detector = CPUDetector()
                profile = cpu_detector.get_power_profile()
                
                info.append(f"Model: {profile['cpu_model']}")
                info.append(f"Cores: {profile['cpu_cores']}")
                info.append(f"Category: {profile['category']}")
                info.append(f"TDP: {profile['tdp_watts']}W")
                info.append(f"Idle Power: {profile['idle_watts']}W")
                info.append(f"Detection: {'Database match' if profile['auto_detected'] else 'Using defaults'}")
                info.append(f"Source: {profile['data_source']}")
                info.append("")
                
                # GPU info
                info.append("GPU INFORMATION")
                info.append("-" * 70)
                gpu_detector = GPUDetector()
                
                if gpu_detector.gpu_info:
                    for i, gpu in enumerate(gpu_detector.gpu_info):
                        info.append(f"GPU {i}: {gpu['name']}")
                        info.append(f"  Vendor: {gpu['vendor']}")
                        info.append(f"  Category: {gpu['category']}")
                        info.append(f"  TDP: {gpu['tdp']}W")
                        info.append(f"  Idle Power: {gpu['idle']}W")
                        info.append(f"  Monitoring: {gpu['monitoring']}")
                        info.append(f"  Detection: {'Database match' if gpu['detected'] else 'Using defaults'}")
                        info.append("")
                else:
                    info.append("No dedicated GPU detected")
                    info.append("")
                
                info.append("=" * 70)
                
                # Update GUI
                text = "\n".join(info)
                self.root.after(0, lambda: self.hardware_text.insert(1.0, text))
                
            except Exception as e:
                error_text = f"Error loading hardware info: {e}"
                self.root.after(0, lambda: self.hardware_text.insert(1.0, error_text))
        
        threading.Thread(target=load_hardware, daemon=True).start()
    
    def _start_monitoring(self):
        """Start monitoring in separate thread."""
        if self.is_running:
            return
        
        # Validate settings
        if self.send_to_api_var.get() and not self.api_endpoint_var.get():
            messagebox.showerror("Error", "Please set API endpoint or disable API sending")
            return
        
        self.is_running = True
        self.start_button.config(state='disabled')
        self.stop_button.config(state='normal')
        self.status_label.config(text="● Running", foreground='green')
        
        # Update tray icon tooltip if available
        if TRAY_AVAILABLE and self.tray_icon:
            self.tray_icon.title = "Carbon Profiling Agent - Running"
        
        self._log("=" * 70, 'info')
        self._log("Starting Device Agent Monitor", 'info')
        self._log("=" * 70, 'info')
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
    
    def _stop_monitoring(self):
        """Stop monitoring."""
        if not self.is_running:
            return
        
        self.is_running = False
        self.start_button.config(state='normal')
        self.stop_button.config(state='disabled')
        self.status_label.config(text="● Stopped", foreground='red')
        
        # Update tray icon tooltip if available
        if TRAY_AVAILABLE and self.tray_icon:
            self.tray_icon.title = "Carbon Profiling Agent - Stopped"
        
        self._log("=" * 70, 'info')
        self._log("Monitoring stopped by user", 'info')
        self._log("=" * 70, 'info')
    
    def _monitor_loop(self):
        """Main monitoring loop."""
        collection_count = 0
        success_count = 0
        fail_count = 0
        
        try:
            # Initialize agent
            self.agent = DeviceAgent(
                device_id=self.config.get_device_id(),
                api_endpoint=self.api_endpoint_var.get()
            )
            
            interval = self.interval_var.get()
            send_to_api = self.send_to_api_var.get()
            
            self.log_queue.put((f"Collection interval: {interval}s", 'info'))
            self.log_queue.put((f"API sending: {'Enabled' if send_to_api else 'Disabled'}", 'info'))
            self.log_queue.put(("", 'info'))
            
            while self.is_running:
                collection_count += 1
                
                # Collect metrics
                metrics = self.agent.collect_metrics()
                sys_metrics = metrics['system_metrics']
                
                # Log collection
                timestamp = now_local().strftime('%H:%M:%S')
                log_msg = (
                    f"[{collection_count}] {timestamp} | "
                    f"CPU: {sys_metrics['cpu_percent']:.1f}% ({sys_metrics['cpu_power_watts']:.2f}W) | "
                    f"GPU: {sys_metrics['gpu_power_watts']:.2f}W | "
                    f"Total: {sys_metrics['total_power_watts']:.2f}W"
                )
                self.log_queue.put((log_msg, 'info'))
                
                # Send to API if enabled
                if send_to_api:
                    success = self.agent.send_to_api(metrics)
                    if success:
                        success_count += 1
                        self.log_queue.put(("  ✓ API send successful", 'success'))
                    else:
                        fail_count += 1
                        self.log_queue.put(("  ✗ API send failed", 'error'))
                
                # Update stats
                success_rate = (success_count / collection_count * 100) if collection_count > 0 else 0
                self.root.after(0, lambda: self._update_stats(collection_count, success_rate))
                
                # Wait for next interval
                for _ in range(interval * 10):  # Check every 100ms
                    if not self.is_running:
                        break
                    time.sleep(0.1)
            
            # Final stats
            self.log_queue.put(("", 'info'))
            self.log_queue.put((f"Final Statistics:", 'info'))
            self.log_queue.put((f"  Total Collections: {collection_count}", 'info'))
            if send_to_api:
                self.log_queue.put((f"  Successful: {success_count}", 'success'))
                self.log_queue.put((f"  Failed: {fail_count}", 'error'))
                self.log_queue.put((f"  Success Rate: {success_rate:.1f}%", 'info'))
            
        except Exception as e:
            self.log_queue.put((f"Error in monitor loop: {e}", 'error'))
            self.root.after(0, self._stop_monitoring)
    
    def _update_stats(self, collections, success_rate):
        """Update statistics display."""
        self.collection_label.config(text=str(collections))
        self.success_rate_label.config(text=f"{success_rate:.1f}%")
    
    def _log(self, message, level='info'):
        """Add message to log."""
        self.log_queue.put((message, level))
    
    def _process_log_queue(self):
        """Process log queue and update GUI."""
        try:
            while True:
                message, level = self.log_queue.get_nowait()
                
                # Add timestamp for non-empty messages
                if message.strip():
                    timestamp = now_local().strftime('%H:%M:%S')
                    full_message = f"[{timestamp}] {message}\n"
                else:
                    full_message = "\n"
                
                # Insert with appropriate tag
                self.log_text.insert(tk.END, full_message, level)
                self.log_text.see(tk.END)
                
        except queue.Empty:
            pass
        
        # Schedule next check
        self.root.after(100, self._process_log_queue)
    
    def _clear_logs(self):
        """Clear log display."""
        self.log_text.delete(1.0, tk.END)
        self._log("Logs cleared", 'info')
    
    def _on_closing(self):
        """Handle window close event."""
        # Check if minimize on close is enabled
        if TRAY_AVAILABLE and self.settings.get('minimize_on_close', False) and self.tray_icon:
            self._minimize_to_tray()
            return
        
        if self.is_running:
            result = messagebox.askyesno(
                "Confirm Exit",
                "Monitoring is still running. Are you sure you want to exit?"
            )
            if not result:
                return
            
            self._stop_monitoring()
            time.sleep(0.5)  # Give thread time to stop
        
        # Stop tray icon
        if TRAY_AVAILABLE and self.tray_icon:
            self.tray_icon.stop()
        
        self.root.destroy()


def main():
    """Main entry point."""
    root = tk.Tk()
    app = DeviceAgentGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
