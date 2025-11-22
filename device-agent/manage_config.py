#!/usr/bin/env python3
"""
Device Configuration Manager
Utility to view, edit, and reset device configuration
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone

CONFIG_FILE = ".device_config.json"

def load_config():
    """Load device configuration."""
    config_path = Path(CONFIG_FILE)
    if not config_path.exists():
        return None

    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return None

def save_config(config):
    """Save device configuration."""
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        print(f"✅ Configuration saved to {CONFIG_FILE}")
        return True
    except Exception as e:
        print(f"❌ Error saving config: {e}")
        return False

def show_config():
    """Display current configuration."""
    config = load_config()

    if not config:
        print("❌ No configuration file found")
        print(f"💡 Run the device agent once to create: python device_agent.py")
        return

    print("\n" + "="*60)
    print("📱 Current Device Configuration")
    print("="*60)
    print(f"Device ID:    {config.get('device_id', 'N/A')}")
    print(f"Device Type:  {config.get('device_type', 'N/A')}")
    print(f"Created At:   {config.get('created_at', 'N/A')}")
    print("="*60 + "\n")

def set_device_type():
    """Interactive device type selection."""
    config = load_config()

    if not config:
        print("❌ No configuration file found")
        print(f"💡 Run the device agent once to create: python device_agent.py")
        return

    print("\n" + "="*60)
    print("🔧 Set Device Type")
    print("="*60)
    print("Available types:")
    print("  1. laptop (TDP: 45W, Idle: 8W)")
    print("  2. desktop (TDP: 95W, Idle: 25W)")
    print("  3. workstation (TDP: 150W, Idle: 40W)")
    print()

    choice = input("Select device type (1-3): ").strip()

    type_map = {
        '1': 'laptop',
        '2': 'desktop',
        '3': 'workstation'
    }

    device_type = type_map.get(choice)

    if device_type:
        config['device_type'] = device_type
        config['updated_at'] = datetime.now(timezone.utc).isoformat()
        if save_config(config):
            print(f"✅ Device type set to: {device_type}")
    else:
        print("❌ Invalid choice")

def reset_device_id():
    """Reset device ID (creates new device in database)."""
    config = load_config()

    if not config:
        print("❌ No configuration file found")
        return

    print("\n" + "="*60)
    print("⚠️  WARNING: Reset Device ID")
    print("="*60)
    print(f"Current Device ID: {config.get('device_id')}")
    print()
    print("This will create a NEW device ID. The old device data")
    print("will remain in the database but won't receive new metrics.")
    print()

    confirm = input("Are you sure? Type 'yes' to confirm: ").strip().lower()

    if confirm == 'yes':
        import random
        import platform
        import hashlib

        # Generate new ID
        system_info = f"{platform.node()}-{platform.machine()}-{random.randint(1000, 9999)}"
        hash_obj = hashlib.md5(system_info.encode())
        new_id = f"device_{hash_obj.hexdigest()[:8]}"

        config['device_id'] = new_id
        config['created_at'] = datetime.now(timezone.utc).isoformat()
        config['updated_at'] = datetime.now(timezone.utc).isoformat()

        if save_config(config):
            print(f"✅ New Device ID: {new_id}")
    else:
        print("❌ Reset cancelled")

def delete_config():
    """Delete configuration file."""
    config_path = Path(CONFIG_FILE)

    if not config_path.exists():
        print("❌ No configuration file found")
        return

    print("\n" + "="*60)
    print("⚠️  WARNING: Delete Configuration")
    print("="*60)
    print("This will delete the device configuration file.")
    print("A new device ID will be generated on next run.")
    print()

    confirm = input("Are you sure? Type 'yes' to confirm: ").strip().lower()

    if confirm == 'yes':
        try:
            config_path.unlink()
            print(f"✅ Configuration file deleted")
        except Exception as e:
            print(f"❌ Error deleting file: {e}")
    else:
        print("❌ Deletion cancelled")

def print_help():
    """Print help message."""
    print("""
Usage: python manage_config.py [command]

Commands:
  show        Display current device configuration (default)
  type        Set device type (laptop/desktop/workstation)
  reset       Reset device ID (creates new device)
  delete      Delete configuration file
  help        Show this help message

Examples:
  python manage_config.py              # Show current config
  python manage_config.py type         # Change device type
  python manage_config.py reset        # Get new device ID
  python manage_config.py delete       # Start fresh
    """)

def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        command = 'show'
    else:
        command = sys.argv[1].lower()

    commands = {
        'show': show_config,
        'type': set_device_type,
        'reset': reset_device_id,
        'delete': delete_config,
        'help': print_help
    }

    func = commands.get(command)

    if func:
        func()
    else:
        print(f"❌ Unknown command: {command}")
        print_help()

if __name__ == "__main__":
    main()
