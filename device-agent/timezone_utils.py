import os
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional


class TimezoneDetector:
    """Detect system timezone across different platforms."""

    _cached_timezone = None
    _cached_timezone_name = None

    @classmethod
    def get_system_timezone(cls) -> ZoneInfo:
        """
        Detect system timezone dynamically.

        Returns:
            ZoneInfo object for system timezone
        """
        if cls._cached_timezone is not None:
            return cls._cached_timezone

        tz_name = cls.get_system_timezone_name()

        try:
            cls._cached_timezone = ZoneInfo(tz_name)
            print(f"Using system timezone: {tz_name}")
            return cls._cached_timezone
        except Exception as e:
            print(f"  Could not load timezone '{tz_name}': {e}")
            print("   Falling back to UTC")
            cls._cached_timezone = ZoneInfo("UTC")
            cls._cached_timezone_name = "UTC"
            return cls._cached_timezone

    @classmethod
    def get_system_timezone_name(cls) -> str:
        """
        Get system timezone name (e.g., 'Asia/Kolkata', 'America/New_York').

        Returns:
            Timezone name string
        """
        if cls._cached_timezone_name is not None:
            return cls._cached_timezone_name

        tz_name = None
        import os

        # Method 1: Check TZ environment variable
        tz_name = os.environ.get('TZ')
        if tz_name:
            cls._cached_timezone_name = tz_name
            return tz_name

        # Method 2: Read /etc/timezone (Debian/Ubuntu)
        try:
            with open('/etc/timezone', 'r') as f:
                tz_name = f.read().strip()
                if tz_name:
                    cls._cached_timezone_name = tz_name
                    return tz_name
        except (FileNotFoundError, PermissionError):
            pass

        # Method 3: Read /etc/localtime symlink (most Linux/Unix)
        try:
            import os.path
            localtime_path = '/etc/localtime'
            if os.path.islink(localtime_path):
                link_target = os.readlink(localtime_path)
                # Extract timezone from path like /usr/share/zoneinfo/Asia/Kolkata
                if 'zoneinfo/' in link_target:
                    tz_name = link_target.split('zoneinfo/')[-1]
                    cls._cached_timezone_name = tz_name
                    return tz_name
        except (OSError, FileNotFoundError):
            pass

        # Method 4: Use timedatectl (systemd-based Linux)
        try:
            result = subprocess.run(
                ['timedatectl', 'show', '--property=Timezone', '--value'],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                tz_name = result.stdout.strip()
                if tz_name and tz_name != 'n/a':
                    cls._cached_timezone_name = tz_name
                    return tz_name
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

        # Method 5: macOS
        try:
            result = subprocess.run(
                ['systemsetup', '-gettimezone'],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                # Output: "Time Zone: Asia/Kolkata"
                output = result.stdout.strip()
                if 'Time Zone:' in output:
                    tz_name = output.split('Time Zone:')[-1].strip()
                    cls._cached_timezone_name = tz_name
                    return tz_name
        except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError):
            pass

        # Method 6: Windows (using PowerShell)
        try:
            result = subprocess.run(
                ['powershell', '-Command', '[System.TimeZoneInfo]::Local.Id'],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                windows_tz = result.stdout.strip()
                # Convert Windows timezone to IANA format
                tz_name = cls._windows_to_iana(windows_tz)
                cls._cached_timezone_name = tz_name
                return tz_name
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

        # Fallback: Use UTC
        print(" Could not detect system timezone, using UTC")
        cls._cached_timezone_name = "UTC"
        return "UTC"

    @staticmethod
    def _windows_to_iana(windows_tz: str) -> str:
        """
        Convert Windows timezone name to IANA timezone name.
        This is a simplified mapping - for production use a comprehensive library.
        """
        mapping = {
            'India Standard Time': 'Asia/Kolkata',
            'Eastern Standard Time': 'America/New_York',
            'Pacific Standard Time': 'America/Los_Angeles',
            'Central Standard Time': 'America/Chicago',
            'GMT Standard Time': 'Europe/London',
            'Central European Standard Time': 'Europe/Paris',
            'China Standard Time': 'Asia/Shanghai',
            'Tokyo Standard Time': 'Asia/Tokyo',
            'AUS Eastern Standard Time': 'Australia/Sydney',
        }

        return mapping.get(windows_tz, 'UTC')

    @classmethod
    def get_current_time(cls) -> datetime:
        """Get current time in system timezone."""
        tz = cls.get_system_timezone()
        return datetime.now(tz)

    @classmethod
    def get_timezone_info(cls) -> dict:
        """Get detailed timezone information."""
        tz_name = cls.get_system_timezone_name()
        tz = cls.get_system_timezone()
        now = cls.get_current_time()

        return {
            'timezone_name': tz_name,
            'timezone': str(tz),
            'current_time': now.isoformat(),
            'utc_offset': now.strftime('%z'),
            'utc_offset_hours': now.utcoffset().total_seconds() / 3600
        }


# Convenience functions for easy use
def get_local_timezone() -> ZoneInfo:
    """Get system timezone as ZoneInfo object."""
    return TimezoneDetector.get_system_timezone()


def get_local_timezone_name() -> str:
    """Get system timezone name (e.g., 'Asia/Kolkata')."""
    return TimezoneDetector.get_system_timezone_name()


def now_local() -> datetime:
    """Get current time in local timezone."""
    return TimezoneDetector.get_current_time()


def get_timezone_display_name() -> str:
    """Get a human-readable timezone display name."""
    tz_name = get_local_timezone_name()
    now = now_local()
    offset = now.strftime('%z')

    # Format like: "Asia/Kolkata (UTC+05:30)"
    return f"{tz_name} (UTC{offset[:3]}:{offset[3:]})"


# Test/Demo function
if __name__ == "__main__":
    print("=" * 70)
    print("Timezone Detection Test")
    print("=" * 70)

    info = TimezoneDetector.get_timezone_info()

    print(f"\n Detected Timezone Information:")
    print(f"   Name: {info['timezone_name']}")
    print(f"   UTC Offset: {info['utc_offset']}")
    print(f"   Current Time: {info['current_time']}")

    print(f"\n Local Time: {now_local().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" Display Name: {get_timezone_display_name()}")

    print("\n" + "=" * 70)

    # Test timezone-aware operations
    print("\n Timezone-Aware Timestamp Examples:")
    print(f"  ISO Format: {now_local().isoformat()}")
    print(f"  Readable: {now_local().strftime('%B %d, %Y at %I:%M:%S %p')}")
    print(f"  With TZ: {now_local().strftime('%Y-%m-%d %H:%M:%S %Z')}")

    print("\n" + "=" * 70)
