"""
Backend Timezone Configuration
Reads timezone from environment or uses system default
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional


class BackendTimezone:
    """Manage timezone for backend services (API, Worker)."""

    _timezone = None
    _timezone_name = None

    @classmethod
    def get_timezone(cls) -> ZoneInfo:
        """
        Get timezone for backend operations.
        Priority:
        1. APP_TIMEZONE environment variable
        2. TZ environment variable
        3. UTC (safe default for servers)
        """
        if cls._timezone is not None:
            return cls._timezone

        tz_name = cls.get_timezone_name()

        try:
            cls._timezone = ZoneInfo(tz_name)
            return cls._timezone
        except Exception as e:
            print(f"⚠️  Invalid timezone '{tz_name}', using UTC: {e}")
            cls._timezone = ZoneInfo("UTC")
            cls._timezone_name = "UTC"
            return cls._timezone

    @classmethod
    def get_timezone_name(cls) -> str:
        """Get timezone name from environment."""
        if cls._timezone_name is not None:
            return cls._timezone_name

        # Priority 1: APP_TIMEZONE (our custom variable)
        tz_name = os.environ.get('APP_TIMEZONE')
        if tz_name:
            cls._timezone_name = tz_name
            return tz_name

        # Priority 2: TZ (standard Unix variable)
        tz_name = os.environ.get('TZ')
        if tz_name:
            cls._timezone_name = tz_name
            return tz_name

        # Default: UTC (safe for servers)
        cls._timezone_name = "UTC"
        return "UTC"

    @classmethod
    def now(cls) -> datetime:
        """Get current time in configured timezone."""
        return datetime.now(cls.get_timezone())

    @classmethod
    def get_postgres_timezone(cls) -> str:
        """Get PostgreSQL-compatible timezone string."""
        return cls.get_timezone_name()


# Convenience functions
def get_tz() -> ZoneInfo:
    """Get configured timezone."""
    return BackendTimezone.get_timezone()


def get_tz_name() -> str:
    """Get timezone name."""
    return BackendTimezone.get_timezone_name()


def now_tz() -> datetime:
    """Get current time in configured timezone."""
    return BackendTimezone.now()


def get_display_name() -> str:
    """Get display name with UTC offset."""
    tz_name = get_tz_name()
    now = now_tz()
    offset = now.strftime('%z')
    return f"{tz_name} (UTC{offset[:3]}:{offset[3:]})"


if __name__ == "__main__":
    print("=" * 60)
    print("Backend Timezone Configuration")
    print("=" * 60)

    print(f"\nConfigured Timezone: {get_tz_name()}")
    print(f"Display Name: {get_display_name()}")
    print(f"Current Time: {now_tz().isoformat()}")
    print(f"PostgreSQL TZ: {BackendTimezone.get_postgres_timezone()}")

    print("\n" + "=" * 60)
    print("Environment Variables:")
    print(f"  APP_TIMEZONE: {os.environ.get('APP_TIMEZONE', '(not set)')}")
    print(f"  TZ: {os.environ.get('TZ', '(not set)')}")
    print("\n💡 Set APP_TIMEZONE or TZ to change timezone")
    print("   Example: export APP_TIMEZONE=Asia/Kolkata")
    print("=" * 60)
