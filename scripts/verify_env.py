#!/usr/bin/env python3
"""
Verification script for ThetaData environment configuration.
This script checks the current ThetaTerminal configuration and validates connectivity.
"""

import subprocess
import sys
from pathlib import Path

CONFIG_FILE = Path("/home/ashler/ThetaData/ThetaTerminal/config_0.properties")


def read_config():
    """Read and parse the ThetaTerminal configuration."""
    if not CONFIG_FILE.exists():
        print(f"❌ Config file not found: {CONFIG_FILE}")
        return None

    config = {}
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    config[key.strip()] = value.strip()
        return config
    except Exception as e:
        print(f"❌ Error reading config file: {e}")
        return None


def get_environment_info(config):
    """Determine the current environment based on FPSS_REGION."""
    fpss_region = config.get("FPSS_REGION", "")
    mdds_region = config.get("MDDS_REGION", "")

    environments = {
        "FPSS_NJ_HOSTS": {
            "name": "PRODUCTION",
            "description": "Stable production servers",
            "suitable_for": "Live trading, production systems",
            "data_type": "Live market data (market hours only)",
        },
        "FPSS_DEV_HOSTS": {
            "name": "DEVELOPMENT",
            "description": "Development servers with historical replay",
            "suitable_for": "Development, testing outside market hours",
            "data_type": "Historical data replay (available 24/7)",
        },
        "FPSS_STAGE_HOSTS": {
            "name": "STAGING",
            "description": "Testing servers with occasional reboots",
            "suitable_for": "Pre-production testing",
            "data_type": "Test data with occasional interruptions",
        },
    }

    env_info = environments.get(
        fpss_region,
        {
            "name": "UNKNOWN",
            "description": "Unrecognized configuration",
            "suitable_for": "Unknown",
            "data_type": "Unknown",
        },
    )

    return {"fpss_region": fpss_region, "mdds_region": mdds_region, **env_info}


def check_terminal_process():
    """Check if ThetaTerminal is currently running."""
    try:
        result = subprocess.run(["pgrep", "-f", "ThetaTerminal.jar"], capture_output=True, text=True)
        if result.returncode == 0:
            pids = result.stdout.strip().split("\n")
            return True, pids
        else:
            return False, []
    except Exception:
        return False, []


def test_connectivity(config):
    """Test basic connectivity to the configured endpoints."""
    import socket

    fpss_hosts = config.get(config.get("FPSS_REGION", ""), "")
    mdds_hosts = config.get(config.get("MDDS_REGION", ""), "")

    results = {"fpss": [], "mdds": []}

    # Test FPSS hosts
    if fpss_hosts:
        for host_port in fpss_hosts.split(","):
            host_port = host_port.strip()
            if ":" in host_port:
                host, port = host_port.split(":")
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(3)
                    result = sock.connect_ex((host, int(port)))
                    sock.close()
                    results["fpss"].append({"host": host_port, "reachable": result == 0})
                except Exception as e:
                    results["fpss"].append({"host": host_port, "reachable": False, "error": str(e)})

    return results


def main():
    """Main verification function."""
    print("ThetaData Environment Verification")
    print("=" * 50)
    print()

    # Read configuration
    config = read_config()
    if not config:
        sys.exit(1)

    # Get environment info
    env_info = get_environment_info(config)

    # Display current configuration
    print("📋 Current Configuration:")
    print(f"   Environment: {env_info['name']}")
    print(f"   FPSS Region: {env_info['fpss_region']}")
    print(f"   MDDS Region: {env_info['mdds_region']}")
    print(f"   Description: {env_info['description']}")
    print(f"   Suitable for: {env_info['suitable_for']}")
    print(f"   Data type: {env_info['data_type']}")
    print()

    # Check if terminal is running
    is_running, pids = check_terminal_process()
    print("🔍 Terminal Status:")
    if is_running:
        print(f"   ✅ ThetaTerminal is running (PIDs: {', '.join(pids)})")
    else:
        print("   ❌ ThetaTerminal is not running")
    print()

    # HTTP server info
    http_port = config.get("HTTP_PORT", "25510")
    ws_port = config.get("WS_PORT", "25520")
    print("🌐 Local Server Ports:")
    print(f"   HTTP API: http://127.0.0.1:{http_port}")
    print(f"   WebSocket: ws://127.0.0.1:{ws_port}")
    print()

    # Environment-specific recommendations
    print("💡 Recommendations:")
    if env_info["name"] == "DEVELOPMENT":
        print("   ✅ Perfect for development outside market hours")
        print("   ✅ Historical data replay available 24/7")
        print("   ⚠️  Expect frequent server reboots (normal)")
    elif env_info["name"] == "PRODUCTION":
        print("   ✅ Stable for production use")
        print("   ✅ Live data during market hours")
        print("   ⚠️  No data outside market hours")
    elif env_info["name"] == "STAGING":
        print("   ✅ Good for testing")
        print("   ⚠️  Occasional reboots expected")

    print()
    print("🔧 To switch environments:")
    print("   Development: ./set_env.sh --dev")
    print("   Production:  ./set_env.sh --prod")
    print("   Staging:     ./set_env.sh --stage")


if __name__ == "__main__":
    main()
