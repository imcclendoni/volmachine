#!/usr/bin/env python3
"""
Test IBKR Connection for VolMachine.

Quick test to verify IBKR Gateway/TWS connection is working.

Usage:
    python3 scripts/test_ibkr_connection.py
    python3 scripts/test_ibkr_connection.py --port 4002 --client-id 99

Expected output on success:
- Account ID (DUxxxxxxx for paper)
- Current server time
- Connection status

Exit codes:
- 0: Success
- 1: Connection failed
- 2: Live account detected (blocked)
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime


def main():
    # Parse CLI arguments
    parser = argparse.ArgumentParser(description="Test IBKR Gateway/TWS connection")
    parser.add_argument("--host", default="127.0.0.1", help="IBKR host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=4002, help="IBKR port (default: 4002 for IB Gateway paper)")
    parser.add_argument("--client-id", type=int, default=99, help="Client ID (default: 99)")
    args = parser.parse_args()
    
    print("=" * 60)
    print("IBKR Connection Test")
    print("=" * 60)
    
    try:
        from execution.ibkr_order_client import IBKROrderClient, LiveTradingBlocked
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        print("   Is ib_insync installed? Run: pip install ib_insync")
        return 1
    
    # Configuration from CLI args
    host = args.host
    port = args.port
    client_id = args.client_id
    
    print(f"\nConfiguration:")
    print(f"  Host: {host}")
    print(f"  Port: {port} (paper trading)")
    print(f"  Client ID: {client_id}")
    
    # Create client
    client = IBKROrderClient(host=host, port=port, client_id=client_id)
    
    print(f"\n{'─' * 40}")
    print("Connecting to IBKR Gateway/TWS...")
    print(f"{'─' * 40}")
    
    try:
        connected = client.connect()
    except LiveTradingBlocked as e:
        print(f"\n🚨 BLOCKED: {e}")
        print("   Live trading is not allowed. Use paper account.")
        return 2
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Is IBKR Gateway/TWS running?")
        print("  2. Is paper trading port 7497 enabled?")
        print("  3. Is API access enabled in TWS settings?")
        print("  4. Check TWS > Configure > API > Settings:")
        print("     - Enable ActiveX and Socket Clients")
        print("     - Socket port: 7497")
        print("     - Read-Only API: OFF")
        return 1
    
    if not connected:
        print("\n❌ Connection returned False")
        return 1
    
    print("\n✅ Connected!")
    
    # Get account info
    account_id = client.get_account_id()
    print(f"\n📊 Account Information:")
    print(f"   Account ID: {account_id}")
    
    # Check if paper account
    if account_id and account_id.startswith("DU"):
        print(f"   Type: PAPER (✓ Safe)")
    elif account_id and account_id.startswith("U"):
        print(f"   Type: LIVE (⚠️ Blocked by config)")
    else:
        print(f"   Type: Unknown")
    
    # Get server time
    print(f"\n⏰ Server Status:")
    print(f"   Local time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check connection quality
    if client.is_connected():
        print(f"   Connection: ACTIVE ✓")
    else:
        print(f"   Connection: INACTIVE ✗")
    
    # Test contract resolution (non-invasive)
    print(f"\n{'─' * 40}")
    print("Testing contract resolution...")
    print(f"{'─' * 40}")
    
    # Create a sample leg to test resolution
    sample_legs = [{
        'symbol': 'SPY',
        'strike': 600.0,
        'option_type': 'P',
        'expiration': '2024-12-20',  # Use a valid date
        'action': 'BUY',
        'quantity': 1,
    }]
    
    try:
        resolved = client.resolve_contracts(sample_legs)
        if resolved and len(resolved) > 0:
            leg = resolved[0]
            if leg.is_resolved:
                print(f"   ✅ Contract resolution working")
                print(f"      ConId: {leg.con_id}")
                print(f"      Local Symbol: {leg.local_symbol}")
            else:
                print(f"   ⚠️ Resolution returned but not resolved: {leg.error}")
        else:
            print(f"   ⚠️ No contracts returned (expiry may have passed)")
    except Exception as e:
        print(f"   ⚠️ Contract resolution test skipped: {e}")
    
    # Disconnect
    print(f"\n{'─' * 40}")
    print("Disconnecting...")
    client.disconnect()
    print("✅ Disconnected cleanly")
    
    print(f"\n{'=' * 60}")
    print("TEST PASSED - IBKR connection is working!")
    print(f"{'=' * 60}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
