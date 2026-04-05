"""
Failure Injection Testing Client
================================

Provides easy interface for triggering controlled system failures
to test crash recovery, transaction rollback, and ACID properties.

Features:
- Multiple crash modes (immediate, signals, exceptions)
- Delayed crashes for mid-transaction testing
- Transaction-specific crash scenarios
- Environment variable configuration
- Integration with Module A transaction system

Usage:
    python failure_client.py --mode immediate --delay 5
    python failure_client.py --transaction-crash --delay 2
    python failure_client.py --env-setup
"""

import requests
import argparse
import time
import json
import os
import sys
from typing import Optional, Dict, Any


class FailureInjectionClient:
    """Client for controlling failure injection in the FastAPI server."""
    
    def __init__(self, base_url: str = "http://localhost:8000", admin_token: Optional[str] = None):
        self.base_url = base_url
        self.session = requests.Session()
        
        if admin_token:
            self.session.headers.update({"Authorization": f"Bearer {admin_token}"})
        
        # Try to login as admin if no token provided
        if not admin_token:
            self._attempt_admin_login()
    
    def _attempt_admin_login(self):
        """Try to login with default admin credentials."""
        try:
            # Try common admin credentials
            admin_credentials = [
                {"username": "admin", "password": "admin123"},
                {"username": "admin", "password": "password"},
                {"username": "cds", "password": "cds123"},
            ]
            
            for creds in admin_credentials:
                try:
                    response = self.session.post(f"{self.base_url}/login", json=creds)
                    if response.status_code == 200:
                        data = response.json()
                        token = data.get("session_token")
                        if token:
                            self.session.headers.update({"Authorization": f"Bearer {token}"})
                            print(f"[OK] Logged in as {creds['username']}")
                            return
                except Exception:
                    continue
            
            print("[WARNING] Could not auto-login as admin. Some features may not work.")
            
        except Exception as e:
            print(f"[WARNING] Admin login failed: {e}")
    
    def enable_failure_injection(
        self, 
        mode: str = "immediate", 
        delay: float = 0
    ) -> Dict[str, Any]:
        """Enable failure injection with specified parameters."""
        try:
            response = self.session.post(
                f"{self.base_url}/admin/failure-injection/enable",
                json={"mode": mode, "delay": delay}
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e), "status_code": getattr(e.response, 'status_code', None)}
    
    def trigger_crash(self) -> Dict[str, Any]:
        """Trigger configured system crash."""
        try:
            response = self.session.post(f"{self.base_url}/admin/failure-injection/trigger")
            response.raise_for_status()
            # Should not reach here if crash works
            return response.json()
        except requests.exceptions.RequestException as e:
            # Expected - server crashed
            return {"message": "Crash triggered successfully", "error": str(e)}
    
    def disable_failure_injection(self) -> Dict[str, Any]:
        """Disable failure injection."""
        try:
            response = self.session.post(f"{self.base_url}/admin/failure-injection/disable")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e), "status_code": getattr(e.response, 'status_code', None)}
    
    def get_status(self) -> Dict[str, Any]:
        """Get current failure injection status."""
        try:
            response = self.session.get(f"{self.base_url}/admin/failure-injection/status")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e), "status_code": getattr(e.response, 'status_code', None)}
    
    def trigger_transaction_crash(self, delay: float = 2.0) -> Dict[str, Any]:
        """Trigger crash during active transaction for ACID testing."""
        try:
            response = self.session.post(
                f"{self.base_url}/admin/failure-injection/transaction-crash",
                json={"delay": delay}
            )
            response.raise_for_status()
            # Should not reach here if crash works
            return response.json()
        except requests.exceptions.RequestException as e:
            # Expected - server crashed during transaction
            return {"message": "Transaction crash triggered successfully", "error": str(e)}
    
    def check_env_triggers(self) -> Dict[str, Any]:
        """Check environment variable crash triggers."""
        try:
            response = self.session.get(f"{self.base_url}/admin/failure-injection/env-check")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e), "status_code": getattr(e.response, 'status_code', None)}
    
    def test_server_connection(self) -> bool:
        """Test if server is reachable."""
        try:
            response = self.session.get(f"{self.base_url}/")
            return response.status_code == 200
        except Exception:
            return False


def demonstrate_crash_recovery():
    """Demonstrate complete crash recovery testing workflow."""
    print("=" * 60)
    print("🧪 CRASH RECOVERY DEMONSTRATION")
    print("=" * 60)
    
    client = FailureInjectionClient()
    
    # Test 1: Basic crash and recovery
    print("\n📋 Test 1: Basic Server Crash")
    print("-" * 30)
    
    if not client.test_server_connection():
        print("❌ Server not reachable. Please start the FastAPI server first.")
        return
    
    print("✅ Server is running")
    
    # Configure crash
    result = client.enable_failure_injection(mode="immediate", delay=3)
    if "error" in result:
        print(f"❌ Failed to enable failure injection: {result['error']}")
        return
    
    print("✅ Failure injection enabled (3-second delay)")
    
    # Trigger crash
    print("⏳ Triggering crash in 3 seconds...")
    start_time = time.time()
    
    crash_result = client.trigger_crash()
    end_time = time.time()
    
    print(f"⏱️ Crash triggered after {end_time - start_time:.2f} seconds")
    print("🔥 Server should now be crashed")
    
    # Wait and test recovery
    print("\n⏳ Waiting 5 seconds for manual server restart...")
    time.sleep(5)
    
    if client.test_server_connection():
        print("✅ Server recovered successfully!")
    else:
        print("❌ Server still down - manual restart required")
    
    # Test 2: Transaction crash
    print("\n📋 Test 2: Transaction Crash Recovery")
    print("-" * 30)
    
    if client.test_server_connection():
        print("⏳ Triggering crash during transaction...")
        tx_result = client.trigger_transaction_crash(delay=1.0)
        print("🔥 Transaction crash triggered")
        
        print("💡 After restart, check audit logs to verify transaction rollback")
        print("   Incomplete transaction data should not be present")


def setup_environment_crashes():
    """Set up environment variables for crash testing."""
    print("🔧 ENVIRONMENT CRASH CONFIGURATION")
    print("=" * 50)
    
    env_configs = {
        "Crash on startup (5s delay)": {
            "CRASH_ON_STARTUP": "true",
            "CRASH_DELAY": "5",
            "CRASH_MODE": "immediate"
        },
        "Crash on any request": {
            "CRASH_ON_REQUEST": "true",
            "CRASH_DELAY": "0",
            "CRASH_MODE": "sigterm"
        },
        "Delayed crash on request": {
            "CRASH_ON_REQUEST": "true", 
            "CRASH_DELAY": "3",
            "CRASH_MODE": "exception"
        }
    }
    
    print("Available environment configurations:")
    for i, (name, config) in enumerate(env_configs.items(), 1):
        print(f"\n{i}. {name}:")
        for key, value in config.items():
            print(f"   export {key}={value}")
    
    print(f"\nExample usage:")
    print(f"  # Set environment variables")
    print(f"  export CRASH_ON_STARTUP=true")
    print(f"  export CRASH_DELAY=5")
    print(f"  ")
    print(f"  # Start server (will crash after 5 seconds)")
    print(f"  python -m uvicorn app.main:app --port 8000")
    
    print(f"\n💡 Use environment crashes for:")
    print(f"   • Testing startup recovery scenarios") 
    print(f"   • Simulating unexpected crashes during load")
    print(f"   • Automated crash testing in CI/CD")


def interactive_crash_testing():
    """Interactive crash testing interface."""
    client = FailureInjectionClient()
    
    if not client.test_server_connection():
        print("❌ Server not reachable. Please start the FastAPI server on localhost:8000")
        return
    
    print("🎮 INTERACTIVE CRASH TESTING")
    print("=" * 40)
    
    while True:
        print("\nOptions:")
        print("1. Check server status")
        print("2. Enable failure injection")
        print("3. Trigger immediate crash")
        print("4. Trigger delayed crash")
        print("5. Transaction crash test")
        print("6. Check environment triggers")
        print("7. Disable failure injection")
        print("0. Exit")
        
        try:
            choice = input("\nEnter choice (0-7): ").strip()
            
            if choice == "0":
                break
            elif choice == "1":
                if client.test_server_connection():
                    status = client.get_status()
                    print(f"✅ Server online")
                    print(f"Status: {json.dumps(status, indent=2)}")
                else:
                    print("❌ Server offline")
            elif choice == "2":
                mode = input("Crash mode (immediate/sigterm/sigkill/exception) [immediate]: ").strip() or "immediate"
                delay = float(input("Delay in seconds [0]: ").strip() or "0")
                result = client.enable_failure_injection(mode, delay)
                print(f"Result: {json.dumps(result, indent=2)}")
            elif choice == "3":
                print("⚠️ Triggering immediate crash...")
                result = client.trigger_crash()
                print(f"Result: {json.dumps(result, indent=2)}")
            elif choice == "4":
                delay = float(input("Delay in seconds [3]: ").strip() or "3")
                client.enable_failure_injection("immediate", delay)
                print(f"⚠️ Crash will trigger in {delay} seconds...")
                result = client.trigger_crash()
                print(f"Result: {json.dumps(result, indent=2)}")
            elif choice == "5":
                delay = float(input("Transaction delay [2.0]: ").strip() or "2.0")
                print("⚠️ Triggering transaction crash...")
                result = client.trigger_transaction_crash(delay)
                print(f"Result: {json.dumps(result, indent=2)}")
            elif choice == "6":
                result = client.check_env_triggers()
                print(f"Environment: {json.dumps(result, indent=2)}")
            elif choice == "7":
                result = client.disable_failure_injection()
                print(f"Result: {json.dumps(result, indent=2)}")
            else:
                print("❌ Invalid choice")
                
        except KeyboardInterrupt:
            print("\n👋 Exiting...")
            break
        except Exception as e:
            print(f"❌ Error: {e}")


def main():
    """Main failure injection testing interface."""
    parser = argparse.ArgumentParser(description="Failure injection testing client")
    parser.add_argument("--mode", choices=["immediate", "sigterm", "sigkill", "exception"], 
                       default="immediate", help="Crash mode")
    parser.add_argument("--delay", type=float, default=0, help="Delay before crash (seconds)")
    parser.add_argument("--transaction-crash", action="store_true", 
                       help="Trigger crash during transaction")
    parser.add_argument("--demo", action="store_true", 
                       help="Run demonstration of crash recovery")
    parser.add_argument("--env-setup", action="store_true",
                       help="Show environment variable setup")
    parser.add_argument("--interactive", action="store_true",
                       help="Interactive testing mode")
    parser.add_argument("--status", action="store_true",
                       help="Check failure injection status")
    
    args = parser.parse_args()
    
    if args.demo:
        demonstrate_crash_recovery()
    elif args.env_setup:
        setup_environment_crashes()
    elif args.interactive:
        interactive_crash_testing()
    elif args.status:
        client = FailureInjectionClient()
        status = client.get_status()
        print(json.dumps(status, indent=2))
    elif args.transaction_crash:
        client = FailureInjectionClient()
        print(f"⚠️ Triggering transaction crash with {args.delay}s delay...")
        result = client.trigger_transaction_crash(args.delay)
        print(f"Result: {json.dumps(result, indent=2)}")
    else:
        # Default: enable and trigger crash
        client = FailureInjectionClient()
        
        print(f"🔧 Enabling failure injection: {args.mode} mode, {args.delay}s delay")
        enable_result = client.enable_failure_injection(args.mode, args.delay)
        print(f"Enable result: {json.dumps(enable_result, indent=2)}")
        
        if "error" not in enable_result:
            print(f"⚠️ Triggering {args.mode} crash...")
            crash_result = client.trigger_crash()
            print(f"Crash result: {json.dumps(crash_result, indent=2)}")


if __name__ == "__main__":
    main()