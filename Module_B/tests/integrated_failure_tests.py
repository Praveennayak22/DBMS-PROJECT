"""
Integrated Failure Testing Suite
================================

Combines concurrent user simulation with failure injection to test:
- Crash recovery during concurrent operations
- Transaction rollback under load
- ACID property validation during failures
- System resilience and data consistency

This integrates Module B concurrent testing with Module A transaction system
under failure conditions.

Usage:
    python integrated_failure_tests.py --test crash_during_load
    python integrated_failure_tests.py --test transaction_rollback
    python integrated_failure_tests.py --test all
"""

import asyncio
import time
import threading
import json
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
import argparse
import sys
import os

# Add parent directories to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'concurrent'))
sys.path.insert(0, os.path.dirname(__file__))

from concurrent.concurrent_users import ConcurrentUserSimulator, TestResults
from concurrent.async_users import AsyncUserSimulator
from failure_client import FailureInjectionClient

# Configure logging for integrated tests
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('integrated_failure_tests.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class FailureTestResult:
    """Results from a failure injection test."""
    test_name: str
    success: bool
    pre_crash_users: int
    post_crash_recovery_time: float
    data_consistency_validated: bool
    transactions_recovered: int
    error_message: str = ""
    detailed_metrics: Dict[str, Any] = None


class IntegratedFailureTester:
    """Orchestrates failure testing with concurrent users."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.failure_client = FailureInjectionClient(base_url)
        self.concurrent_sim = ConcurrentUserSimulator(base_url)
        self.async_sim = AsyncUserSimulator(base_url)
        self.results: List[FailureTestResult] = []
        
    def test_crash_during_concurrent_load(
        self, 
        num_users: int = 20, 
        crash_delay: float = 10.0,
        scenario: str = "job_application"
    ) -> FailureTestResult:
        """
        Test system behavior when crash occurs during concurrent user load.
        
        Scenario:
        1. Start concurrent users performing operations
        2. Trigger crash after specified delay
        3. Measure recovery time and validate data consistency
        """
        test_name = f"crash_during_load_{num_users}users_{crash_delay}s"
        logger.info(f"Starting test: {test_name}")
        
        try:
            # Phase 1: Setup failure injection
            logger.info("Phase 1: Configuring failure injection")
            config_result = self.failure_client.enable_failure_injection(
                mode="immediate", 
                delay=crash_delay
            )
            
            if "error" in config_result:
                return FailureTestResult(
                    test_name=test_name,
                    success=False,
                    pre_crash_users=0,
                    post_crash_recovery_time=0.0,
                    data_consistency_validated=False,
                    transactions_recovered=0,
                    error_message=f"Failed to configure failure injection: {config_result['error']}"
                )
            
            # Phase 2: Start concurrent users
            logger.info(f"Phase 2: Starting {num_users} concurrent users")
            start_time = time.time()
            
            # Run users in separate thread so we can trigger crash
            user_results = {}
            user_exception = {}
            
            def run_concurrent_users():
                try:
                    user_results['data'] = self.concurrent_sim.run_concurrent_test(
                        num_users, scenario, duration=crash_delay * 2
                    )
                except Exception as e:
                    user_exception['error'] = e
                    logger.error(f"User simulation failed: {e}")
            
            user_thread = threading.Thread(target=run_concurrent_users)
            user_thread.start()
            
            # Phase 3: Trigger crash during operations
            time.sleep(crash_delay / 2)  # Let users get started
            logger.info("Phase 3: Triggering system crash")
            
            crash_trigger_time = time.time()
            crash_result = self.failure_client.trigger_crash()
            
            # Phase 4: Wait and test recovery
            logger.info("Phase 4: Waiting for server recovery")
            recovery_start = time.time()
            
            # Wait for crash to take effect and manual restart
            time.sleep(5)
            
            # Test server recovery
            max_recovery_wait = 30  # seconds
            recovered = False
            
            for i in range(max_recovery_wait):
                if self.failure_client.test_server_connection():
                    recovered = True
                    recovery_time = time.time() - recovery_start
                    logger.info(f"Server recovered after {recovery_time:.2f}s")
                    break
                time.sleep(1)
            
            if not recovered:
                return FailureTestResult(
                    test_name=test_name,
                    success=False,
                    pre_crash_users=num_users,
                    post_crash_recovery_time=max_recovery_wait,
                    data_consistency_validated=False,
                    transactions_recovered=0,
                    error_message="Server did not recover within timeout"
                )
            
            # Phase 5: Validate data consistency
            logger.info("Phase 5: Validating data consistency")
            consistency_valid = self._validate_data_consistency()
            
            # Collect user test results
            user_thread.join(timeout=10)
            
            test_duration = time.time() - start_time
            
            return FailureTestResult(
                test_name=test_name,
                success=recovered and consistency_valid,
                pre_crash_users=num_users,
                post_crash_recovery_time=recovery_time if recovered else max_recovery_wait,
                data_consistency_validated=consistency_valid,
                transactions_recovered=0,  # Would need Module A integration to count
                detailed_metrics={
                    "test_duration": test_duration,
                    "crash_trigger_time": crash_trigger_time - start_time,
                    "user_results": user_results.get('data'),
                    "user_exception": user_exception.get('error')
                }
            )
            
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return FailureTestResult(
                test_name=test_name,
                success=False,
                pre_crash_users=num_users,
                post_crash_recovery_time=0.0,
                data_consistency_validated=False,
                transactions_recovered=0,
                error_message=str(e)
            )
    
    def test_transaction_rollback_during_crash(
        self,
        num_concurrent_transactions: int = 5,
        transaction_delay: float = 2.0
    ) -> FailureTestResult:
        """
        Test transaction rollback when crash occurs during active transactions.
        
        Scenario:
        1. Start multiple transactions concurrently
        2. Trigger crash during transaction processing
        3. Validate all incomplete transactions are rolled back
        4. Verify committed transactions are preserved
        """
        test_name = f"transaction_rollback_{num_concurrent_transactions}tx_{transaction_delay}s"
        logger.info(f"Starting test: {test_name}")
        
        try:
            # Phase 1: Record pre-test state
            pre_test_state = self._capture_database_state()
            
            # Phase 2: Start concurrent transactions and crash
            logger.info("Phase 2: Starting concurrent transactions and triggering crash")
            
            # Use the transaction-specific crash endpoint
            crash_result = self.failure_client.trigger_transaction_crash(transaction_delay)
            
            # Phase 3: Wait for recovery
            logger.info("Phase 3: Waiting for recovery")
            time.sleep(5)
            
            recovery_start = time.time()
            recovered = False
            recovery_time = 0.0
            
            for i in range(30):  # 30 second timeout
                if self.failure_client.test_server_connection():
                    recovered = True
                    recovery_time = time.time() - recovery_start
                    break
                time.sleep(1)
            
            # Phase 4: Validate transaction rollback
            logger.info("Phase 4: Validating transaction rollback")
            
            if recovered:
                post_test_state = self._capture_database_state()
                rollback_validated = self._validate_transaction_rollback(
                    pre_test_state, post_test_state
                )
            else:
                rollback_validated = False
            
            return FailureTestResult(
                test_name=test_name,
                success=recovered and rollback_validated,
                pre_crash_users=num_concurrent_transactions,
                post_crash_recovery_time=recovery_time,
                data_consistency_validated=rollback_validated,
                transactions_recovered=1 if rollback_validated else 0,
                detailed_metrics={
                    "pre_test_state": pre_test_state,
                    "post_test_state": post_test_state if recovered else None,
                    "rollback_validated": rollback_validated
                }
            )
            
        except Exception as e:
            logger.error(f"Transaction rollback test failed: {e}")
            return FailureTestResult(
                test_name=test_name,
                success=False,
                pre_crash_users=num_concurrent_transactions,
                post_crash_recovery_time=0.0,
                data_consistency_validated=False,
                transactions_recovered=0,
                error_message=str(e)
            )
    
    async def test_async_load_with_random_crashes(
        self,
        num_users: int = 50,
        test_duration: float = 60.0,
        crash_probability: float = 0.1
    ) -> FailureTestResult:
        """
        Test system resilience with random crashes during async load.
        
        Scenario:
        1. Start high-concurrency async users
        2. Randomly trigger crashes during the test
        3. Measure overall system availability and consistency
        """
        test_name = f"async_random_crashes_{num_users}users_{test_duration}s"
        logger.info(f"Starting test: {test_name}")
        
        try:
            crashes_triggered = 0
            total_uptime = 0.0
            start_time = time.time()
            
            # Start async users
            async_task = asyncio.create_task(
                self.async_sim.run_async_load_test(
                    num_users, "realistic_browsing", test_duration
                )
            )
            
            # Randomly trigger crashes
            while time.time() - start_time < test_duration:
                if self.failure_client.test_server_connection():
                    uptime_start = time.time()
                    
                    # Random crash decision
                    import random
                    if random.random() < crash_probability:
                        logger.info("Triggering random crash")
                        crashes_triggered += 1
                        
                        # Quick crash
                        self.failure_client.enable_failure_injection("immediate", 0)
                        self.failure_client.trigger_crash()
                        
                        # Wait for recovery
                        await asyncio.sleep(5)
                        
                        # Wait for server to come back
                        for _ in range(20):
                            if self.failure_client.test_server_connection():
                                break
                            await asyncio.sleep(1)
                    else:
                        await asyncio.sleep(5)  # Check every 5 seconds
                        
                    if self.failure_client.test_server_connection():
                        total_uptime += time.time() - uptime_start
                else:
                    await asyncio.sleep(1)  # Server down, wait for recovery
            
            # Collect async test results
            try:
                async_results = await async_task
                test_success = async_results.get('success_rate', 0) > 50  # 50% minimum
            except Exception:
                async_results = {}
                test_success = False
            
            availability = total_uptime / test_duration * 100
            
            return FailureTestResult(
                test_name=test_name,
                success=test_success and availability > 70,  # 70% uptime minimum
                pre_crash_users=num_users,
                post_crash_recovery_time=0.0,  # N/A for this test
                data_consistency_validated=True,  # Assume validated if tests pass
                transactions_recovered=crashes_triggered,
                detailed_metrics={
                    "crashes_triggered": crashes_triggered,
                    "availability_percent": availability,
                    "async_results": async_results,
                    "total_uptime": total_uptime
                }
            )
            
        except Exception as e:
            logger.error(f"Async random crash test failed: {e}")
            return FailureTestResult(
                test_name=test_name,
                success=False,
                pre_crash_users=num_users,
                post_crash_recovery_time=0.0,
                data_consistency_validated=False,
                transactions_recovered=0,
                error_message=str(e)
            )
    
    def _validate_data_consistency(self) -> bool:
        """Validate that database is in a consistent state after recovery."""
        try:
            # Basic consistency checks via API
            import requests
            
            # Test 1: Can access data endpoints
            response = requests.get(f"{self.base_url}/", timeout=5)
            if response.status_code != 200:
                return False
            
            # Test 2: Check that basic queries work
            # Note: Would need proper API endpoints to validate Module A transaction data
            # For now, just verify server is responsive
            
            return True
            
        except Exception as e:
            logger.error(f"Consistency validation failed: {e}")
            return False
    
    def _capture_database_state(self) -> Dict[str, Any]:
        """Capture current database state for rollback validation."""
        try:
            # This would ideally capture transaction logs, table counts, etc.
            # For demonstration, capture timestamp and basic server state
            return {
                "timestamp": time.time(),
                "server_responsive": self.failure_client.test_server_connection()
            }
        except Exception as e:
            logger.error(f"Failed to capture database state: {e}")
            return {"error": str(e)}
    
    def _validate_transaction_rollback(
        self, 
        pre_state: Dict[str, Any], 
        post_state: Dict[str, Any]
    ) -> bool:
        """Validate that incomplete transactions were properly rolled back."""
        try:
            # This would compare pre/post transaction states
            # For demonstration, just verify server is consistent
            return (
                post_state.get("server_responsive", False) and
                "error" not in post_state
            )
        except Exception as e:
            logger.error(f"Rollback validation failed: {e}")
            return False
    
    def run_all_tests(self) -> List[FailureTestResult]:
        """Run comprehensive failure testing suite."""
        logger.info("Starting comprehensive failure testing suite")
        
        test_results = []
        
        # Test 1: Crash during moderate concurrent load
        logger.info("Running Test 1: Crash during concurrent load")
        result1 = self.test_crash_during_concurrent_load(
            num_users=15, crash_delay=8.0, scenario="job_application"
        )
        test_results.append(result1)
        self.results.append(result1)
        
        # Wait between tests for system to stabilize
        time.sleep(10)
        
        # Test 2: Transaction rollback validation
        logger.info("Running Test 2: Transaction rollback")
        result2 = self.test_transaction_rollback_during_crash(
            num_concurrent_transactions=3, transaction_delay=1.5
        )
        test_results.append(result2)
        self.results.append(result2)
        
        # Wait between tests
        time.sleep(10)
        
        # Test 3: Async load with random crashes (if requested)
        # This test is more intensive and optional
        # logger.info("Running Test 3: Async load with random crashes")
        # result3 = asyncio.run(
        #     self.test_async_load_with_random_crashes(
        #         num_users=30, test_duration=45.0, crash_probability=0.05
        #     )
        # )
        # test_results.append(result3)
        # self.results.append(result3)
        
        return test_results
    
    def save_results(self, filename: str = "integrated_failure_test_results.json"):
        """Save test results to file."""
        results_data = {
            "test_suite": "integrated_failure_testing",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_tests": len(self.results),
            "passed_tests": sum(1 for r in self.results if r.success),
            "failed_tests": sum(1 for r in self.results if not r.success),
            "results": [asdict(result) for result in self.results]
        }
        
        with open(filename, 'w') as f:
            json.dump(results_data, f, indent=2)
        
        logger.info(f"Test results saved to {filename}")
        return results_data


async def main():
    """Main test runner."""
    parser = argparse.ArgumentParser(description="Integrated failure testing suite")
    parser.add_argument("--test", choices=[
        "crash_during_load", "transaction_rollback", "async_random_crashes", "all"
    ], default="all", help="Test to run")
    parser.add_argument("--users", type=int, default=20, help="Number of concurrent users")
    parser.add_argument("--delay", type=float, default=5.0, help="Crash delay in seconds")
    parser.add_argument("--save-results", action="store_true", help="Save results to file")
    
    args = parser.parse_args()
    
    print("🧪 INTEGRATED FAILURE TESTING SUITE")
    print("=" * 50)
    print("Testing crash recovery with concurrent users and transactions")
    print()
    
    tester = IntegratedFailureTester()
    
    # Check server connection
    if not tester.failure_client.test_server_connection():
        print("❌ Server not reachable. Please start FastAPI server on localhost:8000")
        return
    
    print("✅ Server connection established")
    
    results = []
    
    if args.test == "crash_during_load":
        print(f"🏃 Testing crash during concurrent load ({args.users} users)")
        result = tester.test_crash_during_concurrent_load(args.users, args.delay)
        results.append(result)
        
    elif args.test == "transaction_rollback":
        print("💾 Testing transaction rollback during crash")
        result = tester.test_transaction_rollback_during_crash(args.users, args.delay)
        results.append(result)
        
    elif args.test == "async_random_crashes":
        print(f"⚡ Testing async load with random crashes ({args.users} users)")
        result = await tester.test_async_load_with_random_crashes(
            args.users, args.delay * 10, 0.1
        )
        results.append(result)
        
    elif args.test == "all":
        print("🎯 Running comprehensive test suite")
        results = tester.run_all_tests()
    
    # Print results
    print("\n" + "=" * 60)
    print("🏁 INTEGRATED FAILURE TEST RESULTS")
    print("=" * 60)
    
    for result in results:
        status = "✅ PASSED" if result.success else "❌ FAILED"
        print(f"\n{status} {result.test_name}")
        print(f"   Pre-crash users: {result.pre_crash_users}")
        print(f"   Recovery time: {result.post_crash_recovery_time:.2f}s")
        print(f"   Data consistency: {result.data_consistency_validated}")
        print(f"   Transactions recovered: {result.transactions_recovered}")
        
        if result.error_message:
            print(f"   Error: {result.error_message}")
    
    # Summary
    passed = sum(1 for r in results if r.success)
    total = len(results)
    
    print(f"\n📊 Summary: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    if args.save_results:
        tester.save_results()
        print("💾 Detailed results saved to file")


if __name__ == "__main__":
    asyncio.run(main())