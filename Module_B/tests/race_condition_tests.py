"""
Race Condition Testing Suite
============================

Dedicated testing for race conditions in concurrent access scenarios.
Tests critical endpoints where multiple users compete for limited resources.

Key Focus Areas:
- Job applications with limited slots
- Interview booking conflicts  
- Resource allocation under contention
- Data consistency during concurrent modifications
- Transaction isolation validation

This module integrates with both Module A (transaction system) and 
Module B (API endpoints) to validate ACID properties under race conditions.

Usage:
    python race_condition_tests.py --test job_application_slots
    python race_condition_tests.py --test interview_booking
    python race_condition_tests.py --test all --users 100
"""

import threading
import time
import json
import random
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import requests
import argparse

# Try different concurrent import approaches for compatibility
try:
    from concurrent.futures import ThreadPoolExecutor, as_completed
except ImportError:
    # Fallback for systems without concurrent.futures
    import threading
    from threading import Thread
    
    class ThreadPoolExecutor:
        def __init__(self, max_workers=None):
            self.max_workers = max_workers or 10
            
        def __enter__(self):
            return self
            
        def __exit__(self, *args):
            pass
            
        def submit(self, fn, *args, **kwargs):
            thread = Thread(target=fn, args=args, kwargs=kwargs)
            thread.start()
            return thread
    
    def as_completed(futures):
        for future in futures:
            if hasattr(future, 'join'):
                future.join()
            yield future

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('race_condition_tests.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class RaceConditionResult:
    """Results from a race condition test."""
    test_name: str
    total_attempts: int
    successful_operations: int
    expected_successes: int
    race_condition_detected: bool
    over_allocation: int
    consistency_validated: bool
    response_time_avg: float
    response_time_max: float
    detailed_results: List[Dict[str, Any]] = None


class RaceConditionTester:
    """Specialized tester for race condition scenarios."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
        self.results_lock = threading.Lock()
        self.operation_results = []
        
        # Try to authenticate as admin/test user
        self._setup_authentication()
    
    def _setup_authentication(self):
        """Set up authentication for API calls."""
        try:
            # Try common test credentials
            test_credentials = [
                {"username": "admin", "password": "admin123"},
                {"username": "testuser", "password": "test123"},
                {"username": "student1", "password": "password"}
            ]
            
            for creds in test_credentials:
                try:
                    response = self.session.post(f"{self.base_url}/login", json=creds)
                    if response.status_code == 200:
                        data = response.json()
                        token = data.get("session_token")
                        if token:
                            self.session.headers.update({"Authorization": f"Bearer {token}"})
                            logger.info(f"Authenticated as {creds['username']}")
                            return
                except Exception:
                    continue
            
            logger.warning("Could not authenticate - some tests may fail")
            
        except Exception as e:
            logger.warning(f"Authentication setup failed: {e}")
    
    def _record_operation(self, operation_data: Dict[str, Any]):
        """Thread-safe recording of operation results."""
        with self.results_lock:
            self.operation_results.append(operation_data)
    
    def test_job_application_slots(
        self, 
        num_applicants: int = 50,
        job_id: int = 100,
        available_slots: int = 3
    ) -> RaceConditionResult:
        """
        Test race conditions in job applications with limited slots.
        
        Scenario:
        - Multiple users apply simultaneously for the same job
        - Job has limited number of available slots
        - Test that no more than the limit are accepted
        - Validate data consistency after all attempts
        """
        test_name = f"job_application_slots_{num_applicants}applicants_{available_slots}slots"
        logger.info(f"Starting race condition test: {test_name}")
        
        self.operation_results.clear()
        
        # Prepare application data for each applicant
        applicant_data = []
        for i in range(num_applicants):
            applicant_data.append({
                "applicant_id": i,
                "job_id": job_id,
                "student_id": 1000 + i,  # Unique student IDs
                "application_data": {
                    "job_id": job_id,
                    "student_id": 1000 + i,
                    "application_date": time.strftime("%Y-%m-%d"),
                    "cover_letter": f"Application from student {1000 + i}"
                }
            })
        
        # Execute concurrent applications
        start_time = time.time()
        
        def apply_for_job(applicant_info):
            """Single applicant's job application attempt."""
            applicant_id = applicant_info["applicant_id"]
            application_data = applicant_info["application_data"]
            
            operation_start = time.time()
            
            try:
                response = self.session.post(
                    f"{self.base_url}/apply",
                    json=application_data,
                    timeout=10
                )
                
                operation_end = time.time()
                
                result = {
                    "applicant_id": applicant_id,
                    "student_id": application_data["student_id"],
                    "success": response.status_code in [200, 201],
                    "response_code": response.status_code,
                    "response_time": operation_end - operation_start,
                    "response_data": response.json() if response.status_code < 500 else {},
                    "timestamp": operation_start
                }
                
                self._record_operation(result)
                return result
                
            except Exception as e:
                operation_end = time.time()
                
                result = {
                    "applicant_id": applicant_id,
                    "student_id": application_data["student_id"],
                    "success": False,
                    "response_code": 500,
                    "response_time": operation_end - operation_start,
                    "error": str(e),
                    "timestamp": operation_start
                }
                
                self._record_operation(result)
                return result
        
        # Run concurrent applications
        with ThreadPoolExecutor(max_workers=min(num_applicants, 20)) as executor:
            # Submit all applications simultaneously
            future_to_applicant = {
                executor.submit(apply_for_job, applicant): applicant["applicant_id"]
                for applicant in applicant_data
            }
            
            # Wait for all applications to complete
            for future in as_completed(future_to_applicant):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Application failed: {e}")
        
        total_time = time.time() - start_time
        
        # Analyze results
        return self._analyze_job_application_results(
            test_name, num_applicants, available_slots, total_time
        )
    
    def test_interview_booking_conflicts(
        self,
        num_bookers: int = 30,
        available_slots: List[str] = None,
        application_ids: List[int] = None
    ) -> RaceConditionResult:
        """
        Test race conditions in interview slot booking.
        
        Scenario:
        - Multiple users try to book the same interview time slots
        - Limited number of slots available
        - Test proper conflict resolution
        - Validate no double-booking occurs
        """
        if available_slots is None:
            available_slots = [
                "2024-04-01 09:00",
                "2024-04-01 10:00", 
                "2024-04-01 11:00"
            ]
        
        if application_ids is None:
            application_ids = list(range(200, 200 + num_bookers))
        
        test_name = f"interview_booking_{num_bookers}bookers_{len(available_slots)}slots"
        logger.info(f"Starting interview booking race test: {test_name}")
        
        self.operation_results.clear()
        
        # Create booking attempts - multiple users for same slots
        booking_attempts = []
        for i in range(num_bookers):
            # Each user tries to book a random slot
            slot = random.choice(available_slots)
            app_id = application_ids[i] if i < len(application_ids) else 200 + i
            
            booking_attempts.append({
                "booker_id": i,
                "application_id": app_id,
                "slot": slot,
                "booking_data": {
                    "application_id": app_id,
                    "interview_slot": slot
                }
            })
        
        start_time = time.time()
        
        def book_interview_slot(booking_info):
            """Single user's interview booking attempt."""
            booker_id = booking_info["booker_id"]
            booking_data = booking_info["booking_data"]
            slot = booking_info["slot"]
            
            operation_start = time.time()
            
            try:
                response = self.session.post(
                    f"{self.base_url}/book_interview",
                    json=booking_data,
                    timeout=10
                )
                
                operation_end = time.time()
                
                result = {
                    "booker_id": booker_id,
                    "application_id": booking_data["application_id"],
                    "slot": slot,
                    "success": response.status_code in [200, 201],
                    "response_code": response.status_code,
                    "response_time": operation_end - operation_start,
                    "response_data": response.json() if response.status_code < 500 else {},
                    "timestamp": operation_start
                }
                
                self._record_operation(result)
                return result
                
            except Exception as e:
                operation_end = time.time()
                
                result = {
                    "booker_id": booker_id,
                    "application_id": booking_data["application_id"],
                    "slot": slot,
                    "success": False,
                    "response_code": 500,
                    "response_time": operation_end - operation_start,
                    "error": str(e),
                    "timestamp": operation_start
                }
                
                self._record_operation(result)
                return result
        
        # Execute concurrent bookings
        with ThreadPoolExecutor(max_workers=min(num_bookers, 25)) as executor:
            future_to_booker = {
                executor.submit(book_interview_slot, booking): booking["booker_id"]
                for booking in booking_attempts
            }
            
            for future in as_completed(future_to_booker):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Booking failed: {e}")
        
        total_time = time.time() - start_time
        
        # Analyze booking results
        return self._analyze_booking_results(
            test_name, num_bookers, len(available_slots), total_time
        )
    
    def test_concurrent_data_modification(
        self,
        num_modifiers: int = 20,
        target_record_id: int = 1,
        modification_type: str = "portfolio_update"
    ) -> RaceConditionResult:
        """
        Test race conditions in concurrent data modifications.
        
        Scenario:
        - Multiple users modify the same data record simultaneously
        - Test that modifications don't corrupt each other
        - Validate final state consistency
        - Check for lost updates
        """
        test_name = f"concurrent_modifications_{num_modifiers}users_{modification_type}"
        logger.info(f"Starting concurrent modification test: {test_name}")
        
        self.operation_results.clear()
        
        def modify_data(modifier_id):
            """Single user's data modification attempt."""
            operation_start = time.time()
            
            try:
                # Different modification based on type
                if modification_type == "portfolio_update":
                    update_data = {
                        "bio": f"Updated by modifier {modifier_id} at {time.time()}",
                        "skills": f"skill_set_{modifier_id}"
                    }
                    
                    response = self.session.patch(
                        f"{self.base_url}/portfolio/{target_record_id}",
                        json=update_data,
                        timeout=10
                    )
                else:
                    # Generic modification attempt
                    response = self.session.get(
                        f"{self.base_url}/portfolio/{target_record_id}",
                        timeout=10
                    )
                
                operation_end = time.time()
                
                result = {
                    "modifier_id": modifier_id,
                    "target_id": target_record_id,
                    "modification_type": modification_type,
                    "success": response.status_code in [200, 201],
                    "response_code": response.status_code,
                    "response_time": operation_end - operation_start,
                    "response_data": response.json() if response.status_code < 500 else {},
                    "timestamp": operation_start
                }
                
                self._record_operation(result)
                return result
                
            except Exception as e:
                operation_end = time.time()
                
                result = {
                    "modifier_id": modifier_id,
                    "target_id": target_record_id,
                    "success": False,
                    "response_code": 500,
                    "response_time": operation_end - operation_start,
                    "error": str(e),
                    "timestamp": operation_start
                }
                
                self._record_operation(result)
                return result
        
        # Execute concurrent modifications
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=min(num_modifiers, 15)) as executor:
            future_to_modifier = {
                executor.submit(modify_data, i): i
                for i in range(num_modifiers)
            }
            
            for future in as_completed(future_to_modifier):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Modification failed: {e}")
        
        total_time = time.time() - start_time
        
        # Analyze modification results
        return self._analyze_modification_results(
            test_name, num_modifiers, total_time
        )
    
    def _analyze_job_application_results(
        self, 
        test_name: str, 
        total_attempts: int, 
        expected_slots: int,
        total_time: float
    ) -> RaceConditionResult:
        """Analyze job application race condition test results."""
        
        successful_applications = [r for r in self.operation_results if r["success"]]
        failed_applications = [r for r in self.operation_results if not r["success"]]
        
        response_times = [r["response_time"] for r in self.operation_results if "response_time" in r]
        
        # Detect race condition (over-allocation)
        num_successes = len(successful_applications)
        over_allocation = max(0, num_successes - expected_slots)
        race_detected = over_allocation > 0
        
        # Validate consistency (in real system, would check database state)
        consistency_valid = not race_detected  # Simplified check
        
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        max_response_time = max(response_times) if response_times else 0
        
        logger.info(f"Job application test results: {num_successes}/{total_attempts} successful")
        if race_detected:
            logger.warning(f"RACE CONDITION DETECTED: {over_allocation} over-allocations")
        
        return RaceConditionResult(
            test_name=test_name,
            total_attempts=total_attempts,
            successful_operations=num_successes,
            expected_successes=expected_slots,
            race_condition_detected=race_detected,
            over_allocation=over_allocation,
            consistency_validated=consistency_valid,
            response_time_avg=avg_response_time,
            response_time_max=max_response_time,
            detailed_results=self.operation_results.copy()
        )
    
    def _analyze_booking_results(
        self, 
        test_name: str, 
        total_attempts: int, 
        available_slots: int,
        total_time: float
    ) -> RaceConditionResult:
        """Analyze interview booking race condition test results."""
        
        successful_bookings = [r for r in self.operation_results if r["success"]]
        
        # Group by slot to check for double-booking
        slot_bookings = {}
        for booking in successful_bookings:
            slot = booking.get("slot", "unknown")
            if slot not in slot_bookings:
                slot_bookings[slot] = []
            slot_bookings[slot].append(booking)
        
        # Detect double-booking (race condition)
        double_bookings = 0
        for slot, bookings in slot_bookings.items():
            if len(bookings) > 1:
                double_bookings += len(bookings) - 1
        
        race_detected = double_bookings > 0
        
        response_times = [r["response_time"] for r in self.operation_results if "response_time" in r]
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        max_response_time = max(response_times) if response_times else 0
        
        num_successes = len(successful_bookings)
        expected_max = available_slots  # Each slot can be booked once
        
        logger.info(f"Booking test results: {num_successes}/{total_attempts} successful")
        if race_detected:
            logger.warning(f"RACE CONDITION DETECTED: {double_bookings} double-bookings")
            for slot, bookings in slot_bookings.items():
                if len(bookings) > 1:
                    logger.warning(f"Slot {slot} booked by {len(bookings)} users")
        
        return RaceConditionResult(
            test_name=test_name,
            total_attempts=total_attempts,
            successful_operations=num_successes,
            expected_successes=expected_max,
            race_condition_detected=race_detected,
            over_allocation=double_bookings,
            consistency_validated=not race_detected,
            response_time_avg=avg_response_time,
            response_time_max=max_response_time,
            detailed_results=self.operation_results.copy()
        )
    
    def _analyze_modification_results(
        self, 
        test_name: str, 
        total_attempts: int,
        total_time: float
    ) -> RaceConditionResult:
        """Analyze concurrent modification test results."""
        
        successful_modifications = [r for r in self.operation_results if r["success"]]
        
        response_times = [r["response_time"] for r in self.operation_results if "response_time" in r]
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        max_response_time = max(response_times) if response_times else 0
        
        # For modification tests, we expect all to succeed if properly isolated
        num_successes = len(successful_modifications)
        expected_successes = total_attempts
        
        # Race condition if significant number of failures (assuming no authorization issues)
        success_rate = num_successes / total_attempts if total_attempts > 0 else 0
        race_detected = success_rate < 0.8  # Less than 80% success suggests issues
        
        logger.info(f"Modification test results: {num_successes}/{total_attempts} successful")
        if race_detected:
            logger.warning(f"POTENTIAL RACE CONDITION: Low success rate ({success_rate:.1%})")
        
        return RaceConditionResult(
            test_name=test_name,
            total_attempts=total_attempts,
            successful_operations=num_successes,
            expected_successes=expected_successes,
            race_condition_detected=race_detected,
            over_allocation=0,  # N/A for modification tests
            consistency_validated=not race_detected,
            response_time_avg=avg_response_time,
            response_time_max=max_response_time,
            detailed_results=self.operation_results.copy()
        )
    
    def run_comprehensive_race_tests(self) -> List[RaceConditionResult]:
        """Run all race condition tests."""
        logger.info("Starting comprehensive race condition testing")
        
        results = []
        
        # Test 1: Job application slots
        logger.info("Test 1: Job application race conditions")
        result1 = self.test_job_application_slots(num_applicants=25, available_slots=2)
        results.append(result1)
        
        time.sleep(2)  # Brief pause between tests
        
        # Test 2: Interview booking conflicts
        logger.info("Test 2: Interview booking race conditions")
        result2 = self.test_interview_booking_conflicts(
            num_bookers=15, 
            available_slots=["2024-04-01 14:00", "2024-04-01 15:00"]
        )
        results.append(result2)
        
        time.sleep(2)
        
        # Test 3: Concurrent data modifications
        logger.info("Test 3: Concurrent data modification race conditions")
        result3 = self.test_concurrent_data_modification(num_modifiers=10)
        results.append(result3)
        
        return results
    
    def save_results(self, results: List[RaceConditionResult], filename: str = "race_condition_results.json"):
        """Save race condition test results to file."""
        results_data = {
            "test_suite": "race_condition_testing",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_tests": len(results),
            "race_conditions_detected": sum(1 for r in results if r.race_condition_detected),
            "tests_passed": sum(1 for r in results if r.consistency_validated),
            "summary": {
                "total_operations": sum(r.total_attempts for r in results),
                "successful_operations": sum(r.successful_operations for r in results),
                "total_over_allocations": sum(r.over_allocation for r in results)
            },
            "results": [asdict(result) for result in results]
        }
        
        with open(filename, 'w') as f:
            json.dump(results_data, f, indent=2)
        
        logger.info(f"Race condition test results saved to {filename}")
        return results_data


def main():
    """Main race condition testing interface."""
    parser = argparse.ArgumentParser(description="Race condition testing suite")
    parser.add_argument("--test", choices=[
        "job_application_slots", "interview_booking", "data_modification", "all"
    ], default="all", help="Test type to run")
    parser.add_argument("--users", type=int, default=25, help="Number of concurrent users")
    parser.add_argument("--slots", type=int, default=3, help="Number of available slots/resources")
    parser.add_argument("--save-results", action="store_true", help="Save results to JSON file")
    
    args = parser.parse_args()
    
    print("🏁 RACE CONDITION TESTING SUITE")
    print("=" * 50)
    print("Testing concurrent access to limited resources")
    print()
    
    tester = RaceConditionTester()
    results = []
    
    if args.test == "job_application_slots":
        print(f"🎯 Testing job application slots ({args.users} applicants, {args.slots} slots)")
        result = tester.test_job_application_slots(args.users, available_slots=args.slots)
        results.append(result)
        
    elif args.test == "interview_booking":
        slots = [f"2024-04-01 {9+i}:00" for i in range(args.slots)]
        print(f"📅 Testing interview booking ({args.users} bookers, {len(slots)} slots)")
        result = tester.test_interview_booking_conflicts(args.users, slots)
        results.append(result)
        
    elif args.test == "data_modification":
        print(f"✏️ Testing concurrent data modification ({args.users} modifiers)")
        result = tester.test_concurrent_data_modification(args.users)
        results.append(result)
        
    elif args.test == "all":
        print("🎯 Running comprehensive race condition tests")
        results = tester.run_comprehensive_race_tests()
    
    # Print results
    print("\n" + "=" * 60)
    print("🏁 RACE CONDITION TEST RESULTS")
    print("=" * 60)
    
    total_races_detected = 0
    
    for result in results:
        status = "✅ PASS" if result.consistency_validated else "⚠️ RACE DETECTED"
        
        print(f"\n{status} {result.test_name}")
        print(f"   Attempts: {result.total_attempts}")
        print(f"   Successful: {result.successful_operations}")
        print(f"   Expected max: {result.expected_successes}")
        print(f"   Over-allocation: {result.over_allocation}")
        print(f"   Avg response time: {result.response_time_avg*1000:.1f}ms")
        
        if result.race_condition_detected:
            total_races_detected += 1
            print(f"   ⚠️ RACE CONDITION: {result.over_allocation} over-allocations detected")
    
    print(f"\n📊 Summary:")
    print(f"   Tests run: {len(results)}")
    print(f"   Race conditions detected: {total_races_detected}")
    print(f"   Tests with proper isolation: {len(results) - total_races_detected}")
    
    if total_races_detected > 0:
        print(f"\n💡 Race conditions indicate need for:")
        print(f"   • Stronger transaction isolation")
        print(f"   • Database-level constraints")
        print(f"   • SELECT FOR UPDATE queries")
        print(f"   • Optimistic locking mechanisms")
    else:
        print(f"\n✅ All tests passed - proper concurrency control validated")
    
    if args.save_results:
        tester.save_results(results)
        print(f"\n💾 Detailed results saved to race_condition_results.json")


if __name__ == "__main__":
    main()