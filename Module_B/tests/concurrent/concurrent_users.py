"""
Concurrent User Simulation Scripts for Module B
===============================================

This module provides comprehensive concurrent testing capabilities
for the FastAPI application, simulating real-world multi-user scenarios
with the ACID-compliant transaction system from Module A.

Key Features:
- Multi-threaded user simulation using threading
- Async user simulation using asyncio
- Real-world scenarios: registration, job applications, interviews
- Performance metrics collection
- Race condition detection
- Deadlock testing

Usage:
    python concurrent_users.py --users 10 --scenario job_application
    python async_users.py --users 50 --duration 60
"""

import threading
import asyncio
import aiohttp
import requests
import time
import random
import json
import logging
import sys
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics

MODULE_B_ROOT = Path(__file__).resolve().parents[2]
if str(MODULE_B_ROOT) not in sys.path:
    sys.path.insert(0, str(MODULE_B_ROOT))

from app.db import fetch_one

# Configure logging for concurrent operations
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('concurrent_tests.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class UserAction:
    """Represents a single user action with timing and result."""
    user_id: str
    action: str
    start_time: float
    end_time: float
    success: bool
    response_code: int
    error_message: str = ""
    data: Dict[str, Any] = None
    
    @property
    def duration(self) -> float:
        """Action duration in seconds."""
        return self.end_time - self.start_time


@dataclass
class TestResults:
    """Aggregated results from concurrent user tests."""
    total_actions: int
    successful_actions: int
    failed_actions: int
    average_response_time: float
    min_response_time: float
    max_response_time: float
    actions_per_second: float
    concurrent_users: int
    test_duration: float
    race_conditions_detected: int = 0
    deadlocks_detected: int = 0
    
    @property
    def success_rate(self) -> float:
        """Success rate as percentage."""
        return (self.successful_actions / self.total_actions * 100) if self.total_actions > 0 else 0.0


class APIClient:
    """Thread-safe API client for concurrent testing."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()

    def authenticate(self, username: str, password: str) -> Dict[str, Any]:
        """Authenticate once and store the session token for later requests."""
        response = self.session.post(f"{self.base_url}/login", json={"username": username, "password": password})
        data = response.json()
        if response.status_code == 200 and "session_token" in data:
            self.session.headers.update({"x-session-token": data["session_token"]})
        return {"status_code": response.status_code, "data": data}
        
    def register_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Register a new user through the Module B admin member endpoint."""
        payload = {
            "username": user_data.get("username") or user_data.get("email", "user@example.com").split("@")[0],
            "email": user_data.get("email", f"{user_data.get('name', 'user')}@example.com"),
            "password": user_data.get("password", "password123"),
            "full_name": user_data.get("full_name") or user_data.get("name", "Test User"),
            "role_name": user_data.get("role_name", "Student"),
            "is_active": True,
            "latest_cpi": user_data.get("latest_cpi", 8.0),
            "program": user_data.get("program", "CSE"),
            "discipline": user_data.get("discipline", "CSE"),
            "graduating_year": user_data.get("graduating_year", 2026),
            "active_backlogs": user_data.get("active_backlogs", 0),
            "bio": user_data.get("bio", ""),
            "skills": user_data.get("skills", ""),
            "portfolio_visibility": user_data.get("portfolio_visibility", "private"),
            "group_ids": user_data.get("group_ids", []),
        }
        response = self.session.post(f"{self.base_url}/members", json=payload)
        return {"status_code": response.status_code, "data": response.json()}
    
    def login_user(self, credentials: Dict[str, str]) -> Dict[str, Any]:
        """Login user and get token."""
        response = self.session.post(f"{self.base_url}/login", json=credentials)
        return {"status_code": response.status_code, "data": response.json()}
    
    def get_jobs(self) -> Dict[str, Any]:
        """Get available job postings."""
        response = self.session.get(f"{self.base_url}/jobs")
        return {"status_code": response.status_code, "data": response.json()}

    def get_members(self) -> Dict[str, Any]:
        """Get available student members."""
        response = self.session.get(f"{self.base_url}/members")
        return {"status_code": response.status_code, "data": response.json()}
    
    def apply_for_job(self, job_id: int, student_id: int) -> Dict[str, Any]:
        """Apply for a job (critical section - limited slots)."""
        application_data = {
            "job_id": job_id,
            "student_id": student_id,
            "status": "applied"
        }
        response = self.session.post(f"{self.base_url}/applications", json=application_data)
        return {"status_code": response.status_code, "data": response.json()}
    
    def book_interview(self, application_id: int, slot: str) -> Dict[str, Any]:
        """Update application status to simulate a critical booking/update step."""
        booking_data = {"status": slot}
        response = self.session.patch(f"{self.base_url}/applications/{application_id}", json=booking_data)
        return {"status_code": response.status_code, "data": response.json()}
    
    def get_student_applications(self, student_id: int) -> Dict[str, Any]:
        """Get student's applications."""
        response = self.session.get(f"{self.base_url}/applications")
        return {"status_code": response.status_code, "data": response.json()}


class ConcurrentUserSimulator:
    """Simulate multiple concurrent users performing various actions."""
    
    def __init__(self, base_url: str = "http://localhost:8000", max_workers: int = 50):
        self.base_url = base_url
        self.max_workers = max_workers
        self.results: List[UserAction] = []
        self.results_lock = threading.Lock()
        self.shared_job_id: Optional[int] = None
        self.shared_student_id: Optional[int] = None
        self.available_job_ids: List[int] = []
        self.available_student_ids: List[int] = []

    @staticmethod
    def _extract_first_id(rows: Any, key: str) -> Optional[int]:
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict) and key in row:
                    try:
                        return int(row[key])
                    except (TypeError, ValueError):
                        continue
        if isinstance(rows, dict):
            for candidate_key in ("items", "data", "results"):
                if candidate_key in rows:
                    return ConcurrentUserSimulator._extract_first_id(rows[candidate_key], key)
        return None

    @staticmethod
    def _extract_ids(rows: Any, key: str) -> List[int]:
        ids: List[int] = []
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict) and key in row:
                    try:
                        ids.append(int(row[key]))
                    except (TypeError, ValueError):
                        continue
            return ids
        if isinstance(rows, dict):
            for candidate_key in ("items", "data", "results"):
                if candidate_key in rows:
                    return ConcurrentUserSimulator._extract_ids(rows[candidate_key], key)
        return ids

    @staticmethod
    def _user_index(user_id: str) -> int:
        try:
            return int(user_id.split("_")[-1])
        except (TypeError, ValueError):
            return 0
        
    def _record_action(self, action: UserAction):
        """Thread-safe action recording."""
        with self.results_lock:
            self.results.append(action)
            
    def _execute_user_scenario(self, user_id: str, scenario: str) -> List[UserAction]:
        """Execute a complete user scenario."""
        client = APIClient(self.base_url)
        auth_result = client.authenticate("admin", "admin123")
        if auth_result["status_code"] != 200:
            raise RuntimeError(f"Login failed: {auth_result['data']}")
        actions = []
        
        try:
            if scenario == "new_user_registration":
                actions.extend(self._new_user_registration_scenario(client, user_id))
            elif scenario == "job_application":
                actions.extend(self._job_application_scenario(client, user_id))
            elif scenario == "interview_booking":
                actions.extend(self._interview_booking_scenario(client, user_id))
            elif scenario == "mixed_operations":
                actions.extend(self._mixed_operations_scenario(client, user_id))
            else:
                raise ValueError(f"Unknown scenario: {scenario}")
                
        except Exception as e:
            logger.error(f"User {user_id} scenario failed: {e}")
            actions.append(UserAction(
                user_id=user_id,
                action="scenario_error",
                start_time=time.time(),
                end_time=time.time(),
                success=False,
                response_code=500,
                error_message=str(e)
            ))
            
        return actions
    
    def _new_user_registration_scenario(self, client: APIClient, user_id: str) -> List[UserAction]:
        """Simulate new user registration and initial exploration."""
        actions = []
        
        # Step 1: Register as student
        start_time = time.time()
        user_data = {
            "name": f"TestUser_{user_id}",
            "email": f"test_{user_id}@example.com",
            "password": f"password_{user_id}",
            "role_name": "Student",
            "student_id": int(user_id.split('_')[-1]) + 1000
        }
        
        result = client.register_user(user_data)
        end_time = time.time()
        
        actions.append(UserAction(
            user_id=user_id,
            action="register",
            start_time=start_time,
            end_time=end_time,
            success=result["status_code"] in [200, 201],
            response_code=result["status_code"],
            data=result.get("data")
        ))
        
        # Step 2: Login
        time.sleep(random.uniform(0.1, 0.5))  # Realistic delay
        start_time = time.time()
        login_result = client.login_user({
            "username": user_data["username"] if "username" in user_data else user_data["email"].split("@")[0],
            "password": user_data["password"]
        })
        end_time = time.time()
        
        actions.append(UserAction(
            user_id=user_id,
            action="login",
            start_time=start_time,
            end_time=end_time,
            success=login_result["status_code"] == 200,
            response_code=login_result["status_code"]
        ))
        
        # Step 3: Browse jobs
        time.sleep(random.uniform(0.5, 2.0))
        start_time = time.time()
        jobs_result = client.get_jobs()
        end_time = time.time()
        
        actions.append(UserAction(
            user_id=user_id,
            action="browse_jobs",
            start_time=start_time,
            end_time=end_time,
            success=jobs_result["status_code"] == 200,
            response_code=jobs_result["status_code"],
            data=jobs_result.get("data")
        ))
        
        return actions
    
    def _job_application_scenario(self, client: APIClient, user_id: str) -> List[UserAction]:
        """Simulate competitive job application (race condition prone)."""
        actions = []
        
        # Get available jobs first
        jobs_result = client.get_jobs()
        if jobs_result["status_code"] != 200 or not jobs_result["data"]:
            return []
        
        # Use a single fresh student and one real job so the first request can succeed
        job_id = self.shared_job_id or self._extract_first_id(jobs_result["data"], "job_id") or 100
        student_id = self.shared_student_id or self._extract_first_id(client.get_members().get("data"), "member_id") or 1
        
        # Apply for the job
        start_time = time.time()
        result = client.apply_for_job(job_id, student_id)
        end_time = time.time()
        
        actions.append(UserAction(
            user_id=user_id,
            action="apply_for_job",
            start_time=start_time,
            end_time=end_time,
            success=result["status_code"] in [200, 201],
            response_code=result["status_code"],
            data={"job_id": job_id, "student_id": student_id},
            error_message=result.get("data", {}).get("error", "")
        ))
        
        return actions
    
    def _interview_booking_scenario(self, client: APIClient, user_id: str) -> List[UserAction]:
        """Simulate interview slot booking (limited availability)."""
        actions = []

        # Reuse the admin view of applications to find a real application record
        apps_result = client.get_student_applications(0)
        if apps_result["status_code"] != 200:
            return []
        
        applications = apps_result.get("data", [])
        if not applications:
            return []
        
        # Try to book interview for first application
        application_id = applications[0].get("application_id") or applications[0].get("id")
        if not application_id:
            return []
            
        # All users compete for the same time slot
        interview_slot = "2024-04-01 10:00"
        
        start_time = time.time()
        result = client.book_interview(application_id, interview_slot)
        end_time = time.time()
        
        actions.append(UserAction(
            user_id=user_id,
            action="book_interview",
            start_time=start_time,
            end_time=end_time,
            success=result["status_code"] in [200, 201],
            response_code=result["status_code"],
            data={"application_id": application_id, "slot": interview_slot}
        ))
        
        return actions
    
    def _mixed_operations_scenario(self, client: APIClient, user_id: str) -> List[UserAction]:
        """Mix of different operations for realistic usage patterns."""
        actions = []
        
        # Random sequence of operations
        operations = ["browse_jobs", "apply_for_job", "check_applications"]
        random.shuffle(operations)
        
        for operation in operations:
            time.sleep(random.uniform(0.1, 1.0))
            
            if operation == "browse_jobs":
                start_time = time.time()
                result = client.get_jobs()
                end_time = time.time()
                
                actions.append(UserAction(
                    user_id=user_id,
                    action="browse_jobs",
                    start_time=start_time,
                    end_time=end_time,
                    success=result["status_code"] == 200,
                    response_code=result["status_code"]
                ))
                
            elif operation == "apply_for_job":
                user_idx = self._user_index(user_id)
                if self.available_job_ids:
                    job_id = self.available_job_ids[user_idx % len(self.available_job_ids)]
                else:
                    jobs_result = client.get_jobs()
                    job_id = self._extract_first_id(jobs_result.get("data"), "job_id") or 100

                if self.available_student_ids:
                    student_id = self.available_student_ids[user_idx % len(self.available_student_ids)]
                else:
                    members_result = client.get_members()
                    student_id = self._extract_first_id(members_result.get("data"), "member_id") or 1
                
                start_time = time.time()
                result = client.apply_for_job(job_id, student_id)
                end_time = time.time()
                
                actions.append(UserAction(
                    user_id=user_id,
                    action="apply_for_job",
                    start_time=start_time,
                    end_time=end_time,
                    success=result["status_code"] in [200, 201],
                    response_code=result["status_code"]
                ))
                
        return actions
    
    def run_concurrent_test(self, num_users: int, scenario: str, duration: Optional[float] = None) -> TestResults:
        """Run concurrent user simulation test."""
        logger.info(f"Starting concurrent test: {num_users} users, scenario: {scenario}")
        
        start_time = time.time()
        self.results.clear()

        seed_client = APIClient(self.base_url)
        auth_result = seed_client.authenticate("admin", "admin123")
        if auth_result["status_code"] != 200:
            raise RuntimeError(f"Login failed: {auth_result['data']}")

        jobs_result = seed_client.get_jobs()
        self.shared_job_id = self._extract_first_id(jobs_result.get("data"), "job_id") or 100
        self.available_job_ids = self._extract_ids(jobs_result.get("data"), "job_id")

        pair = fetch_one(
            """
            SELECT j.job_id, s.student_id
            FROM job_postings j
            JOIN students s ON 1=1
            LEFT JOIN applications a ON a.job_id = j.job_id AND a.student_id = s.student_id
            WHERE a.application_id IS NULL
            LIMIT 1
            """
        )
        if pair:
            self.shared_job_id = int(pair["job_id"])
            self.shared_student_id = int(pair["student_id"])
        else:
            members_result = seed_client.get_members()
            self.shared_student_id = self._extract_first_id(members_result.get("data"), "member_id") or 1

        if not self.available_student_ids:
            members_result = seed_client.get_members()
            self.available_student_ids = self._extract_ids(members_result.get("data"), "member_id")
        
        # Create user tasks
        user_ids = [f"user_{i}" for i in range(num_users)]
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all user scenarios
            future_to_user = {
                executor.submit(self._execute_user_scenario, user_id, scenario): user_id
                for user_id in user_ids
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_user, timeout=duration):
                user_id = future_to_user[future]
                try:
                    user_actions = future.result()
                    for action in user_actions:
                        self._record_action(action)
                except Exception as e:
                    logger.error(f"User {user_id} failed: {e}")
        
        end_time = time.time()
        test_duration = end_time - start_time
        
        # Analyze results
        return self._analyze_results(test_duration, num_users)
    
    def _analyze_results(self, test_duration: float, concurrent_users: int) -> TestResults:
        """Analyze collected test results."""
        if not self.results:
            return TestResults(
                total_actions=0,
                successful_actions=0,
                failed_actions=0,
                average_response_time=0.0,
                min_response_time=0.0,
                max_response_time=0.0,
                actions_per_second=0.0,
                concurrent_users=concurrent_users,
                test_duration=test_duration
            )
        
        successful = [r for r in self.results if r.success]
        failed = [r for r in self.results if not r.success]
        durations = [r.duration for r in self.results]
        
        # Detect race conditions (multiple successes for same limited resource)
        race_conditions = self._detect_race_conditions()
        
        return TestResults(
            total_actions=len(self.results),
            successful_actions=len(successful),
            failed_actions=len(failed),
            average_response_time=statistics.mean(durations) if durations else 0.0,
            min_response_time=min(durations) if durations else 0.0,
            max_response_time=max(durations) if durations else 0.0,
            actions_per_second=len(self.results) / test_duration if test_duration > 0 else 0.0,
            concurrent_users=concurrent_users,
            test_duration=test_duration,
            race_conditions_detected=race_conditions
        )
    
    def _detect_race_conditions(self) -> int:
        """Detect potential race conditions in the results."""
        # Group by action type and check for suspicious patterns
        job_applications = [r for r in self.results if r.action == "apply_for_job" and r.success]
        
        # Check if too many applications succeeded for the same job
        job_counts = {}
        for action in job_applications:
            if action.data:
                job_id = action.data.get("job_id")
                job_counts[job_id] = job_counts.get(job_id, 0) + 1
        
        # Assume each job has max 3 slots (would be configurable in real system)
        race_conditions = sum(1 for count in job_counts.values() if count > 3)
        
        if race_conditions > 0:
            logger.warning(f"Detected {race_conditions} potential race conditions in job applications")
        
        return race_conditions
    
    def save_results_to_file(self, filename: str = "concurrent_test_results.json"):
        """Save detailed results to JSON file."""
        results_data = {
            "test_metadata": {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "total_actions": len(self.results)
            },
            "actions": [asdict(action) for action in self.results]
        }
        
        with open(filename, 'w') as f:
            json.dump(results_data, f, indent=2)
        
        logger.info(f"Detailed results saved to {filename}")


# Convenience functions for common test scenarios
def test_job_application_race_condition(num_users: int = 20) -> TestResults:
    """Test race conditions in job applications."""
    simulator = ConcurrentUserSimulator()
    return simulator.run_concurrent_test(num_users, "job_application")


def test_mixed_user_load(num_users: int = 50, duration: float = 60) -> TestResults:
    """Test mixed user operations under load."""
    simulator = ConcurrentUserSimulator()
    return simulator.run_concurrent_test(num_users, "mixed_operations", duration)


def test_new_user_onboarding(num_users: int = 10) -> TestResults:
    """Test concurrent new user registrations."""
    simulator = ConcurrentUserSimulator()
    return simulator.run_concurrent_test(num_users, "new_user_registration")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run concurrent user simulation tests")
    parser.add_argument("--users", type=int, default=10, help="Number of concurrent users")
    parser.add_argument("--scenario", choices=[
        "new_user_registration", "job_application", "interview_booking", "mixed_operations"
    ], default="mixed_operations", help="Test scenario to run")
    parser.add_argument("--duration", type=float, help="Test duration in seconds")
    parser.add_argument("--save-results", action="store_true", help="Save detailed results to file")
    
    args = parser.parse_args()
    
    print(f">> Starting concurrent user simulation...")
    print(f"   Users: {args.users}")
    print(f"   Scenario: {args.scenario}")
    print(f"   Duration: {args.duration or 'unlimited'}")
    print()
    
    simulator = ConcurrentUserSimulator()
    results = simulator.run_concurrent_test(args.users, args.scenario, args.duration)
    
    # Print results
    print("=" * 60)
    print(">> CONCURRENT USER TEST RESULTS")
    print("=" * 60)
    print(f"[USERS] Concurrent Users: {results.concurrent_users}")
    print(f"[TIME] Test Duration: {results.test_duration:.2f}s")
    print(f"[STATS] Total Actions: {results.total_actions}")
    print(f"[SUCCESS] Successful: {results.successful_actions} ({results.success_rate:.1f}%)")
    print(f"[FAILED] Failed: {results.failed_actions}")
    print(f"[PERF] Actions/sec: {results.actions_per_second:.2f}")
    print(f"[LATENCY] Avg Response Time: {results.average_response_time*1000:.1f}ms")
    print(f"[RACE] Race Conditions: {results.race_conditions_detected}")
    print()
    
    if args.save_results:
        simulator.save_results_to_file(f"concurrent_{args.scenario}_{args.users}users.json")
        print(" Detailed results saved to file")