"""
Asynchronous Concurrent User Simulation for Module B
====================================================

High-performance async testing using aiohttp for maximum concurrency.
Can handle hundreds or thousands of concurrent users efficiently.

Features:
- Async/await based concurrent execution
- Higher throughput than threading approach
- Realistic user behavior simulation
- Performance metrics and race condition detection
- Integrates with Module A transaction system testing

Usage:
    python async_users.py --users 100 --duration 60
    python async_users.py --users 500 --scenario stress_test
"""

import asyncio
import aiohttp
import time
import random
import json
import logging
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, AsyncIterator
import statistics
import argparse

# Configure async-safe logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('async_concurrent_tests.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class AsyncUserAction:
    """Async version of user action with additional concurrency metrics."""
    user_id: str
    action: str
    start_time: float
    end_time: float
    success: bool
    response_code: int
    error_message: str = ""
    data: Dict[str, Any] = None
    concurrent_users_at_time: int = 0
    
    @property
    def duration(self) -> float:
        return self.end_time - self.start_time


class AsyncAPIClient:
    """Async API client for high-concurrency testing."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self._session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        """Async context manager entry."""
        connector = aiohttp.TCPConnector(limit=200, limit_per_host=50)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"Content-Type": "application/json"}
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._session:
            await self._session.close()

    async def authenticate(self, username: str, password: str) -> Dict[str, Any]:
        """Authenticate once and store the session token for later requests."""
        try:
            async with self._session.post(
                f"{self.base_url}/login",
                json={"username": username, "password": password}
            ) as response:
                data = await response.json()
                if response.status == 200 and "session_token" in data:
                    self._session.headers["x-session-token"] = data["session_token"]
                return {"status_code": response.status, "data": data}
        except Exception as e:
            return {"status_code": 500, "data": {"error": str(e)}}
    
    async def register_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new member using the Module B admin endpoint."""
        payload = {
            "username": user_data.get("username") or user_data.get("email", "user@example.com").split("@")[0],
            "email": user_data.get("email", f"{user_data.get('name', 'user')}@example.com"),
            "password": user_data.get("password", "password123"),
            "full_name": user_data.get("full_name") or user_data.get("name", "Async User"),
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
        try:
            async with self._session.post(
                f"{self.base_url}/members",
                json=payload
            ) as response:
                data = await response.json()
                return {"status_code": response.status, "data": data}
        except Exception as e:
            return {"status_code": 500, "data": {"error": str(e)}}
    
    async def login_user(self, credentials: Dict[str, str]) -> Dict[str, Any]:
        """Async user login."""
        try:
            async with self._session.post(
                f"{self.base_url}/login",
                json=credentials
            ) as response:
                data = await response.json()
                return {"status_code": response.status, "data": data}
        except Exception as e:
            return {"status_code": 500, "data": {"error": str(e)}}
    
    async def get_jobs(self) -> Dict[str, Any]:
        """Async get job listings."""
        try:
            async with self._session.get(f"{self.base_url}/jobs") as response:
                data = await response.json()
                return {"status_code": response.status, "data": data}
        except Exception as e:
            return {"status_code": 500, "data": {"error": str(e)}}

    async def get_members(self) -> Dict[str, Any]:
        """Async get student members."""
        try:
            async with self._session.get(f"{self.base_url}/members") as response:
                data = await response.json()
                return {"status_code": response.status, "data": data}
        except Exception as e:
            return {"status_code": 500, "data": {"error": str(e)}}
    
    async def apply_for_job(self, job_id: int, student_id: int) -> Dict[str, Any]:
        """Async job application - critical section."""
        application_data = {
            "job_id": job_id,
            "student_id": student_id,
            "status": "applied"
        }
        try:
            async with self._session.post(
                f"{self.base_url}/applications",
                json=application_data
            ) as response:
                data = await response.json()
                return {"status_code": response.status, "data": data}
        except Exception as e:
            return {"status_code": 500, "data": {"error": str(e)}}
    
    async def book_interview(self, application_id: int, slot: str) -> Dict[str, Any]:
        """Async application update to simulate a critical booking/update step."""
        booking_data = {"status": slot}
        try:
            async with self._session.patch(
                f"{self.base_url}/applications/{application_id}",
                json=booking_data
            ) as response:
                data = await response.json()
                return {"status_code": response.status, "data": data}
        except Exception as e:
            return {"status_code": 500, "data": {"error": str(e)}}
    
    async def get_student_applications(self, student_id: int) -> Dict[str, Any]:
        """Async get student applications."""
        try:
            async with self._session.get(f"{self.base_url}/applications") as response:
                data = await response.json()
                return {"status_code": response.status, "data": data}
        except Exception as e:
            return {"status_code": 500, "data": {"error": str(e)}}


class AsyncUserSimulator:
    """High-performance async user simulation."""
    
    def __init__(self, base_url: str = "http://localhost:8000", max_concurrent: int = 500):
        self.base_url = base_url
        self.max_concurrent = max_concurrent
        self.results: List[AsyncUserAction] = []
        self.active_users = 0
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self.available_job_ids: List[int] = []
        self.available_student_ids: List[int] = []

    @staticmethod
    def _extract_ids(rows: Any, key: str) -> List[int]:
        if isinstance(rows, list):
            values: List[int] = []
            for row in rows:
                if isinstance(row, dict) and key in row:
                    try:
                        values.append(int(row[key]))
                    except (TypeError, ValueError):
                        continue
            return values
        if isinstance(rows, dict):
            for candidate_key in ("items", "data", "results"):
                if candidate_key in rows:
                    return AsyncUserSimulator._extract_ids(rows[candidate_key], key)
        return []

    @staticmethod
    def _user_index(user_id: str) -> int:
        try:
            return int(user_id.split("_")[-1])
        except (TypeError, ValueError):
            return 0
        
    async def _execute_async_user_scenario(
        self, 
        user_id: str, 
        scenario: str,
        client: AsyncAPIClient
    ) -> List[AsyncUserAction]:
        """Execute async user scenario with concurrency control."""
        async with self._semaphore:
            self.active_users += 1
            try:
                if scenario == "rapid_registration":
                    return await self._rapid_registration_scenario(client, user_id)
                elif scenario == "job_application_burst":
                    return await self._job_application_burst_scenario(client, user_id)
                elif scenario == "stress_test":
                    return await self._stress_test_scenario(client, user_id)
                elif scenario == "realistic_browsing":
                    return await self._realistic_browsing_scenario(client, user_id)
                else:
                    raise ValueError(f"Unknown scenario: {scenario}")
            finally:
                self.active_users -= 1
    
    async def _rapid_registration_scenario(
        self, 
        client: AsyncAPIClient, 
        user_id: str
    ) -> List[AsyncUserAction]:
        """Rapid user registration to test registration endpoint under load."""
        actions = []
        
        user_data = {
            "username": f"async_{user_id}",
            "name": f"AsyncUser_{user_id}",
            "email": f"async_{user_id}@example.com",
            "password": f"async_password_{user_id}",
            "role_name": "Student",
            "student_id": int(user_id.split('_')[-1]) + 2000
        }
        
        start_time = time.time()
        result = await client.register_user(user_data)
        end_time = time.time()
        
        actions.append(AsyncUserAction(
            user_id=user_id,
            action="rapid_register",
            start_time=start_time,
            end_time=end_time,
            success=result["status_code"] in [200, 201],
            response_code=result["status_code"],
            data=result.get("data"),
            concurrent_users_at_time=self.active_users,
            error_message=result.get("data", {}).get("error", "")
        ))
        
        return actions
    
    async def _job_application_burst_scenario(
        self, 
        client: AsyncAPIClient, 
        user_id: str
    ) -> List[AsyncUserAction]:
        """Burst of job applications to test race conditions."""
        actions = []
        
        # All users compete for the same real job and student profile
        job_id = self.available_job_ids[0] if self.available_job_ids else 100
        student_id = self.available_student_ids[0] if self.available_student_ids else 1
        
        # Small random delay to create realistic timing variations
        await asyncio.sleep(random.uniform(0, 0.1))
        
        start_time = time.time()
        result = await client.apply_for_job(job_id, student_id)
        end_time = time.time()
        
        actions.append(AsyncUserAction(
            user_id=user_id,
            action="burst_apply",
            start_time=start_time,
            end_time=end_time,
            success=result["status_code"] in [200, 201],
            response_code=result["status_code"],
            data={"job_id": job_id, "student_id": student_id},
            concurrent_users_at_time=self.active_users,
            error_message=result.get("data", {}).get("error", "")
        ))
        
        return actions
    
    async def _stress_test_scenario(
        self, 
        client: AsyncAPIClient, 
        user_id: str
    ) -> List[AsyncUserAction]:
        """Intensive operations to stress test the system."""
        actions = []
        user_idx = self._user_index(user_id)

        if self.available_student_ids:
            student_id = self.available_student_ids[user_idx % len(self.available_student_ids)]
        else:
            student_id = 1

        if self.available_job_ids:
            job_id_1 = self.available_job_ids[user_idx % len(self.available_job_ids)]
            job_id_2 = self.available_job_ids[(user_idx + 1) % len(self.available_job_ids)]
        else:
            job_id_1, job_id_2 = 100, 101
        
        # Rapid sequence of operations
        operations = [
            ("browse_jobs", client.get_jobs),
            ("apply_job_1", lambda: client.apply_for_job(
                job_id_1,
                student_id,
            )),
            ("browse_jobs_2", client.get_jobs),
            ("apply_job_2", lambda: client.apply_for_job(
                job_id_2,
                student_id,
            )),
        ]
        
        for op_name, op_func in operations:
            # Minimal delay for stress testing
            await asyncio.sleep(random.uniform(0, 0.05))
            
            start_time = time.time()
            try:
                result = await op_func()
            except Exception as e:
                result = {"status_code": 500, "data": {"error": str(e)}}
            end_time = time.time()
            
            actions.append(AsyncUserAction(
                user_id=user_id,
                action=op_name,
                start_time=start_time,
                end_time=end_time,
                success=result["status_code"] in [200, 201],
                response_code=result["status_code"],
                concurrent_users_at_time=self.active_users
            ))
        
        return actions
    
    async def _realistic_browsing_scenario(
        self, 
        client: AsyncAPIClient, 
        user_id: str
    ) -> List[AsyncUserAction]:
        """Realistic user browsing behavior with think time."""
        actions = []
        
        # Browse jobs
        start_time = time.time()
        jobs_result = await client.get_jobs()
        end_time = time.time()
        
        actions.append(AsyncUserAction(
            user_id=user_id,
            action="browse_jobs",
            start_time=start_time,
            end_time=end_time,
            success=jobs_result["status_code"] == 200,
            response_code=jobs_result["status_code"],
            concurrent_users_at_time=self.active_users
        ))
        
        # Think time (user reads job descriptions)
        await asyncio.sleep(random.uniform(1, 3))
        
        # Apply to a random job
        if jobs_result["status_code"] == 200:
            job_pool = self.available_job_ids or [100, 101, 102, 103, 104]
            student_pool = self.available_student_ids or [1]
            job_id = random.choice(job_pool)
            student_id = random.choice(student_pool)
            
            start_time = time.time()
            apply_result = await client.apply_for_job(job_id, student_id)
            end_time = time.time()
            
            actions.append(AsyncUserAction(
                user_id=user_id,
                action="realistic_apply",
                start_time=start_time,
                end_time=end_time,
                success=apply_result["status_code"] in [200, 201],
                response_code=apply_result["status_code"],
                data={"job_id": job_id},
                concurrent_users_at_time=self.active_users
            ))
        
        return actions
    
    async def run_async_load_test(
        self, 
        num_users: int, 
        scenario: str, 
        duration: Optional[float] = None,
        ramp_up_time: float = 0
    ) -> Dict[str, Any]:
        """Run high-concurrency async load test."""
        logger.info(f"Starting async load test: {num_users} users, scenario: {scenario}")
        
        start_time = time.time()
        self.results.clear()
        
        async with AsyncAPIClient(self.base_url) as client:
            auth_result = await client.authenticate("admin", "admin123")
            if auth_result["status_code"] != 200:
                raise RuntimeError(f"Login failed: {auth_result['data']}")

            jobs_result = await client.get_jobs()
            self.available_job_ids = self._extract_ids(jobs_result.get("data"), "job_id") or [100]

            members_result = await client.get_members()
            self.available_student_ids = self._extract_ids(members_result.get("data"), "member_id") or [1]

            # Create user tasks with optional ramp-up
            tasks = []
            
            for i in range(num_users):
                user_id = f"async_user_{i}"
                
                # Ramp up users gradually if specified
                if ramp_up_time > 0:
                    delay = (i / num_users) * ramp_up_time
                    task = asyncio.create_task(self._delayed_user_scenario(
                        user_id, scenario, client, delay
                    ))
                else:
                    task = asyncio.create_task(
                        self._execute_async_user_scenario(user_id, scenario, client)
                    )
                tasks.append(task)
            
            # Run with timeout if specified
            try:
                if duration:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True),
                        timeout=duration
                    )
                else:
                    await asyncio.gather(*tasks, return_exceptions=True)
            except asyncio.TimeoutError:
                logger.info(f"Test timed out after {duration}s")
                # Cancel remaining tasks
                for task in tasks:
                    if not task.done():
                        task.cancel()
        
        end_time = time.time()
        test_duration = end_time - start_time
        
        # Collect results from completed tasks
        for task in tasks:
            if task.done() and not task.cancelled():
                try:
                    user_actions = task.result()
                    if isinstance(user_actions, list):
                        self.results.extend(user_actions)
                except Exception as e:
                    logger.error(f"Task failed: {e}")
        
        return self._analyze_async_results(test_duration, num_users)
    
    async def _delayed_user_scenario(
        self, 
        user_id: str, 
        scenario: str, 
        client: AsyncAPIClient, 
        delay: float
    ) -> List[AsyncUserAction]:
        """Execute user scenario with initial delay for ramp-up."""
        await asyncio.sleep(delay)
        return await self._execute_async_user_scenario(user_id, scenario, client)
    
    def _analyze_async_results(self, test_duration: float, num_users: int) -> Dict[str, Any]:
        """Analyze async test results."""
        if not self.results:
            return {
                "total_actions": 0,
                "successful_actions": 0,
                "failed_actions": 0,
                "success_rate": 0.0,
                "avg_response_time": 0.0,
                "actions_per_second": 0.0,
                "concurrent_users": num_users,
                "test_duration": test_duration,
                "peak_concurrent_users": 0,
                "race_conditions_detected": 0
            }
        
        successful = [r for r in self.results if r.success]
        failed = [r for r in self.results if not r.success]
        durations = [r.duration for r in self.results]
        concurrent_levels = [r.concurrent_users_at_time for r in self.results]
        
        # Detect race conditions in burst scenarios
        race_conditions = self._detect_async_race_conditions()
        
        return {
            "total_actions": len(self.results),
            "successful_actions": len(successful),
            "failed_actions": len(failed),
            "success_rate": (len(successful) / len(self.results) * 100) if self.results else 0.0,
            "avg_response_time": statistics.mean(durations) if durations else 0.0,
            "min_response_time": min(durations) if durations else 0.0,
            "max_response_time": max(durations) if durations else 0.0,
            "actions_per_second": len(self.results) / test_duration if test_duration > 0 else 0.0,
            "concurrent_users": num_users,
            "test_duration": test_duration,
            "peak_concurrent_users": max(concurrent_levels) if concurrent_levels else 0,
            "race_conditions_detected": race_conditions,
            "error_breakdown": self._get_error_breakdown()
        }
    
    def _detect_async_race_conditions(self) -> int:
        """Detect race conditions in async results."""
        # Focus on burst application scenarios
        burst_applications = [
            r for r in self.results 
            if r.action == "burst_apply" and r.success and r.data
        ]
        
        # Count applications per job
        job_counts = {}
        for action in burst_applications:
            job_id = action.data.get("job_id")
            if job_id:
                job_counts[job_id] = job_counts.get(job_id, 0) + 1
        
        # Detect over-booking (assuming max 3 slots per job)
        race_conditions = sum(1 for count in job_counts.values() if count > 3)
        
        if race_conditions > 0:
            logger.warning(f"Async test detected {race_conditions} race conditions")
            for job_id, count in job_counts.items():
                if count > 3:
                    logger.warning(f"Job {job_id}: {count} applications (expected ≤ 3)")
        
        return race_conditions
    
    def _get_error_breakdown(self) -> Dict[str, int]:
        """Get breakdown of error types."""
        errors = {}
        for result in self.results:
            if not result.success:
                error_type = f"HTTP_{result.response_code}"
                errors[error_type] = errors.get(error_type, 0) + 1
        return errors
    
    def save_async_results(self, filename: str = "async_test_results.json"):
        """Save async test results to file."""
        results_data = {
            "test_metadata": {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "test_type": "async_concurrent",
                "total_actions": len(self.results)
            },
            "actions": [asdict(action) for action in self.results]
        }
        
        with open(filename, 'w') as f:
            json.dump(results_data, f, indent=2)
        
        logger.info(f"Async results saved to {filename}")


# High-level async test functions
async def test_registration_load(num_users: int = 100) -> Dict[str, Any]:
    """Test user registration under high load."""
    simulator = AsyncUserSimulator()
    return await simulator.run_async_load_test(num_users, "rapid_registration")


async def test_job_application_race(num_users: int = 50) -> Dict[str, Any]:
    """Test job application race conditions."""
    simulator = AsyncUserSimulator()
    return await simulator.run_async_load_test(num_users, "job_application_burst")


async def test_system_stress(num_users: int = 200, duration: float = 60) -> Dict[str, Any]:
    """Intensive stress test."""
    simulator = AsyncUserSimulator()
    return await simulator.run_async_load_test(
        num_users, "stress_test", duration=duration, ramp_up_time=10
    )


async def main():
    """Main async test runner."""
    parser = argparse.ArgumentParser(description="Run async concurrent user tests")
    parser.add_argument("--users", type=int, default=100, help="Number of concurrent users")
    parser.add_argument("--scenario", choices=[
        "rapid_registration", "job_application_burst", "stress_test", "realistic_browsing"
    ], default="realistic_browsing", help="Test scenario")
    parser.add_argument("--duration", type=float, help="Test duration in seconds")
    parser.add_argument("--ramp-up", type=float, default=0, help="Ramp-up time in seconds")
    parser.add_argument("--save-results", action="store_true", help="Save results to file")
    
    args = parser.parse_args()
    
    print(f" Starting ASYNC concurrent user simulation...")
    print(f"   Users: {args.users}")
    print(f"   Scenario: {args.scenario}")
    print(f"   Duration: {args.duration or 'unlimited'}")
    print(f"   Ramp-up: {args.ramp_up}s")
    print()
    
    simulator = AsyncUserSimulator()
    results = await simulator.run_async_load_test(
        args.users, args.scenario, args.duration, args.ramp_up
    )
    
    # Print results
    print("=" * 70)
    print(" ASYNC CONCURRENT USER TEST RESULTS")
    print("=" * 70)
    print(f" Concurrent Users: {results['concurrent_users']}")
    print(f" Peak Concurrent: {results['peak_concurrent_users']}")
    print(f"  Test Duration: {results['test_duration']:.2f}s")
    print(f" Total Actions: {results['total_actions']}")
    print(f" Successful: {results['successful_actions']} ({results['success_rate']:.1f}%)")
    print(f" Failed: {results['failed_actions']}")
    print(f" Actions/sec: {results['actions_per_second']:.2f}")
    print(f"  Avg Response: {results['avg_response_time']*1000:.1f}ms")
    print(f" Race Conditions: {results['race_conditions_detected']}")
    
    if results['error_breakdown']:
        print("\n Error Breakdown:")
        for error_type, count in results['error_breakdown'].items():
            print(f"   {error_type}: {count}")
    
    print()
    
    if args.save_results:
        filename = f"async_{args.scenario}_{args.users}users.json"
        simulator.save_async_results(filename)
        print(f" Results saved to {filename}")


if __name__ == "__main__":
    asyncio.run(main())