# Concurrent User Testing for Module B

This directory contains comprehensive concurrent user simulation tools for testing the FastAPI application under multi-user load with the ACID-compliant transaction system.

## Features

### 🔄 **Threading-Based Testing** (`concurrent_users.py`)
- **Multi-threaded** user simulation using ThreadPoolExecutor
- **Realistic scenarios**: Registration, job applications, interview booking
- **Race condition detection** for limited resources
- **Performance metrics** collection and analysis
- **Up to 50 concurrent users** (configurable)

### ⚡ **Async-Based Testing** (`async_users.py`)
- **High-performance** async testing using aiohttp
- **Handles 500+ concurrent users** efficiently
- **Burst testing** for race condition scenarios
- **Advanced metrics** including peak concurrency tracking
- **Ramp-up capabilities** for realistic load patterns

## Quick Start

1. **Ensure FastAPI server is running:**
   ```bash
   cd ../../  # Go to project root
   python -m uvicorn main:app --reload --port 8000
   ```

2. **Run demonstration tests:**
   ```bash
   cd tests/concurrent
   python test_runner.py
   ```

## Individual Test Scripts

### Threading-Based Tests
```bash
# Basic job application race condition (20 users)
python concurrent_users.py --users 20 --scenario job_application

# Mixed operations for 60 seconds (30 users)  
python concurrent_users.py --users 30 --scenario mixed_operations --duration 60

# New user registration load test (15 users)
python concurrent_users.py --users 15 --scenario new_user_registration --save-results
```

### Async-Based Tests
```bash
# High-speed user registration (100 users)
python async_users.py --users 100 --scenario rapid_registration

# Job application burst test (50 users)
python async_users.py --users 50 --scenario job_application_burst

# Stress test with ramp-up (200 users over 60s, 10s ramp-up)
python async_users.py --users 200 --scenario stress_test --duration 60 --ramp-up 10
```

## Test Scenarios

### **new_user_registration / rapid_registration**
- Simulates new users registering and exploring the system
- Tests registration endpoint under load
- Validates user onboarding flow

### **job_application / job_application_burst** 
- **Critical for race condition testing**
- Multiple users apply for same limited-slot job
- Detects over-booking and consistency issues
- Tests ACID properties under contention

### **interview_booking**
- Limited interview slots with multiple applicants
- Tests resource allocation under concurrent access
- Validates booking system integrity

### **mixed_operations / realistic_browsing**
- Combination of browsing, applying, checking status
- Realistic user behavior patterns
- Tests system under varied load

### **stress_test**
- Intensive rapid operations
- Maximum load testing
- System stability validation

## Key Metrics

### **Performance Metrics**
- **Actions per second**: Overall system throughput
- **Response time**: Average, min, max response times
- **Success rate**: Percentage of successful operations
- **Concurrent users**: Peak concurrent user count

### **Correctness Metrics**
- **Race conditions detected**: Over-booking of limited resources
- **Transaction failures**: Failed atomic operations
- **Data consistency**: Validation of ACID properties
- **Error breakdown**: Categorization of failure types

## Integration with Module A

The concurrent tests validate that the **Module A transaction system** works correctly under **Module B API load**:

### **ACID Property Testing**
- **Atomicity**: Multi-table transactions complete fully or rollback completely
- **Consistency**: Database constraints maintained under concurrent load
- **Isolation**: Concurrent transactions don't interfere with each other
- **Durability**: Committed data persists across system restarts

### **Transaction System Validation**
- **Lock contention**: Tests lock manager under concurrent access
- **WAL performance**: Write-ahead logging under high transaction volume  
- **Recovery scenarios**: System behavior during failures
- **Deadlock handling**: Prevention and detection mechanisms

## Expected Results

### **Successful Scenarios**
- **95%+ success rate** for normal operations
- **Race condition detection** for over-booking scenarios
- **Consistent response times** under load
- **No data corruption** or partial transaction states

### **Race Condition Scenarios**
- **Job applications**: Some users will fail when job slots are full
- **Interview booking**: Only one user per time slot should succeed
- **System behavior**: Graceful handling of resource exhaustion

## Troubleshooting

### **Common Issues**
1. **Connection refused**: Ensure FastAPI server is running on port 8000
2. **High failure rate**: Check if database/transaction system is initialized
3. **Slow performance**: Adjust concurrent user limits or add delays
4. **Module A errors**: Verify transaction system components are working

### **Performance Tuning**
- **Threading tests**: Adjust `max_workers` parameter (default: 50)
- **Async tests**: Modify `max_concurrent` semaphore (default: 500)  
- **Network timeouts**: Configure aiohttp timeout settings
- **Database connections**: Ensure adequate connection pool size

## Output Files

Tests can save detailed results to JSON files:
- `concurrent_[scenario]_[users]users.json` - Threading test results
- `async_[scenario]_[users]users.json` - Async test results
- `concurrent_tests.log` - Threading test logs
- `async_concurrent_tests.log` - Async test logs

## Integration Testing

These tools integrate with:
- **Module A**: Transaction system, WAL, lock manager, recovery
- **Module B**: FastAPI endpoints, user authentication, job management
- **Performance monitoring**: Real-time metrics from Module A
- **Failure injection**: Crash simulation and recovery testing

## Next Steps

1. **Scale up testing**: Run with 100-1000+ users
2. **Add failure injection**: Simulate crashes during load
3. **Monitor Module A metrics**: Transaction latency, lock contention
4. **Stress test combinations**: Multiple scenario types simultaneously
5. **Network simulation**: Add latency/packet loss for realistic conditions