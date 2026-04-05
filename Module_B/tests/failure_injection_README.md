# Failure Injection System for Module B

Comprehensive failure injection and crash recovery testing system for validating ACID properties and system resilience under failure conditions.

## 🎯 **Overview**

The failure injection system enables controlled testing of:
- **Crash recovery** during concurrent operations
- **Transaction rollback** when failures occur mid-transaction
- **ACID property validation** under failure conditions
- **System resilience** and data consistency
- **Integration** of Module A (transactions) + Module B (API) under stress

## 🏗️ **System Architecture**

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   Failure Client    │    │  Concurrent Users   │    │ FastAPI + Module A  │
│                     │    │                     │    │                     │
│ • Trigger crashes   │    │ • Threading tests   │    │ • Transaction Mgr   │
│ • Configure delays  │────│ • Async tests       │────│ • WAL System        │
│ • Transaction tests │    │ • Load simulation   │    │ • Lock Manager      │
│ • Environment vars  │    │ • Race conditions   │    │ • Recovery System   │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
          │                          │                          │
          └──────────────────────────┼──────────────────────────┘
                                     │
                          ┌─────────────────────┐
                          │ Integrated Testing  │
                          │                     │
                          │ • Crash + Load      │
                          │ • Transaction Tests │
                          │ • ACID Validation   │
                          │ • Recovery Metrics  │
                          └─────────────────────┘
```

## 🚀 **Quick Start**

### 1. **Basic Crash Testing**
```bash
# Start FastAPI server
python -m uvicorn app.main:app --port 8000

# In another terminal, trigger immediate crash
cd tests
python failure_client.py --mode immediate

# Server crashes immediately, restart manually to test recovery
```

### 2. **Delayed Crash Testing**
```bash
# Enable crash with 5-second delay
python failure_client.py --mode immediate --delay 5

# Server will crash after 5 seconds, allowing time for operations
```

### 3. **Transaction Crash Testing**
```bash
# Crash during active transaction (tests rollback)
python failure_client.py --transaction-crash --delay 2

# Check audit logs after restart to verify transaction rollback
```

### 4. **Interactive Testing**
```bash
# Interactive testing interface
python failure_client.py --interactive

# Follow prompts to configure and trigger various crash scenarios
```

## 📡 **API Endpoints**

All failure injection endpoints require **admin authentication**.

### **Enable Failure Injection**
```http
POST /admin/failure-injection/enable
Content-Type: application/json

{
  "mode": "immediate",     // immediate|sigterm|sigkill|exception
  "delay": 5.0            // seconds to wait before crash
}
```

### **Trigger Crash**
```http
POST /admin/failure-injection/trigger
```
⚠️ **WARNING**: This will immediately crash the server!

### **Transaction Crash Test**
```http
POST /admin/failure-injection/transaction-crash
Content-Type: application/json

{
  "delay": 2.0  // seconds of transaction processing before crash
}
```

### **Check Status**
```http
GET /admin/failure-injection/status
```

### **Disable Failure Injection**
```http
POST /admin/failure-injection/disable
```

## 🔧 **Crash Modes**

| Mode | Behavior | Use Case |
|------|----------|----------|
| `immediate` | `os._exit(1)` - instant termination | Test crash recovery, no cleanup |
| `sigterm` | SIGTERM signal - graceful shutdown | Test planned shutdown scenarios |
| `sigkill` | SIGKILL signal - force kill | Test unexpected termination |
| `exception` | Unhandled Python exception | Test exception handling paths |

## 🌍 **Environment Variable Control**

Set environment variables before starting server for automated crash testing:

```bash
# Crash 5 seconds after startup
export CRASH_ON_STARTUP=true
export CRASH_DELAY=5
export CRASH_MODE=immediate
python -m uvicorn app.main:app

# Crash on any HTTP request
export CRASH_ON_REQUEST=true
export CRASH_DELAY=0
python -m uvicorn app.main:app

# Disable environment crashes
unset CRASH_ON_STARTUP
unset CRASH_ON_REQUEST
```

### **Environment Variables**
- `CRASH_ON_STARTUP`: `true` to crash after startup
- `CRASH_ON_REQUEST`: `true` to crash on first request
- `CRASH_DELAY`: Seconds to wait before crash (default: 0)
- `CRASH_MODE`: Crash type (default: immediate)
- `ENABLE_FAILURE_INJECTION`: Enable system-wide (default: false)

## 🧪 **Integrated Testing**

### **Test Types**

#### 1. **Crash During Concurrent Load**
Tests system behavior when crash occurs during active user sessions:

```bash
python integrated_failure_tests.py --test crash_during_load --users 20 --delay 8
```

**Scenario:**
1. Start 20 concurrent users performing job applications
2. Trigger crash after 8 seconds
3. Measure recovery time and data consistency
4. Validate no partial transactions remain

#### 2. **Transaction Rollback Testing**
Tests ACID atomicity when crash occurs mid-transaction:

```bash
python integrated_failure_tests.py --test transaction_rollback --delay 2
```

**Scenario:**
1. Start multi-table transaction
2. Perform partial operations
3. Crash before commit
4. Verify complete rollback after recovery

#### 3. **Async Load with Random Crashes**
Tests system resilience under random failure conditions:

```bash
python integrated_failure_tests.py --test async_random_crashes --users 50 --delay 60
```

**Scenario:**
1. High-concurrency async users (50+)
2. Random crashes during 60-second test
3. Measure availability and consistency
4. Validate graceful degradation

#### 4. **Full Test Suite**
```bash
python integrated_failure_tests.py --test all --save-results
```

## 📊 **Metrics and Validation**

### **Recovery Metrics**
- **Recovery Time**: Seconds from crash to server responsiveness
- **Data Consistency**: Validation of database integrity post-recovery
- **Transaction Recovery**: Count of properly rolled-back transactions
- **User Impact**: Success/failure rates during and after crashes

### **ACID Validation**
- **Atomicity**: No partial transactions after crash
- **Consistency**: Database constraints maintained
- **Isolation**: No corrupted concurrent operations
- **Durability**: Committed data survives crashes

### **Performance Impact**
- **Pre-crash Performance**: Baseline metrics before failure
- **Post-recovery Performance**: Performance after restart
- **Availability**: Percentage uptime during testing
- **Throughput**: Requests/second before and after crashes

## 🔒 **Security Features**

### **Admin-Only Access**
- All failure endpoints require admin role
- Session token authentication required
- All triggers logged in audit trail

### **Safety Mechanisms**
- Failure injection must be explicitly enabled
- Clear warnings about server termination
- Audit logging of all crash triggers
- Environment variable safeguards

### **Production Safety**
```python
# Disable in production
if os.getenv("ENVIRONMENT") == "production":
    # Failure injection endpoints return 404
    pass
```

## 📁 **File Structure**

```
tests/
├── failure_client.py           # Python client for failure control
├── integrated_failure_tests.py # Combined failure + load testing
├── concurrent/                 # Concurrent user testing
│   ├── concurrent_users.py    # Threading-based simulation
│   ├── async_users.py         # Async high-concurrency testing
│   └── README.md              # Concurrent testing docs
└── failure_injection_README.md # This file
```

## 🎯 **Test Scenarios**

### **Development Testing**
1. **Unit Recovery Tests**: Individual component crash recovery
2. **Integration Tests**: Module A + B under failure
3. **Load Tests**: Concurrent users + crashes
4. **ACID Tests**: Transaction integrity validation

### **Stress Testing**
1. **High Concurrency**: 100+ users with random crashes
2. **Sustained Load**: Long-duration tests with periodic failures
3. **Resource Exhaustion**: Memory/disk limits + crashes
4. **Network Failures**: Simulated network partitions

### **Chaos Engineering**
1. **Random Failures**: Unpredictable crash timing
2. **Cascade Failures**: Multiple component failures
3. **Recovery Storms**: Rapid failure/recovery cycles
4. **Data Corruption**: Partial write scenarios

## 🔧 **Troubleshooting**

### **Common Issues**

#### Server Won't Crash
- Check admin authentication
- Verify failure injection is enabled
- Check environment variable conflicts
- Ensure server process has proper permissions

#### Recovery Takes Too Long
- Check server startup configuration
- Verify database connectivity
- Monitor resource availability
- Check Module A recovery algorithms

#### Data Inconsistency After Crash
- Verify WAL system is enabled
- Check transaction coordinator setup
- Validate Module A integration
- Review audit logs for partial transactions

#### Tests Fail to Connect
- Ensure server is running on correct port
- Check authentication credentials
- Verify network connectivity
- Validate API endpoint availability

### **Debugging Commands**

```bash
# Check failure injection status
python failure_client.py --status

# Test server connectivity
curl http://localhost:8000/

# Check environment triggers
python failure_client.py --interactive
# Then select option 6

# Validate Module A integration
cd ../Module_A
python test_acid_validation.py
```

## 📈 **Performance Expectations**

### **Baseline Performance**
- **Normal Operation**: 15-25 TPS
- **Recovery Time**: <5 seconds for simple crashes
- **Data Consistency**: 100% after proper recovery
- **Availability**: >95% during planned testing

### **Under Failure Conditions**
- **Crash Impact**: Immediate service interruption
- **Recovery Impact**: 2-10 second downtime
- **Consistency**: Maintained through WAL recovery
- **Performance Degradation**: Temporary 10-20% reduction

## 🚨 **Safety Warnings**

⚠️ **PRODUCTION WARNING**: Never enable failure injection in production environments

⚠️ **DATA WARNING**: Crashes may cause data loss if Module A is not properly configured

⚠️ **PROCESS WARNING**: Failure injection terminates the server process immediately

⚠️ **TESTING WARNING**: Always have database backups before extensive crash testing

## 📚 **Integration with Module A**

The failure injection system is designed to work seamlessly with Module A's transaction system:

- **Transaction Manager**: Coordinates rollback during crash recovery
- **WAL System**: Provides crash recovery data integrity
- **Lock Manager**: Handles lock cleanup after crashes
- **Recovery Manager**: Orchestrates system restore after failures

### **Validation Flow**
1. **Pre-crash**: Capture transaction state
2. **Crash**: Trigger controlled failure
3. **Recovery**: Module A recovery algorithms execute
4. **Validation**: Verify ACID properties maintained
5. **Metrics**: Collect performance and consistency data

This system enables comprehensive testing of the entire DBMS under realistic failure conditions, ensuring both Module A and Module B work correctly together even when facing unexpected crashes and system failures.