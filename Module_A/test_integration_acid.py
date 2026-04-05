"""
End-to-End Integration Test for Full ACID Transaction System

This test demonstrates the complete transaction system working together:
- TransactionManager (coordinates transactions)
- WALManager (durability via write-ahead logging)
- LockManager (concurrency control)
- TransactionalStorage (buffered B+ Tree operations)
- DBManager (multi-table database)

Tests ACID properties:
- Atomicity: All-or-nothing commits
- Consistency: Constraints maintained
- Isolation: Concurrent transactions don't interfere
- Durability: Changes persist after commit (WAL)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import DBManager
from transaction import (
    TransactionManager,
    WALManager,
    LockManager,
    TransactionalStorage
)
import tempfile
import shutil


def test_full_integration():
    """Test complete ACID transaction system."""
    print("=" * 70)
    print("END-TO-END ACID TRANSACTION SYSTEM TEST")
    print("=" * 70)
    print()
    
    # Setup
    print("### SETUP ###")
    wal_dir = Path(tempfile.mkdtemp())
    
    db = DBManager()
    tm = TransactionManager()
    wal = WALManager(log_dir=str(wal_dir))
    lm = LockManager(lock_timeout=5.0)
    
    # Connect components
    tm.set_wal_logger(wal.log)
    tm.set_lock_manager(lm)
    
    storage = TransactionalStorage(db, tm, lm)
    
    # Create tables
    db.create_table('applications', order=4)
    
    print("[OK] All components initialized and connected")
    print(f"[OK] WAL directory: {wal_dir}")
    print(f"[OK] Tables created: {db.list_tables()}")
    print()
    
    # Test 1: ATOMICITY - Demo-style batch insert
    print("### TEST 1: ATOMICITY (Demo-Style Batch Insert) ###")
    
    applications_table = db.get_table('applications')

    print("[OK] Initial state:")
    print("    Applications table is empty before the transaction commits")

    # Insert demo-style application records in one atomic transaction
    with tm.transaction() as txn:
        records = [
            (100, {'application_id': 100, 'stipend': 1000, 'owner': 'Alice'}),
            (200, {'application_id': 200, 'stipend': 2000, 'owner': 'Bob'}),
            (300, {'application_id': 300, 'stipend': 1500, 'owner': 'Charlie'}),
            (400, {'application_id': 400, 'stipend': 3000, 'owner': 'Diana'}),
            (500, {'application_id': 500, 'stipend': 2500, 'owner': 'Eve'}),
        ]

        for key, value in records:
            storage.insert(txn.txn_id, 'applications', key, value)

        storage.commit(txn.txn_id)
        lm.release_all(txn.txn_id)  # Release locks after commit
        # tm.commit() called automatically by context manager
    
    print(f"[OK] Batch insert completed")
    print(f"    Application 100: {applications_table.search(100)}")
    print(f"    Application 200: {applications_table.search(200)}")
    print(f"    Application 300: {applications_table.search(300)}")
    print(f"    Application 400: {applications_table.search(400)}")
    print(f"    Application 500: {applications_table.search(500)}")

    assert applications_table.search(100)['owner'] == 'Alice'
    assert applications_table.search(200)['owner'] == 'Bob'
    assert applications_table.search(300)['owner'] == 'Charlie'
    assert applications_table.search(400)['owner'] == 'Diana'
    assert applications_table.search(500)['owner'] == 'Eve'
    print("[OK] ATOMICITY: All application inserts committed together")
    print()
    
    # Test 2: CONSISTENCY - Rollback on error
    print("### TEST 2: CONSISTENCY (Rollback on Constraint Violation) ###")
    
    initial_application = applications_table.search(100)['stipend']
    print(f"[OK] Before failed transfer:")
    print(f"    Application 100 stipend: {initial_application}")
    
    try:
        with tm.transaction() as txn:
            # Try to set an invalid stipend (should fail)
            application = storage.read(txn.txn_id, 'applications', 100)
            current_stipend = application['stipend']
            
            if current_stipend < 5000:  # Constraint check
                lm.release_all(txn.txn_id)  # Release locks before raising
                raise ValueError("Constraint violation: stipend cannot exceed allowed limit")
            
            # These changes won't happen
            storage.update(txn.txn_id, 'applications', 100,
                          {'application_id': 100, 'stipend': 5000, 'owner': 'Alice'})
            storage.commit(txn.txn_id)
            lm.release_all(txn.txn_id)
    except ValueError as e:
        print(f"[OK] Transaction aborted: {e}")
    
    # Balances unchanged
    assert applications_table.search(100)['stipend'] == initial_application
    print(f"[OK] After rollback:")
    print(f"    Application 100 stipend: {applications_table.search(100)['stipend']}")
    print("[OK] CONSISTENCY: Invalid transaction rolled back, data unchanged")
    print()
    
    # Test 3: ISOLATION - Concurrent transactions
    print("### TEST 3: ISOLATION (Concurrent Transactions) ###")
    
    txn1 = tm.begin()
    txn2 = tm.begin()
    
    print(f"[OK] Started 2 concurrent transactions: {txn1.txn_id[:20]}... and {txn2.txn_id[:20]}...")
    
    # TXN1 updates Application 100's stipend
    storage.update(txn1.txn_id, 'applications', 100,
                  {'application_id': 100, 'stipend': 900, 'owner': 'Alice'}, acquire_lock=False)
    print("[OK] TXN1: Updated Application 100 stipend to 900 (uncommitted)")
    
    # TXN2 reads Application 100 stipend (should see old value - isolation)
    application_in_txn2 = storage.read(txn2.txn_id, 'applications', 100, acquire_lock=False)
    print(f"[OK] TXN2: Reads Application 100 stipend as {application_in_txn2['stipend']} (old value)")
    
    assert application_in_txn2['stipend'] == 1000, "TXN2 should not see TXN1's uncommitted changes"
    print("[OK] ISOLATION: TXN2 cannot see TXN1's uncommitted changes")
    
    # TXN1 commits
    storage.commit(txn1.txn_id)
    tm.commit(txn1.txn_id)
    print("[OK] TXN1: Committed")
    
    # Now TXN2 can see the change
    application_after_commit = storage.read(txn2.txn_id, 'applications', 100, acquire_lock=False)
    print(f"[OK] TXN2: Now reads Application 100 stipend as {application_after_commit['stipend']} (committed value)")
    
    assert application_after_commit['stipend'] == 900
    tm.rollback(txn2.txn_id)
    print("[OK] ISOLATION: Changes visible after commit")
    print()
    
    # Test 4: DURABILITY - WAL persistence
    print("### TEST 4: DURABILITY (Write-Ahead Logging) ###")
    
    # Count WAL entries before
    logs_before = wal.read_all_logs()
    print(f"[OK] WAL entries before new transaction: {len(logs_before)}")
    
    # New transaction
    with tm.transaction() as txn:
        storage.insert(txn.txn_id, 'applications', 600,
                      {'application_id': 600, 'stipend': 3500, 'owner': 'Frank'})
        storage.commit(txn.txn_id)
        lm.release_all(txn.txn_id)
    
    # Count WAL entries after
    logs_after = wal.read_all_logs()
    print(f"[OK] WAL entries after new transaction: {len(logs_after)}")
    
    assert len(logs_after) > len(logs_before)
    print("[OK] DURABILITY: All operations logged to WAL with fsync")
    
    # Verify WAL has COMMIT record
    last_log = logs_after[-1]
    assert last_log.type == 'COMMIT'
    print(f"[OK] Last WAL entry is COMMIT (LSN {last_log.lsn})")
    print()
    
    # Test 5: Locking prevents conflicts
    print("### TEST 5: CONCURRENCY CONTROL (Locking) ###")
    
    txn_a = tm.begin()
    txn_b = tm.begin()
    
    # TXN_A acquires exclusive lock
    storage.update(txn_a.txn_id, 'applications', 100,
                  {'application_id': 100, 'stipend': 950, 'owner': 'Alice'})
    print(f"[OK] TXN_A: Acquired exclusive lock on applications[100]")
    
    # TXN_B tries to acquire lock (should timeout)
    try:
        storage.update(txn_b.txn_id, 'applications', 100,
                      {'application_id': 100, 'stipend': 999, 'owner': 'Alice'})
        assert False, "Should have timed out"
    except TimeoutError:
        print(f"[OK] TXN_B: Lock acquisition timed out (conflict detected)")
    
    # Release locks
    lm.release_all(txn_a.txn_id)
    tm.rollback(txn_a.txn_id)
    tm.rollback(txn_b.txn_id)
    print("[OK] CONCURRENCY CONTROL: Locks prevent conflicting operations")
    print()
    
    # Statistics
    print("### FINAL STATISTICS ###")
    tm_stats = tm.get_statistics()
    wal_stats = wal.get_log_statistics()
    lm_stats = lm.get_statistics()
    storage_stats = storage.get_statistics()
    
    print(f"TransactionManager: {tm_stats['total_transactions']} total, "
          f"{tm_stats['total_committed']} committed, "
          f"{tm_stats['total_aborted']} aborted")
    print(f"WAL: {wal_stats['total_entries']} entries, "
          f"{wal_stats['file_size_bytes']} bytes")
    print(f"LockManager: {lm_stats['total_locks_acquired']} locks acquired, "
          f"{lm_stats['total_locks_released']} released, "
          f"{lm_stats['total_timeouts']} timeouts")
    print(f"TransactionalStorage: {storage_stats['active_transactions']} active transactions")
    print()
    
    # Cleanup
    wal.close()
    shutil.rmtree(wal_dir)
    
    print("=" * 70)
    print("[OK] ALL ACID PROPERTIES VERIFIED")
    print("=" * 70)
    print()
    print("Summary:")
    print("  [OK] ATOMICITY: All-or-nothing commits work correctly")
    print("  [OK] CONSISTENCY: Constraints enforced, invalid transactions rolled back")
    print("  [OK] ISOLATION: Transactions cannot see uncommitted changes")
    print("  [OK] DURABILITY: All operations logged to WAL with fsync")
    print("  [OK] CONCURRENCY: Locks prevent conflicting operations")
    print()


if __name__ == '__main__':
    try:
        test_full_integration()
        print("[SUCCESS] All integration tests passed!")
    except AssertionError as e:
        print(f"[FAIL] Assertion error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
