#!/usr/bin/env python3
"""
Comprehensive Failure Scenario Testing Suite

This test suite ensures all ACID requirements are properly met:
• Every operation should either complete fully or not happen at all
• No partial or incorrect data should remain  
• Data should always stay valid
• If a failure occurs in the middle of an operation, all changes should be undone
• The system should not leave incomplete updates
• After restart, committed data should still exist
• Data in the main database and B+ Tree must always match

Tests cover:
• Atomicity: Simulate failure during an operation and verify rollback
• Consistency: Ensure data remains valid after operations
• Durability: Restart system and verify data is still present
• Database + B+ Tree Consistency verification
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path
import threading
import time
import uuid
import signal
import subprocess
import traceback

# Add Module_A to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import DBManager
from transaction import (TransactionCoordinator, TransactionConfig, 
                        TransactionManager, WALManager, LockManager)
from transaction.recovery import RecoveryManager


class FailureSimulationTests:
    """Test suite for comprehensive failure scenarios."""
    
    @staticmethod
    def test_mid_operation_failure_atomicity():
        """Test atomicity when failure occurs in middle of multi-operation transaction."""
        print("=" * 80)
        print("FAILURE SCENARIO TEST 1: Mid-Operation Failure Atomicity")
        print("=" * 80)
        
        # Setup test environment
        wal_dir = Path(tempfile.mkdtemp())
        db = DBManager()
        
        # Create tables
        db.create_table('accounts', order=4)
        db.create_table('transfers', order=4)
        db.create_table('audit_log', order=4)
        
        config = TransactionConfig(lock_timeout=5.0)
        coordinator = TransactionCoordinator(db, str(wal_dir), config, auto_recover=False)
        
        try:
            # Setup initial state
            print("Setting up initial state...")
            setup_txn = coordinator.begin_transaction()
            coordinator.insert(setup_txn.txn_id, 'accounts', 1, 
                             {'name': 'Alice', 'balance': 1000})
            coordinator.insert(setup_txn.txn_id, 'accounts', 2, 
                             {'name': 'Bob', 'balance': 500})
            coordinator.commit(setup_txn.txn_id)
            
            print("✓ Initial state: Alice $1000, Bob $500")
            
            # Start complex multi-operation transaction
            print("\nStarting multi-operation transaction...")
            txn = coordinator.begin_transaction()
            
            # Operation 1: Debit Alice (SUCCESS)
            alice = db.get_table('accounts').search(1)
            coordinator.update(txn.txn_id, 'accounts', 1,
                             {'name': alice['name'], 'balance': alice['balance'] - 300})
            print("✓ Operation 1: Debited Alice $300 (buffer)")
            
            # Operation 2: Credit Bob (SUCCESS) 
            bob = db.get_table('accounts').search(2)
            coordinator.update(txn.txn_id, 'accounts', 2,
                             {'name': bob['name'], 'balance': bob['balance'] + 300})
            print("✓ Operation 2: Credited Bob $300 (buffer)")
            
            # Operation 3: Create transfer record (SUCCESS)
            coordinator.insert(txn.txn_id, 'transfers', 1,
                             {'from_acc': 1, 'to_acc': 2, 'amount': 300, 'timestamp': time.time()})
            print("✓ Operation 3: Created transfer record (buffer)")
            
            # SIMULATE FAILURE before audit log creation
            print("\n🔥 SIMULATING FAILURE BEFORE OPERATION 4...")
            
            try:
                # Operation 4: This will "fail" before execution
                raise Exception("Simulated system failure during transaction")
                
                # This should never execute
                coordinator.insert(txn.txn_id, 'audit_log', 1,
                                 {'action': 'transfer', 'amount': 300, 'timestamp': time.time()})
                
            except Exception as e:
                print(f"❌ Failure occurred: {e}")
                print("🔄 Rolling back transaction...")
                coordinator.rollback(txn.txn_id)
                
            # Verify ATOMICITY: NO partial changes should remain
            print("\n🔍 Verifying atomicity after rollback...")
            
            final_alice = db.get_table('accounts').search(1)
            final_bob = db.get_table('accounts').search(2)
            transfer_record = db.get_table('transfers').search(1)
            audit_record = db.get_table('audit_log').search(1)
            
            # ALL operations should be rolled back
            assert final_alice['balance'] == 1000, f"Alice balance should be 1000, got {final_alice['balance']}"
            assert final_bob['balance'] == 500, f"Bob balance should be 500, got {final_bob['balance']}"
            assert transfer_record is None, "Transfer record should not exist after rollback"
            assert audit_record is None, "Audit record should not exist after rollback"
            
            print("✅ ATOMICITY VERIFIED:")
            print(f"   • Alice balance: ${final_alice['balance']} (unchanged)")
            print(f"   • Bob balance: ${final_bob['balance']} (unchanged)")
            print(f"   • Transfer record: {transfer_record} (not created)")
            print(f"   • Audit record: {audit_record} (not created)")
            print("✅ NO PARTIAL UPDATES REMAIN - ATOMICITY PRESERVED")
            
        finally:
            coordinator.shutdown()
            shutil.rmtree(wal_dir)
    
    @staticmethod
    def test_btree_database_consistency():
        """Test that B+ Tree and database always remain consistent."""
        print("=" * 80)
        print("FAILURE SCENARIO TEST 2: B+ Tree Database Consistency")
        print("=" * 80)
        
        # Setup test environment
        wal_dir = Path(tempfile.mkdtemp())
        db = DBManager()
        
        # Create tables
        db.create_table('users', order=4)
        db.create_table('orders', order=4)
        
        config = TransactionConfig(lock_timeout=5.0)
        coordinator = TransactionCoordinator(db, str(wal_dir), config, auto_recover=False)
        
        try:
            print("Testing B+ Tree <-> Database consistency during operations...")
            
            # Test 1: Normal operations maintain consistency
            print("\n--- Normal Operations Test ---")
            txn1 = coordinator.begin_transaction()
            
            coordinator.insert(txn1.txn_id, 'users', 1, {'name': 'Alice', 'email': 'alice@example.com'})
            coordinator.insert(txn1.txn_id, 'orders', 1, {'user_id': 1, 'product': 'laptop', 'price': 999})
            
            # Check consistency before commit (should be in buffer)
            user_in_buffer = coordinator.read(txn1.txn_id, 'users', 1)
            order_in_buffer = coordinator.read(txn1.txn_id, 'orders', 1)
            
            assert user_in_buffer is not None, "User should be readable in transaction buffer"
            assert order_in_buffer is not None, "Order should be readable in transaction buffer"
            
            # Check B+ Tree directly (should not have data yet)
            user_in_btree = db.get_table('users').search(1)
            order_in_btree = db.get_table('orders').search(1)
            
            assert user_in_btree is None, "User should NOT be in B+ Tree before commit"
            assert order_in_btree is None, "Order should NOT be in B+ Tree before commit"
            
            print("✓ Buffer contains data, B+ Tree does not (before commit) - CONSISTENT")
            
            # Commit and verify consistency
            coordinator.commit(txn1.txn_id)
            
            user_after_commit = db.get_table('users').search(1)
            order_after_commit = db.get_table('orders').search(1)
            
            assert user_after_commit is not None, "User should be in B+ Tree after commit"
            assert order_after_commit is not None, "Order should be in B+ Tree after commit"
            assert user_after_commit['name'] == 'Alice', "User data should match"
            assert order_after_commit['price'] == 999, "Order data should match"
            
            print("✓ Data persisted to B+ Tree after commit - CONSISTENT")
            
            # Test 2: Rollback maintains consistency
            print("\n--- Rollback Operations Test ---")
            txn2 = coordinator.begin_transaction()
            
            coordinator.update(txn2.txn_id, 'users', 1, 
                             {'name': 'Alice Updated', 'email': 'alice.new@example.com'})
            coordinator.insert(txn2.txn_id, 'orders', 2, 
                             {'user_id': 1, 'product': 'mouse', 'price': 50})
            
            # Verify buffer has changes
            updated_user = coordinator.read(txn2.txn_id, 'users', 1)
            new_order = coordinator.read(txn2.txn_id, 'orders', 2)
            
            assert updated_user['name'] == 'Alice Updated', "Buffer should have updated user"
            assert new_order['price'] == 50, "Buffer should have new order"
            
            # Rollback
            coordinator.rollback(txn2.txn_id)
            
            # Verify B+ Tree unchanged
            user_after_rollback = db.get_table('users').search(1)
            order2_after_rollback = db.get_table('orders').search(2)
            
            assert user_after_rollback['name'] == 'Alice', "User should be unchanged after rollback"
            assert order2_after_rollback is None, "New order should not exist after rollback"
            
            print("✓ B+ Tree unchanged after rollback - CONSISTENT")
            
            print("✅ B+ TREE DATABASE CONSISTENCY VERIFIED")
            
        finally:
            coordinator.shutdown()
            shutil.rmtree(wal_dir)
    
    @staticmethod
    def test_crash_recovery_durability():
        """Test durability after system crash and restart."""
        print("=" * 80)
        print("FAILURE SCENARIO TEST 3: Crash Recovery Durability")
        print("=" * 80)
        
        # Setup persistent test environment  
        wal_dir = Path(tempfile.mkdtemp())
        
        # Phase 1: Setup and commit data before "crash"
        print("--- Phase 1: Pre-Crash Operations ---")
        
        db1 = DBManager()
        db1.create_table('customers', order=4)
        db1.create_table('accounts', order=4)
        db1.create_table('transactions', order=4)
        
        config = TransactionConfig(lock_timeout=5.0)
        coordinator1 = TransactionCoordinator(db1, str(wal_dir), config, auto_recover=False)
        
        committed_data = []
        
        try:
            # Transaction 1: Customer setup (WILL BE COMMITTED)
            txn1 = coordinator1.begin_transaction()
            coordinator1.insert(txn1.txn_id, 'customers', 1, {'name': 'Alice', 'email': 'alice@test.com'})
            coordinator1.insert(txn1.txn_id, 'accounts', 1, {'customer_id': 1, 'balance': 1000})
            coordinator1.commit(txn1.txn_id)
            committed_data.append(('Customer Alice', 1000))
            print("✓ Transaction 1 committed: Customer Alice with $1000")
            
            # Transaction 2: Transfer (WILL BE COMMITTED)
            txn2 = coordinator1.begin_transaction()
            coordinator1.insert(txn2.txn_id, 'customers', 2, {'name': 'Bob', 'email': 'bob@test.com'})
            coordinator1.insert(txn2.txn_id, 'accounts', 2, {'customer_id': 2, 'balance': 500})
            coordinator1.insert(txn2.txn_id, 'transactions', 1, 
                               {'from_acc': 1, 'to_acc': 2, 'amount': 200})
            
            # Update balances
            coordinator1.update(txn2.txn_id, 'accounts', 1, {'customer_id': 1, 'balance': 800})
            coordinator1.update(txn2.txn_id, 'accounts', 2, {'customer_id': 2, 'balance': 700})
            coordinator1.commit(txn2.txn_id)
            committed_data.append(('Transfer completed', 200))
            print("✓ Transaction 2 committed: Transfer $200 from Alice to Bob")
            
            # Transaction 3: Incomplete (WILL BE ROLLED BACK)
            print("\n🔥 Starting incomplete transaction (will simulate crash)...")
            txn3 = coordinator1.begin_transaction()
            coordinator1.insert(txn3.txn_id, 'customers', 3, {'name': 'Charlie', 'email': 'charlie@test.com'})
            coordinator1.insert(txn3.txn_id, 'accounts', 3, {'customer_id': 3, 'balance': 300})
            
            # DON'T COMMIT - simulate crash
            print("💥 SIMULATING CRASH - incomplete transaction not committed")
            
        finally:
            coordinator1.shutdown()
            
        # Phase 2: System restart and recovery
        print("\n--- Phase 2: System Restart and Recovery ---")
        
        # Create fresh database instances (simulating restart)
        db2 = DBManager()
        db2.create_table('customers', order=4)
        db2.create_table('accounts', order=4)
        db2.create_table('transactions', order=4)
        
        # Recovery happens automatically on coordinator startup
        print("🔄 Starting recovery process...")
        coordinator2 = TransactionCoordinator(db2, str(wal_dir), config, auto_recover=True)
        
        try:
            # Verify DURABILITY: Committed data should survive restart
            print("\n🔍 Verifying durability after recovery...")
            
            # Check committed customers and accounts
            alice = db2.get_table('customers').search(1)
            bob = db2.get_table('customers').search(2)
            alice_acc = db2.get_table('accounts').search(1)
            bob_acc = db2.get_table('accounts').search(2)
            transfer = db2.get_table('transactions').search(1)
            
            assert alice is not None, "Alice should survive restart"
            assert bob is not None, "Bob should survive restart"
            assert alice_acc is not None, "Alice's account should survive restart"
            assert bob_acc is not None, "Bob's account should survive restart"
            assert transfer is not None, "Transfer record should survive restart"
            
            assert alice['name'] == 'Alice', "Alice's data should be correct"
            assert bob['name'] == 'Bob', "Bob's data should be correct"
            assert alice_acc['balance'] == 800, f"Alice balance should be 800, got {alice_acc['balance']}"
            assert bob_acc['balance'] == 700, f"Bob balance should be 700, got {bob_acc['balance']}"
            assert transfer['amount'] == 200, f"Transfer amount should be 200, got {transfer['amount']}"
            
            print("✅ COMMITTED DATA SURVIVED RESTART:")
            print(f"   • Alice: {alice['name']} - Balance: ${alice_acc['balance']}")
            print(f"   • Bob: {bob['name']} - Balance: ${bob_acc['balance']}")
            print(f"   • Transfer: ${transfer['amount']}")
            
            # Verify ATOMICITY: Incomplete transaction should be rolled back
            charlie = db2.get_table('customers').search(3)
            charlie_acc = db2.get_table('accounts').search(3)
            
            assert charlie is None, "Charlie should NOT exist (incomplete transaction)"
            assert charlie_acc is None, "Charlie's account should NOT exist (incomplete transaction)"
            
            print("✅ INCOMPLETE TRANSACTION ROLLED BACK:")
            print(f"   • Charlie: {charlie} (not recovered)")
            print(f"   • Charlie's account: {charlie_acc} (not recovered)")
            
            print("✅ DURABILITY AND ATOMICITY VERIFIED AFTER CRASH RECOVERY")
            
        finally:
            coordinator2.shutdown()
            shutil.rmtree(wal_dir)
    
    @staticmethod
    def test_concurrent_failure_isolation():
        """Test that failures in one transaction don't affect others (Isolation)."""
        print("=" * 80)
        print("FAILURE SCENARIO TEST 4: Concurrent Transaction Isolation")
        print("=" * 80)
        
        # Setup test environment
        wal_dir = Path(tempfile.mkdtemp())
        db = DBManager()
        
        # Create table
        db.create_table('inventory', order=4)
        
        config = TransactionConfig(lock_timeout=10.0)
        coordinator = TransactionCoordinator(db, str(wal_dir), config, auto_recover=False)
        
        try:
            # Setup initial inventory
            print("Setting up initial inventory...")
            setup_txn = coordinator.begin_transaction()
            coordinator.insert(setup_txn.txn_id, 'inventory', 1, {'product': 'Widget A', 'stock': 100})
            coordinator.insert(setup_txn.txn_id, 'inventory', 2, {'product': 'Widget B', 'stock': 50})
            coordinator.commit(setup_txn.txn_id)
            
            print("✓ Initial inventory: Widget A (100), Widget B (50)")
            
            # Start multiple concurrent transactions
            print("\n--- Starting Concurrent Transactions ---")
            
            # Transaction 1: Will succeed
            txn1 = coordinator.begin_transaction()
            print("Started Transaction 1 (will succeed)")
            
            # Transaction 2: Will fail
            txn2 = coordinator.begin_transaction()  
            print("Started Transaction 2 (will fail)")
            
            # Transaction 1 operations (will succeed)
            coordinator.update(txn1.txn_id, 'inventory', 1, 
                             {'product': 'Widget A', 'stock': 95})  # Reduce by 5
            print("✓ Txn1: Reduced Widget A stock by 5")
            
            # Transaction 2 operations (will fail)
            coordinator.update(txn2.txn_id, 'inventory', 2, 
                             {'product': 'Widget B', 'stock': 45})  # Reduce by 5
            print("✓ Txn2: Reduced Widget B stock by 5")
            
            # Commit Transaction 1 (SUCCESS)
            coordinator.commit(txn1.txn_id)
            print("✅ Transaction 1 committed successfully")
            
            # Simulate failure in Transaction 2
            print("\n🔥 SIMULATING FAILURE IN TRANSACTION 2...")
            try:
                # Simulate some error condition
                raise Exception("Simulated error in Transaction 2")
                
            except Exception as e:
                print(f"❌ Transaction 2 failed: {e}")
                coordinator.rollback(txn2.txn_id)
                print("🔄 Transaction 2 rolled back")
            
            # Verify ISOLATION: Transaction 1's changes should remain,
            # Transaction 2's changes should be rolled back
            print("\n🔍 Verifying isolation after mixed success/failure...")
            
            widget_a = db.get_table('inventory').search(1)
            widget_b = db.get_table('inventory').search(2)
            
            assert widget_a['stock'] == 95, f"Widget A should be 95 (Txn1 success), got {widget_a['stock']}"
            assert widget_b['stock'] == 50, f"Widget B should be 50 (Txn2 rollback), got {widget_b['stock']}"
            
            print("✅ ISOLATION VERIFIED:")
            print(f"   • Widget A stock: {widget_a['stock']} (Transaction 1 changes preserved)")
            print(f"   • Widget B stock: {widget_b['stock']} (Transaction 2 changes rolled back)")
            print("✅ FAILED TRANSACTION DID NOT AFFECT SUCCESSFUL TRANSACTION")
            
        finally:
            coordinator.shutdown()
            shutil.rmtree(wal_dir)


def run_comprehensive_failure_tests():
    """Run all comprehensive failure scenario tests."""
    print("🚀 STARTING COMPREHENSIVE FAILURE SCENARIO TESTING")
    print("=" * 100)
    
    test_results = []
    
    tests = [
        ("Mid-Operation Failure Atomicity", FailureSimulationTests.test_mid_operation_failure_atomicity),
        ("B+ Tree Database Consistency", FailureSimulationTests.test_btree_database_consistency),
        ("Crash Recovery Durability", FailureSimulationTests.test_crash_recovery_durability),
        ("Concurrent Failure Isolation", FailureSimulationTests.test_concurrent_failure_isolation),
    ]
    
    for test_name, test_func in tests:
        try:
            print(f"\n🧪 Running: {test_name}")
            test_func()
            test_results.append((test_name, "PASSED", None))
            print(f"✅ {test_name}: PASSED\n")
            
        except Exception as e:
            error_msg = f"{e}\n{traceback.format_exc()}"
            test_results.append((test_name, "FAILED", error_msg))
            print(f"❌ {test_name}: FAILED")
            print(f"Error: {error_msg}\n")
    
    # Summary
    print("=" * 100)
    print("🏁 COMPREHENSIVE FAILURE TESTING COMPLETE")
    print("=" * 100)
    
    passed = sum(1 for _, status, _ in test_results if status == "PASSED")
    failed = sum(1 for _, status, _ in test_results if status == "FAILED")
    
    for test_name, status, error in test_results:
        status_icon = "✅" if status == "PASSED" else "❌"
        print(f"{status_icon} {test_name}: {status}")
        if error:
            print(f"   Error: {error.split(chr(10))[0]}")  # First line of error
    
    print(f"\n📊 RESULTS: {passed} PASSED, {failed} FAILED")
    
    if failed == 0:
        print("🎉 ALL FAILURE SCENARIOS HANDLED CORRECTLY!")
        print("✅ System meets all ACID requirements under failure conditions")
    else:
        print("⚠️  Some failure scenarios need attention")
    
    return failed == 0


if __name__ == "__main__":
    success = run_comprehensive_failure_tests()
    sys.exit(0 if success else 1)