#!/usr/bin/env python3
"""
Performance Monitoring Demo

A simple demonstration of the transaction system's performance monitoring capabilities.
This creates a sample monitoring session and shows the key metrics and reports.
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path
import time

# Add Module_A to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import DBManager
from transaction import TransactionCoordinator, TransactionConfig
from performance_monitor import PerformanceMonitor, MonitoredTransactionCoordinator


def run_monitoring_demo():
    """Run a comprehensive monitoring demonstration."""
    print("=" * 60)
    print("TRANSACTION SYSTEM PERFORMANCE MONITORING DEMO")
    print("=" * 60)
    
    # Setup environment
    wal_dir = Path(tempfile.mkdtemp())
    monitor_dir = Path(tempfile.mkdtemp()) 
    
    db = DBManager()
    db.create_table('applications', order=4)
    db.create_table('orders', order=4)
    
    print(f"Monitor directory: {monitor_dir}")
    print(f"WAL directory: {wal_dir}")
    
    try:
        # Create performance monitor
        monitor = PerformanceMonitor(log_dir=str(monitor_dir))
        print("[OK] Performance monitor initialized")
        
        # Create monitored transaction coordinator
        config = TransactionConfig(lock_timeout=3.0)
        base_coordinator = TransactionCoordinator(db, str(wal_dir), config, auto_recover=False)
        coordinator = MonitoredTransactionCoordinator(base_coordinator, monitor)
        print("[OK] Monitored transaction coordinator created")
        
        # Simulate a realistic workload
        print("\n--- Running Sample Workload ---")
        
        # Phase 1: Application setup
        print("Phase 1: Setting up applications...")
        for i in range(5):
            txn = coordinator.begin_transaction()
            coordinator.insert(txn.txn_id, 'applications', i, 
                             {'name': f'Application_{i}', 'stipend': (i+1) * 1000})
            coordinator.commit(txn.txn_id)
        
        # Phase 2: Order processing 
        print("Phase 2: Processing orders...")
        for i in range(10):
            txn = coordinator.begin_transaction()
            
            # Create order
            coordinator.insert(txn.txn_id, 'orders', i,
                             {'application_id': i % 5, 'amount': (i+1) * 50, 'status': 'pending'})
            
            # Update application (simulate charge)
            application_id = i % 5
            current_stipend = (application_id + 1) * 1000 - (i * 50)  # Simplified calculation
            coordinator.update(txn.txn_id, 'applications', application_id,
                             {'name': f'Application_{application_id}', 'stipend': current_stipend})
            
            # Complete order
            coordinator.update(txn.txn_id, 'orders', i,
                             {'application_id': application_id, 'amount': (i+1) * 50, 'status': 'completed'})
            
            coordinator.commit(txn.txn_id)
            
            time.sleep(0.01)  # Simulate processing delay
        
        # Phase 3: Some rollbacks
        print("Phase 3: Testing rollback scenarios...")
        for i in range(3):
            txn = coordinator.begin_transaction()
            coordinator.insert(txn.txn_id, 'orders', 100 + i,
                             {'application_id': i, 'amount': 99999, 'status': 'failed'})
            coordinator.rollback(txn.txn_id)  # Simulate failed transaction
        
        print(f"[OK] Workload completed: 18 transactions (15 commits, 3 rollbacks)")
        
        # Capture final metrics
        final_metrics = monitor.capture_system_metrics()
        final_stats = monitor.get_current_stats()
        
        # Display performance summary
        print("\n--- Performance Summary ---")
        print(f"Total Transactions: {final_stats['total_transactions']}")
        print(f"Runtime: {final_stats['runtime_seconds']:.2f} seconds")
        print(f"Throughput: {final_stats['transactions_per_second']:.2f} TPS")
        print(f"Average Latency: {final_stats.get('avg_latency_ms', 0):.2f} ms")
        print(f"Median Latency: {final_stats.get('median_latency_ms', 0):.2f} ms")
        print(f"95th Percentile: {final_stats.get('p95_latency_ms', 0):.2f} ms")
        print(f"Lock Waits: {final_stats['total_lock_waits']}")
        print(f"Lock Contention: {final_stats.get('lock_contention_ratio', 0):.2f}%")
        
        # Generate and display full report
        print("\n--- Full Performance Report ---")
        report = monitor.generate_report()
        print(report)
        
        # Show CSV log samples
        print("\n--- Sample Log Entries ---")
        
        if monitor.transaction_log_file.exists():
            print("Transaction Log (transactions.csv):")
            with open(monitor.transaction_log_file, 'r') as f:
                lines = f.readlines()
                print(f"  Header: {lines[0].strip()}")
                for i, line in enumerate(lines[1:4], 1):  # First 3 data rows
                    print(f"  Row {i}: {line.strip()}")
                print(f"  ... ({len(lines)-1} total transaction records)")
        
        if monitor.system_log_file.exists():
            print("\nSystem Metrics Log (system_metrics.csv):")
            with open(monitor.system_log_file, 'r') as f:
                lines = f.readlines()
                print(f"  Header: {lines[0].strip()}")
                if len(lines) > 1:
                    print(f"  Latest: {lines[-1].strip()}")
                print(f"  ... ({len(lines)-1} total metric snapshots)")
        
        # Export detailed metrics
        monitor.export_json('demo_metrics.json')
        json_file = monitor_dir / 'demo_metrics.json'
        print(f"\nDetailed metrics exported to: {json_file}")
        
        print("\n" + "=" * 60)
        print("DEMO COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("\nKey Features Demonstrated:")
        print("  - Real-time transaction latency tracking")
        print("  - Automatic CSV logging for historical analysis")
        print("  - Comprehensive performance statistics")
        print("  - Lock contention monitoring")
        print("  - JSON export for integration")
        print("  - Multi-phase workload analysis")
        
        return True
        
    except Exception as e:
        print(f"\nDemo failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        try:
            coordinator.shutdown()
        except:
            pass
        
        # Note: Keep temp directories for user inspection
        print(f"\nFiles available for inspection:")
        print(f"  Monitor logs: {monitor_dir}")
        print(f"  WAL files: {wal_dir}")


if __name__ == '__main__':
    success = run_monitoring_demo()
    if success:
        print("\nPerformance monitoring system is ready for production use!")
    else:
        print("\nPerformance monitoring demo failed.")
    
    sys.exit(0 if success else 1)