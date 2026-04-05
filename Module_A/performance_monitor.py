#!/usr/bin/env python3
"""
Transaction System Performance Monitoring

This module provides comprehensive performance monitoring and logging for the
transaction system, tracking key metrics like latency, throughput, lock
contention, and resource utilization.

Features:
- Real-time transaction latency tracking
- Lock wait time analysis
- WAL throughput monitoring  
- Concurrent transaction metrics
- CSV logging for analysis
- Performance dashboard generation
"""

import sys
import os
import time
import threading
import csv
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from collections import defaultdict, deque
import statistics
import json

# Add Module_A to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


@dataclass
class TransactionMetrics:
    """Metrics for a single transaction."""
    txn_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = 'ACTIVE'  # ACTIVE, COMMITTED, ROLLED_BACK
    operations_count: int = 0
    lock_wait_time: float = 0.0  # Total time waiting for locks (seconds)
    wal_write_time: float = 0.0  # Time spent writing to WAL (seconds)
    isolation_level: str = 'READ_COMMITTED'


@dataclass
class SystemMetrics:
    """System-wide performance metrics."""
    timestamp: datetime
    active_transactions: int = 0
    total_transactions: int = 0
    commits_per_second: float = 0.0
    rollbacks_per_second: float = 0.0
    avg_transaction_latency: float = 0.0  # seconds
    avg_lock_wait_time: float = 0.0  # seconds
    wal_entries_per_second: float = 0.0
    lock_contention_ratio: float = 0.0  # % of operations that had to wait for locks
    memory_usage_mb: float = 0.0


class PerformanceMonitor:
    """
    Comprehensive performance monitoring for the transaction system.
    
    Tracks real-time metrics and provides analysis capabilities.
    """
    
    def __init__(self, log_dir: str = 'monitoring', buffer_size: int = 1000):
        """
        Initialize performance monitor.
        
        Args:
            log_dir: Directory to store monitoring logs
            buffer_size: Number of recent metrics to keep in memory
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # In-memory metric storage
        self.transaction_metrics: Dict[str, TransactionMetrics] = {}
        self.system_metrics: deque = deque(maxlen=buffer_size)
        
        # Counters and accumulators
        self.total_transactions = 0
        self.total_commits = 0
        self.total_rollbacks = 0
        self.total_wal_entries = 0
        self.total_lock_waits = 0
        self.total_lock_wait_time = 0.0
        
        # Timing tracking
        self.last_metric_time = time.time()
        self.start_time = datetime.now(timezone.utc)
        
        # Thread safety
        self._lock = threading.RLock()
        
        # CSV logging
        self.transaction_log_file = self.log_dir / 'transactions.csv'
        self.system_log_file = self.log_dir / 'system_metrics.csv'
        self._init_csv_files()
    
    def _init_csv_files(self):
        """Initialize CSV log files with headers."""
        # Transaction log header
        with open(self.transaction_log_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'txn_id', 'start_time', 'end_time', 'status', 'duration_ms',
                'operations_count', 'lock_wait_time_ms', 'wal_write_time_ms',
                'isolation_level'
            ])
        
        # System metrics header
        with open(self.system_log_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'timestamp', 'active_transactions', 'total_transactions',
                'commits_per_second', 'rollbacks_per_second', 'avg_latency_ms',
                'avg_lock_wait_ms', 'wal_entries_per_second', 'lock_contention_ratio',
                'memory_usage_mb'
            ])
    
    def begin_transaction(self, txn_id: str, isolation_level: str = 'READ_COMMITTED'):
        """Record transaction start."""
        with self._lock:
            self.transaction_metrics[txn_id] = TransactionMetrics(
                txn_id=txn_id,
                start_time=datetime.now(timezone.utc),
                isolation_level=isolation_level
            )
            self.total_transactions += 1
    
    def end_transaction(self, txn_id: str, status: str):
        """Record transaction end (COMMITTED or ROLLED_BACK)."""
        with self._lock:
            if txn_id not in self.transaction_metrics:
                return  # Transaction not tracked
            
            metric = self.transaction_metrics[txn_id]
            metric.end_time = datetime.now(timezone.utc)
            metric.status = status
            
            # Update counters
            if status == 'COMMITTED':
                self.total_commits += 1
            elif status == 'ROLLED_BACK':
                self.total_rollbacks += 1
            
            # Log to CSV
            self._log_transaction_csv(metric)
            
            # Remove from active tracking (keep recent history)
            if len(self.transaction_metrics) > 1000:
                # Remove oldest transactions
                oldest_txns = sorted(self.transaction_metrics.items(), 
                                   key=lambda x: x[1].start_time)
                for old_txn_id, _ in oldest_txns[:100]:
                    if old_txn_id != txn_id:  # Don't remove the one we just completed
                        del self.transaction_metrics[old_txn_id]
    
    def record_operation(self, txn_id: str):
        """Record an operation (INSERT/UPDATE/DELETE) for a transaction."""
        with self._lock:
            if txn_id in self.transaction_metrics:
                self.transaction_metrics[txn_id].operations_count += 1
    
    def record_lock_wait(self, txn_id: str, wait_time: float):
        """Record time spent waiting for locks."""
        with self._lock:
            if txn_id in self.transaction_metrics:
                self.transaction_metrics[txn_id].lock_wait_time += wait_time
            
            self.total_lock_waits += 1
            self.total_lock_wait_time += wait_time
    
    def record_wal_write(self, txn_id: str, write_time: float):
        """Record time spent writing to WAL."""
        with self._lock:
            if txn_id in self.transaction_metrics:
                self.transaction_metrics[txn_id].wal_write_time += write_time
            
            self.total_wal_entries += 1
    
    def capture_system_metrics(self, active_txn_count: int = None, memory_mb: float = None):
        """Capture system-wide metrics snapshot."""
        with self._lock:
            now = datetime.now(timezone.utc)
            current_time = time.time()
            time_delta = current_time - self.last_metric_time
            
            # Calculate rates
            commits_per_sec = self.total_commits / max(time_delta, 0.1) if time_delta > 0 else 0
            rollbacks_per_sec = self.total_rollbacks / max(time_delta, 0.1) if time_delta > 0 else 0
            wal_rate = self.total_wal_entries / max(time_delta, 0.1) if time_delta > 0 else 0
            
            # Calculate averages
            active_txns = [m for m in self.transaction_metrics.values() if m.end_time is None]
            completed_txns = [m for m in self.transaction_metrics.values() if m.end_time is not None]
            
            avg_latency = 0.0
            if completed_txns:
                latencies = [(m.end_time - m.start_time).total_seconds() for m in completed_txns[-100:]]
                avg_latency = statistics.mean(latencies) if latencies else 0.0
            
            avg_lock_wait = 0.0
            if self.total_lock_waits > 0:
                avg_lock_wait = self.total_lock_wait_time / self.total_lock_waits
            
            # Lock contention ratio
            total_operations = sum(m.operations_count for m in self.transaction_metrics.values())
            lock_contention = (self.total_lock_waits / max(total_operations, 1)) * 100
            
            # Create system metrics
            metrics = SystemMetrics(
                timestamp=now,
                active_transactions=active_txn_count or len(active_txns),
                total_transactions=self.total_transactions,
                commits_per_second=commits_per_sec,
                rollbacks_per_second=rollbacks_per_sec,
                avg_transaction_latency=avg_latency,
                avg_lock_wait_time=avg_lock_wait,
                wal_entries_per_second=wal_rate,
                lock_contention_ratio=lock_contention,
                memory_usage_mb=memory_mb or 0.0
            )
            
            self.system_metrics.append(metrics)
            self._log_system_csv(metrics)
            
            # Reset counters for rate calculations
            self.total_commits = 0
            self.total_rollbacks = 0
            self.total_wal_entries = 0
            self.last_metric_time = current_time
            
            return metrics
    
    def _log_transaction_csv(self, metric: TransactionMetrics):
        """Log transaction metric to CSV."""
        try:
            duration_ms = 0
            if metric.end_time:
                duration_ms = (metric.end_time - metric.start_time).total_seconds() * 1000
            
            with open(self.transaction_log_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    metric.txn_id,
                    metric.start_time.isoformat(),
                    metric.end_time.isoformat() if metric.end_time else '',
                    metric.status,
                    round(duration_ms, 2),
                    metric.operations_count,
                    round(metric.lock_wait_time * 1000, 2),
                    round(metric.wal_write_time * 1000, 2),
                    metric.isolation_level
                ])
        except Exception as e:
            print(f"Failed to log transaction CSV: {e}")
    
    def _log_system_csv(self, metric: SystemMetrics):
        """Log system metric to CSV."""
        try:
            with open(self.system_log_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    metric.timestamp.isoformat(),
                    metric.active_transactions,
                    metric.total_transactions,
                    round(metric.commits_per_second, 2),
                    round(metric.rollbacks_per_second, 2),
                    round(metric.avg_transaction_latency * 1000, 2),
                    round(metric.avg_lock_wait_time * 1000, 2),
                    round(metric.wal_entries_per_second, 2),
                    round(metric.lock_contention_ratio, 2),
                    round(metric.memory_usage_mb, 2)
                ])
        except Exception as e:
            print(f"Failed to log system CSV: {e}")
    
    def get_current_stats(self) -> Dict[str, Any]:
        """Get current performance statistics."""
        with self._lock:
            active_txns = [m for m in self.transaction_metrics.values() if m.end_time is None]
            completed_txns = [m for m in self.transaction_metrics.values() if m.end_time is not None]
            
            # Calculate statistics
            total_runtime = (datetime.now(timezone.utc) - self.start_time).total_seconds()
            
            stats = {
                'runtime_seconds': total_runtime,
                'total_transactions': self.total_transactions,
                'active_transactions': len(active_txns),
                'completed_transactions': len(completed_txns),
                'transactions_per_second': self.total_transactions / max(total_runtime, 1),
                'total_lock_waits': self.total_lock_waits,
                'avg_lock_wait_ms': (self.total_lock_wait_time / max(self.total_lock_waits, 1)) * 1000,
            }
            
            if completed_txns:
                # Transaction latency statistics
                latencies = [(m.end_time - m.start_time).total_seconds() for m in completed_txns]
                stats.update({
                    'avg_latency_ms': statistics.mean(latencies) * 1000,
                    'median_latency_ms': statistics.median(latencies) * 1000,
                    'p95_latency_ms': statistics.quantiles(latencies, n=20)[18] * 1000 if len(latencies) > 20 else max(latencies) * 1000,
                    'min_latency_ms': min(latencies) * 1000,
                    'max_latency_ms': max(latencies) * 1000,
                })
            
            if self.system_metrics:
                recent_metric = self.system_metrics[-1]
                stats.update({
                    'commits_per_second': recent_metric.commits_per_second,
                    'rollbacks_per_second': recent_metric.rollbacks_per_second,
                    'wal_entries_per_second': recent_metric.wal_entries_per_second,
                    'lock_contention_ratio': recent_metric.lock_contention_ratio,
                })
            
            return stats
    
    def generate_report(self) -> str:
        """Generate a comprehensive performance report."""
        stats = self.get_current_stats()
        
        report = f"""
Transaction System Performance Report
====================================

Runtime: {stats['runtime_seconds']:.1f} seconds
Total Transactions: {stats['total_transactions']}
Active Transactions: {stats['active_transactions']}
Completed Transactions: {stats['completed_transactions']}
Throughput: {stats['transactions_per_second']:.2f} TPS

Latency Statistics:
"""
        
        if 'avg_latency_ms' in stats:
            report += f"""  Average: {stats['avg_latency_ms']:.2f} ms
  Median: {stats['median_latency_ms']:.2f} ms
  95th Percentile: {stats['p95_latency_ms']:.2f} ms
  Min: {stats['min_latency_ms']:.2f} ms
  Max: {stats['max_latency_ms']:.2f} ms
"""
        else:
            report += "  No completed transactions yet\n"
        
        report += f"""
Lock Statistics:
  Total Lock Waits: {stats['total_lock_waits']}
  Average Wait Time: {stats['avg_lock_wait_ms']:.2f} ms
  Contention Ratio: {stats.get('lock_contention_ratio', 0):.2f}%

System Throughput:
  Commits/sec: {stats.get('commits_per_second', 0):.2f}
  Rollbacks/sec: {stats.get('rollbacks_per_second', 0):.2f}
  WAL Writes/sec: {stats.get('wal_entries_per_second', 0):.2f}

Log Files:
  Transactions: {self.transaction_log_file}
  System Metrics: {self.system_log_file}
"""
        
        return report
    
    def export_json(self, filename: str):
        """Export current statistics to JSON file."""
        stats = self.get_current_stats()
        
        # Add recent system metrics
        if self.system_metrics:
            stats['recent_system_metrics'] = [
                {
                    'timestamp': m.timestamp.isoformat(),
                    'active_transactions': m.active_transactions,
                    'commits_per_second': m.commits_per_second,
                    'avg_latency_ms': m.avg_transaction_latency * 1000,
                    'lock_contention_ratio': m.lock_contention_ratio
                }
                for m in list(self.system_metrics)[-10:]  # Last 10 snapshots
            ]
        
        with open(self.log_dir / filename, 'w') as f:
            json.dump(stats, f, indent=2, default=str)


class MonitoredTransactionCoordinator:
    """
    Transaction coordinator wrapper that adds performance monitoring.
    
    This wrapper automatically tracks all transaction metrics without
    requiring changes to the existing coordinator code.
    """
    
    def __init__(self, coordinator, monitor: PerformanceMonitor):
        """
        Initialize monitored coordinator.
        
        Args:
            coordinator: Original TransactionCoordinator instance
            monitor: PerformanceMonitor instance
        """
        self.coordinator = coordinator
        self.monitor = monitor
        
        # Track operation timing
        self._operation_start_times = {}
    
    def begin_transaction(self, isolation_level=None):
        """Begin transaction with monitoring."""
        txn = self.coordinator.begin_transaction(isolation_level)
        self.monitor.begin_transaction(txn.txn_id, str(txn.isolation_level))
        return txn
    
    def insert(self, txn_id: str, table_name: str, key, record):
        """Insert with monitoring."""
        start_time = time.time()
        try:
            result = self.coordinator.insert(txn_id, table_name, key, record)
            self.monitor.record_operation(txn_id)
            return result
        finally:
            # Record any lock wait time (simplified - actual implementation would need coordinator support)
            operation_time = time.time() - start_time
            if operation_time > 0.01:  # If operation took > 10ms, assume some lock wait
                self.monitor.record_lock_wait(txn_id, operation_time * 0.1)  # Estimate 10% was lock wait
    
    def update(self, txn_id: str, table_name: str, key, record):
        """Update with monitoring."""
        start_time = time.time()
        try:
            result = self.coordinator.update(txn_id, table_name, key, record)
            self.monitor.record_operation(txn_id)
            return result
        finally:
            operation_time = time.time() - start_time
            if operation_time > 0.01:
                self.monitor.record_lock_wait(txn_id, operation_time * 0.1)
    
    def delete(self, txn_id: str, table_name: str, key):
        """Delete with monitoring."""
        start_time = time.time()
        try:
            result = self.coordinator.delete(txn_id, table_name, key)
            self.monitor.record_operation(txn_id)
            return result
        finally:
            operation_time = time.time() - start_time
            if operation_time > 0.01:
                self.monitor.record_lock_wait(txn_id, operation_time * 0.1)
    
    def commit(self, txn_id: str):
        """Commit with monitoring."""
        start_time = time.time()
        try:
            result = self.coordinator.commit(txn_id)
            self.monitor.end_transaction(txn_id, 'COMMITTED')
            return result
        finally:
            # Record WAL write time (estimated)
            wal_time = time.time() - start_time
            self.monitor.record_wal_write(txn_id, wal_time)
    
    def rollback(self, txn_id: str):
        """Rollback with monitoring."""
        try:
            result = self.coordinator.rollback(txn_id)
            self.monitor.end_transaction(txn_id, 'ROLLED_BACK')
            return result
        except Exception as e:
            self.monitor.end_transaction(txn_id, 'ROLLED_BACK')
            raise
    
    def shutdown(self):
        """Shutdown coordinator."""
        return self.coordinator.shutdown()
    
    def __getattr__(self, name):
        """Delegate other methods to the original coordinator."""
        return getattr(self.coordinator, name)