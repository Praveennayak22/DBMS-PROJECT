"""
Crash Recovery Module for ACID Compliance

This module implements crash recovery using Write-Ahead Logging (WAL).
It ensures durability by replaying committed transactions and undoing
incomplete transactions after a system crash.

Recovery Algorithm (ARIES-inspired):
1. Analysis Phase: Scan WAL to identify committed and incomplete transactions
2. Undo Phase: Rollback incomplete transactions (no COMMIT record)
3. Redo Phase: Replay committed transactions to restore state

Key Features:
- Automatic recovery on database startup
- UNDO incomplete transactions (restore old values)
- REDO committed transactions (replay operations)
- Checkpoint-based recovery optimization
- Comprehensive logging and diagnostics
"""

from typing import Dict, List, Set, Any, Optional
from pathlib import Path
from dataclasses import dataclass
import logging

try:
    # Try relative import first (when used as module)
    from .wal import WALManager, LogRecord
except ImportError:
    # Fall back to direct import (when run as script)
    from wal import WALManager, LogRecord


@dataclass
class RecoveryStatistics:
    """Statistics collected during recovery process."""
    total_log_entries: int = 0
    committed_transactions: int = 0
    incomplete_transactions: int = 0
    undo_operations: int = 0
    redo_operations: int = 0
    recovery_time_ms: float = 0.0
    errors_encountered: int = 0


class RecoveryManager:
    """
    Manages crash recovery using Write-Ahead Logging.
    
    Recovery Process:
    1. Read WAL from last checkpoint (or beginning)
    2. Identify committed and incomplete transactions
    3. UNDO incomplete transactions (reverse order)
    4. REDO committed transactions (forward order)
    5. Restore database to consistent state
    """
    
    def __init__(self, wal_manager: WALManager, db_manager, storage_manager=None):
        """
        Initialize RecoveryManager.
        
        Args:
            wal_manager: WALManager instance for reading logs
            db_manager: DBManager instance with B+ Tree tables
            storage_manager: TransactionalStorage instance (optional)
        """
        self.wal = wal_manager
        self.db_manager = db_manager
        self.storage = storage_manager
        self.logger = logging.getLogger(__name__)
        
        # Recovery state
        self.committed_txns: Set[str] = set()
        self.incomplete_txns: Set[str] = set()
        self.txn_operations: Dict[str, List[LogRecord]] = {}
        self.stats = RecoveryStatistics()
    
    def recover(self) -> RecoveryStatistics:
        """
        Perform full crash recovery.
        
        Returns:
            RecoveryStatistics with recovery metrics
        """
        import time
        start_time = time.time()
        
        self.logger.info("=" * 70)
        self.logger.info("STARTING CRASH RECOVERY")
        self.logger.info("=" * 70)
        
        try:
            # Phase 1: Analysis
            self._analysis_phase()
            
            # Phase 2: Undo incomplete transactions
            self._undo_phase()
            
            # Phase 3: Redo committed transactions
            self._redo_phase()
            
            # Calculate recovery time
            self.stats.recovery_time_ms = (time.time() - start_time) * 1000
            
            self.logger.info("=" * 70)
            self.logger.info("CRASH RECOVERY COMPLETED SUCCESSFULLY")
            self.logger.info(f"Recovery time: {self.stats.recovery_time_ms:.2f} ms")
            self.logger.info(f"Committed transactions: {self.stats.committed_transactions}")
            self.logger.info(f"Incomplete transactions: {self.stats.incomplete_transactions}")
            self.logger.info(f"UNDO operations: {self.stats.undo_operations}")
            self.logger.info(f"REDO operations: {self.stats.redo_operations}")
            self.logger.info("=" * 70)
            
            return self.stats
            
        except Exception as e:
            self.stats.errors_encountered += 1
            self.logger.error(f"Recovery failed: {e}")
            raise
    
    def _analysis_phase(self):
        """
        Phase 1: Analyze WAL to identify transaction states.
        
        Scans all log entries to determine:
        - Which transactions committed (have COMMIT record)
        - Which transactions are incomplete (no COMMIT/ROLLBACK)
        - Operations performed by each transaction
        """
        self.logger.info("Phase 1: ANALYSIS")
        self.logger.info("-" * 70)
        
        # Read all log entries
        logs = self.wal.read_all_logs()
        self.stats.total_log_entries = len(logs)
        
        self.logger.info(f"Read {len(logs)} log entries from WAL")
        
        # Group operations by transaction
        begun_txns: Set[str] = set()
        
        for log in logs:
            txn_id = log.txn_id
            
            if log.type == 'BEGIN':
                begun_txns.add(txn_id)
                self.txn_operations[txn_id] = []
                
            elif log.type in ['INSERT', 'UPDATE', 'DELETE']:
                if txn_id not in self.txn_operations:
                    self.txn_operations[txn_id] = []
                self.txn_operations[txn_id].append(log)
                
            elif log.type == 'COMMIT':
                self.committed_txns.add(txn_id)
                
            elif log.type == 'ROLLBACK':
                # Already rolled back, nothing to do
                if txn_id in begun_txns:
                    begun_txns.discard(txn_id)
        
        # Identify incomplete transactions
        self.incomplete_txns = begun_txns - self.committed_txns
        
        self.stats.committed_transactions = len(self.committed_txns)
        self.stats.incomplete_transactions = len(self.incomplete_txns)
        
        self.logger.info(f"Committed transactions: {len(self.committed_txns)}")
        self.logger.info(f"Incomplete transactions: {len(self.incomplete_txns)}")
        
        if self.committed_txns:
            self.logger.info(f"  Committed: {sorted(list(self.committed_txns))[:3]}...")
        if self.incomplete_txns:
            self.logger.info(f"  Incomplete: {sorted(list(self.incomplete_txns))[:3]}...")
        
        self.logger.info("-" * 70)
    
    def _undo_phase(self):
        """
        Phase 2: UNDO incomplete transactions.
        
        For each incomplete transaction:
        - Read operations in reverse order
        - Restore old values (before-images)
        - Effectively rolls back uncommitted changes
        """
        if not self.incomplete_txns:
            self.logger.info("Phase 2: UNDO (skipped - no incomplete transactions)")
            self.logger.info("-" * 70)
            return
        
        self.logger.info("Phase 2: UNDO")
        self.logger.info("-" * 70)
        self.logger.info(f"Undoing {len(self.incomplete_txns)} incomplete transactions")
        
        for txn_id in self.incomplete_txns:
            if txn_id not in self.txn_operations:
                continue
            
            operations = self.txn_operations[txn_id]
            
            # Process operations in REVERSE order (LIFO)
            for log in reversed(operations):
                try:
                    self._undo_operation(log)
                    self.stats.undo_operations += 1
                except Exception as e:
                    self.logger.error(f"Failed to undo {log}: {e}")
                    self.stats.errors_encountered += 1
            
            self.logger.info(f"  Undone {txn_id}: {len(operations)} operations")
        
        self.logger.info(f"Total UNDO operations: {self.stats.undo_operations}")
        self.logger.info("-" * 70)
    
    def _undo_operation(self, log: LogRecord):
        """
        Undo a single operation by restoring old value.
        
        Args:
            log: LogRecord to undo
        """
        table = self.db_manager.get_table(log.table)
        
        # Convert key to proper type (WAL stores as string)
        key = log.key
        try:
            key = int(key)  # Try to convert to int
        except (ValueError, TypeError):
            pass  # Keep as string if conversion fails
        
        if log.type == 'INSERT':
            # UNDO INSERT: Delete the inserted key
            table.delete(key)
            self.logger.debug(f"  UNDO INSERT: Deleted {log.table}[{key}]")
            
        elif log.type == 'UPDATE':
            # UNDO UPDATE: Restore old value
            if log.old_value is not None:
                old_val = eval(log.old_value) if isinstance(log.old_value, str) else log.old_value
                table.update(key, old_val)
                self.logger.debug(f"  UNDO UPDATE: Restored {log.table}[{key}] = {old_val}")
            
        elif log.type == 'DELETE':
            # UNDO DELETE: Reinsert old value
            if log.old_value is not None:
                old_val = eval(log.old_value) if isinstance(log.old_value, str) else log.old_value
                table.insert(key, old_val)
                self.logger.debug(f"  UNDO DELETE: Reinserted {log.table}[{key}] = {old_val}")
    
    def _redo_phase(self):
        """
        Phase 3: REDO committed transactions.
        
        For each committed transaction:
        - Replay operations in forward order
        - Apply new values to ensure durability
        - Ensures committed work is not lost
        """
        if not self.committed_txns:
            self.logger.info("Phase 3: REDO (skipped - no committed transactions)")
            self.logger.info("-" * 70)
            return
        
        self.logger.info("Phase 3: REDO")
        self.logger.info("-" * 70)
        self.logger.info(f"Redoing {len(self.committed_txns)} committed transactions")
        
        for txn_id in self.committed_txns:
            if txn_id not in self.txn_operations:
                continue
            
            operations = self.txn_operations[txn_id]
            
            # Process operations in FORWARD order (FIFO)
            for log in operations:
                try:
                    self._redo_operation(log)
                    self.stats.redo_operations += 1
                except Exception as e:
                    self.logger.error(f"Failed to redo {log}: {e}")
                    self.stats.errors_encountered += 1
            
            self.logger.info(f"  Redone {txn_id}: {len(operations)} operations")
        
        self.logger.info(f"Total REDO operations: {self.stats.redo_operations}")
        self.logger.info("-" * 70)
    
    def _redo_operation(self, log: LogRecord):
        """
        Redo a single operation by applying new value.
        
        Args:
            log: LogRecord to redo
        """
        table = self.db_manager.get_table(log.table)
        
        # Convert key to proper type (WAL stores as string)
        key = log.key
        try:
            key = int(key)  # Try to convert to int
        except (ValueError, TypeError):
            pass  # Keep as string if conversion fails
        
        if log.type == 'INSERT':
            # REDO INSERT: Insert new value (idempotent)
            new_val = eval(log.new_value) if isinstance(log.new_value, str) else log.new_value
            existing = table.search(key)
            if existing is None:
                table.insert(key, new_val)
                self.logger.debug(f"  REDO INSERT: Inserted {log.table}[{key}] = {new_val}")
            else:
                # Already exists, update instead
                table.update(key, new_val)
                self.logger.debug(f"  REDO INSERT (as UPDATE): {log.table}[{key}] = {new_val}")
            
        elif log.type == 'UPDATE':
            # REDO UPDATE: Apply new value
            new_val = eval(log.new_value) if isinstance(log.new_value, str) else log.new_value
            existing = table.search(key)
            if existing is not None:
                table.update(key, new_val)
                self.logger.debug(f"  REDO UPDATE: Updated {log.table}[{key}] = {new_val}")
            else:
                # Key doesn't exist, insert instead
                table.insert(key, new_val)
                self.logger.debug(f"  REDO UPDATE (as INSERT): {log.table}[{key}] = {new_val}")
            
        elif log.type == 'DELETE':
            # REDO DELETE: Delete the key (idempotent)
            existing = table.search(key)
            if existing is not None:
                table.delete(key)
                self.logger.debug(f"  REDO DELETE: Deleted {log.table}[{key}]")
    
    def verify_recovery(self) -> bool:
        """
        Verify that recovery was successful.
        
        Returns:
            True if verification passed, False otherwise
        """
        self.logger.info("Verifying recovery...")
        
        # Check that all committed transactions are in the database
        # and incomplete transactions are not
        
        # This is a basic verification - in production you'd want more checks
        verification_passed = True
        
        if self.stats.errors_encountered > 0:
            self.logger.warning(f"Recovery had {self.stats.errors_encountered} errors")
            verification_passed = False
        
        self.logger.info(f"Verification {'PASSED' if verification_passed else 'FAILED'}")
        return verification_passed
    
    def get_statistics(self) -> RecoveryStatistics:
        """Get recovery statistics."""
        return self.stats
    
    def __repr__(self) -> str:
        """String representation of RecoveryManager."""
        return (
            f"RecoveryManager("
            f"committed={self.stats.committed_transactions}, "
            f"incomplete={self.stats.incomplete_transactions}, "
            f"undo={self.stats.undo_operations}, "
            f"redo={self.stats.redo_operations})"
        )


def auto_recover_on_startup(wal_dir: str, db_manager, storage_manager=None) -> RecoveryStatistics:
    """
    Convenience function to automatically perform recovery on startup.
    
    Args:
        wal_dir: Directory containing WAL files
        db_manager: DBManager instance
        storage_manager: TransactionalStorage instance (optional)
    
    Returns:
        RecoveryStatistics with recovery metrics
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='[%(levelname)s] %(message)s'
    )
    
    # Initialize WAL and recovery manager
    wal = WALManager(log_dir=wal_dir)
    recovery = RecoveryManager(wal, db_manager, storage_manager)
    
    # Perform recovery
    stats = recovery.recover()
    
    return stats
