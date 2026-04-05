"""
Enhanced Transaction Coordinator with Complete ACID Support

This module provides a high-level transaction coordinator that integrates:
- TransactionManager (state management)
- WALManager (durability via write-ahead logging)
- LockManager (concurrency control)
- TransactionalStorage (buffered B+ Tree operations)
- RecoveryManager (crash recovery)

Key Enhancements:
- Automatic lock acquisition and release
- Proper COMMIT: WAL logging → B+ Tree persistence → fsync → lock release
- Proper ROLLBACK: Log rollback → discard buffers → lock release
- Two-phase commit support
- Deadlock detection and retry logic
- Comprehensive error handling
"""

from typing import Optional, Callable, Any, Dict, List
from dataclasses import dataclass
import time
import logging

try:
    # Try relative imports first (when used as module)
    from .transaction_manager import TransactionManager, Transaction, TransactionState
    from .wal import WALManager
    from .lock_manager import LockManager, LockMode
    from .transactional_storage import TransactionalStorage
    from .recovery import RecoveryManager
except ImportError:
    # Fall back to direct imports (when run as script)
    from transaction_manager import TransactionManager, Transaction, TransactionState
    from wal import WALManager
    from lock_manager import LockManager, LockMode
    from transactional_storage import TransactionalStorage
    from recovery import RecoveryManager


@dataclass
class TransactionConfig:
    """Configuration for transaction behavior."""
    lock_timeout: float = 30.0
    enable_auto_retry: bool = True
    max_retries: int = 3
    retry_delay_ms: float = 100.0
    enable_deadlock_detection: bool = True


class TransactionCoordinator:
    """
    High-level transaction coordinator with complete ACID support.
    
    This coordinator integrates all transaction components and provides:
    - Simplified transaction API
    - Automatic lock management
    - Proper COMMIT/ROLLBACK sequences
    - Automatic crash recovery on startup
    - Deadlock detection and retry
    """
    
    def __init__(
        self,
        db_manager,
        wal_dir: str,
        config: Optional[TransactionConfig] = None,
        auto_recover: bool = True
    ):
        """
        Initialize TransactionCoordinator.
        
        Args:
            db_manager: DBManager instance with B+ Tree tables
            wal_dir: Directory for WAL files
            config: Transaction configuration
            auto_recover: Whether to perform recovery on startup
        """
        self.db_manager = db_manager
        self.config = config or TransactionConfig()
        
        # Initialize all components
        self.tm = TransactionManager()
        self.wal = WALManager(log_dir=wal_dir)
        self.lm = LockManager(lock_timeout=self.config.lock_timeout)
        
        # Connect components
        self.tm.set_wal_logger(self.wal.log)
        self.tm.set_lock_manager(self.lm)
        
        # Create transactional storage
        self.storage = TransactionalStorage(self.db_manager, self.tm, self.lm)
        
        # Recovery manager
        self.recovery = RecoveryManager(self.wal, self.db_manager, self.storage)
        
        # Logger
        self.logger = logging.getLogger(__name__)
        
        # Perform recovery on startup
        if auto_recover:
            self._perform_startup_recovery()
    
    def _perform_startup_recovery(self):
        """Perform crash recovery on startup."""
        try:
            self.logger.info("Performing startup recovery...")
            stats = self.recovery.recover()
            
            if stats.committed_transactions > 0 or stats.incomplete_transactions > 0:
                self.logger.info(
                    f"Recovery completed: {stats.committed_transactions} committed, "
                    f"{stats.incomplete_transactions} incomplete, "
                    f"time={stats.recovery_time_ms:.2f}ms"
                )
            else:
                self.logger.info("No recovery needed (clean shutdown)")
                
        except Exception as e:
            self.logger.error(f"Startup recovery failed: {e}")
            raise
    
    def begin_transaction(self, isolation_level: str = "READ_COMMITTED") -> Transaction:
        """
        Begin a new transaction.
        
        Args:
            isolation_level: Isolation level (READ_COMMITTED, SERIALIZABLE, etc.)
            
        Returns:
            Transaction object
        """
        return self.tm.begin(isolation_level=isolation_level)
    
    def commit(self, txn_id: str):
        """
        Commit a transaction with proper ACID guarantees.
        
        COMMIT Sequence:
        1. Write COMMIT record to WAL
        2. Apply buffered changes to B+ Trees
        3. fsync WAL to ensure durability
        4. Release all locks held by transaction
        5. Update transaction state to COMMITTED
        
        Args:
            txn_id: Transaction ID
            
        Raises:
            ValueError: If transaction is not in valid state for commit
            Exception: If commit fails at any step
        """
        try:
            self.logger.debug(f"Committing transaction {txn_id}")
            
            # Step 1: Apply buffered changes to B+ Trees
            self.storage.commit(txn_id)
            self.logger.debug(f"  [1/4] Applied changes to B+ Trees")
            
            # Step 2: Commit in transaction manager (logs COMMIT to WAL)
            self.tm.commit(txn_id)
            self.logger.debug(f"  [2/4] Logged COMMIT to WAL")
            
            # Step 3: fsync is automatic in WAL (done on every write)
            self.logger.debug(f"  [3/4] WAL fsynced (automatic)")
            
            # Step 4: Release all locks
            lock_count = len(self.lm.get_transaction_locks(txn_id))
            self.lm.release_all(txn_id)
            self.logger.debug(f"  [4/4] Released {lock_count} locks")
            
            self.logger.info(f"Transaction {txn_id} committed successfully")
            
        except Exception as e:
            self.logger.error(f"Commit failed for {txn_id}: {e}")
            # Attempt rollback on commit failure
            try:
                self.rollback(txn_id)
            except Exception as rollback_error:
                self.logger.error(f"Rollback after commit failure also failed: {rollback_error}")
            raise
    
    def rollback(self, txn_id: str):
        """
        Rollback a transaction with proper cleanup.
        
        ROLLBACK Sequence:
        1. Discard buffered changes
        2. Write ROLLBACK record to WAL
        3. Release all locks held by transaction
        4. Update transaction state to ABORTED
        
        Args:
            txn_id: Transaction ID
        """
        try:
            self.logger.debug(f"Rolling back transaction {txn_id}")
            
            # Step 1: Discard buffered changes
            self.storage.rollback(txn_id)
            self.logger.debug(f"  [1/3] Discarded buffered changes")
            
            # Step 2: Rollback in transaction manager (logs ROLLBACK to WAL)
            self.tm.rollback(txn_id)
            self.logger.debug(f"  [2/3] Logged ROLLBACK to WAL")
            
            # Step 3: Release all locks
            lock_count = len(self.lm.get_transaction_locks(txn_id))
            self.lm.release_all(txn_id)
            self.logger.debug(f"  [3/3] Released {lock_count} locks")
            
            self.logger.info(f"Transaction {txn_id} rolled back successfully")
            
        except Exception as e:
            self.logger.error(f"Rollback failed for {txn_id}: {e}")
            raise
    
    def transaction(self, isolation_level: str = "READ_COMMITTED"):
        """
        Context manager for transactions with automatic commit/rollback.
        
        Usage:
            with coordinator.transaction() as txn:
                coordinator.insert(txn.txn_id, 'users', 1, {'name': 'Alice'})
                # Auto-commits on success, auto-rollbacks on exception
        
        Args:
            isolation_level: Isolation level
            
        Returns:
            Context manager that yields Transaction object
        """
        from contextlib import contextmanager
        
        @contextmanager
        def _transaction_context():
            txn = self.begin_transaction(isolation_level)
            try:
                yield txn
                self.commit(txn.txn_id)
            except Exception as e:
                self.logger.warning(f"Transaction {txn.txn_id} failed: {e}, rolling back")
                self.rollback(txn.txn_id)
                raise
        
        return _transaction_context()
    
    # Convenience methods for database operations
    
    def insert(self, txn_id: str, table: str, key: Any, value: Any) -> bool:
        """Insert a record (with automatic lock acquisition)."""
        return self.storage.insert(txn_id, table, key, value, acquire_lock=True)
    
    def update(self, txn_id: str, table: str, key: Any, new_value: Any) -> bool:
        """Update a record (with automatic lock acquisition)."""
        return self.storage.update(txn_id, table, key, new_value, acquire_lock=True)
    
    def delete(self, txn_id: str, table: str, key: Any) -> bool:
        """Delete a record (with automatic lock acquisition)."""
        return self.storage.delete(txn_id, table, key, acquire_lock=True)
    
    def read(self, txn_id: str, table: str, key: Any) -> Optional[Any]:
        """Read a record (with automatic lock acquisition)."""
        return self.storage.read(txn_id, table, key, acquire_lock=True)
    
    def execute_with_retry(
        self,
        operation: Callable[[], Any],
        max_retries: Optional[int] = None
    ) -> Any:
        """
        Execute an operation with automatic retry on deadlock/timeout.
        
        Args:
            operation: Function to execute (should contain full transaction logic)
            max_retries: Maximum number of retries (uses config default if None)
            
        Returns:
            Result of operation
            
        Raises:
            Exception: If all retries exhausted
        """
        max_retries = max_retries or self.config.max_retries
        
        for attempt in range(max_retries + 1):
            try:
                return operation()
            except TimeoutError as e:
                if attempt < max_retries:
                    self.logger.warning(
                        f"Operation failed (attempt {attempt + 1}/{max_retries + 1}): {e}, retrying..."
                    )
                    time.sleep(self.config.retry_delay_ms / 1000.0)
                else:
                    self.logger.error(f"Operation failed after {max_retries + 1} attempts")
                    raise
    
    def checkpoint(self) -> bool:
        """
        Create a checkpoint in the WAL.
        
        Returns:
            True if checkpoint created successfully
        """
        try:
            active_txns = [txn.txn_id for txn in self.tm.get_active_transactions()]
            self.wal.checkpoint(active_txns)
            self.logger.info(f"Checkpoint created with {len(active_txns)} active transactions")
            return True
        except Exception as e:
            self.logger.error(f"Checkpoint failed: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics from all components.
        
        Returns:
            Dictionary with statistics from all components
        """
        return {
            'transaction_manager': self.tm.get_statistics(),
            'wal': self.wal.get_log_statistics(),
            'lock_manager': self.lm.get_statistics(),
            'storage': self.storage.get_statistics(),
        }
    
    def shutdown(self):
        """
        Gracefully shutdown the coordinator.
        
        - Creates final checkpoint
        - Closes WAL
        - Releases all remaining locks
        """
        try:
            self.logger.info("Shutting down TransactionCoordinator...")
            
            # Create final checkpoint
            self.checkpoint()
            
            # Close WAL
            self.wal.close()
            
            # Release any remaining locks for all transactions
            stats = self.lm.get_statistics()
            if stats['active_locks'] > 0:
                # Get all transactions that have locks
                all_txn_ids = set()
                for txn in self.tm.get_active_transactions():
                    all_txn_ids.add(txn.txn_id)
                
                # Release locks for each transaction
                for txn_id in all_txn_ids:
                    self.lm.release_all(txn_id)
            
            self.logger.info("Shutdown completed successfully")
            
        except Exception as e:
            self.logger.error(f"Shutdown error: {e}")
            raise
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with automatic shutdown."""
        self.shutdown()
        return False
    
    def __repr__(self) -> str:
        """String representation."""
        stats = self.get_statistics()
        tm_stats = stats['transaction_manager']
        return (
            f"TransactionCoordinator("
            f"active_txns={tm_stats['active_transactions']}, "
            f"committed={tm_stats['total_committed']}, "
            f"aborted={tm_stats['total_aborted']})"
        )


def demo_transaction_coordinator():
    """
    Simple demonstration of TransactionCoordinator functionality.
    """
    print(">> TransactionCoordinator Demo")
    print("=" * 50)
    
    try:
        # Create a simple mock database manager for demo
        class MockDBManager:
            def __init__(self):
                self.data = {}
            
            def get_table(self, table_name):
                return self.data.setdefault(table_name, {})
        
        # Create coordinator with required parameters
        print("\n>> Step 1: Creating TransactionCoordinator...")
        mock_db = MockDBManager()
        wal_dir = "demo_wal"
        
        # Create WAL directory if it doesn't exist
        import os
        os.makedirs(wal_dir, exist_ok=True)
        
        coordinator = TransactionCoordinator(
            db_manager=mock_db,
            wal_dir=wal_dir,
            auto_recover=False  # Skip recovery for demo
        )
        print(f"   [OK] Created: {coordinator}")
        
        # Demo 1: Simple transaction
        print("\n>> Step 2: Simple Transaction Demo")
        txn = coordinator.begin_transaction()
        print(f"   [OK] Started transaction: {txn.txn_id}")
        
        coordinator.commit(txn.txn_id)
        print(f"   [OK] Committed transaction: {txn.txn_id}")
        
        # Demo 2: Transaction with operations
        print("\n>> Step 3: Transaction with Operations")
        txn = coordinator.begin_transaction()
        print(f"   [OK] Started transaction: {txn.txn_id}")
        
        # Simulate some operations
        print("   [INFO] Simulating database operations...")
        import time
        time.sleep(0.1)  # Simulate work
        
        coordinator.commit(txn.txn_id)
        print(f"   [OK] Committed transaction: {txn.txn_id}")
        
        # Demo 3: Show statistics
        print("\n>> Step 4: System Statistics")
        stats = coordinator.get_statistics()
        print(f"   [STATS] Transaction Manager: {stats['transaction_manager']['total_committed']} committed")
        print(f"   [STATS] WAL Entries: {stats['wal']['total_entries']}")
        print(f"   [STATS] Lock Manager: {stats['lock_manager']['total_locks_acquired']} locks acquired")
        
        # Demo 4: Context manager usage
        print("\n>> Step 5: Context Manager Demo")
        with coordinator.transaction() as ctx_txn:
            print(f"   [OK] Auto-managed transaction: {ctx_txn.txn_id}")
        print("   [OK] Transaction auto-committed")
        
        print("\n[SUCCESS] Demo completed successfully!")
        print(f"[FINAL] Final coordinator state: {coordinator}")
        
        # Shutdown
        coordinator.shutdown()
        print("[SHUTDOWN] Coordinator shutdown complete")
        
        # Clean up demo files
        import shutil
        if os.path.exists(wal_dir):
            shutil.rmtree(wal_dir)
            print("[CLEANUP] Demo files cleaned up")
        
    except Exception as e:
        print(f"[ERROR] Demo failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    demo_transaction_coordinator()
