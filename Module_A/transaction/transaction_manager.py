"""
Transaction Manager - Coordinates ACID transactions across multiple B+ Trees.

Provides:
- BEGIN: Start a new transaction
- COMMIT: Atomically persist all changes
- ROLLBACK: Undo all changes in a transaction
- Multi-table transaction coordination
- Transaction state tracking
"""

import uuid
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Any, Set, Callable
from contextlib import contextmanager


class TransactionState(Enum):
    """States of a transaction lifecycle."""
    ACTIVE = "ACTIVE"           # Transaction is in progress
    PREPARING = "PREPARING"     # Preparing to commit (validation phase)
    COMMITTED = "COMMITTED"     # Successfully committed
    ABORTED = "ABORTED"         # Rolled back due to error or explicit rollback
    FAILED = "FAILED"           # Failed during commit


class IsolationLevel(Enum):
    """Transaction isolation levels."""
    READ_UNCOMMITTED = "READ_UNCOMMITTED"  # Not implemented (dirty reads allowed)
    READ_COMMITTED = "READ_COMMITTED"      # Default - no dirty reads
    REPEATABLE_READ = "REPEATABLE_READ"    # No phantom reads
    SERIALIZABLE = "SERIALIZABLE"          # Full isolation


class TransactionError(Exception):
    """Base exception for transaction-related errors."""
    pass


class DeadlockError(TransactionError):
    """Raised when a deadlock is detected."""
    pass


class ConcurrencyError(TransactionError):
    """Raised when concurrent modification conflicts occur."""
    pass


@dataclass
class TransactionOperation:
    """Represents a single operation within a transaction."""
    operation_type: str  # 'INSERT', 'UPDATE', 'DELETE'
    table_name: str
    key: Any
    old_value: Optional[Any] = None  # For rollback
    new_value: Optional[Any] = None  # For redo
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class Transaction:
    """
    Represents a single transaction.
    
    Attributes:
        txn_id: Unique transaction identifier
        state: Current state of the transaction
        isolation_level: Isolation level for this transaction
        start_time: When the transaction began
        operations: List of operations performed in this transaction
        tables_accessed: Set of table names accessed by this transaction
        locks_held: Set of lock identifiers held by this transaction
    """
    txn_id: str
    state: TransactionState
    isolation_level: IsolationLevel
    start_time: datetime
    operations: List[TransactionOperation] = field(default_factory=list)
    tables_accessed: Set[str] = field(default_factory=set)
    locks_held: Set[str] = field(default_factory=set)
    committed_at: Optional[datetime] = None
    aborted_at: Optional[datetime] = None
    
    def add_operation(self, op: TransactionOperation):
        """Add an operation to this transaction."""
        self.operations.append(op)
        self.tables_accessed.add(op.table_name)
    
    def duration(self) -> float:
        """Get transaction duration in seconds."""
        if self.committed_at:
            return (self.committed_at - self.start_time).total_seconds()
        elif self.aborted_at:
            return (self.aborted_at - self.start_time).total_seconds()
        else:
            return (datetime.now(timezone.utc) - self.start_time).total_seconds()


class TransactionManager:
    """
    Manages transactions across multiple tables/B+ Trees.
    
    Features:
    - Unique transaction ID generation
    - Transaction state tracking
    - Multi-table operation coordination
    - BEGIN/COMMIT/ROLLBACK operations
    - Context manager support for automatic cleanup
    
    Usage:
        tm = TransactionManager()
        
        # Method 1: Explicit control
        txn = tm.begin()
        try:
            # ... perform operations ...
            tm.commit(txn.txn_id)
        except Exception:
            tm.rollback(txn.txn_id)
        
        # Method 2: Context manager (recommended)
        with tm.transaction() as txn:
            # ... perform operations ...
            # auto-commits on success, auto-rolls back on exception
    """
    
    def __init__(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED):
        """
        Initialize the Transaction Manager.
        
        Args:
            isolation_level: Default isolation level for transactions
        """
        self.default_isolation_level = isolation_level
        
        # Transaction tracking
        self._transactions: Dict[str, Transaction] = {}
        self._lock = threading.RLock()  # Protects transaction manager state
        
        # Statistics
        self._total_committed = 0
        self._total_aborted = 0
        self._total_failed = 0
        
        # Callbacks for integration with WAL and Lock Manager
        self._wal_logger: Optional[Callable] = None
        self._lock_manager: Optional[Any] = None
        self._recovery_manager: Optional[Any] = None
    
    def set_wal_logger(self, wal_logger: Callable):
        """Set the Write-Ahead Logger for durability."""
        self._wal_logger = wal_logger
    
    def set_lock_manager(self, lock_manager: Any):
        """Set the Lock Manager for concurrency control."""
        self._lock_manager = lock_manager
    
    def set_recovery_manager(self, recovery_manager: Any):
        """Set the Recovery Manager for crash recovery."""
        self._recovery_manager = recovery_manager
    
    def begin(self, isolation_level: Optional[Any] = None) -> Transaction:
        """
        Start a new transaction.
        
        Args:
            isolation_level: Isolation level for this transaction (default: manager's default)
                            Can be string or IsolationLevel enum
        
        Returns:
            Transaction object with unique ID
        
        Raises:
            TransactionError: If transaction creation fails
        """
        with self._lock:
            # Generate unique transaction ID
            txn_id = self._generate_txn_id()
            
            # Convert isolation level to enum if it's a string
            iso_level = isolation_level or self.default_isolation_level
            if isinstance(iso_level, str):
                try:
                    iso_level = IsolationLevel[iso_level]
                except KeyError:
                    # Try by value
                    iso_level = IsolationLevel(iso_level)
            
            # Create transaction object
            txn = Transaction(
                txn_id=txn_id,
                state=TransactionState.ACTIVE,
                isolation_level=iso_level,
                start_time=datetime.now(timezone.utc),
            )
            
            # Register transaction
            self._transactions[txn_id] = txn
            
            # Log to WAL if available
            if self._wal_logger:
                self._wal_logger({
                    'type': 'BEGIN',
                    'txn_id': txn_id,
                    'isolation_level': txn.isolation_level.value,
                    'timestamp': txn.start_time.isoformat(),
                })
            
            return txn
    
    def commit(self, txn_id: str) -> bool:
        """
        Commit a transaction, making all changes permanent.
        
        Args:
            txn_id: Transaction ID to commit
        
        Returns:
            True if commit successful
        
        Raises:
            TransactionError: If transaction doesn't exist or is not ACTIVE
        """
        with self._lock:
            txn = self._get_transaction(txn_id)
            
            # Validate state
            if txn.state != TransactionState.ACTIVE:
                raise TransactionError(
                    f"Cannot commit transaction {txn_id} in state {txn.state.value}"
                )
            
            try:
                # Change state to PREPARING
                txn.state = TransactionState.PREPARING
                
                # Log commit to WAL (write-ahead before actual commit)
                if self._wal_logger:
                    self._wal_logger({
                        'type': 'COMMIT',
                        'txn_id': txn_id,
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'tables': list(txn.tables_accessed),
                        'operation_count': len(txn.operations),
                    })
                
                # At this point, changes should already be applied to B+ Trees
                # (buffered writes will be flushed here in later implementation)
                
                # Mark as committed
                txn.state = TransactionState.COMMITTED
                txn.committed_at = datetime.now(timezone.utc)
                
                # Release all locks
                if self._lock_manager:
                    for lock_id in txn.locks_held:
                        self._lock_manager.release(txn_id, lock_id)
                    txn.locks_held.clear()
                
                # Update statistics
                self._total_committed += 1
                
                return True
                
            except Exception as e:
                # Commit failed - mark as FAILED and rollback
                txn.state = TransactionState.FAILED
                self._total_failed += 1
                
                # Attempt rollback
                self._rollback_internal(txn)
                
                raise TransactionError(f"Commit failed for transaction {txn_id}: {e}")
    
    def rollback(self, txn_id: str) -> bool:
        """
        Rollback a transaction, undoing all changes.
        
        Args:
            txn_id: Transaction ID to rollback
        
        Returns:
            True if rollback successful
        
        Raises:
            TransactionError: If transaction doesn't exist
        """
        with self._lock:
            txn = self._get_transaction(txn_id)
            
            # Can only rollback ACTIVE or FAILED transactions
            if txn.state not in (TransactionState.ACTIVE, TransactionState.FAILED):
                raise TransactionError(
                    f"Cannot rollback transaction {txn_id} in state {txn.state.value}"
                )
            
            return self._rollback_internal(txn)
    
    def _rollback_internal(self, txn: Transaction) -> bool:
        """
        Internal rollback implementation.
        
        Args:
            txn: Transaction object to rollback
        
        Returns:
            True if rollback successful
        """
        try:
            # Log rollback to WAL
            if self._wal_logger:
                self._wal_logger({
                    'type': 'ROLLBACK',
                    'txn_id': txn.txn_id,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                })
            
            # Undo operations in reverse order
            for op in reversed(txn.operations):
                # Actual rollback logic will be implemented when integrating with B+ Tree
                # For now, we just track that rollback should happen
                pass
            
            # Mark as aborted
            txn.state = TransactionState.ABORTED
            txn.aborted_at = datetime.now(timezone.utc)
            
            # Release all locks
            if self._lock_manager:
                for lock_id in txn.locks_held:
                    self._lock_manager.release(txn.txn_id, lock_id)
                txn.locks_held.clear()
            
            # Update statistics
            self._total_aborted += 1
            
            return True
            
        except Exception as e:
            raise TransactionError(f"Rollback failed for transaction {txn.txn_id}: {e}")
    
    def log_operation(
        self,
        txn_id: str,
        operation_type: str,
        table_name: str,
        key: Any,
        old_value: Optional[Any] = None,
        new_value: Optional[Any] = None,
    ):
        """
        Log an operation to a transaction.
        
        Args:
            txn_id: Transaction ID
            operation_type: 'INSERT', 'UPDATE', or 'DELETE'
            table_name: Name of the table being modified
            key: Key being operated on
            old_value: Previous value (for UPDATE/DELETE)
            new_value: New value (for INSERT/UPDATE)
        
        Raises:
            TransactionError: If transaction is not ACTIVE
        """
        with self._lock:
            txn = self._get_transaction(txn_id)
            
            if txn.state != TransactionState.ACTIVE:
                raise TransactionError(
                    f"Cannot log operation to transaction {txn_id} in state {txn.state.value}"
                )
            
            # Create operation record
            op = TransactionOperation(
                operation_type=operation_type,
                table_name=table_name,
                key=key,
                old_value=old_value,
                new_value=new_value,
            )
            
            # Add to transaction
            txn.add_operation(op)
            
            # Log to WAL
            if self._wal_logger:
                self._wal_logger({
                    'type': operation_type,
                    'txn_id': txn_id,
                    'table': table_name,
                    'key': str(key),
                    'old_value': str(old_value) if old_value is not None else None,
                    'new_value': str(new_value) if new_value is not None else None,
                    'timestamp': op.timestamp.isoformat(),
                })
    
    def get_transaction(self, txn_id: str) -> Optional[Transaction]:
        """
        Get a transaction by ID.
        
        Args:
            txn_id: Transaction ID
        
        Returns:
            Transaction object or None if not found
        """
        with self._lock:
            return self._transactions.get(txn_id)
    
    def get_active_transactions(self) -> List[Transaction]:
        """Get all currently active transactions."""
        with self._lock:
            return [
                txn for txn in self._transactions.values()
                if txn.state == TransactionState.ACTIVE
            ]
    
    def cleanup_completed_transactions(self, keep_last_n: int = 1000):
        """
        Remove old completed transactions from memory.
        
        Args:
            keep_last_n: Number of recent transactions to keep for history
        """
        with self._lock:
            completed = [
                txn for txn in self._transactions.values()
                if txn.state in (TransactionState.COMMITTED, TransactionState.ABORTED)
            ]
            
            # Sort by completion time
            completed.sort(
                key=lambda t: t.committed_at or t.aborted_at or t.start_time,
                reverse=True
            )
            
            # Remove old transactions
            for txn in completed[keep_last_n:]:
                del self._transactions[txn.txn_id]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get transaction statistics."""
        with self._lock:
            active_count = len(self.get_active_transactions())
            
            return {
                'total_transactions': len(self._transactions),
                'active_transactions': active_count,
                'total_committed': self._total_committed,
                'total_aborted': self._total_aborted,
                'total_failed': self._total_failed,
                'success_rate': (
                    self._total_committed / max(1, self._total_committed + self._total_aborted + self._total_failed)
                ),
            }
    
    @contextmanager
    def transaction(self, isolation_level: Optional[IsolationLevel] = None):
        """
        Context manager for transactions with automatic commit/rollback.
        
        Usage:
            with tm.transaction() as txn:
                # perform operations
                tm.log_operation(txn.txn_id, 'INSERT', 'users', key, None, value)
                # auto-commits on success, auto-rolls back on exception
        
        Args:
            isolation_level: Isolation level for this transaction
        
        Yields:
            Transaction object
        """
        txn = self.begin(isolation_level)
        
        try:
            yield txn
            self.commit(txn.txn_id)
        except Exception as e:
            # Rollback on any exception
            try:
                self.rollback(txn.txn_id)
            except Exception as rollback_error:
                # Log rollback failure but raise original exception
                print(f"WARNING: Rollback failed for transaction {txn.txn_id}: {rollback_error}")
            raise e
    
    def _get_transaction(self, txn_id: str) -> Transaction:
        """
        Internal helper to get transaction with validation.
        
        Args:
            txn_id: Transaction ID
        
        Returns:
            Transaction object
        
        Raises:
            TransactionError: If transaction not found
        """
        txn = self._transactions.get(txn_id)
        if txn is None:
            raise TransactionError(f"Transaction {txn_id} not found")
        return txn
    
    def _generate_txn_id(self) -> str:
        """
        Generate a unique transaction ID.
        
        Returns:
            Unique transaction ID string
        """
        # Format: TXN-<timestamp>-<uuid>
        timestamp = int(time.time() * 1000)  # milliseconds
        unique_id = uuid.uuid4().hex[:8]
        return f"TXN-{timestamp}-{unique_id}"
    
    def __repr__(self) -> str:
        """String representation of TransactionManager."""
        stats = self.get_statistics()
        return (
            f"TransactionManager("
            f"active={stats['active_transactions']}, "
            f"committed={stats['total_committed']}, "
            f"aborted={stats['total_aborted']}, "
            f"success_rate={stats['success_rate']:.2%})"
        )
