"""
Lock Manager - Provides concurrency control for transactions.

Implements:
- Shared locks (S) - Multiple readers can hold simultaneously
- Exclusive locks (X) - Only one writer, blocks all others
- Lock compatibility matrix
- Deadlock prevention via ordered locking
- Lock timeout support
- Integration with TransactionManager

Lock Compatibility Matrix:
           | S | X |
        -----------
        S  | ✓ | ✗ |
        X  | ✗ | ✗ |
        
where ✓ = compatible, ✗ = incompatible
"""

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Set, Optional, List, Tuple
from collections import defaultdict


class LockMode(Enum):
    """Lock modes."""
    SHARED = "S"       # Read lock - multiple transactions can hold
    EXCLUSIVE = "X"    # Write lock - only one transaction can hold


@dataclass
class Lock:
    """Represents a lock on a resource."""
    resource_id: str
    mode: LockMode
    holders: Set[str] = field(default_factory=set)  # Transaction IDs holding this lock
    waiting: List[Tuple[str, LockMode]] = field(default_factory=list)  # (txn_id, mode) waiting
    acquired_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def is_compatible(self, requested_mode: LockMode, requesting_txn: str) -> bool:
        """
        Check if a requested lock mode is compatible with current holders.
        
        Args:
            requested_mode: Lock mode being requested
            requesting_txn: Transaction ID requesting the lock
        
        Returns:
            True if compatible (can be granted immediately)
        """
        # If requesting transaction already holds the lock, check if upgrade is needed
        if requesting_txn in self.holders:
            # Already have the lock
            if requested_mode == self.mode:
                return True  # Same mode, already granted
            elif self.mode == LockMode.SHARED and requested_mode == LockMode.EXCLUSIVE:
                # Lock upgrade: S -> X
                # Only allowed if this txn is the only holder
                return len(self.holders) == 1
            else:
                # Lock downgrade: X -> S (always allowed)
                return True
        
        # New request from different transaction
        if requested_mode == LockMode.SHARED:
            # Shared lock compatible with other shared locks only
            return self.mode == LockMode.SHARED
        else:
            # Exclusive lock compatible with nothing
            return len(self.holders) == 0


class LockManager:
    """
    Manages locks for concurrent transaction access.
    
    Features:
    - Shared (S) and Exclusive (X) locks
    - Table-level or row-level locking (configurable via resource_id)
    - Deadlock prevention via ordered resource locking
    - Lock timeout support
    - Lock upgrade/downgrade
    - Transaction cleanup on commit/rollback
    
    Usage:
        lm = LockManager()
        
        # Acquire locks
        lm.acquire('TXN-1', 'table:users', LockMode.SHARED)      # Read lock
        lm.acquire('TXN-2', 'table:products', LockMode.EXCLUSIVE) # Write lock
        
        # Release locks
        lm.release('TXN-1', 'table:users')
        
        # Release all locks for a transaction
        lm.release_all('TXN-1')
    """
    
    def __init__(self, lock_timeout: float = 30.0, enable_deadlock_detection: bool = True):
        """
        Initialize Lock Manager.
        
        Args:
            lock_timeout: Maximum time (seconds) to wait for a lock
            enable_deadlock_detection: Whether to detect deadlocks (not implemented yet)
        """
        self.lock_timeout = lock_timeout
        self.enable_deadlock_detection = enable_deadlock_detection
        
        # Lock table: resource_id -> Lock object
        self._locks: Dict[str, Lock] = {}
        
        # Transaction lock tracking: txn_id -> set of resource_ids
        self._txn_locks: Dict[str, Set[str]] = defaultdict(set)
        
        # Global lock for lock manager operations
        self._manager_lock = threading.RLock()
        
        # Lock wait statistics
        self._total_locks_acquired = 0
        self._total_locks_released = 0
        self._total_lock_waits = 0
        self._total_timeouts = 0
    
    def acquire(
        self,
        txn_id: str,
        resource_id: str,
        mode: LockMode,
        timeout: Optional[float] = None
    ) -> bool:
        """
        Acquire a lock on a resource.
        
        Args:
            txn_id: Transaction ID requesting the lock
            resource_id: Resource identifier (e.g., 'table:users', 'row:users:1')
            mode: Lock mode (SHARED or EXCLUSIVE)
            timeout: Override default lock timeout (seconds)
        
        Returns:
            True if lock acquired, False if timeout
        
        Raises:
            TimeoutError: If lock cannot be acquired within timeout
        """
        timeout = timeout if timeout is not None else self.lock_timeout
        start_time = time.time()
        
        with self._manager_lock:
            # Get or create lock for this resource
            if resource_id not in self._locks:
                # No existing lock - grant immediately
                lock = Lock(resource_id=resource_id, mode=mode)
                lock.holders.add(txn_id)
                self._locks[resource_id] = lock
                self._txn_locks[txn_id].add(resource_id)
                self._total_locks_acquired += 1
                return True
            
            lock = self._locks[resource_id]
            
            # Check compatibility
            if lock.is_compatible(mode, txn_id):
                # Compatible - grant immediately
                if txn_id in lock.holders:
                    # Lock upgrade/downgrade
                    if mode == LockMode.EXCLUSIVE and lock.mode == LockMode.SHARED:
                        # Upgrade S -> X
                        lock.mode = LockMode.EXCLUSIVE
                else:
                    # New holder
                    lock.holders.add(txn_id)
                    self._txn_locks[txn_id].add(resource_id)
                
                self._total_locks_acquired += 1
                return True
        
        # Incompatible - need to wait
        self._total_lock_waits += 1
        
        # Wait for lock to become available
        while time.time() - start_time < timeout:
            time.sleep(0.01)  # Small sleep to avoid busy-waiting
            
            with self._manager_lock:
                lock = self._locks.get(resource_id)
                if lock is None or lock.is_compatible(mode, txn_id):
                    # Lock released or became compatible
                    if resource_id not in self._locks:
                        lock = Lock(resource_id=resource_id, mode=mode)
                        lock.holders.add(txn_id)
                        self._locks[resource_id] = lock
                    else:
                        lock = self._locks[resource_id]
                        lock.holders.add(txn_id)
                        if mode == LockMode.EXCLUSIVE:
                            lock.mode = LockMode.EXCLUSIVE
                    
                    self._txn_locks[txn_id].add(resource_id)
                    self._total_locks_acquired += 1
                    return True
        
        # Timeout
        self._total_timeouts += 1
        raise TimeoutError(
            f"Transaction {txn_id} could not acquire {mode.value} lock on {resource_id} "
            f"within {timeout} seconds"
        )
    
    def release(self, txn_id: str, resource_id: str) -> bool:
        """
        Release a lock on a resource.
        
        Args:
            txn_id: Transaction ID releasing the lock
            resource_id: Resource identifier
        
        Returns:
            True if lock was released, False if lock didn't exist
        """
        with self._manager_lock:
            if resource_id not in self._locks:
                return False
            
            lock = self._locks[resource_id]
            
            if txn_id not in lock.holders:
                return False  # Transaction didn't hold this lock
            
            # Remove transaction from holders
            lock.holders.remove(txn_id)
            self._txn_locks[txn_id].discard(resource_id)
            self._total_locks_released += 1
            
            # If no more holders, remove the lock
            if len(lock.holders) == 0:
                del self._locks[resource_id]
            
            return True
    
    def release_all(self, txn_id: str) -> int:
        """
        Release all locks held by a transaction.
        
        Args:
            txn_id: Transaction ID
        
        Returns:
            Number of locks released
        """
        with self._manager_lock:
            resource_ids = list(self._txn_locks.get(txn_id, set()))
            count = 0
            
            for resource_id in resource_ids:
                if self.release(txn_id, resource_id):
                    count += 1
            
            # Clean up transaction entry
            if txn_id in self._txn_locks:
                del self._txn_locks[txn_id]
            
            return count
    
    def has_lock(self, txn_id: str, resource_id: str) -> bool:
        """
        Check if a transaction holds a lock on a resource.
        
        Args:
            txn_id: Transaction ID
            resource_id: Resource identifier
        
        Returns:
            True if transaction holds the lock
        """
        with self._manager_lock:
            if resource_id not in self._locks:
                return False
            return txn_id in self._locks[resource_id].holders
    
    def get_lock_mode(self, txn_id: str, resource_id: str) -> Optional[LockMode]:
        """
        Get the lock mode held by a transaction on a resource.
        
        Args:
            txn_id: Transaction ID
            resource_id: Resource identifier
        
        Returns:
            LockMode if lock is held, None otherwise
        """
        with self._manager_lock:
            if resource_id not in self._locks:
                return None
            lock = self._locks[resource_id]
            if txn_id in lock.holders:
                return lock.mode
            return None
    
    def get_transaction_locks(self, txn_id: str) -> Dict[str, LockMode]:
        """
        Get all locks held by a transaction.
        
        Args:
            txn_id: Transaction ID
        
        Returns:
            Dictionary mapping resource_id -> LockMode
        """
        with self._manager_lock:
            result = {}
            for resource_id in self._txn_locks.get(txn_id, set()):
                if resource_id in self._locks:
                    result[resource_id] = self._locks[resource_id].mode
            return result
    
    def get_lock_holders(self, resource_id: str) -> Set[str]:
        """
        Get all transactions holding a lock on a resource.
        
        Args:
            resource_id: Resource identifier
        
        Returns:
            Set of transaction IDs
        """
        with self._manager_lock:
            if resource_id not in self._locks:
                return set()
            return self._locks[resource_id].holders.copy()
    
    def get_statistics(self) -> Dict[str, any]:
        """Get lock manager statistics."""
        with self._manager_lock:
            active_locks = len(self._locks)
            active_transactions = len(self._txn_locks)
            
            return {
                'active_locks': active_locks,
                'active_transactions': active_transactions,
                'total_locks_acquired': self._total_locks_acquired,
                'total_locks_released': self._total_locks_released,
                'total_lock_waits': self._total_lock_waits,
                'total_timeouts': self._total_timeouts,
                'lock_timeout_seconds': self.lock_timeout,
            }
    
    def acquire_table_lock(self, txn_id: str, table_name: str, for_write: bool = False) -> bool:
        """
        Convenience method to acquire a table-level lock.
        
        Args:
            txn_id: Transaction ID
            table_name: Name of the table
            for_write: True for exclusive lock, False for shared lock
        
        Returns:
            True if lock acquired
        """
        resource_id = f"table:{table_name}"
        mode = LockMode.EXCLUSIVE if for_write else LockMode.SHARED
        return self.acquire(txn_id, resource_id, mode)
    
    def acquire_row_lock(self, txn_id: str, table_name: str, key: any, for_write: bool = False) -> bool:
        """
        Convenience method to acquire a row-level lock.
        
        Args:
            txn_id: Transaction ID
            table_name: Name of the table
            key: Primary key of the row
            for_write: True for exclusive lock, False for shared lock
        
        Returns:
            True if lock acquired
        """
        resource_id = f"row:{table_name}:{key}"
        mode = LockMode.EXCLUSIVE if for_write else LockMode.SHARED
        return self.acquire(txn_id, resource_id, mode)
    
    def acquire_multiple_table_locks(
        self,
        txn_id: str,
        tables: List[str],
        for_write: bool = False
    ) -> bool:
        """
        Acquire locks on multiple tables in alphabetical order (prevents deadlock).
        
        Args:
            txn_id: Transaction ID
            tables: List of table names
            for_write: True for exclusive locks, False for shared locks
        
        Returns:
            True if all locks acquired
        """
        # Sort tables alphabetically to ensure consistent ordering
        sorted_tables = sorted(tables)
        
        try:
            for table in sorted_tables:
                self.acquire_table_lock(txn_id, table, for_write)
            return True
        except TimeoutError:
            # Release any locks acquired so far
            self.release_all(txn_id)
            raise
    
    def __repr__(self) -> str:
        """String representation of LockManager."""
        stats = self.get_statistics()
        return (
            f"LockManager("
            f"active_locks={stats['active_locks']}, "
            f"active_txns={stats['active_transactions']}, "
            f"waits={stats['total_lock_waits']}, "
            f"timeouts={stats['total_timeouts']})"
        )
