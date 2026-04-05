"""
Transactional Storage Layer for B+ Tree Integration

This module provides a transactional wrapper around B+ Tree tables that:
1. Buffers changes in memory until COMMIT
2. Captures before-images for ROLLBACK
3. Integrates with Transaction Manager, WAL, and Lock Manager
4. Ensures ACID properties for database operations

Architecture:
- Each transaction maintains a local buffer of pending changes
- Changes are isolated from other transactions until commit
- On COMMIT: Apply buffered changes to actual B+ Trees
- On ROLLBACK: Discard buffered changes, restore old values if needed
"""

from typing import Any, Dict, Optional, List, Tuple
from dataclasses import dataclass, field
from enum import Enum
import threading


class OperationType(Enum):
    """Types of database operations."""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


@dataclass
class PendingOperation:
    """Represents a pending operation in a transaction."""
    operation: OperationType
    table: str
    key: Any
    old_value: Optional[Any] = None  # For UPDATE and DELETE (rollback support)
    new_value: Optional[Any] = None  # For INSERT and UPDATE
    
    def __repr__(self):
        if self.operation == OperationType.INSERT:
            return f"INSERT({self.table}[{self.key}] = {self.new_value})"
        elif self.operation == OperationType.UPDATE:
            return f"UPDATE({self.table}[{self.key}]: {self.old_value} -> {self.new_value})"
        else:  # DELETE
            return f"DELETE({self.table}[{self.key}] = {self.old_value})"


class TransactionalStorage:
    """
    Manages transactional access to database tables.
    
    Features:
    - Buffered writes (changes held in memory until commit)
    - Before-image capture for rollback
    - Integration with Transaction Manager
    - Integration with Lock Manager for concurrency control
    - Read-your-own-writes within a transaction
    """
    
    def __init__(self, db_manager, transaction_manager=None, lock_manager=None):
        """
        Initialize transactional storage.
        
        Args:
            db_manager: DBManager instance with tables
            transaction_manager: TransactionManager instance (optional)
            lock_manager: LockManager instance (optional)
        """
        self.db_manager = db_manager
        self.transaction_manager = transaction_manager
        self.lock_manager = lock_manager
        
        # Transaction buffers: {txn_id: {(table, key): PendingOperation}}
        self._transaction_buffers: Dict[str, Dict[Tuple[str, Any], PendingOperation]] = {}
        self._lock = threading.RLock()
    
    def _get_buffer_key(self, table: str, key: Any) -> Tuple[str, Any]:
        """Create a unique key for transaction buffer."""
        return (table, key)
    
    def _get_transaction_buffer(self, txn_id: str) -> Dict[Tuple[str, Any], PendingOperation]:
        """Get or create buffer for transaction."""
        with self._lock:
            if txn_id not in self._transaction_buffers:
                self._transaction_buffers[txn_id] = {}
            return self._transaction_buffers[txn_id]
    
    def insert(self, txn_id: str, table_name: str, key: Any, value: Any, 
               acquire_lock: bool = True) -> bool:
        """
        Insert a record (buffered until commit).
        
        Args:
            txn_id: Transaction ID
            table_name: Table name
            key: Record key
            value: Record value
            acquire_lock: Whether to acquire lock (default True)
            
        Returns:
            True if inserted successfully
            
        Raises:
            KeyError: If table doesn't exist
            ValueError: If key already exists
            TimeoutError: If lock acquisition times out
        """
        # Acquire exclusive lock if requested
        if acquire_lock and self.lock_manager:
            self.lock_manager.acquire_row_lock(txn_id, table_name, key, for_write=True)
        
        # Check if key exists in underlying table
        table = self.db_manager.get_table(table_name)
        existing = table.search(key)
        
        # Check buffer for this transaction
        buffer = self._get_transaction_buffer(txn_id)
        buffer_key = self._get_buffer_key(table_name, key)
        
        # Check if already exists (in table or buffer)
        if existing is not None:
            # Check if deleted in buffer
            if buffer_key in buffer and buffer[buffer_key].operation == OperationType.DELETE:
                # Can reinsert after delete
                pass
            else:
                raise ValueError(f"Key {key} already exists in table {table_name}")
        
        # Check if already in buffer as non-deleted
        if buffer_key in buffer and buffer[buffer_key].operation != OperationType.DELETE:
            raise ValueError(f"Key {key} already inserted in this transaction")
        
        # Buffer the insert operation
        operation = PendingOperation(
            operation=OperationType.INSERT,
            table=table_name,
            key=key,
            old_value=None,
            new_value=value
        )
        buffer[buffer_key] = operation
        
        # Log to transaction manager
        if self.transaction_manager:
            self.transaction_manager.log_operation(
                txn_id, 'INSERT', table_name, key=key, new_value=value
            )
        
        return True
    
    def update(self, txn_id: str, table_name: str, key: Any, new_value: Any,
               acquire_lock: bool = True) -> bool:
        """
        Update a record (buffered until commit).
        
        Args:
            txn_id: Transaction ID
            table_name: Table name
            key: Record key
            new_value: New record value
            acquire_lock: Whether to acquire lock (default True)
            
        Returns:
            True if updated successfully, False if key doesn't exist
            
        Raises:
            KeyError: If table doesn't exist
            TimeoutError: If lock acquisition times out
        """
        # Acquire exclusive lock if requested
        if acquire_lock and self.lock_manager:
            self.lock_manager.acquire_row_lock(txn_id, table_name, key, for_write=True)
        
        # Get current value (considering buffer)
        old_value = self.read(txn_id, table_name, key, acquire_lock=False)
        
        if old_value is None:
            return False  # Key doesn't exist
        
        # Buffer the update operation
        buffer = self._get_transaction_buffer(txn_id)
        buffer_key = self._get_buffer_key(table_name, key)
        
        operation = PendingOperation(
            operation=OperationType.UPDATE,
            table=table_name,
            key=key,
            old_value=old_value,
            new_value=new_value
        )
        buffer[buffer_key] = operation
        
        # Log to transaction manager
        if self.transaction_manager:
            self.transaction_manager.log_operation(
                txn_id, 'UPDATE', table_name, key=key,
                old_value=old_value, new_value=new_value
            )
        
        return True
    
    def delete(self, txn_id: str, table_name: str, key: Any,
               acquire_lock: bool = True) -> bool:
        """
        Delete a record (buffered until commit).
        
        Args:
            txn_id: Transaction ID
            table_name: Table name
            key: Record key
            acquire_lock: Whether to acquire lock (default True)
            
        Returns:
            True if deleted successfully, False if key doesn't exist
            
        Raises:
            KeyError: If table doesn't exist
            TimeoutError: If lock acquisition times out
        """
        # Acquire exclusive lock if requested
        if acquire_lock and self.lock_manager:
            self.lock_manager.acquire_row_lock(txn_id, table_name, key, for_write=True)
        
        # Get current value (considering buffer)
        old_value = self.read(txn_id, table_name, key, acquire_lock=False)
        
        if old_value is None:
            return False  # Key doesn't exist
        
        # Buffer the delete operation
        buffer = self._get_transaction_buffer(txn_id)
        buffer_key = self._get_buffer_key(table_name, key)
        
        operation = PendingOperation(
            operation=OperationType.DELETE,
            table=table_name,
            key=key,
            old_value=old_value,
            new_value=None
        )
        buffer[buffer_key] = operation
        
        # Log to transaction manager
        if self.transaction_manager:
            self.transaction_manager.log_operation(
                txn_id, 'DELETE', table_name, key=key, old_value=old_value
            )
        
        return True
    
    def read(self, txn_id: str, table_name: str, key: Any,
             acquire_lock: bool = True) -> Optional[Any]:
        """
        Read a record (considers buffered changes - read your own writes).
        
        Args:
            txn_id: Transaction ID
            table_name: Table name
            key: Record key
            acquire_lock: Whether to acquire shared lock (default True)
            
        Returns:
            Record value if found, None otherwise
            
        Raises:
            KeyError: If table doesn't exist
            TimeoutError: If lock acquisition times out
        """
        # Acquire shared lock if requested
        if acquire_lock and self.lock_manager:
            self.lock_manager.acquire_row_lock(txn_id, table_name, key, for_write=False)
        
        # Check transaction buffer first (read your own writes)
        buffer = self._get_transaction_buffer(txn_id)
        buffer_key = self._get_buffer_key(table_name, key)
        
        if buffer_key in buffer:
            op = buffer[buffer_key]
            if op.operation == OperationType.DELETE:
                return None  # Deleted in this transaction
            else:
                return op.new_value  # INSERT or UPDATE
        
        # Not in buffer, read from underlying table
        table = self.db_manager.get_table(table_name)
        return table.search(key)
    
    def commit(self, txn_id: str):
        """
        Commit transaction: Apply all buffered changes to B+ Trees.
        
        Args:
            txn_id: Transaction ID
            
        Raises:
            ValueError: If transaction has no buffer or commit fails
        """
        with self._lock:
            if txn_id not in self._transaction_buffers:
                return  # No changes to commit
            
            buffer = self._transaction_buffers[txn_id]
            
            # Apply all buffered operations
            for (table_name, key), op in buffer.items():
                table = self.db_manager.get_table(table_name)
                
                if op.operation == OperationType.INSERT:
                    # Check if this was INSERT after DELETE in same transaction
                    # In that case, treat as UPDATE
                    existing = table.search(key)
                    if existing is not None:
                        # Was deleted earlier, now reinserting - treat as update
                        table.update(key, op.new_value)
                    else:
                        table.insert(key, op.new_value)
                    
                elif op.operation == OperationType.UPDATE:
                    # For UPDATE after INSERT, the key might not exist yet
                    existing = table.search(key)
                    if existing is not None:
                        success = table.update(key, op.new_value)
                        if not success:
                            raise ValueError(f"Update failed: key {key} not found in {table_name}")
                    else:
                        # INSERT followed by UPDATE in same transaction
                        # Apply as INSERT with final value
                        table.insert(key, op.new_value)
                    
                elif op.operation == OperationType.DELETE:
                    # For DELETE after INSERT, key might not exist in table yet
                    existing = table.search(key)
                    if existing is not None:
                        success = table.delete(key)
                        if not success:
                            raise ValueError(f"Delete failed: key {key} not found in {table_name}")
                    # else: INSERT then DELETE in same txn, net effect is nothing
            
            # Clear transaction buffer
            del self._transaction_buffers[txn_id]
    
    def rollback(self, txn_id: str):
        """
        Rollback transaction: Discard all buffered changes.
        
        Args:
            txn_id: Transaction ID
        """
        with self._lock:
            if txn_id in self._transaction_buffers:
                del self._transaction_buffers[txn_id]
    
    def get_pending_operations(self, txn_id: str) -> List[PendingOperation]:
        """
        Get list of pending operations for a transaction.
        
        Args:
            txn_id: Transaction ID
            
        Returns:
            List of pending operations
        """
        buffer = self._get_transaction_buffer(txn_id)
        return list(buffer.values())
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about transactional storage."""
        with self._lock:
            return {
                'active_transactions': len(self._transaction_buffers),
                'total_pending_operations': sum(
                    len(buf) for buf in self._transaction_buffers.values()
                ),
                'transactions': {
                    txn_id: {
                        'pending_operations': len(buf),
                        'operations': [str(op) for op in buf.values()]
                    }
                    for txn_id, buf in self._transaction_buffers.items()
                }
            }
