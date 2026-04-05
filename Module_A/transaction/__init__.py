"""
Transaction management module for ACID compliance.

This module provides transaction management, write-ahead logging,
crash recovery, and locking mechanisms for the B+ Tree database.
"""

try:
    # Try relative imports first (when used as module)
    from .transaction_manager import (
        TransactionManager,
        Transaction,
        TransactionState,
        TransactionError,
        DeadlockError,
        IsolationLevel,
    )
    from .wal import WALManager, LogRecord
    from .lock_manager import LockManager, LockMode, Lock
    from .transactional_storage import TransactionalStorage, OperationType, PendingOperation
    from .recovery import RecoveryManager, RecoveryStatistics, auto_recover_on_startup
    from .coordinator import TransactionCoordinator, TransactionConfig
except ImportError:
    # Fall back to direct imports (when run as script)
    from transaction_manager import (
        TransactionManager,
        Transaction,
        TransactionState,
        TransactionError,
        DeadlockError,
        IsolationLevel,
    )
    from wal import WALManager, LogRecord
    from lock_manager import LockManager, LockMode, Lock
    from transactional_storage import TransactionalStorage, OperationType, PendingOperation
    from recovery import RecoveryManager, RecoveryStatistics, auto_recover_on_startup
    from coordinator import TransactionCoordinator, TransactionConfig

__all__ = [
    "TransactionManager",
    "Transaction",
    "TransactionState",
    "TransactionError",
    "DeadlockError",
    "IsolationLevel",
    "WALManager",
    "LogRecord",
    "LockManager",
    "LockMode",
    "Lock",
    "TransactionalStorage",
    "OperationType",
    "PendingOperation",
    "RecoveryManager",
    "RecoveryStatistics",
    "auto_recover_on_startup",
    "TransactionCoordinator",
    "TransactionConfig",
]
