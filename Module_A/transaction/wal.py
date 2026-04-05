"""
Write-Ahead Logging (WAL) System for durability.

Provides:
- Append-only log file with fsync for durability
- Log record format for all transaction operations
- Checkpoint mechanism to truncate old logs
- Recovery support (read logs for crash recovery)

WAL Record Format (JSONL - JSON Lines):
{
    "type": "BEGIN|INSERT|UPDATE|DELETE|COMMIT|ROLLBACK|CHECKPOINT",
    "txn_id": "TXN-...",
    "timestamp": "ISO8601",
    "table": "table_name",  # for data operations
    "key": "key_value",
    "old_value": "...",     # for UPDATE/DELETE
    "new_value": "...",     # for INSERT/UPDATE
    "lsn": 12345            # Log Sequence Number
}
"""

import os
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, TextIO
from pathlib import Path


@dataclass
class LogRecord:
    """Represents a single log entry."""
    lsn: int  # Log Sequence Number (monotonic counter)
    type: str  # BEGIN, INSERT, UPDATE, DELETE, COMMIT, ROLLBACK, CHECKPOINT
    txn_id: str
    timestamp: datetime
    table: Optional[str] = None
    key: Optional[Any] = None
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'lsn': self.lsn,
            'type': self.type,
            'txn_id': self.txn_id,
            'timestamp': self.timestamp.isoformat(),
            'table': self.table,
            'key': str(self.key) if self.key is not None else None,
            'old_value': str(self.old_value) if self.old_value is not None else None,
            'new_value': str(self.new_value) if self.new_value is not None else None,
            'metadata': self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LogRecord':
        """Create LogRecord from dictionary."""
        return cls(
            lsn=data['lsn'],
            type=data['type'],
            txn_id=data['txn_id'],
            timestamp=datetime.fromisoformat(data['timestamp']),
            table=data.get('table'),
            key=data.get('key'),
            old_value=data.get('old_value'),
            new_value=data.get('new_value'),
            metadata=data.get('metadata', {}),
        )


class WALManager:
    """
    Write-Ahead Log Manager for transaction durability.
    
    Features:
    - Append-only log file (JSONL format)
    - fsync after every write for durability
    - Log Sequence Number (LSN) generation
    - Checkpoint support to truncate old logs
    - Thread-safe writes
    
    Usage:
        wal = WALManager(log_dir='Module_A/data')
        
        # Log transaction operations
        wal.log_begin('TXN-123')
        wal.log_insert('TXN-123', 'users', key=1, value={'name': 'Alice'})
        wal.log_commit('TXN-123')
        
        # Recovery
        logs = wal.read_all_logs()
        for log in logs:
            # Process log entry
            pass
    """
    
    def __init__(self, log_dir: str = 'data', log_filename: str = 'wal.log'):
        """
        Initialize WAL Manager.
        
        Args:
            log_dir: Directory to store log files
            log_filename: Name of the log file
        """
        self.log_dir = Path(log_dir)
        self.log_filepath = self.log_dir / log_filename
        self.checkpoint_filepath = self.log_dir / f'{log_filename}.checkpoint'
        
        # Create log directory if it doesn't exist
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Log Sequence Number (monotonic counter)
        self._lsn = 0
        self._lock = threading.RLock()
        
        # Open log file in append mode
        self._log_file: Optional[TextIO] = None
        self._open_log_file()
        
        # Initialize LSN from existing log
        self._initialize_lsn()
    
    def _open_log_file(self):
        """Open log file for appending."""
        self._log_file = open(self.log_filepath, 'a', encoding='utf-8', buffering=1)
    
    def _initialize_lsn(self):
        """Initialize LSN from the last log entry."""
        if self.log_filepath.exists() and self.log_filepath.stat().st_size > 0:
            # Read last line to get last LSN
            try:
                with open(self.log_filepath, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            entry = json.loads(line)
                            self._lsn = max(self._lsn, entry.get('lsn', 0))
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                print(f"Warning: Could not initialize LSN from log file: {e}")
    
    def _next_lsn(self) -> int:
        """Generate next Log Sequence Number."""
        with self._lock:
            self._lsn += 1
            return self._lsn
    
    def _write_log(self, log_record: LogRecord):
        """
        Write a log record to the WAL file.
        
        Args:
            log_record: LogRecord to write
        """
        with self._lock:
            # Convert to JSON and write
            log_line = json.dumps(log_record.to_dict()) + '\n'
            self._log_file.write(log_line)
            
            # Force write to disk (fsync for durability)
            self._log_file.flush()
            os.fsync(self._log_file.fileno())
    
    def log(self, log_data: Dict[str, Any]) -> int:
        """
        Generic log method (used by TransactionManager).
        
        Args:
            log_data: Dictionary with log information
        
        Returns:
            LSN of the written log record
        """
        lsn = self._next_lsn()
        
        # Extract fields
        log_type = log_data.get('type', 'UNKNOWN')
        txn_id = log_data.get('txn_id', '')
        
        # Create log record
        record = LogRecord(
            lsn=lsn,
            type=log_type,
            txn_id=txn_id,
            timestamp=datetime.now(timezone.utc),
            table=log_data.get('table'),
            key=log_data.get('key'),
            old_value=log_data.get('old_value'),
            new_value=log_data.get('new_value'),
            metadata={k: v for k, v in log_data.items() 
                     if k not in ('type', 'txn_id', 'table', 'key', 'old_value', 'new_value')},
        )
        
        self._write_log(record)
        return lsn
    
    def log_begin(self, txn_id: str, isolation_level: str = 'READ_COMMITTED') -> int:
        """Log transaction BEGIN."""
        return self.log({
            'type': 'BEGIN',
            'txn_id': txn_id,
            'isolation_level': isolation_level,
        })
    
    def log_commit(self, txn_id: str, tables: Optional[List[str]] = None, operation_count: int = 0) -> int:
        """Log transaction COMMIT."""
        return self.log({
            'type': 'COMMIT',
            'txn_id': txn_id,
            'tables': tables or [],
            'operation_count': operation_count,
        })
    
    def log_rollback(self, txn_id: str) -> int:
        """Log transaction ROLLBACK."""
        return self.log({
            'type': 'ROLLBACK',
            'txn_id': txn_id,
        })
    
    def log_insert(self, txn_id: str, table: str, key: Any, value: Any) -> int:
        """Log INSERT operation."""
        return self.log({
            'type': 'INSERT',
            'txn_id': txn_id,
            'table': table,
            'key': key,
            'new_value': value,
        })
    
    def log_update(self, txn_id: str, table: str, key: Any, old_value: Any, new_value: Any) -> int:
        """Log UPDATE operation."""
        return self.log({
            'type': 'UPDATE',
            'txn_id': txn_id,
            'table': table,
            'key': key,
            'old_value': old_value,
            'new_value': new_value,
        })
    
    def log_delete(self, txn_id: str, table: str, key: Any, old_value: Any) -> int:
        """Log DELETE operation."""
        return self.log({
            'type': 'DELETE',
            'txn_id': txn_id,
            'table': table,
            'key': key,
            'old_value': old_value,
        })
    
    def log_checkpoint(self, checkpoint_lsn: int, active_transactions: List[str]) -> int:
        """
        Log CHECKPOINT marker.
        
        Args:
            checkpoint_lsn: LSN up to which all transactions are complete
            active_transactions: List of transaction IDs still active
        
        Returns:
            LSN of the checkpoint record
        """
        return self.log({
            'type': 'CHECKPOINT',
            'txn_id': 'SYSTEM',
            'checkpoint_lsn': checkpoint_lsn,
            'active_transactions': active_transactions,
        })
    
    def read_all_logs(self) -> List[LogRecord]:
        """
        Read all log records from the WAL file.
        
        Returns:
            List of LogRecord objects
        """
        if not self.log_filepath.exists():
            return []
        
        logs = []
        with open(self.log_filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    data = json.loads(line)
                    log_record = LogRecord.from_dict(data)
                    logs.append(log_record)
                except json.JSONDecodeError as e:
                    print(f"Warning: Could not parse log line: {line[:100]}... Error: {e}")
                except Exception as e:
                    print(f"Warning: Error processing log record: {e}")
        
        return logs
    
    def read_logs_from_lsn(self, start_lsn: int) -> List[LogRecord]:
        """
        Read log records starting from a specific LSN.
        
        Args:
            start_lsn: LSN to start reading from (inclusive)
        
        Returns:
            List of LogRecord objects with LSN >= start_lsn
        """
        all_logs = self.read_all_logs()
        return [log for log in all_logs if log.lsn >= start_lsn]
    
    def get_transactions_from_logs(self) -> Dict[str, List[LogRecord]]:
        """
        Group log records by transaction ID.
        
        Returns:
            Dictionary mapping txn_id -> list of LogRecords
        """
        logs = self.read_all_logs()
        transactions = {}
        
        for log in logs:
            if log.txn_id not in transactions:
                transactions[log.txn_id] = []
            transactions[log.txn_id].append(log)
        
        return transactions
    
    def find_incomplete_transactions(self) -> List[str]:
        """
        Find transactions that have BEGIN but no COMMIT/ROLLBACK.
        
        Returns:
            List of incomplete transaction IDs
        """
        transactions = self.get_transactions_from_logs()
        incomplete = []
        
        for txn_id, logs in transactions.items():
            # Check if has BEGIN but no COMMIT/ROLLBACK
            has_begin = any(log.type == 'BEGIN' for log in logs)
            has_end = any(log.type in ('COMMIT', 'ROLLBACK') for log in logs)
            
            if has_begin and not has_end:
                incomplete.append(txn_id)
        
        return incomplete
    
    def checkpoint(self, active_transactions: Optional[List[str]] = None) -> int:
        """
        Create a checkpoint and truncate old log entries.
        
        Args:
            active_transactions: List of currently active transaction IDs
        
        Returns:
            LSN of the checkpoint
        """
        with self._lock:
            checkpoint_lsn = self._lsn
            active_txns = active_transactions or []
            
            # Log checkpoint
            chk_lsn = self.log_checkpoint(checkpoint_lsn, active_txns)
            
            # Write checkpoint metadata to separate file
            checkpoint_data = {
                'checkpoint_lsn': checkpoint_lsn,
                'checkpoint_time': datetime.now(timezone.utc).isoformat(),
                'active_transactions': active_txns,
            }
            
            with open(self.checkpoint_filepath, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            
            return chk_lsn
    
    def truncate_logs_before_checkpoint(self):
        """
        Truncate log file by removing entries before last checkpoint.
        WARNING: Only safe if all transactions before checkpoint are complete.
        """
        if not self.checkpoint_filepath.exists():
            print("No checkpoint file found, cannot truncate")
            return
        
        with self._lock:
            # Read checkpoint
            with open(self.checkpoint_filepath, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)
            
            checkpoint_lsn = checkpoint_data['checkpoint_lsn']
            
            # Read all logs
            all_logs = self.read_all_logs()
            
            # Keep only logs after checkpoint
            logs_to_keep = [log for log in all_logs if log.lsn > checkpoint_lsn]
            
            # Close current log file
            self._log_file.close()
            
            # Rewrite log file with only logs after checkpoint
            with open(self.log_filepath, 'w', encoding='utf-8') as f:
                for log in logs_to_keep:
                    f.write(json.dumps(log.to_dict()) + '\n')
                f.flush()
                os.fsync(f.fileno())
            
            # Reopen log file
            self._open_log_file()
            
            print(f"Truncated log file: kept {len(logs_to_keep)} entries after checkpoint LSN {checkpoint_lsn}")
    
    def get_log_statistics(self) -> Dict[str, Any]:
        """Get statistics about the log file."""
        if not self.log_filepath.exists():
            return {
                'log_file_exists': False,
                'total_entries': 0,
                'file_size_bytes': 0,
            }
        
        logs = self.read_all_logs()
        
        # Count by type
        type_counts = {}
        for log in logs:
            type_counts[log.type] = type_counts.get(log.type, 0) + 1
        
        return {
            'log_file_exists': True,
            'log_file_path': str(self.log_filepath),
            'total_entries': len(logs),
            'file_size_bytes': self.log_filepath.stat().st_size,
            'current_lsn': self._lsn,
            'type_counts': type_counts,
            'incomplete_transactions': self.find_incomplete_transactions(),
        }
    
    def close(self):
        """Close the log file."""
        if self._log_file and not self._log_file.closed:
            self._log_file.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close log file."""
        self.close()
    
    def __repr__(self) -> str:
        """String representation."""
        stats = self.get_log_statistics()
        return (
            f"WALManager("
            f"entries={stats['total_entries']}, "
            f"lsn={self._lsn}, "
            f"size={stats['file_size_bytes']} bytes)"
        )
