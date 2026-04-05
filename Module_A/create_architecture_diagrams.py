#!/usr/bin/env python3
"""
Transaction System Architecture Diagram Generator

Creates visual representations of the transaction system architecture
using both ASCII art and Graphviz formats.
"""

def generate_ascii_architecture():
    """Generate ASCII art architecture diagram."""
    return '''
Transaction System Architecture
==============================

┌─────────────────────────────────────────────────────────────────────────────┐
│                          APPLICATION LAYER                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                     TransactionCoordinator                                  │
│                    ┌─────────────────────┐                                 │
│                    │  Context Manager    │                                 │
│                    │  Retry Logic       │                                 │
│                    │  Error Handling    │                                 │
│                    └─────────────────────┘                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                        TRANSACTION ENGINE                                   │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │ TransactionMgr  │  │   LockManager   │  │ RecoveryManager │             │
│  │                 │  │                 │  │                 │             │
│  │ • Lifecycle     │  │ • S/X Locks     │  │ • Analysis      │             │
│  │ • State Track   │  │ • Deadlock Det  │  │ • Undo Phase    │             │
│  │ • Isolation     │  │ • Timeout       │  │ • Redo Phase    │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
├─────────────────────────────────────────────────────────────────────────────┤
│                         STORAGE LAYER                                       │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │TransactionalStg │  │   WALManager    │  │  DBManager      │             │
│  │                 │  │                 │  │                 │             │
│  │ • Buffering     │  │ • Log Entries   │  │ • Multi-table   │             │
│  │ • Read-own-wrt  │  │ • Persistence   │  │ • Table Mgmt    │             │
│  │ • Before Image  │  │ • Checkpoints   │  │ • Schema        │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
├─────────────────────────────────────────────────────────────────────────────┤
│                         DATA STRUCTURES                                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │   B+ Trees      │  │   Hash Tables   │  │    Indexes      │             │
│  │                 │  │                 │  │                 │             │
│  │ • Ordered Data  │  │ • Fast Lookup   │  │ • Key Access    │             │
│  │ • Range Query   │  │ • Lock Tables   │  │ • Efficient     │             │
│  │ • Leaf Linking  │  │ • Txn Tracking  │  │ • Persistent    │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────┘

Transaction Flow:
================

1. BEGIN → TransactionManager creates transaction
2. OPERATIONS → Buffered in TransactionalStorage + Logged in WAL
3. LOCKS → Acquired via LockManager for isolation
4. COMMIT → WAL persistence + B+ Tree updates + Lock release
5. ROLLBACK → Discard buffers + Release locks
6. RECOVERY → WAL replay on startup (Analysis → Undo → Redo)

ACID Properties Implementation:
==============================

Atomicity:    Buffered operations + WAL + Rollback capability
Consistency:  Constraint validation + Referential integrity  
Isolation:    Lock-based concurrency + READ_COMMITTED
Durability:   Write-Ahead Logging + Force-write policy
'''

def generate_graphviz_diagram():
    """Generate Graphviz DOT format architecture diagram."""
    return '''
digraph TransactionArchitecture {
    rankdir=TB;
    node [shape=box, style=rounded];
    
    // Application Layer
    subgraph cluster_app {
        label="Application Layer";
        style=filled;
        color=lightblue;
        
        App [label="User Application"];
        Coord [label="TransactionCoordinator\\n• Context Manager\\n• Retry Logic\\n• Error Handling"];
    }
    
    // Transaction Engine
    subgraph cluster_engine {
        label="Transaction Engine";
        style=filled;
        color=lightgreen;
        
        TxnMgr [label="TransactionManager\\n• Lifecycle\\n• State Tracking\\n• Isolation"];
        LockMgr [label="LockManager\\n• S/X Locks\\n• Deadlock Detection\\n• Timeout"];
        Recovery [label="RecoveryManager\\n• Analysis Phase\\n• Undo Phase\\n• Redo Phase"];
    }
    
    // Storage Layer  
    subgraph cluster_storage {
        label="Storage Layer";
        style=filled;
        color=lightyellow;
        
        TxnStorage [label="TransactionalStorage\\n• Buffering\\n• Read-own-writes\\n• Before Images"];
        WAL [label="WALManager\\n• Log Entries\\n• Persistence\\n• Checkpoints"];
        DBMgr [label="DBManager\\n• Multi-table\\n• Schema\\n• Table Management"];
    }
    
    // Data Structures
    subgraph cluster_data {
        label="Data Structures";
        style=filled;
        color=lightcoral;
        
        BPTree [label="B+ Trees\\n• Ordered Storage\\n• Range Queries\\n• Leaf Linking"];
        HashTbl [label="Hash Tables\\n• Fast Lookup\\n• Lock Tables\\n• Transaction Tracking"];
    }
    
    // Connections
    App -> Coord;
    Coord -> TxnMgr;
    Coord -> LockMgr;
    Coord -> TxnStorage;
    
    TxnMgr -> WAL;
    TxnMgr -> Recovery;
    
    LockMgr -> HashTbl;
    
    Recovery -> WAL;
    Recovery -> BPTree;
    
    TxnStorage -> BPTree;
    WAL -> BPTree;
    
    DBMgr -> BPTree;
    DBMgr -> HashTbl;
    
    // Performance Monitoring (side component)
    Monitor [label="PerformanceMonitor\\n• Real-time Metrics\\n• CSV Logging\\n• Reports", shape=ellipse, color=orange];
    Coord -> Monitor [style=dashed, label="monitors"];
}
'''

def create_architecture_files():
    """Create architecture documentation files."""
    
    # Create ASCII architecture file
    with open('architecture_ascii.txt', 'w') as f:
        f.write(generate_ascii_architecture())
    
    # Create Graphviz DOT file
    with open('architecture.dot', 'w') as f:
        f.write(generate_graphviz_diagram())
    
    print("Architecture diagrams created:")
    print("  - architecture_ascii.txt (text diagram)")
    print("  - architecture.dot (Graphviz source)")
    print("\nTo generate PNG from Graphviz:")
    print("  dot -Tpng architecture.dot -o architecture.png")

if __name__ == '__main__':
    create_architecture_files()
    
    # Display ASCII version
    print(generate_ascii_architecture())