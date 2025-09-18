# GetSnapshotData

## Location
src/backend/storage/ipc/procarray.c: 2177 - 2535

## Overview
The core function that constructs a snapshot containing information about currently running transactions, providing the foundation for MVCC (Multi-Version Concurrency Control) visibility decisions.

## Definition


## Detailed Description
GetSnapshotData creates a comprehensive snapshot that captures the state of all running transactions at a specific point in time. This snapshot is essential for MVCC, determining which tuples are visible to the current transaction.

The function constructs a snapshot containing:
- **xmin**: The lowest still-running transaction ID (all XIDs < xmin are finished)
- **xmax**: The highest completed transaction ID + 1 (all XIDs >= xmax are still running)
- **xip array**: List of running transaction IDs in the range xmin <= xid < xmax
- **subxip array**: List of running subtransaction IDs

The function operates differently based on recovery state:
- **Normal operation**: Scans the ProcArray to collect active transaction IDs, filtering out VACUUM processes and logical decoding backends
- **Hot Standby**: Uses KnownAssignedXids since the distinction between top-level and subtransactions is not maintained during recovery

Key optimizations include:
- **Snapshot reuse**: Calls GetSnapshotDataReuse() to avoid expensive rebuilds when possible
- **Memory management**: Reuses previously allocated xip/subxip arrays when available
- **Efficient scanning**: Uses atomic reads and memory barriers for safe concurrent access

The function also updates global visibility bounds (GlobalVis*Rels) and backend-global variables (TransactionXmin, RecentXmin) to coordinate transaction management across the system.

## Parameters / Member Variables
- : A pre-allocated Snapshot structure to populate with current transaction state information

## Dependencies
- Functions called/Symbols referenced:
  - GetSnapshotDataReuse (optimization for snapshot reuse)
  - GetMaxSnapshotXidCount, GetMaxSnapshotSubxidCount (array sizing)
  - RecoveryInProgress (determines operational mode)
  - KnownAssignedXidsGetAndSetXmin (Hot Standby transaction collection)
  - GetCurrentCommandId (command counter management)
  - Various transaction ID manipulation functions
- Called from (representative examples):
  - GetTransactionSnapshot (primary entry point)
  - GetLatestSnapshot
  - GetNonHistoricCatalogSnapshot
  - SetTransactionSnapshot

## Notes and Other Information
- Requires ProcArrayLock in shared mode during execution
- The snapshot's subxid data may be marked as overflowed if too many subtransactions exist
- Memory allocation for xip/subxip arrays is done outside the lock for better performance
- During Hot Standby, all XIDs are stored in subxip[] for simplicity, leaving xip[] empty
- The function handles both bootstrap mode and normal transaction processing
- Critical for maintaining transaction isolation and implementing PostgreSQL's MVCC model
- Updates global visibility state used by vacuum and other maintenance operations