# GetSnapshotData

## Location
[src/backend/storage/ipc/procarray.c:2177-2535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L2177-L2535)

## Overview
The core function that constructs a snapshot containing information about currently running transactions, providing the foundation for MVCC (Multi-Version Concurrency Control) visibility decisions.

## Definition

```c
structs.)
	 */
	if (snapshot->xip == NULL)
	{
		/*
		 * First call for this snapshot. Snapshot is same size whether or not
		 * we are in recovery, see later comments.
		 */
		snapshot->xip = (TransactionId *)
			malloc(GetMaxSnapshotXidCount() * sizeof(TransactionId));
		if (snapshot->xip == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
		Assert(snapshot->subxip == NULL);
		snapshot->subxip = (TransactionId *)
			malloc(GetMaxSnapshotSubxidCount() * sizeof(TransactionId));
		if (snapshot->subxip == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
	}

	/*
	 * It is sufficient to get shared lock on ProcArrayLock, even if we are
	 * going to set MyProc->xmin.
	 */
	LWLockAcquire(ProcArrayLock, LW_SHARED);
```
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
- `snapshot`: A pre-allocated Snapshot structure to populate with current transaction state information

## Dependencies
- Functions called/Symbols referenced:
  - [GetSnapshotDataReuse](GetSnapshotDataReuse.md) (optimization for snapshot reuse)
  - [GetMaxSnapshotXidCount](GetMaxSnapshotXidCount.md), GetMaxSnapshotSubxidCount (array sizing)
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (determines operational mode)
  - [KnownAssignedXidsGetAndSetXmin](../K/KnownAssignedXidsGetAndSetXmin.md) (Hot Standby transaction collection)
  - [GetCurrentCommandId](GetCurrentCommandId.md) (command counter management)
  - Various transaction ID manipulation functions
- Called from (representative examples):
  - GetTransactionSnapshot (primary entry point)
  - GetLatestSnapshot
  - [GetNonHistoricCatalogSnapshot](GetNonHistoricCatalogSnapshot.md)
  - SetTransactionSnapshot

## Notes and Other Information
- Requires ProcArrayLock in shared mode during execution
- The snapshot's subxid data may be marked as overflowed if too many subtransactions exist
- Memory allocation for xip/subxip arrays is done outside the lock for better performance
- During Hot Standby, all XIDs are stored in subxip[] for simplicity, leaving xip[] empty
- The function handles both bootstrap mode and normal transaction processing
- Critical for maintaining transaction isolation and implementing PostgreSQL's MVCC model
- Updates global visibility state used by vacuum and other maintenance operations