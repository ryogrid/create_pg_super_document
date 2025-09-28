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
  - [GetTransactionSnapshot](GetTransactionSnapshot.md) (primary entry point)
  - [GetLatestSnapshot](GetLatestSnapshot.md)
  - [GetNonHistoricCatalogSnapshot](GetNonHistoricCatalogSnapshot.md)
  - [SetTransactionSnapshot](../S/SetTransactionSnapshot.md)

## Notes and Other Information
- Requires ProcArrayLock in shared mode during execution
- The snapshot's subxid data may be marked as overflowed if too many subtransactions exist
- Memory allocation for xip/subxip arrays is done outside the lock for better performance
- During Hot Standby, all XIDs are stored in subxip[] for simplicity, leaving xip[] empty
- The function handles both bootstrap mode and normal transaction processing
- Critical for maintaining transaction isolation and implementing PostgreSQL's MVCC model
- Updates global visibility state used by vacuum and other maintenance operations

## Simplified Source

```c
// Simplified version of GetSnapshotData
Snapshot GetSnapshotData(Snapshot snapshot) {
    ProcArrayStruct *arrayP = procArray;
    TransactionId *other_xids = ProcGlobal->xids;
    TransactionId xmin, xmax;
    int count = 0;
    int subcount = 0;
    bool suboverflowed = false;

    Assert(snapshot != NULL);

    // Allocate XID arrays if this is the first call
    if (snapshot->xip == NULL) {
        snapshot->xip = (TransactionId *)
            malloc(GetMaxSnapshotXidCount() * sizeof(TransactionId));
        if (snapshot->xip == NULL) {
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        }

        snapshot->subxip = (TransactionId *)
            malloc(GetMaxSnapshotSubxidCount() * sizeof(TransactionId));
        if (snapshot->subxip == NULL) {
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        }
    }

    // Acquire shared lock on process array
    LWLockAcquire(ProcArrayLock, LW_SHARED);

    // Try to reuse existing snapshot data
    if (GetSnapshotDataReuse(snapshot)) {
        LWLockRelease(ProcArrayLock);
        return snapshot;
    }

    // Get current transaction state
    FullTransactionId latest_completed = TransamVariables->latestCompletedXid;
    TransactionId myxid = other_xids[MyProc->pgxactoff];

    // Set xmax to next XID after latest completed
    xmax = XidFromFullTransactionId(latest_completed);
    TransactionIdAdvance(xmax);
    xmin = xmax; // Start with xmax, then find actual minimum

    // Include our own XID in xmin calculation
    if (TransactionIdIsNormal(myxid) && NormalTransactionIdPrecedes(myxid, xmin)) {
        xmin = myxid;
    }

    snapshot->takenDuringRecovery = RecoveryInProgress();

    if (!snapshot->takenDuringRecovery) {
        // Normal operation: scan proc array for active transactions
        int numProcs = arrayP->numProcs;
        TransactionId *xip = snapshot->xip;

        for (int pgxactoff = 0; pgxactoff < numProcs; pgxactoff++) {
            TransactionId xid = UINT32_ACCESS_ONCE(other_xids[pgxactoff]);
            uint8 statusFlags;

            // Skip invalid XIDs and our own XID
            if (xid == InvalidTransactionId || pgxactoff == MyProc->pgxactoff) {
                continue;
            }

            // Skip XIDs >= xmax (they're considered running anyway)
            if (!NormalTransactionIdPrecedes(xid, xmax)) {
                continue;
            }

            // Skip VACUUM and logical decoding processes
            statusFlags = ProcGlobal->statusFlags[pgxactoff];
            if (statusFlags & (PROC_IN_LOGICAL_DECODING | PROC_IN_VACUUM)) {
                continue;
            }

            // Update xmin and add to snapshot
            if (NormalTransactionIdPrecedes(xid, xmin)) {
                xmin = xid;
            }
            xip[count++] = xid;

            // Collect subtransaction XIDs if not overflowed
            if (!suboverflowed) {
                XidCacheStatus *subxidState = &ProcGlobal->subxidStates[pgxactoff];
                if (subxidState->overflowed) {
                    suboverflowed = true;
                } else if (subxidState->count > 0) {
                    PGPROC *proc = &allProcs[arrayP->pgprocnos[pgxactoff]];
                    pg_read_barrier();
                    memcpy(snapshot->subxip + subcount, proc->subxids.xids,
                           subxidState->count * sizeof(TransactionId));
                    subcount += subxidState->count;
                }
            }
        }
    } else {
        // Hot Standby: use known assigned XIDs
        subcount = KnownAssignedXidsGetAndSetXmin(snapshot->subxip, &xmin, xmax);
        if (TransactionIdPrecedesOrEquals(xmin, procArray->lastOverflowedXid)) {
            suboverflowed = true;
        }
    }

    // Update backend globals and release lock
    if (!TransactionIdIsValid(MyProc->xmin)) {
        MyProc->xmin = TransactionXmin = xmin;
    }

    LWLockRelease(ProcArrayLock);

    // Update global visibility bounds for vacuum coordination
    // (GlobalVis* variables update logic simplified)

    RecentXmin = xmin;

    // Fill in snapshot structure
    snapshot->xmin = xmin;
    snapshot->xmax = xmax;
    snapshot->xcnt = count;
    snapshot->subxcnt = subcount;
    snapshot->suboverflowed = suboverflowed;
    snapshot->snapXactCompletionCount = TransamVariables->xactCompletionCount;
    snapshot->curcid = GetCurrentCommandId(false);

    // Initialize snapshot metadata
    snapshot->active_count = 0;
    snapshot->regd_count = 0;
    snapshot->copied = false;
    snapshot->lsn = InvalidXLogRecPtr;
    snapshot->whenTaken = 0;

    return snapshot;
}
```

Key simplifications made:
- Condensed the complex ProcArray scanning logic into essential steps
- Simplified memory allocation and error handling
- Removed detailed global visibility update logic (noted as simplified)
- Consolidated Hot Standby vs normal operation paths
- Preserved all critical functionality while dramatically reducing complexity
- Maintained proper locking and memory barriers where essential