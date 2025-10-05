# GetConflictingVirtualXIDs

## Location
[src/backend/storage/ipc/procarray.c:3416-3489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3416-L3489)

## Overview
Returns an array of currently active Virtual Transaction IDs (VXIDs) that may conflict with recovery operations, specifically designed for conflict resolution during recovery on standby servers.

## Definition

```c
VirtualTransactionId *
GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid)
```
## Detailed Description
GetConflictingVirtualXIDs is specialized for hot standby conflict resolution scenarios where recovery processes need to identify active transactions that might conflict with cleanup operations. Unlike GetCurrentVirtualXIDs, this function is specifically optimized for recovery conflict detection and uses different filtering logic.

The function uses a static array that is allocated once with malloc (rather than palloc) to minimize overhead from repeated allocation/deallocation during recovery operations. The result includes a terminator entry for easy iteration. The function excludes prepared transactions (processes with pid == 0) and can optionally filter by database OID.

The conflict detection logic is based on transaction snapshot horizons - transactions with xmin values that could potentially see data being cleaned up by recovery operations are considered conflicting. The function works under shared ProcArrayLock to allow concurrent snapshot taking while ensuring consistency.

## Parameters / Member Variables
- `limitXmin`: Transaction ID cutoff for conflict detection; transactions with xmin ≤ limitXmin may conflict. If InvalidTransactionId, all transactions are considered conflicting
- `dbOid`: Database OID filter; if valid, only includes transactions from this database. If invalid, includes all databases

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - [TransactionIdFollows](../T/TransactionIdFollows.md)
  - GET_VXID_FROM_PGPROC
  - VirtualTransactionIdIsValid
  - UINT32_ACCESS_ONCE
- Called from (representative examples):
  - [ResolveRecoveryConflictWithSnapshot](../R/ResolveRecoveryConflictWithSnapshot.md) (in storage/ipc/standby.c)
  - [ResolveRecoveryConflictWithTablespace](../R/ResolveRecoveryConflictWithTablespace.md) (in storage/ipc/standby.c)

## Notes and Other Information
- Uses static allocation with malloc instead of palloc for performance reasons during recovery
- **IMPORTANT**: Callers must NOT pfree the result - the array is reused across calls
- The result array includes a terminator entry with INVALID_PROC_NUMBER and InvalidLocalTransactionId
- Excludes prepared transactions (pid == 0) from conflict detection
- Designed specifically for recovery conflict resolution, not general transaction monitoring
- Uses shared locking to allow concurrent snapshot operations while ensuring consistency
- Invalid pxmin values are ignored because backends without snapshots cannot conflict with cleanup operations
- The function's conflict detection logic is optimized for cleanup records that remove tuple versions from committed transactions

## Simplified Source

```c
VirtualTransactionId *GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid) {
    static VirtualTransactionId *vxids;
    int count = 0;

    // Allocate static array on first use (reused for performance)
    if (vxids == NULL) {
        vxids = malloc(sizeof(VirtualTransactionId) * (procArray->maxProcs + 1));
        if (vxids == NULL)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    // Scan all active processes
    for (int index = 0; index < procArray->numProcs; index++) {
        int pgprocno = procArray->pgprocnos[index];
        PGPROC *proc = &allProcs[pgprocno];

        // Skip prepared transactions (pid == 0)
        if (proc->pid == 0)
            continue;

        // Filter by database if specified
        if (OidIsValid(dbOid) && proc->databaseId != dbOid)
            continue;

        // Check if transaction conflicts with cleanup operations
        TransactionId pxmin = UINT32_ACCESS_ONCE(proc->xmin);
        if (!TransactionIdIsValid(limitXmin) ||
            (TransactionIdIsValid(pxmin) && !TransactionIdFollows(pxmin, limitXmin))) {

            VirtualTransactionId vxid;
            GET_VXID_FROM_PGPROC(vxid, *proc);
            if (VirtualTransactionIdIsValid(vxid))
                vxids[count++] = vxid;
        }
    }

    LWLockRelease(ProcArrayLock);

    // Add terminator entry
    vxids[count].procNumber = INVALID_PROC_NUMBER;
    vxids[count].localTransactionId = InvalidLocalTransactionId;

    return vxids;
}
```