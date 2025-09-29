# TransactionIdIsInProgress

## Location
[src/backend/storage/ipc/procarray.c:1402-1633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L1402-L1633)

## Overview
TransactionIdIsInProgress determines whether a given transaction is currently running in any backend process, using multiple optimization strategies and fallback mechanisms.

## Definition
```c
bool TransactionIdIsInProgress(TransactionId xid)
```

## Detailed Description
This function is a critical component of PostgreSQL's transaction visibility system, determining whether a specific transaction ID is currently active. It employs a sophisticated multi-step approach with performance optimizations:

**Step 1: Quick shortcuts**
- Rejects transactions older than RecentXmin (cannot be running)
- Uses cached results for recently checked transactions
- Handles current transaction and its subtransactions without shared memory access

**Step 2: Main transaction ID check**
- Scans ProcGlobal->xids array for direct matches
- Most efficient path for top-level transactions

**Step 3: Cached subtransaction check**
- Examines PGPROC subxids arrays for subtransaction matches
- Limited to cached subtransactions (PGPROC_MAX_CACHED_SUBXIDS)

**Step 4: Hot Standby mode**
- Checks KnownAssignedXids list for transactions running on primary
- Handles overflow scenarios where complete information isn't available

**Step 5: Subtrans tree traversal (slowest path)**
- When caches overflow, searches pg_subtrans to find topmost parent
- Verifies if the topmost transaction is in the collected XIDs
- Only executed when other methods fail or caches are incomplete

The function maintains performance counters for each path to monitor optimization effectiveness and includes comprehensive caching to avoid repeated expensive operations.

## Parameters / Member Variables
- `xid`: The transaction ID to check for active status

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdPrecedes](TransactionIdPrecedes.md) (for XID ordering and age checks)
  - TransactionIdEquals (for exact XID matching)
  - [TransactionIdIsCurrentTransactionId](TransactionIdIsCurrentTransactionId.md) (to handle own transaction)
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (to determine if in Hot Standby mode)
  - [KnownAssignedXidExists](../K/KnownAssignedXidExists.md) (for Hot Standby transaction checks)
  - [KnownAssignedXidsGet](../K/KnownAssignedXidsGet.md) (to collect XIDs for subtrans lookup)
  - [TransactionIdDidAbort](TransactionIdDidAbort.md) (to check if transaction was aborted)
  - [SubTransGetTopmostTransaction](../S/SubTransGetTopmostTransaction.md) (to find topmost parent in subtrans tree)
  - XidFromFullTransactionId (for transaction ID conversion)
- Called from (representative examples):
  - [HeapTupleSatisfiesSelf](../H/HeapTupleSatisfiesSelf.md) (tuple visibility checks)
  - [HeapTupleSatisfiesUpdate](../H/HeapTupleSatisfiesUpdate.md) (update visibility checks)
  - [HeapTupleSatisfiesDirty](../H/HeapTupleSatisfiesDirty.md) (dirty read visibility checks)
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md) (heap tuple processing)
  - [XactLockTableWait](../X/XactLockTableWait.md) (transaction locking)
  - [MultiXactIdIsRunning](../M/MultiXactIdIsRunning.md) (multixact processing)

## Notes and Other Information
- Critical performance function called frequently during tuple visibility checks
- Uses static memory allocation to avoid repeated malloc/free overhead
- Employs memory barriers and atomic access for concurrent safety
- Caches negative results to avoid repeated expensive pg_subtrans lookups
- In Hot Standby mode, allocates larger workspace to handle KnownAssignedXids
- The overflow mechanism ensures correctness even when cache limits are exceeded
- Performance is optimized for the common case where transactions are found in main XIDs or cached subxids
- Must hold ProcArrayLock (shared) during shared memory examination phases

## Simplified Source

```c
bool TransactionIdIsInProgress(TransactionId xid)
{
    static TransactionId *xids = NULL;
    static TransactionId *other_xids;
    XidCacheStatus *other_subxidstates;
    int nxids = 0;
    ProcArrayStruct *arrayP = procArray;
    TransactionId topxid;
    TransactionId latestCompletedXid;
    int mypgxactoff;
    int numProcs;
    int j;

    // Don't bother checking a transaction older than RecentXmin
    if (TransactionIdPrecedes(xid, RecentXmin)) {
        xc_by_recent_xmin_inc();
        return false;
    }

    // We may have just checked the status of this transaction
    if (TransactionIdEquals(cachedXidIsNotInProgress, xid)) {
        xc_by_known_xact_inc();
        return false;
    }

    // Handle our own transaction (and subtransactions) without shared memory access
    if (TransactionIdIsCurrentTransactionId(xid)) {
        xc_by_my_xact_inc();
        return true;
    }

    // Allocate workspace to remember main XIDs
    if (xids == NULL) {
        int maxxids = RecoveryInProgress() ? TOTAL_MAX_CACHED_SUBXIDS : arrayP->maxProcs;
        xids = (TransactionId *) malloc(maxxids * sizeof(TransactionId));
        if (xids == NULL)
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

    other_xids = ProcGlobal->xids;
    other_subxidstates = ProcGlobal->subxidStates;

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    // Check latestCompletedXid; if the target Xid is after that, it's surely still running
    latestCompletedXid = XidFromFullTransactionId(TransamVariables->latestCompletedXid);
    if (TransactionIdPrecedes(latestCompletedXid, xid)) {
        LWLockRelease(ProcArrayLock);
        xc_by_latest_xid_inc();
        return true;
    }

    // No shortcuts, gotta grovel through the array
    mypgxactoff = MyProc->pgxactoff;
    numProcs = arrayP->numProcs;
    for (int pgxactoff = 0; pgxactoff < numProcs; pgxactoff++) {
        int pgprocno;
        PGPROC *proc;
        TransactionId pxid;
        int pxids;

        // Ignore ourselves --- dealt with it above
        if (pgxactoff == mypgxactoff)
            continue;

        // Fetch xid just once - see GetNewTransactionId
        pxid = UINT32_ACCESS_ONCE(other_xids[pgxactoff]);

        if (!TransactionIdIsValid(pxid))
            continue;

        // Step 1: check the main Xid
        if (TransactionIdEquals(pxid, xid)) {
            LWLockRelease(ProcArrayLock);
            xc_by_main_xid_inc();
            return true;
        }

        // We can ignore main Xids that are younger than the target Xid
        if (TransactionIdPrecedes(xid, pxid))
            continue;

        // Step 2: check the cached child-Xids arrays
        pxids = other_subxidstates[pgxactoff].count;
        pg_read_barrier();  // pairs with barrier in GetNewTransactionId()
        pgprocno = arrayP->pgprocnos[pgxactoff];
        proc = &allProcs[pgprocno];
        for (j = pxids - 1; j >= 0; j--) {
            TransactionId cxid = UINT32_ACCESS_ONCE(proc->subxids.xids[j]);

            if (TransactionIdEquals(cxid, xid)) {
                LWLockRelease(ProcArrayLock);
                xc_by_child_xid_inc();
                return true;
            }
        }

        // Save the main Xid for step 4. We only need to remember main Xids
        // that have uncached children.
        if (other_subxidstates[pgxactoff].overflowed)
            xids[nxids++] = pxid;
    }

    // Step 3: in hot standby mode, check the known-assigned-xids list
    if (RecoveryInProgress()) {
        // none of the PGPROC entries should have XIDs in hot standby mode
        Assert(nxids == 0);

        if (KnownAssignedXidExists(xid)) {
            LWLockRelease(ProcArrayLock);
            xc_by_known_assigned_inc();
            return true;
        }

        // If the KnownAssignedXids overflowed, we have to check pg_subtrans too
        if (TransactionIdPrecedesOrEquals(xid, procArray->lastOverflowedXid))
            nxids = KnownAssignedXidsGet(xids, xid);
    }

    LWLockRelease(ProcArrayLock);

    // If none of the relevant caches overflowed, we know the Xid is not running
    if (nxids == 0) {
        xc_no_overflow_inc();
        cachedXidIsNotInProgress = xid;
        return false;
    }

    // Step 4: have to check pg_subtrans
    xc_slow_answer_inc();

    if (TransactionIdDidAbort(xid)) {
        cachedXidIsNotInProgress = xid;
        return false;
    }

    // Check whether the transaction tree it belongs to is still running
    topxid = SubTransGetTopmostTransaction(xid);
    Assert(TransactionIdIsValid(topxid));
    if (!TransactionIdEquals(topxid, xid) && pg_lfind32(topxid, xids, nxids))
        return true;

    cachedXidIsNotInProgress = xid;
    return false;
}
```