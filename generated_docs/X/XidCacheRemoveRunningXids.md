# XidCacheRemoveRunningXids

## Location
[src/backend/storage/ipc/procarray.c:3990-4077](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3990-L4077)

## Overview
Removes a group of transaction IDs from the list of known-running subtransactions for the current backend, typically used during transaction abort operations.

## Definition
```c
void XidCacheRemoveRunningXids(TransactionId xid, int nxids, const TransactionId *xids, TransactionId latestXid)
```

## Detailed Description
This function removes specified transaction IDs from the current backend's subtransaction cache. It is primarily used during transaction abort operations to clean up the list of running subtransactions. The function operates on both a primary transaction ID and an array of subtransaction IDs, removing all of them from the cache.

The function performs several optimizations:
- Scans backwards through arrays to achieve better performance when removing many XIDs
- Uses memory barriers to ensure proper ordering of writes
- Updates global transaction completion tracking
- Handles cache overflow scenarios gracefully

The removal process maintains consistency in the global process array while ensuring other backends can observe the changes correctly.

## Parameters / Member Variables
- `xid`: The primary transaction ID to remove from the subtransaction cache
- `nxids`: The number of subtransaction IDs in the xids array
- `xids`: Array of subtransaction IDs to remove from the cache
- `latestXid`: The latest (highest) transaction ID among the group being removed

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdEquals (for XID comparison)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (for exclusive process array locking)
  - pg_write_barrier (for memory ordering)
  - [MaintainLatestCompletedXid](../M/MaintainLatestCompletedXid.md) (for global completion tracking)
  - elog (for warning messages)
- Called from (representative examples):
  - [RecordTransactionAbort](../R/RecordTransactionAbort.md) (in xact.c)

## Notes and Other Information
- Requires exclusive ProcArrayLock to ensure atomic removal operations
- Handles cache overflow scenarios where XIDs may not be found in the cache
- Uses backward scanning for O(N) instead of O(N²) performance when removing many XIDs
- May issue warnings if XIDs are not found, but this can be normal in error recovery scenarios
- Updates global transaction completion count to notify other processes
- Memory barriers ensure proper visibility of changes to other backends
- Function can be called multiple times for the same subtransaction during error recovery

## Simplified Source

```c
void XidCacheRemoveRunningXids(TransactionId xid,
                               int nxids, const TransactionId *xids,
                               TransactionId latestXid)
{
    int i, j;
    XidCacheStatus *mysubxidstat;

    Assert(TransactionIdIsValid(xid));

    // Acquire exclusive lock on process array
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);

    mysubxidstat = &ProcGlobal->subxidStates[MyProc->pgxactoff];

    // Remove XIDs from the xids[] array (scan backwards for efficiency)
    for (i = nxids - 1; i >= 0; i--)
    {
        TransactionId anxid = xids[i];

        for (j = MyProc->subxidStatus.count - 1; j >= 0; j--)
        {
            if (TransactionIdEquals(MyProc->subxids.xids[j], anxid))
            {
                // Replace found XID with last XID in array
                MyProc->subxids.xids[j] = MyProc->subxids.xids[MyProc->subxidStatus.count - 1];
                pg_write_barrier();
                mysubxidstat->count--;
                MyProc->subxidStatus.count--;
                break;
            }
        }

        // Warn if XID not found (unless cache overflowed)
        if (j < 0 && !MyProc->subxidStatus.overflowed)
            elog(WARNING, "did not find subXID %u in MyProc", anxid);
    }

    // Remove the main XID
    for (j = MyProc->subxidStatus.count - 1; j >= 0; j--)
    {
        if (TransactionIdEquals(MyProc->subxids.xids[j], xid))
        {
            MyProc->subxids.xids[j] = MyProc->subxids.xids[MyProc->subxidStatus.count - 1];
            pg_write_barrier();
            mysubxidstat->count--;
            MyProc->subxidStatus.count--;
            break;
        }
    }

    if (j < 0 && !MyProc->subxidStatus.overflowed)
        elog(WARNING, "did not find subXID %u in MyProc", xid);

    // Update global transaction completion tracking
    MaintainLatestCompletedXid(latestXid);
    TransamVariables->xactCompletionCount++;

    LWLockRelease(ProcArrayLock);
}
```

**Simplified Logic:**
1. Acquire exclusive lock on the process array
2. For each XID in the input array, search and remove from subtransaction cache
3. Remove the main XID from the cache
4. Use "swap with last element" technique for efficient array removal
5. Update global completion tracking with the latest XID
6. Release the process array lock

**Key Points:**
- Scans backwards through arrays for O(N) performance instead of O(N²)
- Uses exclusive locking to ensure atomic cache modifications
- Handles cache overflow scenarios gracefully with warnings
- Updates global transaction completion counters
- Uses memory barriers for proper visibility across backends