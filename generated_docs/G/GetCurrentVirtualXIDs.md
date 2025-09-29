# GetCurrentVirtualXIDs

## Location
[src/backend/storage/ipc/procarray.c:3323-3415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3323-L3415)

## Overview
Returns an array of currently active Virtual Transaction IDs (VXIDs) from the process array, with various filtering options to control which VXIDs are included.

## Definition

```c
VirtualTransactionId *
GetCurrentVirtualXIDs(TransactionId limitXmin, bool excludeXmin0,
					  bool allDbs, int excludeVacuum,
					  int *nvxids)
```
## Detailed Description
GetCurrentVirtualXIDs scans the process array to collect currently active Virtual Transaction IDs, applying various filters based on the provided parameters. The function is essential for determining which transactions are currently active in the system, which is crucial for operations like waiting for older snapshots to complete.

The function operates under a shared ProcArrayLock to ensure consistency while reading the process array. It allocates memory for the maximum possible number of VXIDs and then filters the results based on the caller's requirements. The caller's own process is always excluded from the results.

The filtering mechanism allows for sophisticated control over which VXIDs are returned, supporting use cases like waiting for transactions with older snapshots to complete, excluding vacuum processes, or limiting results to specific databases.

## Parameters / Member Variables
- `limitXmin`: Skip processes with xmin > limitXmin (if not InvalidTransactionId)
- `excludeXmin0`: If true, skip processes with xmin = 0 (invalid transaction ID)
- `allDbs`: If false, only include processes from the current database
- `excludeVacuum`: Bit mask to exclude processes with matching status flags (typically vacuum-related)
- `nvxids`: Output parameter returning the number of valid VXIDs in the result array

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - [TransactionIdPrecedesOrEquals](../T/TransactionIdPrecedesOrEquals.md)
  - GET_VXID_FROM_PGPROC
  - VirtualTransactionIdIsValid
  - UINT32_ACCESS_ONCE
- Called from (representative examples):
  - [WaitForOlderSnapshots](../W/WaitForOlderSnapshots.md) (in commands/indexcmds.c)

## Notes and Other Information
- The function allocates memory using palloc() - caller is responsible for freeing
- Race conditions are possible due to shared locking, but are handled safely through proper lock ordering
- The limitXmin and excludeXmin0 parameters help skip backends whose snapshots are not older than a reference snapshot
- Memory ordering considerations are addressed through proper use of UINT32_ACCESS_ONCE when reading xmin values
- The function always excludes the caller's own process from the results
- Status flags are used to filter out specific types of processes (like vacuum operations)

## Simplified Source

```c
VirtualTransactionId *
GetCurrentVirtualXIDs(TransactionId limitXmin, bool excludeXmin0,
                      bool allDbs, int excludeVacuum, int *nvxids)
{
    VirtualTransactionId *vxids;
    ProcArrayStruct *arrayP = procArray;
    int count = 0;
    int index;

    // Allocate result array for maximum possible entries
    vxids = (VirtualTransactionId *)
        palloc(sizeof(VirtualTransactionId) * arrayP->maxProcs);

    // Lock process array for reading
    LWLockAcquire(ProcArrayLock, LW_SHARED);

    // Scan all active processes
    for (index = 0; index < arrayP->numProcs; index++)
    {
        int pgprocno = arrayP->pgprocnos[index];
        PGPROC *proc = &allProcs[pgprocno];
        uint8 statusFlags = ProcGlobal->statusFlags[index];

        // Skip our own process
        if (proc == MyProc)
            continue;

        // Skip if excluded by vacuum flags
        if (excludeVacuum & statusFlags)
            continue;

        // Check database filter
        if (allDbs || proc->databaseId == MyDatabaseId)
        {
            // Get process's xmin (oldest visible transaction)
            TransactionId pxmin = UINT32_ACCESS_ONCE(proc->xmin);

            // Skip if excluding processes with no xmin
            if (excludeXmin0 && !TransactionIdIsValid(pxmin))
                continue;

            // Skip if xmin is newer than our limit
            if (!TransactionIdIsValid(limitXmin) ||
                TransactionIdPrecedesOrEquals(pxmin, limitXmin))
            {
                VirtualTransactionId vxid;

                // Extract virtual transaction ID
                GET_VXID_FROM_PGPROC(vxid, *proc);
                if (VirtualTransactionIdIsValid(vxid))
                    vxids[count++] = vxid;
            }
        }
    }

    LWLockRelease(ProcArrayLock);

    *nvxids = count;
    return vxids;
}
```