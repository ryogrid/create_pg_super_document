# GetMaxSnapshotSubxidCount

## Location
[src/backend/storage/ipc/procarray.c:2080-2094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L2080-L2094)

## Overview
Returns the maximum size for snapshot sub-XID arrays, providing a constant value used by snapshot management to allocate properly sized arrays for tracking subtransactions.

## Definition


## Detailed Description
GetMaxSnapshotSubxidCount is a simple accessor function that returns the TOTAL_MAX_CACHED_SUBXIDS constant. This value represents the maximum number of subtransaction IDs that can be stored in various snapshot-related data structures. The function exists primarily to export this internal constant for use by snapmgr.c and other components that need to allocate arrays for storing subtransaction information.

The returned value is calculated as , where:
- PGPROC_MAX_CACHED_SUBXIDS (64) is the maximum number of cached subtransaction IDs per backend
- PROCARRAY_MAXPROCS is MaxBackends + max_prepared_xacts
- The +1 accounts for the main transaction ID

This sizing ensures that snapshot data structures can accommodate subtransaction information from all possible concurrent backends and prepared transactions during Hot Standby processing.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - TOTAL_MAX_CACHED_SUBXIDS (constant)
- Called from (representative examples):
  - [GetSnapshotData](GetSnapshotData.md)
  - SetTransactionSnapshot
  - [ExportSnapshot](../E/ExportSnapshot.md)
  - ImportSnapshot

## Notes and Other Information
- This function was created specifically to export the TOTAL_MAX_CACHED_SUBXIDS constant for use by snapmgr.c
- The value is used during Hot Standby processing where multiple data structures (KnownAssignedXids in shared memory, and local structures in various backends) must be identically sized
- The sizing calculation ensures compatibility between GetSnapshotData(), TransactionIdIsInProgress(), and GetRunningTransactionData()
- All snapshot-related data structures that may be copied wholesale must use this same size to maintain consistency