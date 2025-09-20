# replorigin_session_get_progress

## Location
[src/backend/replication/logical/origin.c:1237-1268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1237-L1268)

## Overview
Retrieves the current replication progress (remote LSN) from the active replication origin session, with optional WAL flushing to ensure durability up to the local LSN.

## Definition

```c
XLogRecPtr
replorigin_session_get_progress(bool flush)
```
## Detailed Description
This function queries the current replication origin session to determine how far replication has progressed by returning the remote LSN (Log Sequence Number). It provides an efficient way to check replication progress without needing to search through the global replication state array.

The function operates by:
1. Acquiring a shared lock on the session's replication state
2. Reading both the remote and local LSN values
3. Releasing the lock
4. Optionally flushing WAL up to the local LSN if requested and valid
5. Returning the remote LSN as the progress indicator

The optional flush parameter allows callers to ensure that all changes up to the local LSN are durably written to disk before returning the progress information.

## Parameters / Member Variables
- : boolean flag indicating whether to flush WAL up to the local LSN before returning

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease (with LW_SHARED mode)
  - [XLogFlush](../X/XLogFlush.md)
  - InvalidXLogRecPtr (for validation)
- Called from (representative examples):
  - [pg_replication_origin_session_progress](../p/pg_replication_origin_session_progress.md)
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md)
  - [run_apply_worker](run_apply_worker.md)

## Notes and Other Information
- Requires an active replication origin session (established via replorigin_session_setup())
- Uses shared locking to allow concurrent reads while preventing inconsistent state during writes
- Returns the remote LSN, which represents the furthest point successfully replicated from the remote system
- When flush=true, ensures WAL durability up to the local LSN before returning
- Commonly used by replication workers to determine starting points for replication or to report progress
- More efficient than general progress querying functions due to cached session state access