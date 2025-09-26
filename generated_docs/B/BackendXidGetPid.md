# BackendXidGetPid

## Location
[src/backend/storage/ipc/procarray.c:3255-3289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3255-L3289)

## Overview
Retrieves the process ID (PID) of the backend that owns a given transaction ID (XID), primarily used for determining which process holds specific locks.

## Definition
int BackendXidGetPid(TransactionId xid)

## Detailed Description
BackendXidGetPid performs a reverse lookup to find the process ID of the PostgreSQL backend that is currently running a transaction with the specified transaction ID. This function is particularly valuable for lock management and debugging, as it allows the system to identify which backend process owns a particular transaction or lock.

The function searches through the active process array, comparing transaction IDs in the global xids array against the target XID. It only considers main transaction IDs, not subtransaction IDs, which makes it suitable for identifying the primary transaction holder. The function uses ProcArrayLock in shared mode to ensure consistent access to the transaction state during the search.

Important limitations include that not every transaction has an assigned XID (read-only transactions may not get XIDs), and the function returns 0 for prepared transactions since they are not associated with active backend processes. The returned PID information may become stale quickly due to transaction completion or process termination.

## Parameters / Member Variables
- `xid`: The transaction ID to search for

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire
  - LWLockRelease
  - ProcArrayLock (global lock)
  - procArray (global variable)
  - ProcGlobal (global variable)
  - allProcs (global array)
- Called from (representative examples):
  - Functions declared in src/include/storage/procarray.h

## Notes and Other Information
- Returns 0 if XID is not found, invalid, or belongs to a prepared transaction
- Only considers main transaction IDs, not subtransaction IDs
- Uses ProcArrayLock in shared mode for thread-safe access
- Primarily useful for lock ownership determination and debugging
- Not all transactions have assigned XIDs (read-only transactions may not)
- Safe to use with XIDs found on disk, but caller must ensure query remains meaningful
- The returned PID may become invalid if the transaction completes or process terminates
- The function is declared in src/include/storage/procarray.h