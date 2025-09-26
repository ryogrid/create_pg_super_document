# TransactionIdIsInProgress

## Location
src/backend/storage/ipc/procarray.c: 1402 - 1633

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
  - TransactionIdPrecedes (for XID ordering and age checks)
  - TransactionIdEquals (for exact XID matching)
  - TransactionIdIsCurrentTransactionId (to handle own transaction)
  - RecoveryInProgress (to determine if in Hot Standby mode)
  - KnownAssignedXidExists (for Hot Standby transaction checks)
  - KnownAssignedXidsGet (to collect XIDs for subtrans lookup)
  - TransactionIdDidAbort (to check if transaction was aborted)
  - SubTransGetTopmostTransaction (to find topmost parent in subtrans tree)
  - XidFromFullTransactionId (for transaction ID conversion)
- Called from (representative examples):
  - HeapTupleSatisfiesSelf (tuple visibility checks)
  - HeapTupleSatisfiesUpdate (update visibility checks)
  - HeapTupleSatisfiesDirty (dirty read visibility checks)
  - compute_new_xmax_infomask (heap tuple processing)
  - XactLockTableWait (transaction locking)
  - MultiXactIdIsRunning (multixact processing)

## Notes and Other Information
- Critical performance function called frequently during tuple visibility checks
- Uses static memory allocation to avoid repeated malloc/free overhead
- Employs memory barriers and atomic access for concurrent safety
- Caches negative results to avoid repeated expensive pg_subtrans lookups
- In Hot Standby mode, allocates larger workspace to handle KnownAssignedXids
- The overflow mechanism ensures correctness even when cache limits are exceeded
- Performance is optimized for the common case where transactions are found in main XIDs or cached subxids
- Must hold ProcArrayLock (shared) during shared memory examination phases