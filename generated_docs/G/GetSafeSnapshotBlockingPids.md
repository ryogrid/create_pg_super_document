# GetSafeSnapshotBlockingPids

## Location
[src/backend/storage/lmgr/predicate.c:1618-1671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L1618-L1671)

## Overview
Returns the process IDs of all transactions blocking a specified process that is currently waiting in GetSafeSnapshot.

## Definition
int GetSafeSnapshotBlockingPids(int blocked_pid, int *output, int output_size)

## Detailed Description
This function provides diagnostic information about blocking relationships for READ ONLY DEFERRABLE transactions waiting for a safe snapshot. It identifies which processes are preventing a deferrable transaction from proceeding by causing potential unsafe conflicts.

The function performs the following steps:
1. Acquires a shared lock on the SerializableXactHashLock
2. Searches the active transaction list to find the SERIALIZABLEXACT for the blocked process
3. Verifies that the found transaction is currently waiting in GetSafeSnapshot (deferrable waiting state)
4. Iterates through the possibleUnsafeConflicts list to collect blocking process IDs
5. Fills the output buffer with PIDs until the buffer is full or all conflicts are processed
6. Returns the number of PIDs written to the output buffer

This function is primarily used for monitoring and debugging serializable isolation issues, allowing administrators and testing frameworks to understand why deferrable transactions are waiting.

## Parameters / Member Variables
- : Process ID of the potentially blocked transaction
- : Array to store the blocking process IDs
- : Maximum number of PIDs that can be stored in the output array
- Returns: Number of blocking PIDs written to the output array (0 if not blocked)

## Dependencies
- Functions called/Symbols referenced:
  - dlist_iter
  - SERIALIZABLEXACT
  - LW_SHARED
  - dlist_foreach
  - dlist_container
  - SxactIsDeferrableWaiting
  - RWConflict
  - RWConflictData
- Called from (representative examples):
  - pg_safe_snapshot_blocking_pids (system function)
  - pg_isolation_test_session_is_blocked (testing framework)
  - InvalidSerializableXact

## Notes and Other Information
- Returns 0 if the specified PID is not currently blocked in GetSafeSnapshot
- The output list may be truncated if more conflicts exist than the buffer can hold
- Uses linear search through the active transaction list to find the blocked transaction
- Only processes transactions in the SXACT_FLAG_DEFERRABLE_WAITING state
- Part of PostgreSQL's diagnostic and monitoring infrastructure for SSI
- Primarily used by system functions and isolation testing frameworks
- Located in src/backend/storage/lmgr/predicate.c:1618-1671