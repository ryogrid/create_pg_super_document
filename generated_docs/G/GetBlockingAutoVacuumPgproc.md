# GetBlockingAutoVacuumPgproc

## Location
[src/backend/storage/lmgr/deadlock.c:287-308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/deadlock.c#L287-L308)

## Overview
Returns the PGPROC of the autovacuum process that is blocking another process, resetting the saved pointer after retrieval.

## Definition
PGPROC *GetBlockingAutoVacuumPgproc(void)

## Detailed Description
GetBlockingAutoVacuumPgproc is a simple accessor function that retrieves and clears the globally stored pointer to an autovacuum process that is blocking another process. This function is used as part of the deadlock detection mechanism to identify when a regular user process is being blocked by an autovacuum worker.

The function implements a one-time retrieval pattern - once the blocking autovacuum process pointer is returned, it is immediately reset to NULL. This ensures that the information is consumed exactly once and prevents stale references.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [PGPROC](../P/PGPROC.md) (struct type)
  - blocking_autovacuum_proc (global variable)
- Called from (representative examples):
  - ProcSleep
  - LockHashPartitionLockByProc

## Notes and Other Information
- The function accesses the global variable blocking_autovacuum_proc which is set during deadlock detection
- The pointer is reset to NULL immediately after retrieval to ensure one-time consumption
- This is typically used when a process determines it is blocked by autovacuum and needs to identify the specific autovacuum worker
- The function is simple but crucial for proper handling of autovacuum blocking scenarios in the lock manager
- Part of the mechanism that allows user processes to potentially cancel blocking autovacuum operations