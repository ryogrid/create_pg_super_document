# pg_blocking_pids

## Location
[src/backend/utils/adt/lockfuncs.c:466-572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/lockfuncs.c#L466-L572)

## Overview
pg_blocking_pids identifies and returns an array of process IDs (PIDs) that are blocking a given PID from acquiring locks, including both hard blocks (held locks) and soft blocks (queue position).

## Definition
Datum pg_blocking_pids(PG_FUNCTION_ARGS)

## Detailed Description
pg_blocking_pids analyzes PostgreSQL's lock manager state to determine which processes are preventing a given process from acquiring requested locks. The function identifies two types of blocking situations: hard blocks (where another process holds a conflicting lock) and soft blocks (where another process is ahead in the lock wait queue for a conflicting lock request). The function is designed to work correctly with parallel queries by reporting lock group leaders rather than individual worker PIDs, and it handles the complexities of lock groups where multiple processes may share locks. The function collects a snapshot of the lock manager state and processes each blocked procedure in the target's lock group to build a comprehensive list of blocking PIDs.

## Parameters / Member Variables
- `blocked_pid`: INT32 - The process ID of the process whose blocking PIDs should be identified

The function returns an array of INT32 values representing the PIDs of processes that are blocking the specified process.

## Dependencies
- Functions called/Symbols referenced:
  - [GetBlockerStatusData](../G/GetBlockerStatusData.md) (retrieves lock blocking information)
  - GetLockTagsMethodTable (gets lock method table for lock type)
  - [construct_array_builtin](../c/construct_array_builtin.md) (constructs PostgreSQL array result)
  - [palloc](palloc.md) (PostgreSQL memory allocation)
- Referenced types:
  - [BlockedProcsData](../B/BlockedProcsData.md), BlockedProcData, LockInstanceData
  - LockMethod (lock method table structure)
- Called from:
  - [pg_isolation_test_session_is_blocked](pg_isolation_test_session_is_blocked.md) (isolation testing framework)
  - SQL queries and monitoring applications
  - Lock analysis and debugging tools

## Notes and Other Information
- The function handles parallel query scenarios by working with lock groups and reporting group leader PIDs
- Duplicate PIDs may appear in the result when multiple waiters are blocked by the same PID or multiple blockers share the same group leader
- The function does not eliminate duplicates from the result array for performance reasons
- Predicate locks are not considered since they do not block regular operations
- The function distinguishes between hard blocks (conflicting held locks) and soft blocks (queue position conflicts)
- Lock group members never block each other, which is handled by comparing leaderPid values
- The result array is allocated with space for the maximum possible number of entries (total number of reported PROCLOCKs)
- The function provides essential functionality for deadlock detection and lock wait analysis
- Used internally by PostgreSQL's isolation testing framework to determine session blocking relationships