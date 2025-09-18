# RememberSimpleDeadLock

## Location
[src/backend/storage/lmgr/deadlock.c:1144-1159](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/deadlock.c#L1144-L1159)

## Overview
Sets up deadlock information for DeadLockReport when ProcSleep detects a trivial two-way deadlock between two processes.

## Definition
```c
void RememberSimpleDeadLock(PGPROC *proc1, LOCKMODE lockmode, LOCK *lock, PGPROC *proc2)
```

## Detailed Description
RememberSimpleDeadLock is a utility function that populates the global `deadlockDetails` array when a simple two-process deadlock is detected. This function is specifically designed for the common case where two processes are blocking each other in a circular wait.

The function stores information about both processes involved in the deadlock:
1. The first process (`proc1`) that wants to acquire a lock but is blocked
2. The second process (`proc2`) that is already waiting and would be blocked by proc1

This information is later used by `DeadLockReport` to generate comprehensive error messages for both client and server logs.

## Parameters / Member Variables
- `proc1`: Pointer to the PGPROC structure of the first process that wants to block for the lock
- `lockmode`: The lock mode that proc1 is attempting to acquire  
- `lock`: Pointer to the LOCK structure representing the lock that proc1 wants to acquire
- `proc2`: Pointer to the PGPROC structure of the second process that is already waiting and would be blocked by proc1

## Dependencies
- Functions called/Symbols referenced:
  - [PGPROC](../P/PGPROC.md) (struct type)
  - LOCKMODE (type)
  - LOCK (struct type)
  - DEADLOCK_INFO (struct type)
  - deadlockDetails (global array)
  - nDeadlockDetails (global variable)

- Called from (representative examples):
  - ProcSleep (src/backend/storage/lmgr/proc.c:1160)

## Notes and Other Information
- This function handles the simple case of two-process deadlocks, which are the most common type
- Sets `nDeadlockDetails = 2` to indicate exactly two processes are involved
- The function accesses the `waitLock` and `waitLockMode` fields from proc2's PGPROC structure to get information about what proc2 is waiting for
- This is part of PostgreSQL's deadlock detection optimization - simple cases are handled more efficiently than complex multi-process deadlock cycles
- The populated `deadlockDetails` array is consumed by `DeadLockReport` to format detailed error messages
- This function assumes that the deadlock detection logic has already determined that a two-way deadlock exists between the specified processes