# DumpAllLocks

## Location
[src/backend/storage/lmgr/lock.c:4117-4173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L4117-L4173)

## Overview
Dumps all locks in the PostgreSQL lock manager system by iterating through the entire PROCLOCK hash table for comprehensive debugging purposes.

## Definition

```c
void
DumpAllLocks(void)
```
## Detailed Description
The DumpAllLocks function is a comprehensive debugging utility that prints information about every lock in the PostgreSQL lock manager system. Unlike DumpLocks which focuses on a specific process, this function provides a global view of all locks by iterating through the entire LockMethodProcLockHash hash table. It examines every PROCLOCK entry and the associated LOCK structures to provide complete visibility into the system's lock state.

The function first checks if the current process (MyProc) is waiting on any lock and reports that information. Then it systematically walks through all PROCLOCK entries in the hash table, printing details about each process lock and its corresponding lock object.

## Parameters / Member Variables
This function takes no parameters as it operates on global lock manager data structures.

## Dependencies
- Functions called/Symbols referenced:
  - LOCK_PRINT (macro for printing lock information)
  - PROCLOCK_PRINT (macro for printing process lock information)
  - [hash_seq_init](../h/hash_seq_init.md) (initialize hash table sequential scan)
  - [hash_seq_search](../h/hash_seq_search.md) (get next entry in hash table scan)
  - elog (error logging function)
- Data structures used:
  - [PGPROC](../P/PGPROC.md) (process structure)
  - [PROCLOCK](../P/PROCLOCK.md) (process lock structure)
  - [LOCK](../L/LOCK.md) (lock structure)
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md) (hash table sequential scan status)
- Global variables accessed:
  - MyProc (current process)
  - LockMethodProcLockHash (global PROCLOCK hash table)
- Called from (representative examples):
  - [CheckDeadLock](../C/CheckDeadLock.md) (deadlock detection in proc.c)
  - LockHashPartitionLockByProc (via macro in lock.h)

## Notes and Other Information
- The caller is responsible for acquiring appropriate LWLocks before calling this function to ensure consistent lock state during the comprehensive dump operation.
- This function provides a global view of all locks in the system, making it particularly useful for debugging complex lock interactions and deadlock situations.
- The function includes error checking to handle cases where a PROCLOCK entry has a NULL myLock pointer, logging such anomalies.
- Uses hash table sequential scanning to efficiently iterate through all entries in the PROCLOCK hash table.
- This is primarily a debugging function and the output is only visible when appropriate debug flags are enabled in the build.
- The comprehensive nature of this function makes it potentially expensive to call in production systems with many locks.