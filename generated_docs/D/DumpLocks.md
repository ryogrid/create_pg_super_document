# DumpLocks

## Location
src/backend/storage/lmgr/lock.c: 4084 - 4116

## Overview
Dumps all locks held by a given PostgreSQL process (PGPROC) for debugging purposes.

## Definition


## Detailed Description
The DumpLocks function is a debugging utility that prints detailed information about all locks currently held by a specific PostgreSQL process. It iterates through all lock partitions and examines the process's myProcLocks lists to display information about each PROCLOCK and associated LOCK structure. The function also shows if the process is currently waiting on any lock.

This function is primarily used for diagnostic purposes during development and debugging of lock-related issues in PostgreSQL. It provides visibility into the lock state of a process across all lock partitions.

## Parameters / Member Variables
- : Pointer to a PGPROC structure representing the PostgreSQL process whose locks should be dumped. If NULL, the function returns without doing anything.

## Dependencies
- Functions called/Symbols referenced:
  - LOCK_PRINT (macro for printing lock information)
  - PROCLOCK_PRINT (macro for printing process lock information)
  - dlist_foreach (doubly-linked list iteration)
  - dlist_container (container retrieval macro)
- Data structures used:
  - [PGPROC](../P/PGPROC.md) (process structure)
  - [PROCLOCK](../P/PROCLOCK.md) (process lock structure)
  - LOCK (lock structure)
  - [dlist_head](../d/dlist_head.md) (doubly-linked list head)
  - [dlist_iter](../d/dlist_iter.md) (doubly-linked list iterator)
- Constants used:
  - NUM_LOCK_PARTITIONS (number of lock hash partitions)
- Called from (representative examples):
  - LockHashPartitionLockByProc (via macro in lock.h)

## Notes and Other Information
- The caller is responsible for acquiring appropriate LWLocks before calling this function to ensure consistent lock state during the dump operation.
- The function iterates through all NUM_LOCK_PARTITIONS lock partitions to examine the complete lock state of the process.
- Uses Assert() to verify that each PROCLOCK's myProc field matches the input proc parameter, ensuring data structure consistency.
- The function first checks if the process is waiting on any lock (proc->waitLock) and prints that information before dumping held locks.
- This is a debugging function and the output is only visible when appropriate debug flags are enabled in the build.