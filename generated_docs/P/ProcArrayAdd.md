# ProcArrayAdd

## Location
src/backend/storage/ipc/procarray.c: 468 - 564

## Overview
Adds a specified PGPROC structure to the shared process array, maintaining the array in sorted order for optimal cache locality during traversals.

## Definition


## Detailed Description
ProcArrayAdd inserts a new process entry into the shared process array (procArray). The function maintains the array sorted by PGPROC number to optimize cache locality when traversing the array. It acquires both ProcArrayLock and XidGenLock to ensure atomic updates to all related structures.

The function performs several key operations:
1. Finds the correct insertion point to maintain sorted order
2. Shifts existing entries to make room for the new process
3. Updates not only the pgprocnos array but also corresponding entries in xids, subxidStates, and statusFlags arrays
4. Adjusts the pgxactoff field for all affected processes

The sorted arrangement improves performance during frequent operations like snapshot building and visibility checking, where the process array is traversed regularly.

## Parameters / Member Variables
- : Pointer to the PGPROC structure to be added to the shared array

## Dependencies
- Functions called/Symbols referenced:
  - GetNumberFromPGProc
  - LWLockAcquire
  - LWLockRelease
  - ereport
  - memmove
  - ProcArrayStruct
  - ProcGlobal
  - NUM_AUXILIARY_PROCS

- Called from (representative examples):
  - InitProcessPhase2
  - MarkAsPrepared

## Notes and Other Information
- Requires exclusive locks on both ProcArrayLock and XidGenLock to prevent race conditions
- The function will terminate the process with FATAL error if the array is full (should not happen in normal operation)
- Maintains sorted order by PGPROC number for cache efficiency
- Updates multiple parallel arrays (pgprocnos, xids, subxidStates, statusFlags) atomically
- Adjusts pgxactoff values for all processes that are shifted in the array
- Lock release order is reversed from acquisition order to minimize lock contention
- The sorting overhead is justified because array access is much more frequent than addition/removal