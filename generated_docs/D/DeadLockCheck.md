# DeadLockCheck

## Location
[src/backend/storage/lmgr/deadlock.c:217-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/deadlock.c#L217-L286)

## Overview
Checks for deadlocks involving a given process and attempts to resolve them by rearranging lock wait queues, returning the deadlock state result.

## Definition
DeadLockState DeadLockCheck(PGPROC *proc)

## Detailed Description
DeadLockCheck is the main entry point for deadlock detection and resolution. It analyzes the lock dependency graph starting from a given process to detect deadlock cycles. When deadlocks are found, the function attempts to resolve them by rearranging the order of processes in lock wait queues (soft deadlock resolution). If no resolution is possible, it returns DS_HARD_DEADLOCK.

The function operates in several phases:
1. Initialize constraint tracking variables
2. Recursively search for deadlocks using DeadLockCheckRecurse
3. If deadlocks exist but cannot be resolved, record details for reporting
4. Apply any queue rearrangements needed to break soft deadlocks  
5. Wake up processes that can now proceed after rearrangements

The caller must already hold locks on all partitions of the lock tables before calling this function.

## Parameters / Member Variables
- : Pointer to the PGPROC structure representing the process to check for deadlocks

## Dependencies
- Functions called/Symbols referenced:
  - [DeadLockCheckRecurse](DeadLockCheckRecurse.md)
  - FindLockCycle
  - [dclist_count](../d/dclist_count.md)
  - [dclist_init](../d/dclist_init.md)
  - [dclist_push_tail](../d/dclist_push_tail.md)
  - [GetLocksMethodTable](../G/GetLocksMethodTable.md)
  - [ProcLockWakeup](../P/ProcLockWakeup.md)
  - [PrintLockQueue](../P/PrintLockQueue.md) (debug only)
  - [LOCK](../L/LOCK.md) (struct type)
  - [PGPROC](../P/PGPROC.md) (struct type)
  - [dclist_head](../d/dclist_head.md) (struct type)
- Called from (representative examples):
  - [CheckDeadLock](../C/CheckDeadLock.md)
  - LockHashPartitionLockByProc

## Notes and Other Information
- Returns different DeadLockState values: DS_NO_DEADLOCK, DS_SOFT_DEADLOCK, DS_BLOCKED_BY_AUTOVACUUM, or DS_HARD_DEADLOCK
- Soft deadlocks can be resolved by reordering wait queues, while hard deadlocks require transaction abort
- The function handles special cases like being blocked by autovacuum processes
- Deadlock details are recorded for later reporting but not printed immediately to avoid holding locks during I/O
- Queue rearrangements are applied atomically after deadlock analysis completes
- The function may wake up previously blocked processes after successful queue rearrangement