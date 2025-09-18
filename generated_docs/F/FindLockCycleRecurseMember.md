# FindLockCycleRecurseMember

## Location
src/backend/storage/lmgr/deadlock.c: 533 - 786

## Overview
FindLockCycleRecurseMember is a detailed helper function that examines the lock dependencies of a specific process member, checking for both hard-blocking and soft-blocking relationships in PostgreSQL's deadlock detection algorithm.

## Definition
static bool FindLockCycleRecurseMember(PGPROC *checkProc, PGPROC *checkProcLeader, int depth, EDGE *softEdges, int *nSoftEdges)

## Detailed Description
This function performs the detailed analysis of lock conflicts for a single process member within the deadlock detection algorithm. It examines two types of blocking relationships:

1. **Hard blocks**: Processes that already hold conflicting locks, creating immediate blocking relationships
2. **Soft blocks**: Processes ahead in the lock wait queue whose requests conflict with the current process

The function handles lock groups by considering the group leader when determining conflicts and ensures that processes within the same lock group don't block each other. It also includes special handling for autovacuum processes, allowing them to be canceled when they directly block user processes.

For relation extension locks, the function immediately returns false as these locks cannot participate in deadlock cycles by design.

## Parameters / Member Variables
- : The specific PGPROC being examined for lock conflicts
- : The leader of checkProc's lock group (may be checkProc itself)
- : Current depth in the deadlock detection recursion
- : Output array to collect soft edge information for potential deadlock resolution
- : Output parameter tracking the number of soft edges found

## Dependencies
- Functions called/Symbols referenced:
  - GetLocksMethodTable
  - dlist_foreach
  - dlist_container
  - dclist_foreach
  - [FindLockCycleRecurse](FindLockCycleRecurse.md)
  - LOCK_LOCKTAG
  - LOCKBIT_ON
- Called from (representative examples):
  - [FindLockCycleRecurse](FindLockCycleRecurse.md)

## Notes and Other Information
- Skips relation extension locks as they cannot cause deadlocks by design
- Distinguishes between hard blocks (holding conflicting locks) and soft blocks (waiting ahead in queue)
- Handles hypothetical wait queue reorderings when testing deadlock resolution strategies
- Sets global variable blocking_autovacuum_proc when an autovacuum directly blocks the current process
- Fills deadlockDetails[] array when cycles are detected to provide debugging information
- Uses both regular wait queue traversal and hypothetical reordered queues from TopoSort results
- Critical for accurate deadlock detection in complex locking scenarios involving lock groups