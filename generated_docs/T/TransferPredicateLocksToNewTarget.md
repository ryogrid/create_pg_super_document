# TransferPredicateLocksToNewTarget

## Location
[src/backend/storage/lmgr/predicate.c:2720-2926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2720-L2926)

## Overview
Transfers or copies all predicate locks from an old lock target to a new lock target, supporting index page operations like splits and combines while maintaining serializable isolation guarantees.

## Definition

```c
static bool
TransferPredicateLocksToNewTarget(PREDICATELOCKTARGETTAG oldtargettag,
								  PREDICATELOCKTARGETTAG newtargettag,
								  bool removeOld)
```
## Detailed Description
TransferPredicateLocksToNewTarget is a complex function that handles the migration of predicate locks during structural changes in the database, particularly during index operations. The function performs several critical operations:

1. **Lock acquisition strategy**: Acquires partition locks in ascending order to prevent deadlocks, handling cases where old and new targets are in the same or different partitions
2. **Memory management**: Uses scratch space for guaranteed success when removeOld is true, handling out-of-memory conditions gracefully when copying locks
3. **Lock migration**: Iterates through all predicate locks on the old target, creating corresponding locks on the new target while preserving commit sequence numbers
4. **Cleanup operations**: Optionally removes the old target and its locks when removeOld is true, ensuring proper cleanup of data structures

The function is essential for maintaining serializable isolation during index maintenance operations where lock targets need to be restructured without losing track of existing serialization constraints.

## Parameters / Member Variables
- `oldtargettag`: The tag identifying the source lock target from which locks will be transferred
- `newtargettag`: The tag identifying the destination lock target to which locks will be transferred
- `removeOld`: Boolean flag indicating whether to remove the old locks and target after transfer (true for move operation, false for copy operation)
## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md)
  - PredicateLockTargetTagHashCode
  - PredicateLockHashPartitionLock
  - [RemoveScratchTarget](../R/RemoveScratchTarget.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - [dlist_init](../d/dlist_init.md)
  - dlist_foreach_modify
  - dlist_container
  - [dlist_delete](../d/dlist_delete.md)
  - PredicateLockHashCodeFromTargetHashCode
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - [DeleteLockTarget](../D/DeleteLockTarget.md)
  - [RemoveTargetIfNoLongerUsed](../R/RemoveTargetIfNoLongerUsed.md)
  - [RestoreScratchTarget](../R/RestoreScratchTarget.md)
- Called from (representative examples):
  - [PredicateLockPageSplit](../P/PredicateLockPageSplit.md)
  - [SerialControl](../S/SerialControl.md)

## Notes and Other Information
- Static function - internal to predicate locking subsystem
- Returns false on out-of-memory conditions when copying locks, but guaranteed to succeed when removeOld is true
- Critical warning about removeOld flag: can only be used safely when replacing with coarser-granularity locks or when certain no future references will occur
- Uses sophisticated deadlock avoidance by acquiring partition locks in ascending address order
- Preserves commit sequence numbers during lock transfer to maintain serialization semantics
- Part of the infrastructure supporting index page splits/combines in serializable transactions
- Handles duplicate lock detection by updating commit sequence numbers to the maximum value