# LOCALPREDICATELOCK

## Location
[src/include/storage/predicate_internals.h:347-355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L347-L355)

## Overview
LOCALPREDICATELOCK represents a local optimization copy of predicate lock data, maintained per-transaction for fast access without acquiring shared memory locks.

## Definition

```c
typedef struct LOCALPREDICATELOCK
{
	/* hash key */
	PREDICATELOCKTARGETTAG tag; /* unique identifier of lockable object */

	/* data */
	bool		held;			/* is lock held, or just its children?	*/
	int			childLocks;		/* number of child locks currently held */
} LOCALPREDICATELOCK;
```
## Detailed Description
LOCALPREDICATELOCK is an optimization structure that maintains a local, per-transaction copy of predicate lock information to avoid expensive shared memory operations. Each serializable transaction creates its own local hash table containing these structures, which mirror information also stored in the global PREDICATELOCK table. The primary purpose is to efficiently determine when multiple fine-grained locks should be promoted to a single coarser-grained lock. Since this data is not protected by locks and serves only as an optimization heuristic, it is allowed to become slightly inconsistent in corner cases where maintaining exact synchronization would be too expensive. The hash table is created when the transaction acquires its serializable snapshot and destroyed when the transaction completes.

## Parameters / Member Variables
- : A PREDICATELOCKTARGETTAG structure that uniquely identifies the lockable database object (serves as hash key)
- : Boolean flag indicating whether the lock is directly held on this target or only its child objects are locked
- : Integer count of the number of child locks currently held under this target

## Dependencies
- Functions called/Symbols referenced:
  - PREDICATELOCKTARGETTAG
- Called from (representative examples):
  - CreateLocalPredicateLockHash
  - PredicateLockExists
  - CheckAndPromotePredicateLockRequest
  - DecrementParentLocks
  - PredicateLockAcquire

## Notes and Other Information
- Strictly an optimization structure - not required for correctness
- Maintained in process-local memory, not shared memory
- Used primarily for lock promotion decisions (fine-grained to coarse-grained)
- Allowed to drift from exact synchronization with global PREDICATELOCK data for performance
- Lifetime tied to the serializable transaction's snapshot duration
- Enables efficient lock hierarchy management without expensive shared memory operations
- Critical for performance of workloads with many fine-grained predicate locks