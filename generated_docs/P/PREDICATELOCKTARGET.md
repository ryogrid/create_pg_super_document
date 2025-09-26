# PREDICATELOCKTARGET

## Location
src/include/storage/predicate_internals.h: 284 - 292

## Overview
PREDICATELOCKTARGET represents a database object on which predicate locks are maintained, serving as a shared memory structure that tracks all predicate locks associated with a specific lockable object.

## Definition

```c
typedef struct PREDICATELOCKTARGET
{
	/* hash key */
	PREDICATELOCKTARGETTAG tag; /* unique identifier of lockable object */

	/* data */
	dlist_head	predicateLocks; /* list of PREDICATELOCK objects assoc. with
								 * predicate lock target */
} PREDICATELOCKTARGET;
```
## Detailed Description
PREDICATELOCKTARGET is a core data structure in PostgreSQL's predicate locking system for serializable isolation level. It represents any database object (relation, page, tuple, etc.) that can have predicate locks placed on it. The structure is maintained in a shared memory hash table, where entries are dynamically added when the first predicate lock is requested on an object and removed when the last lock is released. Each target maintains a linked list of all predicate locks currently held on that object, enabling efficient conflict detection during serializable transaction processing.

## Parameters / Member Variables
- : A PREDICATELOCKTARGETTAG structure that uniquely identifies the lockable database object (serves as the hash key)
- : A doubly-linked list header containing all PREDICATELOCK structures associated with this target

## Dependencies
- Functions called/Symbols referenced:
  - PREDICATELOCKTARGETTAG
  - dlist_head
- Called from (representative examples):
  - InitPredicateLocks
  - CreatePredicateLock
  - RemoveTargetIfNoLongerUsed
  - DeleteChildTargetLocks
  - CheckTargetForConflictsIn
  - TransferPredicateLocksToNewTarget

## Notes and Other Information
- Maintained in shared memory hash table for concurrent access across transactions
- Lifecycle managed automatically - created on first predicate lock request, destroyed when last lock is removed
- Critical for serializable snapshot isolation implementation
- Used extensively in predicate.c for conflict detection and lock management
- Hash key (tag) determines uniqueness and lookup efficiency in the shared hash table