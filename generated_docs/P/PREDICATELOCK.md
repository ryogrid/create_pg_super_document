# PREDICATELOCK

## Location
[src/include/storage/predicate_internals.h:317-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/predicate_internals.h#L317-L328)

## Overview
PREDICATELOCK represents an individual predicate lock within PostgreSQL's serializable isolation system, linking a specific database object with the transaction that holds the lock.

## Definition

```c
typedef struct PREDICATELOCK
{
	/* hash key */
	PREDICATELOCKTAG tag;		/* unique identifier of lock */

	/* data */
	dlist_node	targetLink;		/* list link in PREDICATELOCKTARGET's list of
								 * predicate locks */
	dlist_node	xactLink;		/* list link in SERIALIZABLEXACT's list of
								 * predicate locks */
	SerCommitSeqNo commitSeqNo; /* only used for summarized predicate locks */
} PREDICATELOCK;
```
## Detailed Description
PREDICATELOCK is the central data structure representing individual predicate locks in PostgreSQL's serializable snapshot isolation implementation. Each instance represents a specific lock held by a serializable transaction on a particular database object. The structure enables efficient bidirectional navigation through linked lists - it can be accessed both from the target object's perspective (via targetLink) and from the transaction's perspective (via xactLink). Predicate locks can be created when database objects are read or through promotion of multiple fine-grained locks into coarser-grained ones. They are automatically cleaned up when the associated serializable transaction completes or when locks are combined into broader-scope locks.

## Parameters / Member Variables
- : A PREDICATELOCKTAG structure that uniquely identifies this specific predicate lock (serves as hash key)
- : Doubly-linked list node for inclusion in the PREDICATELOCKTARGET's list of all locks on that target
- : Doubly-linked list node for inclusion in the SERIALIZABLEXACT's list of all locks held by that transaction
- : Commit sequence number used specifically for summarized predicate locks to track ordering

## Dependencies
- Functions called/Symbols referenced:
  - PREDICATELOCKTAG
  - dlist_node
  - SerCommitSeqNo
- Called from (representative examples):
  - CreatePredicateLock
  - DeleteChildTargetLocks
  - TransferPredicateLocksToNewTarget
  - CheckTargetForConflictsIn
  - ClearOldPredicateLocks
  - ReleaseOneSerializableXact

## Notes and Other Information
- Maintained in shared memory hash table for concurrent access across transactions
- Supports lock promotion mechanisms for combining fine-grained locks into coarser-grained ones
- Essential for serializable conflict detection algorithms
- commitSeqNo field only relevant for summarized locks used in cleanup optimization
- Lifecycle tied to both the target object and the holding transaction
- Enables efficient iteration over all locks for a given target or transaction through dual linked list membership