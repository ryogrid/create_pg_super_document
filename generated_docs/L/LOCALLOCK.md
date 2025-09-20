# LOCALLOCK

## Location
[src/include/storage/lock.h:426-441](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/lock.h#L426-L441)

## Overview
LOCALLOCK represents a backend's local view of a lock it has acquired or is interested in, maintaining reference counts and pointers to shared memory structures.

## Definition

```c
typedef struct LOCALLOCK
{
	/* tag */
	LOCALLOCKTAG tag;			/* unique identifier of locallock entry */

	/* data */
	uint32		hashcode;		/* copy of LOCKTAG's hash value */
	LOCK	   *lock;			/* associated LOCK object, if any */
	PROCLOCK   *proclock;		/* associated PROCLOCK object, if any */
	int64		nLocks;			/* total number of times lock is held */
	int			numLockOwners;	/* # of relevant ResourceOwners */
	int			maxLockOwners;	/* allocated size of array */
	LOCALLOCKOWNER *lockOwners; /* dynamically resizable array */
	bool		holdsStrongLockCount;	/* bumped FastPathStrongRelationLocks */
	bool		lockCleared;	/* we read all sinval msgs for lock */
} LOCALLOCK;
```
## Detailed Description
LOCALLOCK is the cornerstone of PostgreSQL's local lock management system. Each backend maintains a hash table of LOCALLOCK entries for all locks it has acquired or attempted to acquire. This structure serves as the local representation of lock state, enabling efficient lock reference counting, fast-path optimizations, and proper resource cleanup.

The structure maintains both local state (reference counts, resource owner information) and pointers to shared memory structures (LOCK and PROCLOCK objects). For fast-path locks, the shared memory pointers may be NULL since these locks bypass the shared lock table. The dynamic array of LOCALLOCKOWNER entries allows tracking lock ownership across multiple resource owners, supporting PostgreSQL's nested transaction model.

## Parameters / Member Variables
- : LOCALLOCKTAG that uniquely identifies this lock entry, combining object identifier and lock mode
- : Cached hash value from the LOCKTAG for efficient hash table operations
- : Pointer to the shared LOCK object, or NULL for fast-path locks
- : Pointer to the shared PROCLOCK object, or NULL for fast-path locks
- : Total reference count indicating how many times this lock has been acquired
- : Number of active entries in the lockOwners array
- : Allocated capacity of the lockOwners array
- : Dynamic array of LOCALLOCKOWNER structures tracking ownership per resource owner
- : Boolean indicating if this lock contributes to the FastPathStrongRelationLocks count
- : Boolean indicating whether all relevant shared invalidation messages have been processed

## Dependencies
- Functions called/Symbols referenced:
  - [LOCALLOCKTAG](LOCALLOCKTAG.md)
  - LOCK
  - [PROCLOCK](../P/PROCLOCK.md)
  - [LOCALLOCKOWNER](LOCALLOCKOWNER.md)
- Called from (representative examples):
  - [LockAcquireExtended](LockAcquireExtended.md)
  - [LockRelease](LockRelease.md)
  - [LockHeldByMe](LockHeldByMe.md)
  - [LockReleaseAll](LockReleaseAll.md)
  - [GrantLockLocal](../G/GrantLockLocal.md)
  - [FastPathGetRelationLockEntry](../F/FastPathGetRelationLockEntry.md)

## Notes and Other Information
LOCALLOCK entries persist across multiple lock acquisitions and releases, serving as a cache for lock state. The structure supports both normal locks (with valid shared memory pointers) and fast-path locks (with NULL shared memory pointers). The dynamic lockOwners array grows as needed to accommodate locks held by multiple resource owners, which is essential for proper cleanup during subtransaction abort. The lockCleared flag helps optimize shared invalidation message processing by avoiding redundant reads.