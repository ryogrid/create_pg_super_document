# LOCALLOCKOWNER

## Location
[src/include/storage/lock.h:414-424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/lock.h#L414-L424)

## Overview
LOCALLOCKOWNER tracks ownership and reference counts for locks held by specific resource owners, enabling proper lock management and cleanup per transaction or session.

## Definition

```c
typedef struct LOCALLOCKOWNER
{
	/*
	 * Note: if owner is NULL then the lock is held on behalf of the session;
	 * otherwise it is held on behalf of my current transaction.
	 *
	 * Must use a forward struct reference to avoid circularity.
	 */
	struct ResourceOwnerData *owner;
	int64		nLocks;			/* # of times held by this owner */
} LOCALLOCKOWNER;
```
## Detailed Description
LOCALLOCKOWNER is a critical component of PostgreSQL's resource management system that tracks how many times a particular lock has been acquired by a specific resource owner. This structure enables proper hierarchical lock management where locks can be held at different levels (session-level or transaction-level) and ensures correct cleanup when transactions abort or complete.

The structure supports PostgreSQL's nested transaction model by allowing locks to be associated with different resource owners. When a subtransaction aborts, only locks held by that specific resource owner are released, while parent transaction locks remain intact. The reference counting mechanism (nLocks) handles cases where the same lock is acquired multiple times by the same owner.

## Parameters / Member Variables
- `*owner`: Pointer to the ResourceOwnerData structure that owns this lock; NULL indicates the lock is held at session level rather than transaction level
- `nLocks`: Reference count indicating how many times this lock has been acquired by the specified owner
## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerData
- Called from (representative examples):
  - [LockAcquireExtended](LockAcquireExtended.md)
  - [LockRelease](LockRelease.md)
  - [GrantLockLocal](../G/GrantLockLocal.md)
  - [LockReleaseAll](LockReleaseAll.md)
  - LockReassignOwner
  - [AtPrepare_Locks](../A/AtPrepare_Locks.md)

## Notes and Other Information
The distinction between session-level (owner = NULL) and transaction-level locks is crucial for PostgreSQL's MVCC implementation. Session-level locks survive transaction boundaries, while transaction-level locks are automatically released on transaction end. The reference counting mechanism prevents premature lock release when the same lock is acquired multiple times within the same resource owner context, ensuring proper lock lifecycle management in complex transaction hierarchies.