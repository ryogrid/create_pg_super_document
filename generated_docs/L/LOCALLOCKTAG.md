# LOCALLOCKTAG

## Location
[src/include/storage/lock.h:408-412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/lock.h#L408-L412)

## Overview
LOCALLOCKTAG serves as a unique identifier for entries in a backend's local lock hash table, combining a lock identifier with a specific lock mode.

## Definition

```c
typedef struct LOCALLOCKTAG
{
	LOCKTAG		lock;			/* identifies the lockable object */
	LOCKMODE	mode;			/* lock mode for this table entry */
} LOCALLOCKTAG;
```
## Detailed Description
LOCALLOCKTAG is a key structure used in PostgreSQL's local lock management system. Each backend process maintains a local hash table that tracks locks it has acquired or is interested in, and LOCALLOCKTAG serves as the unique key for entries in this table. The combination of a specific lockable object (identified by LOCKTAG) and a particular lock mode creates a unique identifier, allowing the same object to have multiple entries if held in different lock modes.

This local tracking mechanism enables multiple requests for the same lock to be handled without additional shared memory accesses, improving performance. The structure is essential for the fast-path locking mechanism and helps maintain accurate reference counts for lock acquisitions per ResourceOwner.

## Parameters / Member Variables
- `lock`: A LOCKTAG structure that uniquely identifies the lockable object (relation, transaction, etc.)
- `mode`: The specific lock mode (LOCKMODE) being requested or held on the identified object
## Dependencies
- Functions called/Symbols referenced:
  - LOCKTAG
  - LOCKMODE
- Called from (representative examples):
  - [LockAcquireExtended](LockAcquireExtended.md)
  - [LockRelease](LockRelease.md)
  - [LockHeldByMe](LockHeldByMe.md)
  - [LockHasWaiters](LockHasWaiters.md)
  - InitLocks

## Notes and Other Information
LOCALLOCKTAG is used as a hash key in the local lock table, allowing backends to track multiple lock modes on the same object separately. This design supports PostgreSQL's hierarchical locking system where a transaction might hold different types of locks on the same resource. The structure is crucial for the fast-path locking optimization, where frequently used locks can be managed locally without contending for shared memory structures.