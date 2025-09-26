# PROCLOCKTAG

## Location
[src/include/storage/lock.h:362-367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/lock.h#L362-L367)

## Overview
PROCLOCKTAG is the key data structure used to identify individual lock holdings in PostgreSQL's shared memory. It uniquely identifies the combination of a lockable object and the backend process that holds or awaits that lock.

## Definition

```c
typedef struct PROCLOCKTAG
{
	/* NB: we assume this struct contains no padding! */
	LOCK	   *myLock;			/* link to per-lockable-object information */
	PGPROC	   *myProc;			/* link to PGPROC of owning backend */
} PROCLOCKTAG;
```
## Detailed Description
PROCLOCKTAG serves as the hash key for looking up PROCLOCK objects in the proclock hashtable. It represents the association between a specific lock (identified by LOCK structure) and a specific backend process (identified by PGPROC structure).

The structure is designed to be compact with no padding, containing only two pointers that together uniquely identify a lock holder/waiter combination. The use of pointers is safe because the PROCLOCKTAG only needs to be unique for the lifespan of the PROCLOCK, and it will never outlive either the lock or the process.

This design allows PostgreSQL to efficiently track which backends hold which locks, enabling:
- Multiple backends to hold different modes of the same lock
- Proper lock conflict detection
- Deadlock detection algorithms
- Lock release operations when transactions commit/abort

The structure is fundamental to PostgreSQL's multi-process locking system, as it bridges the gap between locks (resources) and processes (lock holders).

## Parameters / Member Variables
- : Pointer to the LOCK structure representing the lockable object being held/awaited
- : Pointer to the PGPROC structure representing the backend process that holds or awaits the lock

## Dependencies
- Functions called/Symbols referenced:
  - LOCK
  - PGPROC
- Called from (representative examples):
  - SetupLockInTable
  - LockRelease
  - ProcLockHashCode
  - FastPathGetRelationLockEntry
  - LockRefindAndRelease
  - PostPrepare_Locks

## Notes and Other Information
- Designed to contain no padding for efficient memory usage and hashing
- Used as the hash key for the proclock hashtable
- The combination of myLock and myProc pointers must be unique within the system
- Lifetime is bounded by the shorter of the lock's lifetime or the process's lifetime
- Essential for tracking lock ownership in a multi-process environment
- Used extensively in deadlock detection where relationships between processes and locks must be analyzed
- Critical for proper lock cleanup when processes terminate or transactions end
- The pointer-based design is safe because both referenced objects persist as long as the PROCLOCKTAG is needed
- Enables efficient lookup of lock holdings for specific process/lock combinations