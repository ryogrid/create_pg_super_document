# LOCK

## Location
src/include/storage/lock.h: 308 - 322

## Overview
LOCK represents a lockable object in PostgreSQL's shared memory lock manager. It contains all information about granted and pending locks on a specific resource, including wait queues and lock mode statistics.

## Definition

```c
typedef struct LOCK
{
	/* hash key */
	LOCKTAG		tag;			/* unique identifier of lockable object */

	/* data */
	LOCKMASK	grantMask;		/* bitmask for lock types already granted */
	LOCKMASK	waitMask;		/* bitmask for lock types awaited */
	dlist_head	procLocks;		/* list of PROCLOCK objects assoc. with lock */
	dclist_head waitProcs;		/* list of PGPROC objects waiting on lock */
	int			requested[MAX_LOCKMODES];	/* counts of requested locks */
	int			nRequested;		/* total of requested[] array */
	int			granted[MAX_LOCKMODES]; /* counts of granted locks */
	int			nGranted;		/* total of granted[] array */
} LOCK;
```
## Detailed Description
The LOCK structure is the central data structure for managing locks on specific resources in PostgreSQL. Each lockable object (relation, page, tuple, etc.) that has locks requested or granted gets a LOCK entry in the shared lock hashtable.

The structure efficiently tracks both current state (what locks are granted) and pending state (what locks are being waited for) using bitmasks for quick conflict checking and arrays for detailed statistics. The procLocks list connects to all PROCLOCK structures that represent individual backend holdings of this lock, while waitProcs maintains the queue of processes waiting to acquire conflicting locks.

Key design features:
- Uses LOCKTAG as hash key for efficient lookup
- Bitmasks provide fast lock conflict detection
- Separate tracking of requested vs granted locks
- Maintains proper wait queues for lock acquisition ordering
- Statistics arrays support detailed lock monitoring and debugging

The lock manager uses this structure to implement PostgreSQL's multi-granularity locking protocol, deadlock detection, and fair lock scheduling.

## Parameters / Member Variables
- : LOCKTAG that uniquely identifies the lockable object (serves as hash key)
- : Bitmask indicating which lock types are currently granted on this object
- : Bitmask indicating which lock types have processes waiting
- : Doubly-linked list of PROCLOCK objects representing individual backend holdings
- : Doubly-linked circular list of PGPROC objects waiting for this lock
- : Array counting requested locks by mode (includes already granted locks)
- : Total count of all requested locks across all modes
- : Array counting granted locks by mode
- : Total count of all granted locks across all modes

## Dependencies
- Functions called/Symbols referenced:
  - LOCKTAG
  - LOCKMASK  
  - dlist_head
  - dclist_head
  - MAX_LOCKMODES
- Called from (representative examples):
  - SetupLockInTable
  - LockAcquireExtended
  - LockRelease
  - LockCheckConflicts
  - GrantLock
  - UnGrantLock
  - GetLockConflicts

## Notes and Other Information
- Stored in the shared lock hashtable with LOCKTAG as the hash key
- The requested[] counts include already granted locks, so requested[i] >= granted[i] always
- Lock counts represent backends, not individual lock acquisitions within a backend
- The waitProcs queue implements fair lock scheduling using FIFO ordering
- Used extensively in deadlock detection algorithms 
- Critical for lock conflict resolution and granting decisions
- The structure supports PostgreSQL's hierarchical locking with different lock modes
- Cleanup occurs when nGranted and nRequested both reach zero
- Essential for monitoring lock contention through system views like pg_locks