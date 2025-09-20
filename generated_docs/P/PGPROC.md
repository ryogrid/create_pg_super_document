# PGPROC

## Location
[src/include/storage/proc.h:162-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/proc.h#L162-L369)

## Overview
PGPROC is a critical shared memory structure representing each backend process in PostgreSQL, containing all per-process state needed for transaction management, locking, and inter-process coordination.

## Definition

```c
struct PGPROC
{
	/* proc->links MUST BE FIRST IN STRUCT (see ProcSleep,ProcWakeup,etc) */
	dlist_node	links;			/* list link if process is in a list */
	dlist_head *procgloballist; /* procglobal list that owns this PGPROC */

	PGSemaphore sem;			/* ONE semaphore to sleep on */
	ProcWaitStatus waitStatus;

	Latch		procLatch;		/* generic latch for process */


	TransactionId xid;			/* id of top-level transaction currently being
								 * executed by this proc, if running and XID
								 * is assigned; else InvalidTransactionId.
								 * mirrored in ProcGlobal->xids[pgxactoff] */

	TransactionId xmin;			/* minimal running XID as it was when we were
								 * starting our xact, excluding LAZY VACUUM:
								 * vacuum must not remove tuples deleted by
								 * xid >= xmin ! */

	int			pid;			/* Backend's process ID; 0 if prepared xact */

	int			pgxactoff;		/* offset into various ProcGlobal->arrays with
								 * data mirrored from this PGPROC */

	/*
	 * Currently running top-level transaction's virtual xid. Together these
	 * form a VirtualTransactionId, but we don't use that struct because this
	 * is not atomically assignable as whole, and we want to enforce code to
	 * consider both parts separately.  See comments at VirtualTransactionId.
	 */
	struct
	{
		ProcNumber	procNumber; /* For regular backends, equal to
								 * GetNumberFromPGProc(proc).  For prepared
								 * xacts, ID of the original backend that
								 * processed the transaction. For unused
								 * PGPROC entries, INVALID_PROC_NUMBER. */
		LocalTransactionId lxid;	/* local id of top-level transaction
									 * currently * being executed by this
									 * proc, if running; else
									 * InvalidLocalTransactionId */
	}			vxid;

	/* These fields are zero while a backend is still starting up: */
	Oid			databaseId;		/* OID of database this backend is using */
	Oid			roleId;			/* OID of role using this backend */

	Oid			tempNamespaceId;	/* OID of temp schema this backend is
									 * using */

	bool		isBackgroundWorker; /* true if not a regular backend. */

	/*
	 * While in hot standby mode, shows that a conflict signal has been sent
	 * for the current transaction. Set/cleared while holding ProcArrayLock,
	 * though not required. Accessed without lock, if needed.
	 */
	bool		recoveryConflictPending;

	/* Info about LWLock the process is currently waiting for, if any. */
	uint8		lwWaiting;		/* see LWLockWaitState */
	uint8		lwWaitMode;		/* lwlock mode being waited for */
	proclist_node lwWaitLink;	/* position in LW lock wait list */

	/* Support for condition variables. */
	proclist_node cvWaitLink;	/* position in CV wait list */

	/* Info about lock the process is currently waiting for, if any. */
	/* waitLock and waitProcLock are NULL if not currently waiting. */
	LOCK	   *waitLock;		/* Lock object we're sleeping on ... */
	PROCLOCK   *waitProcLock;	/* Per-holder info for awaited lock */
	LOCKMODE	waitLockMode;	/* type of lock we're waiting for */
	LOCKMASK	heldLocks;		/* bitmask for lock types already held on this
								 * lock object by this backend */
	pg_atomic_uint64 waitStart; /* time at which wait for lock acquisition
								 * started */

	int			delayChkptFlags;	/* for DELAY_CHKPT_* flags */

	uint8		statusFlags;	/* this backend's status flags, see PROC_*
								 * above. mirrored in
								 * ProcGlobal->statusFlags[pgxactoff] */

	/*
	 * Info to allow us to wait for synchronous replication, if needed.
	 * waitLSN is InvalidXLogRecPtr if not waiting; set only by user backend.
	 * syncRepState must not be touched except by owning process or WALSender.
	 * syncRepLinks used only while holding SyncRepLock.
	 */
	XLogRecPtr	waitLSN;		/* waiting for this LSN or higher */
	int			syncRepState;	/* wait state for sync rep */
	dlist_node	syncRepLinks;	/* list link if process is in syncrep queue */

	/*
	 * All PROCLOCK objects for locks held or awaited by this backend are
	 * linked into one of these lists, according to the partition number of
	 * their lock.
	 */
	dlist_head	myProcLocks[NUM_LOCK_PARTITIONS];

	XidCacheStatus subxidStatus;	/* mirrored with
									 * ProcGlobal->subxidStates[i] */
	struct XidCache subxids;	/* cache for subtransaction XIDs */

	/* Support for group XID clearing. */
	/* true, if member of ProcArray group waiting for XID clear */
	bool		procArrayGroupMember;
	/* next ProcArray group member waiting for XID clear */
	pg_atomic_uint32 procArrayGroupNext;

	/*
	 * latest transaction id among the transaction's main XID and
	 * subtransactions
	 */
	TransactionId procArrayGroupMemberXid;

	uint32		wait_event_info;	/* proc's wait information */

	/* Support for group transaction status update. */
	bool		clogGroupMember;	/* true, if member of clog group */
	pg_atomic_uint32 clogGroupNext; /* next clog group member */
	TransactionId clogGroupMemberXid;	/* transaction id of clog group member */
	XidStatus	clogGroupMemberXidStatus;	/* transaction status of clog
											 * group member */
	int64		clogGroupMemberPage;	/* clog page corresponding to
										 * transaction id of clog group member */
	XLogRecPtr	clogGroupMemberLsn; /* WAL location of commit record for clog
									 * group member */

	/* Lock manager data, recording fast-path locks taken by this backend. */
	LWLock		fpInfoLock;		/* protects per-backend fast-path state */
	uint64		fpLockBits;		/* lock modes held for each fast-path slot */
	Oid			fpRelId[FP_LOCK_SLOTS_PER_BACKEND]; /* slots for rel oids */
	bool		fpVXIDLock;		/* are we holding a fast-path VXID lock? */
	LocalTransactionId fpLocalTransactionId;	/* lxid for fast-path VXID
												 * lock */

	/*
	 * Support for lock groups.  Use LockHashPartitionLockByProc on the group
	 * leader to get the LWLock protecting these fields.
	 */
	PGPROC	   *lockGroupLeader;	/* lock group leader, if I'm a member */
	dlist_head	lockGroupMembers;	/* list of members, if I'm a leader */
	dlist_node	lockGroupLink;	/* my member link, if I'm a member */
};
```
## Detailed Description
PGPROC is the fundamental per-process structure in PostgreSQL's shared memory architecture. Each backend process (including regular user backends, background workers, and auxiliary processes) has an associated PGPROC structure that contains all the state information necessary for that process to participate in PostgreSQL's transaction management, locking, and inter-process coordination systems.

The structure serves multiple critical roles:
1. **Transaction Management**: Stores the current transaction ID (xid) and transaction visibility information (xmin)
2. **Lock Management**: Tracks locks held and awaited by the process, including fast-path locks for optimization
3. **Process Coordination**: Provides mechanisms for inter-process signaling via semaphores and latches
4. **Subtransaction Caching**: Contains XidCache for efficient subtransaction visibility checking
5. **Group Processing**: Supports group-based optimizations for transaction commits and clog updates
6. **Wait Events**: Tracks what the process is currently waiting for (locks, I/O, etc.)

Some fields are mirrored in dense arrays within ProcGlobal for performance optimization, allowing tight loops to access frequently-used data without cache misses from the full PGPROC structure.

## Parameters / Member Variables
- : List linkage for when process is in various wait queues (MUST be first field)
- : Pointer to the global list that owns this PGPROC
- : Process semaphore for blocking/waking operations
- : Current wait status of the process
- : Generic latch for process signaling
- : Current top-level transaction ID (mirrored in dense arrays)
- : Minimum running XID for vacuum coordination
- : Backend process ID (0 for prepared transactions)
- : Offset into ProcGlobal dense arrays
- : Virtual transaction ID components (procNumber + lxid)
- : OID of database this backend is connected to
- : OID of role/user for this backend
- : OID of temporary schema for this backend
- : Flag indicating background worker vs regular backend
- : Hot standby conflict signal flag
- : LWLock wait state information
- : Mode of LWLock being waited for
- : Position in LWLock wait queue
- : Position in condition variable wait queue
- : Lock object currently being waited for
- : Per-holder information for awaited lock
- : Type of lock mode being requested
- : Bitmask of lock types already held
- : Timestamp when lock wait began
- : Checkpoint delay flags (DELAY_CHKPT_*)
- : Process status flags (mirrored in dense arrays)
- : LSN being waited for in synchronous replication
- : Synchronous replication wait state
- : Linkage in synchronous replication queue
- : Per-partition lists of locks held by this process
- : Status of subtransaction cache
- : Cache of subtransaction XIDs (XidCache structure)
- : Flag for group XID clearing participation
- : Next member in group XID clearing chain
- : Transaction ID for group processing
- : Current wait event information for monitoring
- : Flag for group clog update participation
- : Next member in clog group update chain
- : Transaction ID for clog group update
- : Transaction status for clog group update
- : Clog page for group update
- : WAL LSN for group commit record
- : LWLock protecting fast-path lock state
- : Bitmask of fast-path lock modes held
- : Relation OIDs for fast-path lock slots
- : Flag indicating fast-path virtual XID lock held
- : Local transaction ID for fast-path VXID lock
- : Pointer to lock group leader (if member)
- : List of lock group members (if leader)
- : Linkage as member of a lock group

## Dependencies
- Functions called/Symbols referenced:
  - ProcNumber (for virtual transaction ID)
  - LocalTransactionId (for virtual transaction ID)
  - [XidCache](../X/XidCache.md) (for subtransaction caching)
  - Various lock and latch types (LOCK, PROCLOCK, LWLock, etc.)
- Called from (representative examples):
  - InitProcess (process initialization)
  - [ProcArrayAdd](ProcArrayAdd.md) (adding to process array)
  - ProcSleep (lock waiting)
  - [GetSnapshotData](../G/GetSnapshotData.md) (transaction visibility)
  - [DeadLockCheck](../D/DeadLockCheck.md) (deadlock detection)

## Notes and Other Information
- The  field MUST be the first field in the structure for proper operation of ProcSleep/ProcWakeup functions
- Many fields are mirrored in dense arrays within ProcGlobal for performance optimization
- Access to mirrored fields requires holding ProcArrayLock or XidGenLock to prevent race conditions
- Prepared transactions have special PGPROC entries with pid=0
- The structure supports both regular backends and various types of background worker processes
- Fast-path locking provides optimization for frequently-accessed relation locks
- Group processing mechanisms (group XID clearing, group clog updates) reduce contention for high-throughput workloads
- Lock groups allow parallel workers to share lock state for improved concurrency