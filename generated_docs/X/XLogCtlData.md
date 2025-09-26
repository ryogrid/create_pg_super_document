# XLogCtlData

## Location
[src/backend/access/transam/xlog.c:451-555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L451-L555)

## Overview
XLogCtlData is the master shared-memory control structure that contains all the global state for PostgreSQL's Write-Ahead Log (WAL) system, coordinating WAL operations across all backend processes.

## Definition

```c
typedef struct XLogCtlData
{
	XLogCtlInsert Insert;

	/* Protected by info_lck: */
	XLogwrtRqst LogwrtRqst;
	XLogRecPtr	RedoRecPtr;		/* a recent copy of Insert->RedoRecPtr */
	FullTransactionId ckptFullXid;	/* nextXid of latest checkpoint */
	XLogRecPtr	asyncXactLSN;	/* LSN of newest async commit/abort */
	XLogRecPtr	replicationSlotMinLSN;	/* oldest LSN needed by any slot */

	XLogSegNo	lastRemovedSegNo;	/* latest removed/recycled XLOG segment */

	/* Fake LSN counter, for unlogged relations. */
	pg_atomic_uint64 unloggedLSN;

	/* Time and LSN of last xlog segment switch. Protected by WALWriteLock. */
	pg_time_t	lastSegSwitchTime;
	XLogRecPtr	lastSegSwitchLSN;

	/* These are accessed using atomics -- info_lck not needed */
	pg_atomic_uint64 logInsertResult;	/* last byte + 1 inserted to buffers */
	pg_atomic_uint64 logWriteResult;	/* last byte + 1 written out */
	pg_atomic_uint64 logFlushResult;	/* last byte + 1 flushed */

	/*
	 * Latest initialized page in the cache (last byte position + 1).
	 *
	 * To change the identity of a buffer (and InitializedUpTo), you need to
	 * hold WALBufMappingLock.  To change the identity of a buffer that's
	 * still dirty, the old page needs to be written out first, and for that
	 * you need WALWriteLock, and you need to ensure that there are no
	 * in-progress insertions to the page by calling
	 * WaitXLogInsertionsToFinish().
	 */
	XLogRecPtr	InitializedUpTo;

	/*
	 * These values do not change after startup, although the pointed-to pages
	 * and xlblocks values certainly do.  xlblocks values are protected by
	 * WALBufMappingLock.
	 */
	char	   *pages;			/* buffers for unwritten XLOG pages */
	pg_atomic_uint64 *xlblocks; /* 1st byte ptr-s + XLOG_BLCKSZ */
	int			XLogCacheBlck;	/* highest allocated xlog buffer index */

	/*
	 * InsertTimeLineID is the timeline into which new WAL is being inserted
	 * and flushed. It is zero during recovery, and does not change once set.
	 *
	 * If we create a new timeline when the system was started up,
	 * PrevTimeLineID is the old timeline's ID that we forked off from.
	 * Otherwise it's equal to InsertTimeLineID.
	 *
	 * We set these fields while holding info_lck. Most that reads these
	 * values knows that recovery is no longer in progress and so can safely
	 * read the value without a lock, but code that could be run either during
	 * or after recovery can take info_lck while reading these values.
	 */
	TimeLineID	InsertTimeLineID;
	TimeLineID	PrevTimeLineID;

	/*
	 * SharedRecoveryState indicates if we're still in crash or archive
	 * recovery.  Protected by info_lck.
	 */
	RecoveryState SharedRecoveryState;

	/*
	 * InstallXLogFileSegmentActive indicates whether the checkpointer should
	 * arrange for future segments by recycling and/or PreallocXlogFiles().
	 * Protected by ControlFileLock.  Only the startup process changes it.  If
	 * true, anyone can use InstallXLogFileSegment().  If false, the startup
	 * process owns the exclusive right to install segments, by reading from
	 * the archive and possibly replacing existing files.
	 */
	bool		InstallXLogFileSegmentActive;

	/*
	 * WalWriterSleeping indicates whether the WAL writer is currently in
	 * low-power mode (and hence should be nudged if an async commit occurs).
	 * Protected by info_lck.
	 */
	bool		WalWriterSleeping;

	/*
	 * During recovery, we keep a copy of the latest checkpoint record here.
	 * lastCheckPointRecPtr points to start of checkpoint record and
	 * lastCheckPointEndPtr points to end+1 of checkpoint record.  Used by the
	 * checkpointer when it wants to create a restartpoint.
	 *
	 * Protected by info_lck.
	 */
	XLogRecPtr	lastCheckPointRecPtr;
	XLogRecPtr	lastCheckPointEndPtr;
	CheckPoint	lastCheckPoint;

	/*
	 * lastFpwDisableRecPtr points to the start of the last replayed
	 * XLOG_FPW_CHANGE record that instructs full_page_writes is disabled.
	 */
	XLogRecPtr	lastFpwDisableRecPtr;

	slock_t		info_lck;		/* locks shared variables shown above */
} XLogCtlData;
```
## Detailed Description
XLogCtlData serves as the central nervous system for PostgreSQL's WAL subsystem, containing all shared state necessary for coordinating WAL operations across multiple backend processes. This structure manages everything from WAL insertion and buffer management to recovery state tracking and checkpoint coordination.

The structure is carefully designed with different locking strategies for different types of data: the info_lck spinlock protects frequently accessed metadata, atomic operations handle high-contention counters, and specialized locks like WALWriteLock and WALBufMappingLock protect specific subsystems.

Key responsibilities include tracking WAL insertion progress, managing the WAL buffer cache, coordinating write and flush operations, maintaining timeline information for point-in-time recovery, and providing the infrastructure for crash recovery and online backup operations.

## Parameters / Member Variables
- `Insert`: XLogCtlInsert structure managing WAL insertion operations and locking
- `LogwrtRqst`: Write and flush request tracking (protected by info_lck)
- `RedoRecPtr`: Recent copy of the current redo point for insertions
- `ckptFullXid`: Transaction ID from the latest checkpoint
- `asyncXactLSN`: LSN of the most recent asynchronous commit or abort
- `replicationSlotMinLSN`: Oldest LSN still needed by any replication slot
- `lastRemovedSegNo`: Most recently removed or recycled WAL segment number
- `unloggedLSN`: Fake LSN counter for unlogged relations (atomic)
- `lastSegSwitchTime`: Timestamp of last WAL segment switch
- `lastSegSwitchLSN`: LSN at last WAL segment switch
- `logInsertResult`: Last byte position + 1 inserted to buffers (atomic)
- `logWriteResult`: Last byte position + 1 written to disk (atomic)
- `logFlushResult`: Last byte position + 1 flushed to disk (atomic)
- `InitializedUpTo`: Latest initialized page position in WAL buffer cache
- `*pages`: Buffer array for unwritten WAL pages
- `*xlblocks`: Array of block end positions for WAL buffers (atomic)
- `XLogCacheBlck`: Highest allocated WAL buffer index
- `InsertTimeLineID`: Timeline ID for current WAL insertion and flushing
- `PrevTimeLineID`: Previous timeline ID before fork
- `SharedRecoveryState`: Current recovery state (crash/archive recovery)
- `InstallXLogFileSegmentActive`: Controls WAL segment installation rights
- `WalWriterSleeping`: Indicates if WAL writer is in low-power mode
- `lastCheckPointRecPtr`: Start position of last checkpoint record
- `lastCheckPointEndPtr`: End position + 1 of last checkpoint record
- `lastCheckPoint`: Copy of the latest checkpoint record
- `lastFpwDisableRecPtr`: Start of last full-page-write disable record
- `info_lck`: Spinlock protecting shared variables
## Dependencies
- Functions called/Symbols referenced:
  - [XLogCtlInsert](XLogCtlInsert.md) (WAL insertion control structure)
  - [XLogwrtRqst](XLogwrtRqst.md) (write request structure)
  - [FullTransactionId](../F/FullTransactionId.md) (transaction ID type)
  - XLogSegNo (WAL segment number type)
  - [pg_atomic_uint64](../p/pg_atomic_uint64.md) (atomic 64-bit integer type)
  - pg_time_t (time type)
  - [RecoveryState](../R/RecoveryState.md) (recovery state enumeration)
  - [CheckPoint](../C/CheckPoint.md) (checkpoint record structure)
  - [slock_t](../s/slock_t.md) (spinlock type)
- Called from (representative examples):
  - [WalInsertClass](../W/WalInsertClass.md)
  - [XLOGShmemSize](XLOGShmemSize.md)
  - [XLOGShmemInit](XLOGShmemInit.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)

## Notes and Other Information
- Central shared-memory structure for all WAL operations in PostgreSQL
- Uses multiple locking strategies optimized for different access patterns
- Critical for crash recovery, point-in-time recovery, and online backup functionality
- Timeline management enables branching recovery scenarios
- Atomic counters provide high-performance tracking of WAL progress
- Buffer management coordinates WAL page allocation and initialization
- Checkpoint tracking supports both crash recovery and performance optimization
- Replication slot integration ensures WAL retention for streaming replication
- Carefully designed memory layout and locking to minimize contention in high-concurrency environments