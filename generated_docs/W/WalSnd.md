# WalSnd

## Location
[src/include/replication/walsender_private.h:42-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/walsender_private.h#L42-L86)

## Overview
WalSnd is a shared memory structure that represents the state and control information for each WAL sender process in PostgreSQL's replication system. Each walsender process has its own WalSnd struct in shared memory to track replication progress and coordinate with standby servers.

## Definition

```c
typedef struct WalSnd
{
	pid_t		pid;			/* this walsender's PID, or 0 if not active */

	WalSndState state;			/* this walsender's state */
	XLogRecPtr	sentPtr;		/* WAL has been sent up to this point */
	bool		needreload;		/* does currently-open file need to be
								 * reloaded? */

	/*
	 * The xlog locations that have been written, flushed, and applied by
	 * standby-side. These may be invalid if the standby-side has not offered
	 * values yet.
	 */
	XLogRecPtr	write;
	XLogRecPtr	flush;
	XLogRecPtr	apply;

	/* Measured lag times, or -1 for unknown/none. */
	TimeOffset	writeLag;
	TimeOffset	flushLag;
	TimeOffset	applyLag;

	/*
	 * The priority order of the standby managed by this WALSender, as listed
	 * in synchronous_standby_names, or 0 if not-listed.
	 */
	int			sync_standby_priority;

	/* Protects shared variables in this structure. */
	slock_t		mutex;

	/*
	 * Pointer to the walsender's latch. Used by backends to wake up this
	 * walsender when it has work to do. NULL if the walsender isn't active.
	 */
	Latch	   *latch;

	/*
	 * Timestamp of the last message received from standby.
	 */
	TimestampTz replyTime;

	ReplicationKind kind;
} WalSnd;
```
## Detailed Description
The WalSnd structure serves as the central coordination point for each WAL sender process in PostgreSQL's streaming replication architecture. It maintains critical state information about the replication connection, tracks replication progress, and provides synchronization mechanisms between the walsender process and other backend processes.

This structure is stored in shared memory and protected by a spinlock mutex to ensure thread-safe access. The structure tracks both the sender-side state (what has been sent) and receiver-side acknowledgments (what has been written, flushed, and applied on the standby). This bidirectional tracking enables features like synchronous replication and lag monitoring.

The struct is designed to support both physical and logical replication through the ReplicationKind field, and integrates with PostgreSQL's synchronous replication infrastructure through the sync_standby_priority field.

## Parameters / Member Variables
- `pid`: Process ID of the walsender process; 0 indicates an inactive slot
- `state`: Current state of the walsender (WalSndState enum values like WALSNDSTATE_STARTUP, WALSNDSTATE_STREAMING, etc.)
- `sentPtr`: WAL location up to which data has been sent to the standby
- `needreload`: Flag indicating whether the currently open WAL file needs to be reloaded (used for WAL file rotation)
- `write`: WAL location confirmed as written by the standby server
- `flush`: WAL location confirmed as flushed to disk by the standby server
- `apply`: WAL location confirmed as applied/replayed by the standby server
- `writeLag`: Measured time lag for write acknowledgments (-1 if unknown)
- `flushLag`: Measured time lag for flush acknowledgments (-1 if unknown)
- `applyLag`: Measured time lag for apply acknowledgments (-1 if unknown)
- `sync_standby_priority`: Priority in synchronous_standby_names list (0 if not listed)
- `mutex`: Spinlock protecting shared variables in this structure
- `*latch`: Pointer to walsender's latch for inter-process communication (NULL if inactive)
- `replyTime`: Timestamp of the last message received from the standby
- `kind`: Type of replication (physical or logical)
## Dependencies
- Functions called/Symbols referenced:
  - pid_t
  - WalSndState
  - XLogRecPtr
  - TimeOffset
  - [slock_t](../s/slock_t.md)
  - [Latch](../L/Latch.md)
  - TimestampTz
  - ReplicationKind

- Called from (representative examples):
  - [SyncRepGetCandidateStandbys](../S/SyncRepGetCandidateStandbys.md)
  - [ProcessStandbyReplyMessage](../P/ProcessStandbyReplyMessage.md)
  - [ProcessStandbyHSFeedbackMessage](../P/ProcessStandbyHSFeedbackMessage.md)
  - [InitWalSenderSlot](../I/InitWalSenderSlot.md)
  - [WalSndKill](WalSndKill.md)
  - [XLogSendPhysical](../X/XLogSendPhysical.md)
  - [XLogSendLogical](../X/XLogSendLogical.md)
  - [WalSndSetState](WalSndSetState.md)
  - [WalSndShmemInit](WalSndShmemInit.md)

## Notes and Other Information
- The structure is protected by a spinlock mutex, but some members are only written by the walsender process itself and can be read without holding the spinlock
- The  and  fields always require the spinlock for all accesses
- WAL locations (write, flush, apply) may be invalid if the standby has not yet offered values
- Lag measurements are critical for monitoring replication performance and detecting replication delays
- The structure supports PostgreSQL's synchronous replication by tracking standby priorities and acknowledgment states
- Memory for WalSnd structures is allocated in shared memory during PostgreSQL startup
- The structure is defined in src/include/replication/walsender_private.h:42-86