# LogicalRepWorker

## Location
[src/include/replication/worker_internal.h:37-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/worker_internal.h#L37-L95)

## Overview
LogicalRepWorker is a core data structure that represents a logical replication worker process in PostgreSQL's logical replication system, managing both apply workers and table synchronization workers.

## Definition

```c
typedef struct LogicalRepWorker
{
	/* What type of worker is this? */
	LogicalRepWorkerType type;

	/* Time at which this worker was launched. */
	TimestampTz launch_time;

	/* Indicates if this slot is used or free. */
	bool		in_use;

	/* Increased every time the slot is taken by new worker. */
	uint16		generation;

	/* Pointer to proc array. NULL if not running. */
	PGPROC	   *proc;

	/* Database id to connect to. */
	Oid			dbid;

	/* User to use for connection (will be same as owner of subscription). */
	Oid			userid;

	/* Subscription id for the worker. */
	Oid			subid;

	/* Used for initial table synchronization. */
	Oid			relid;
	char		relstate;
	XLogRecPtr	relstate_lsn;
	slock_t		relmutex;

	/*
	 * Used to create the changes and subxact files for the streaming
	 * transactions.  Upon the arrival of the first streaming transaction or
	 * when the first-time leader apply worker times out while sending changes
	 * to the parallel apply worker, the fileset will be initialized, and it
	 * will be deleted when the worker exits.  Under this, separate buffiles
	 * would be created for each transaction which will be deleted after the
	 * transaction is finished.
	 */
	FileSet    *stream_fileset;

	/*
	 * PID of leader apply worker if this slot is used for a parallel apply
	 * worker, InvalidPid otherwise.
	 */
	pid_t		leader_pid;

	/* Indicates whether apply can be performed in parallel. */
	bool		parallel_apply;

	/* Stats. */
	XLogRecPtr	last_lsn;
	TimestampTz last_send_time;
	TimestampTz last_recv_time;
	XLogRecPtr	reply_lsn;
	TimestampTz reply_time;
} LogicalRepWorker;
```
## Detailed Description
LogicalRepWorker is a shared memory structure that tracks the state and configuration of individual logical replication worker processes. It serves as the central control structure for managing both apply workers (which apply changes from the publisher) and table synchronization workers (which perform initial data synchronization for new tables). The structure supports parallel apply functionality where multiple workers can process changes concurrently, with leader-follower relationships tracked through the leader_pid field.

## Parameters / Member Variables
- `type`: The type of logical replication worker (apply, sync, parallel apply, etc.)
- `launch_time`: Timestamp when this worker process was started
- `in_use`: Boolean flag indicating whether this worker slot is currently active
- `generation`: Counter incremented each time the slot is reassigned to prevent stale references
- `*proc`: Pointer to the process array entry for this worker, NULL when not running
- `dbid`: Object ID of the target database the worker connects to
- `userid`: Object ID of the user account used for replication connections
- `subid`: Object ID of the subscription this worker serves
- `relid`: Object ID of the relation being synchronized (for table sync workers)
- `relstate`: Current synchronization state of the relation
- `relstate_lsn`: LSN position associated with the relation state
- `relmutex`: Spinlock protecting relation state information
- `*stream_fileset`: File set for managing streaming transaction data files
- `leader_pid`: Process ID of the leader worker for parallel apply workers
- `parallel_apply`: Flag indicating if this worker supports parallel processing
- `last_lsn`: Last LSN position processed by this worker
- `last_send_time`: Timestamp of last message sent by this worker
- `last_recv_time`: Timestamp of last message received by this worker
- `reply_lsn`: LSN position of last reply sent to publisher
- `reply_time`: Timestamp of last reply sent to publisher
## Dependencies
- Functions called/Symbols referenced:
  - [LogicalRepWorkerType](LogicalRepWorkerType.md)
  - [PGPROC](../P/PGPROC.md)
  - [slock_t](../s/slock_t.md)
  - [FileSet](../F/FileSet.md)
  - pid_t
- Called from (representative examples):
  - [logicalrep_worker_find](../l/logicalrep_worker_find.md)
  - [logicalrep_worker_launch](../l/logicalrep_worker_launch.md)
  - [logicalrep_worker_stop_internal](../l/logicalrep_worker_stop_internal.md)
  - [logicalrep_worker_detach](../l/logicalrep_worker_detach.md)
  - [ApplyLauncherShmemInit](../A/ApplyLauncherShmemInit.md)

## Notes and Other Information
This structure is allocated in shared memory and is the primary means of communication and coordination between the logical replication launcher process and individual worker processes. The generation counter mechanism helps prevent race conditions when workers are rapidly started and stopped. The parallel apply functionality allows for improved performance by enabling concurrent processing of changes from different transactions, with proper coordination through the leader_pid mechanism.