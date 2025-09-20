# ParallelApplyWorkerShared

## Location
[src/include/replication/worker_internal.h:138-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/worker_internal.h#L138-L183)

## Overview
ParallelApplyWorkerShared is a shared memory structure that facilitates communication and coordination between the leader apply worker and parallel apply workers in PostgreSQL's logical replication system.

## Definition

```c
typedef struct ParallelApplyWorkerShared
{
	slock_t		mutex;

	TransactionId xid;

	/*
	 * State used to ensure commit ordering.
	 *
	 * The parallel apply worker will set it to PARALLEL_TRANS_FINISHED after
	 * handling the transaction finish commands while the apply leader will
	 * wait for it to become PARALLEL_TRANS_FINISHED before proceeding in
	 * transaction finish commands (e.g. STREAM_COMMIT/STREAM_PREPARE/
	 * STREAM_ABORT).
	 */
	ParallelTransState xact_state;

	/* Information from the corresponding LogicalRepWorker slot. */
	uint16		logicalrep_worker_generation;
	int			logicalrep_worker_slot_no;

	/*
	 * Indicates whether there are pending streaming blocks in the queue. The
	 * parallel apply worker will check it before starting to wait.
	 */
	pg_atomic_uint32 pending_stream_count;

	/*
	 * XactLastCommitEnd from the parallel apply worker. This is required by
	 * the leader worker so it can update the lsn_mappings.
	 */
	XLogRecPtr	last_commit_end;

	/*
	 * After entering PARTIAL_SERIALIZE mode, the leader apply worker will
	 * serialize changes to the file, and share the fileset with the parallel
	 * apply worker when processing the transaction finish command. Then the
	 * parallel apply worker will apply all the spooled messages.
	 *
	 * FileSet is used here instead of SharedFileSet because we need it to
	 * survive after releasing the shared memory so that the leader apply
	 * worker can re-use the same fileset for the next streaming transaction.
	 */
	PartialFileSetState fileset_state;
	FileSet		fileset;
} ParallelApplyWorkerShared;
```
## Detailed Description
ParallelApplyWorkerShared serves as the primary communication mechanism between leader and parallel apply workers in PostgreSQL's parallel logical replication system. It maintains transaction state information, coordinates commit ordering through the xact_state field, and manages file-based serialization of transaction data when memory limits are exceeded. The structure ensures proper synchronization and ordering of parallel transaction processing while allowing for efficient data sharing between worker processes.

## Parameters / Member Variables
- `mutex`: Spinlock for protecting concurrent access to shared data
- `xid`: Transaction ID of the current transaction being processed
- `xact_state`: State flag ensuring proper commit ordering between parallel workers
- `logicalrep_worker_generation`: Generation counter from the associated LogicalRepWorker slot
- `logicalrep_worker_slot_no`: Slot number of the corresponding LogicalRepWorker
- `pending_stream_count`: Atomic counter tracking pending streaming blocks in the queue
- `last_commit_end`: LSN position of the last committed transaction end
- `fileset_state`: State information for partial file serialization mode
- `fileset`: File set used for serializing transaction data when in partial serialize mode
## Dependencies
- Functions called/Symbols referenced:
  - [slock_t](../s/slock_t.md)
  - ParallelTransState
  - [pg_atomic_uint32](../p/pg_atomic_uint32.md)
  - PartialFileSetState
  - [FileSet](../F/FileSet.md)
- Called from (representative examples):
  - [pa_setup_dsm](../p/pa_setup_dsm.md)
  - [ParallelApplyWorkerMain](ParallelApplyWorkerMain.md)
  - [pa_set_xact_state](../p/pa_set_xact_state.md)
  - [pa_get_xact_state](../p/pa_get_xact_state.md)
  - [pa_set_fileset_state](../p/pa_set_fileset_state.md)

## Notes and Other Information
This structure is allocated in dynamic shared memory (DSM) and is crucial for maintaining transactional consistency in parallel apply scenarios. The fileset mechanism allows for spilling large transactions to disk when memory pressure occurs, ensuring that parallel processing can continue even with very large streaming transactions. The atomic pending_stream_count helps optimize worker waiting patterns and reduces unnecessary wake-ups.