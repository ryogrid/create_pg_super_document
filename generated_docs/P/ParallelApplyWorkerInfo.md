# ParallelApplyWorkerInfo

## Location
[src/include/replication/worker_internal.h:188-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/worker_internal.h#L188-L218)

## Overview
ParallelApplyWorkerInfo is a management structure that coordinates communication and resource allocation between leader apply workers and parallel apply workers in PostgreSQL's logical replication system.

## Definition

```c
typedef struct ParallelApplyWorkerInfo
{
	/*
	 * This queue is used to send changes from the leader apply worker to the
	 * parallel apply worker.
	 */
	shm_mq_handle *mq_handle;

	/*
	 * This queue is used to transfer error messages from the parallel apply
	 * worker to the leader apply worker.
	 */
	shm_mq_handle *error_mq_handle;

	dsm_segment *dsm_seg;

	/*
	 * Indicates whether the leader apply worker needs to serialize the
	 * remaining changes to a file due to timeout when attempting to send data
	 * to the parallel apply worker via shared memory.
	 */
	bool		serialize_changes;

	/*
	 * True if the worker is being used to process a parallel apply
	 * transaction. False indicates this worker is available for re-use.
	 */
	bool		in_use;

	ParallelApplyWorkerShared *shared;
} ParallelApplyWorkerInfo;
```
## Detailed Description
ParallelApplyWorkerInfo serves as the primary management interface for parallel apply workers from the leader worker's perspective. It encapsulates all the communication channels, memory management, and state tracking needed to coordinate with a parallel worker process. The structure manages shared memory queues for bidirectional communication, tracks worker availability, and handles fallback mechanisms when shared memory communication becomes inefficient due to timeouts or capacity issues.

## Parameters / Member Variables
- `*mq_handle`: Handle to the shared memory queue for sending transaction changes from leader to parallel worker
- `*error_mq_handle`: Handle to the shared memory queue for receiving error messages from parallel worker to leader
- `*dsm_seg`: Dynamic shared memory segment associated with this parallel worker
- `serialize_changes`: Flag indicating whether to fall back to file-based serialization due to shared memory timeouts
- `in_use`: Boolean flag tracking whether this worker slot is currently processing a transaction
- `*shared`: Pointer to the shared memory structure containing coordination data with the parallel worker
## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_handle](../s/shm_mq_handle.md)
  - [dsm_segment](../d/dsm_segment.md)
  - [ParallelApplyWorkerShared](ParallelApplyWorkerShared.md)
- Called from (representative examples):
  - [pa_launch_parallel_worker](../p/pa_launch_parallel_worker.md)
  - [pa_allocate_worker](../p/pa_allocate_worker.md)
  - [pa_free_worker](../p/pa_free_worker.md)
  - [pa_send_data](../p/pa_send_data.md)
  - [HandleParallelApplyMessages](../H/HandleParallelApplyMessages.md)
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md)

## Notes and Other Information
This structure is maintained by the leader apply worker to manage its pool of parallel workers. The dual message queue design enables efficient bidirectional communication while maintaining error isolation. The serialize_changes mechanism provides a fallback when shared memory becomes a bottleneck, allowing large transactions to be processed via file-based serialization. Worker reuse is facilitated through the in_use flag, enabling efficient resource utilization across multiple streaming transactions.