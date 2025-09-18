# ParallelApplyWorkerInfo

## Location
[src/include/replication/worker_internal.h:188-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/worker_internal.h#L188-L218)

## Overview
ParallelApplyWorkerInfo is a management structure that coordinates communication and resource allocation between leader apply workers and parallel apply workers in PostgreSQL's logical replication system.

## Definition


## Detailed Description
ParallelApplyWorkerInfo serves as the primary management interface for parallel apply workers from the leader worker's perspective. It encapsulates all the communication channels, memory management, and state tracking needed to coordinate with a parallel worker process. The structure manages shared memory queues for bidirectional communication, tracks worker availability, and handles fallback mechanisms when shared memory communication becomes inefficient due to timeouts or capacity issues.

## Parameters / Member Variables
- : Handle to the shared memory queue for sending transaction changes from leader to parallel worker
- : Handle to the shared memory queue for receiving error messages from parallel worker to leader
- : Dynamic shared memory segment associated with this parallel worker
- : Flag indicating whether to fall back to file-based serialization due to shared memory timeouts
- : Boolean flag tracking whether this worker slot is currently processing a transaction
- : Pointer to the shared memory structure containing coordination data with the parallel worker

## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_handle](../s/shm_mq_handle.md)
  - dsm_segment
  - [ParallelApplyWorkerShared](ParallelApplyWorkerShared.md)
- Called from (representative examples):
  - [pa_launch_parallel_worker](../p/pa_launch_parallel_worker.md)
  - [pa_allocate_worker](../p/pa_allocate_worker.md)
  - [pa_free_worker](../p/pa_free_worker.md)
  - pa_send_data
  - [HandleParallelApplyMessages](../H/HandleParallelApplyMessages.md)
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md)

## Notes and Other Information
This structure is maintained by the leader apply worker to manage its pool of parallel workers. The dual message queue design enables efficient bidirectional communication while maintaining error isolation. The serialize_changes mechanism provides a fallback when shared memory becomes a bottleneck, allowing large transactions to be processed via file-based serialization. Worker reuse is facilitated through the in_use flag, enabling efficient resource utilization across multiple streaming transactions.