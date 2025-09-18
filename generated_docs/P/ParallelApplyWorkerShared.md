# ParallelApplyWorkerShared

## Location
[src/include/replication/worker_internal.h:138-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/worker_internal.h#L138-L183)

## Overview
ParallelApplyWorkerShared is a shared memory structure that facilitates communication and coordination between the leader apply worker and parallel apply workers in PostgreSQL's logical replication system.

## Definition


## Detailed Description
ParallelApplyWorkerShared serves as the primary communication mechanism between leader and parallel apply workers in PostgreSQL's parallel logical replication system. It maintains transaction state information, coordinates commit ordering through the xact_state field, and manages file-based serialization of transaction data when memory limits are exceeded. The structure ensures proper synchronization and ordering of parallel transaction processing while allowing for efficient data sharing between worker processes.

## Parameters / Member Variables
- : Spinlock for protecting concurrent access to shared data
- : Transaction ID of the current transaction being processed
- : State flag ensuring proper commit ordering between parallel workers
- : Generation counter from the associated LogicalRepWorker slot
- : Slot number of the corresponding LogicalRepWorker
- : Atomic counter tracking pending streaming blocks in the queue
- : LSN position of the last committed transaction end
- : State information for partial file serialization mode
- : File set used for serializing transaction data when in partial serialize mode

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