# ParallelApplyWorkerMain

## Location
[src/backend/replication/logical/applyparallelworker.c:857-988](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L857-L988)

## Overview
ParallelApplyWorkerMain is the entry point function for PostgreSQL logical replication parallel apply worker processes, handling complete worker initialization and launching the main processing loop.

## Definition
```c
void ParallelApplyWorkerMain(Datum main_arg)
```

## Detailed Description
This function serves as the comprehensive initialization and entry point for parallel apply workers in PostgreSQL's logical replication system. It performs extensive setup operations including signal handling, shared memory attachment, message queue setup, worker registration, and replication origin configuration.

The function first establishes signal handlers for configuration reload (SIGHUP), shutdown requests (SIGINT), and termination (SIGTERM). It then attaches to the dynamic shared memory segment created by the leader apply worker, validates the segment's magic number, and retrieves shared information and message queue handles.

After attaching to the logical replication worker slot and registering shutdown callbacks, it establishes bidirectional communication channels - both for receiving work messages and for sending error messages back to the leader. The function sets up replication origin tracking, registers system cache callbacks for monitoring subscription relation state changes, and finally launches the main processing loop via LogicalParallelApplyLoop.

The function includes an assertion at the end that should never be reached, as parallel apply workers only terminate through signals or errors.

## Parameters / Member Variables
- `main_arg`: Datum containing the worker slot number as an integer, used to identify which logical replication worker slot this process should attach to

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [pqsignal](../p/pqsignal.md) (for SIGHUP, SIGINT, SIGTERM)
  - [SignalHandlerForConfigReload](../S/SignalHandlerForConfigReload.md)
  - [SignalHandlerForShutdownRequest](../S/SignalHandlerForShutdownRequest.md)
  - [die](../d/die.md)
  - [BackgroundWorkerUnblockSignals](../B/BackgroundWorkerUnblockSignals.md)
  - [dsm_attach](../d/dsm_attach.md)
  - [shm_toc_attach](../s/shm_toc_attach.md)
  - [shm_toc_lookup](../s/shm_toc_lookup.md)
  - [shm_mq_set_receiver](../s/shm_mq_set_receiver.md)
  - [shm_mq_set_sender](../s/shm_mq_set_sender.md)
  - [shm_mq_attach](../s/shm_mq_attach.md)
  - [logicalrep_worker_attach](../l/logicalrep_worker_attach.md)
  - [before_shmem_exit](../b/before_shmem_exit.md)
  - [pa_shutdown](../p/pa_shutdown.md)
  - [pq_redirect_to_shm_mq](../p/pq_redirect_to_shm_mq.md)
  - [pq_set_parallel_leader](../p/pq_set_parallel_leader.md)
  - [InitializeLogRepWorker](../I/InitializeLogRepWorker.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [ReplicationOriginNameForLogicalRep](../R/ReplicationOriginNameForLogicalRep.md)
  - [replorigin_by_name](../r/replorigin_by_name.md)
  - [replorigin_session_setup](../r/replorigin_session_setup.md)
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md)
  - [invalidate_syncing_table_states](../i/invalidate_syncing_table_states.md)
  - [set_apply_error_context_origin](../s/set_apply_error_context_origin.md)
  - [LogicalParallelApplyLoop](../L/LogicalParallelApplyLoop.md)
- Called from (representative examples):
  - PostgreSQL background worker framework (via BackgroundWorkerHandle)

## Notes and Other Information
- This is a public function (non-static), callable from the background worker framework
- The function performs comprehensive initialization before entering the main processing loop
- Establishes both work message reception and error message transmission channels
- Sets up replication origin tracking without monopolizing the origin (shared with leader)
- Includes proper cleanup registration through before_shmem_exit callback
- The final Assert(false) ensures developers are aware if the function unexpectedly returns
- Part of PostgreSQL's parallel logical replication architecture for improved replication performance and scalability