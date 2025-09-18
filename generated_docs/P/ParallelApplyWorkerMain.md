# ParallelApplyWorkerMain

## Location
src/backend/replication/logical/applyparallelworker.c: 857 - 988

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
  - DatumGetInt32
  - pqsignal (for SIGHUP, SIGINT, SIGTERM)
  - SignalHandlerForConfigReload
  - SignalHandlerForShutdownRequest
  - die
  - BackgroundWorkerUnblockSignals
  - dsm_attach
  - shm_toc_attach
  - shm_toc_lookup
  - shm_mq_set_receiver
  - shm_mq_set_sender
  - shm_mq_attach
  - logicalrep_worker_attach
  - before_shmem_exit
  - pa_shutdown
  - pq_redirect_to_shm_mq
  - pq_set_parallel_leader
  - InitializeLogRepWorker
  - StartTransactionCommand
  - CommitTransactionCommand
  - ReplicationOriginNameForLogicalRep
  - replorigin_by_name
  - replorigin_session_setup
  - CacheRegisterSyscacheCallback
  - invalidate_syncing_table_states
  - set_apply_error_context_origin
  - LogicalParallelApplyLoop
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