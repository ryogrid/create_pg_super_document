# pa_allocate_worker

## Location
src/backend/replication/logical/applyparallelworker.c: 470 - 517

## Overview
Allocates a parallel apply worker for a specific transaction ID and tracks it in a hash table for later use.

## Definition


## Detailed Description
This function serves as the main entry point for allocating parallel apply workers to specific transactions in PostgreSQL logical replication. It performs safety checks, attempts to launch a worker, initializes the parallel apply worker hash table if needed, and establishes the mapping between transaction ID and worker. The function also updates shared memory state to communicate the transaction assignment to the worker process.

## Parameters / Member Variables
- : The transaction ID that the parallel worker will process

## Dependencies
- Functions called/Symbols referenced:
  - pa_can_start
  - pa_launch_parallel_worker
  - hash_create
  - hash_search
  - MemSet
  - SpinLockAcquire
  - SpinLockRelease
  - elog
- Called from:
  - apply_handle_stream_start

## Notes and Other Information
- Performs pa_can_start() check before attempting worker allocation to ensure conditions are appropriate
- Initializes ParallelApplyTxnHash hash table on first use with 16 initial buckets
- Uses ApplyContext memory context for hash table allocation 
- Creates ParallelApplyWorkerEntry mapping transaction ID to worker info
- Updates shared memory state with transaction ID and sets state to PARALLEL_TRANS_UNKNOWN
- Marks worker as in_use and sets serialize_changes to false
- Will return early (no allocation) if pa_can_start() returns false or worker launch fails
- Throws ERROR if hash table entry already exists for the given transaction ID (indicates corruption)
- Part of PostgreSQL's logical replication parallel processing system located in src/backend/replication/logical/applyparallelworker.c:470-517