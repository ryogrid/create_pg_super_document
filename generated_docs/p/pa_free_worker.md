# pa_free_worker

## Location
src/backend/replication/logical/applyparallelworker.c: 556 - 594

## Overview
Makes a parallel apply worker available for reuse by removing it from the transaction hash table and either stopping it or marking it as available based on pool size and serialization state.

## Definition


## Detailed Description
This function is responsible for cleaning up and managing parallel apply workers after they complete their transaction work. It removes the worker's entry from the ParallelApplyTxnHash table to prevent further use with the current transaction. The function implements a worker pool management strategy where it either:

1. Stops and frees the worker if there are enough workers in the pool (more than half of max_parallel_apply_workers_per_subscription) or if the worker has serialized changes due to send timeouts
2. Simply marks the worker as available for reuse if the pool needs more workers

The serialization check is particularly important because when a leader apply worker serializes transaction data due to send timeouts, the message queue may be in an inconsistent state that's difficult to clean up, so it's safer to stop the worker entirely.

## Parameters / Member Variables
- : Pointer to ParallelApplyWorkerInfo structure representing the worker to be freed. Must be currently in use and have finished its transaction.

## Dependencies
- Functions called/Symbols referenced:
  - am_parallel_apply_worker (assertion check)
  - pa_get_xact_state (verify transaction state)
  - hash_search (remove from ParallelApplyTxnHash)
  - logicalrep_pa_worker_stop (stop the worker process)
  - pa_free_worker_info (free worker info structure)
- Called from (representative examples):
  - pa_xact_finish

## Notes and Other Information
- Function contains several assertions to ensure it's called in the correct context (not from a parallel worker, worker is in use, transaction is finished)
- Implements a worker pool management strategy to balance resource usage
- Special handling for workers that have serialized changes due to message queue issues
- Part of the parallel apply worker infrastructure for logical replication