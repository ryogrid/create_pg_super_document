# pa_free_worker

## Location
[src/backend/replication/logical/applyparallelworker.c:556-594](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L556-L594)

## Overview
Makes a parallel apply worker available for reuse by removing it from the transaction hash table and either stopping it or marking it as available based on pool size and serialization state.

## Definition

```c
static void
pa_free_worker(ParallelApplyWorkerInfo *winfo)
```
## Detailed Description
This function is responsible for cleaning up and managing parallel apply workers after they complete their transaction work. It removes the worker's entry from the ParallelApplyTxnHash table to prevent further use with the current transaction. The function implements a worker pool management strategy where it either:

1. Stops and frees the worker if there are enough workers in the pool (more than half of max_parallel_apply_workers_per_subscription) or if the worker has serialized changes due to send timeouts
2. Simply marks the worker as available for reuse if the pool needs more workers

The serialization check is particularly important because when a leader apply worker serializes transaction data due to send timeouts, the message queue may be in an inconsistent state that's difficult to clean up, so it's safer to stop the worker entirely.

## Parameters / Member Variables
- `*winfo`: Pointer to ParallelApplyWorkerInfo structure representing the worker to be freed. Must be currently in use and have finished its transaction.
## Dependencies
- Functions called/Symbols referenced:
  - [am_parallel_apply_worker](../a/am_parallel_apply_worker.md) (assertion check)
  - [pa_get_xact_state](pa_get_xact_state.md) (verify transaction state)
  - [hash_search](../h/hash_search.md) (remove from ParallelApplyTxnHash)
  - [logicalrep_pa_worker_stop](../l/logicalrep_pa_worker_stop.md) (stop the worker process)
  - [pa_free_worker_info](pa_free_worker_info.md) (free worker info structure)
- Called from (representative examples):
  - [pa_xact_finish](pa_xact_finish.md)

## Notes and Other Information
- Function contains several assertions to ensure it's called in the correct context (not from a parallel worker, worker is in use, transaction is finished)
- Implements a worker pool management strategy to balance resource usage
- Special handling for workers that have serialized changes due to message queue issues
- Part of the parallel apply worker infrastructure for logical replication