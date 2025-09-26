# logicalrep_pa_worker_stop

## Location
[src/backend/replication/logical/launcher.c:646-688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L646-L688)

## Overview
Stops a logical replication parallel apply worker, handling the special cleanup requirements for parallel worker termination.

## Definition

```c
void
logicalrep_pa_worker_stop(ParallelApplyWorkerInfo *winfo)
```
## Detailed Description
This function terminates a parallel apply worker, which requires special handling compared to regular subscription workers. The key differences are:
1. Uses SIGINT instead of SIGTERM for cleaner shutdown of parallel workers
2. Handles shared memory message queue cleanup before stopping the worker
3. Uses generation numbers and slot numbers from the parallel worker info structure
4. Detaches from error message queues to prevent the leader worker from receiving messages from a stopped worker

The function first extracts worker identification information (slot number and generation) from the shared parallel worker info, then performs message queue cleanup, and finally delegates to the internal stop function.

## Parameters / Member Variables
- : Pointer to ParallelApplyWorkerInfo structure containing information about the parallel apply worker to stop

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire/SpinLockRelease: Protects access to shared parallel worker information
  - shm_mq_detach: Detaches from the error message queue handle
  - LWLockAcquire/LWLockRelease: Manages LogicalRepWorkerLock for worker access
  - isParallelApplyWorker: Validates that this is indeed a parallel apply worker
  - logicalrep_worker_stop_internal: Performs the actual worker termination with SIGINT

- Called from:
  - pa_free_worker: Used during parallel worker cleanup and resource deallocation

## Notes and Other Information
- Specifically designed for parallel apply workers, not regular subscription workers
- Uses SIGINT instead of SIGTERM to ensure clean parallel worker shutdown
- Includes generation-based checking to avoid stopping wrong workers in reused slots
- Performs critical message queue cleanup to prevent leader worker communication issues
- Validates slot numbers against max_logical_replication_workers limit
- Thread-safe through proper use of spinlocks and LWLocks