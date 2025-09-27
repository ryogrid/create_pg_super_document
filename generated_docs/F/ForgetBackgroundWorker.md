# ForgetBackgroundWorker

## Location
[src/backend/postmaster/bgworker.c:432-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L432-L466)

## Overview
Removes a background worker from the postmaster's private list and marks its shared memory slot as unused, completing the worker cleanup process.

## Definition

```c
void
ForgetBackgroundWorker(slist_mutable_iter *cur)
```
## Detailed Description
This function performs the final cleanup steps when a background worker is no longer needed. It removes the worker from the postmaster's private BackgroundWorkerList and marks the corresponding shared memory slot as available for reuse. The function uses a mutable iterator parameter to enable efficient deletion during list traversal without requiring additional searches. It handles parallel worker accounting by incrementing the parallel_terminate_count for parallel workers, and uses memory barriers to ensure proper ordering of shared memory updates. The caller is responsible for any necessary notification to bgw_notify_pid processes.

## Parameters / Member Variables
- : A mutable iterator pointing to the worker to be removed from the list (allows efficient deletion during traversal)

## Dependencies
- Functions called/Symbols referenced:
  -  (extracts RegisteredBgWorker from list node)
  -  (removes current item from list)
  -  (ensures proper memory ordering)
  -  (logs the unregistration event)
  -  (frees allocated memory)
  -  (debug assertions)
  - Constants: , 
  - Types: , , 
  - Global variables: , 

- Called from (representative examples):
  -  (src/backend/postmaster/bgworker.c:508)
  -  (src/backend/postmaster/bgworker.c:567)
  -  (src/backend/postmaster/bgworker.c:604)
  -  (src/backend/postmaster/postmaster.c:1569)

## Notes and Other Information
- Must only be invoked in the postmaster process
- Uses mutable iterator pattern for efficient list modification during traversal
- Handles parallel worker accounting automatically
- Uses memory barriers to ensure safe shared memory updates
- Caller retains responsibility for notifying interested processes
- Logs worker unregistration at DEBUG1 level
- Performs safety assertions to validate worker state before cleanup
- Critical for preventing memory leaks and maintaining accurate worker accounting
- Part of the worker lifecycle management infrastructure

## Simplified Source

```c
// Simplified version of ForgetBackgroundWorker
void ForgetBackgroundWorker(slist_mutable_iter *cur) {
    RegisteredBgWorker *worker;
    BackgroundWorkerSlot *slot;

    // Step 1: Get the worker from the list iterator
    worker = slist_container(RegisteredBgWorker, rw_lnode, cur->cur);

    // Step 2: Find the corresponding shared memory slot
    slot = &BackgroundWorkerData->slot[worker->rw_shmem_slot];

    // Step 3: Update parallel worker accounting if needed
    if (worker->rw_worker.bgw_flags & BGWORKER_CLASS_PARALLEL) {
        BackgroundWorkerData->parallel_terminate_count++;
    }

    // Step 4: Use memory barrier for safe shared memory update
    pg_memory_barrier();
    slot->in_use = false;

    // Step 5: Log the unregistration
    ereport(DEBUG1, (errmsg_internal("unregistering background worker \"%s\"",
                                     worker->rw_worker.bgw_name)));

    // Step 6: Remove from list and free memory
    slist_delete_current(cur);
    pfree(worker);
}
```

Key simplifications made:
- Removed detailed assertions for clarity (kept essential logic)
- Added step-by-step comments to show the cleanup process
- Simplified variable names for better readability
- Focused on the main execution path
- Preserved critical memory barrier and accounting logic