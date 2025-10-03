# ForgetUnstartedBackgroundWorkers

## Location
[src/backend/postmaster/bgworker.c:547-584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L547-L584)

## Overview
Cancels all not-yet-started background worker requests that have waiting processes during database shutdown, notifying the waiting processes and cleaning up the registrations.

## Definition

```c
void
ForgetUnstartedBackgroundWorkers(void)
```
## Detailed Description
This function is called during normal ("smart" or "fast") database shutdown to handle background workers that were registered but never started. It iterates through all registered background workers and identifies those that haven't been started yet (slot->pid == InvalidPid) and have processes waiting for them (bgw_notify_pid != 0). For each such worker, it completely removes the registration and sends a SIGUSR1 signal to notify the waiting process. This prevents processes from waiting indefinitely for background workers that will never start due to shutdown. The approach of canceling registrations entirely is considered acceptable during shutdown since the server is terminating anyway.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - slist_foreach_modify (macro for safely iterating and modifying singly-linked lists)
  - slist_container (macro to get container structure from list node)
  - [ForgetBackgroundWorker](ForgetBackgroundWorker.md) (removes worker registration)
  - kill (system call for sending signals)
  - SIGUSR1 (signal constant)
  - InvalidPid (constant representing invalid process ID)
- Data structures used:
  - [slist_mutable_iter](../s/slist_mutable_iter.md)
  - [RegisteredBgWorker](../R/RegisteredBgWorker.md)
  - [BackgroundWorkerSlot](../B/BackgroundWorkerSlot.md)
  - BackgroundWorkerList (global list of registered background workers)
  - BackgroundWorkerData (global shared memory structure)
- Called from (representative examples):
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md)

## Notes and Other Information
- This function should only be called from the postmaster process
- Called specifically during "smart" or "fast" database shutdown scenarios
- Uses slist_foreach_modify to safely iterate and remove items from the list
- Includes assertion to ensure shared memory slot index is within bounds
- Prevents indefinite waiting for background workers that cannot start due to shutdown
- Part of PostgreSQL's graceful shutdown process for background worker management
- The complete cancellation of registrations is intentionally "overkill" but acceptable during shutdown

## Simplified Source

```c
// Simplified version of ForgetUnstartedBackgroundWorkers
void ForgetUnstartedBackgroundWorkers(void) {
    slist_mutable_iter iter;

    // Iterate through all registered background workers
    slist_foreach_modify(iter, &BackgroundWorkerList) {
        RegisteredBgWorker *rw;
        BackgroundWorkerSlot *slot;

        rw = slist_container(RegisteredBgWorker, rw_lnode, iter.cur);
        Assert(rw->rw_shmem_slot < max_worker_processes);
        slot = &BackgroundWorkerData->slot[rw->rw_shmem_slot];

        // Check if worker is not yet started and has someone waiting
        if (slot->pid == InvalidPid && rw->rw_worker.bgw_notify_pid != 0) {
            // Save notification PID before removing worker
            int notify_pid = rw->rw_worker.bgw_notify_pid;

            // Remove the worker registration entirely
            ForgetBackgroundWorker(&iter);

            // Notify the waiting process that worker won't start
            if (notify_pid != 0) {
                kill(notify_pid, SIGUSR1);
            }
        }
    }
}
```

Key simplifications made:
- Added descriptive comments for each major operation
- Clarified the conditions for worker removal
- Explained the purpose of saving the notification PID
- Simplified the logic for worker removal and notification
- Maintained the essential shutdown cleanup functionality
- Preserved the safety assertion and notification mechanism