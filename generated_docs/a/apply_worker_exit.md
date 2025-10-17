# apply_worker_exit

## Location
[src/backend/replication/logical/worker.c:3844-3874](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L3844-L3874)

## Overview
apply_worker_exit provides a controlled exit mechanism for logical replication apply workers when subscription parameters change, handling different worker types appropriately.

## Definition

```c
static void
apply_worker_exit(void)
```
## Detailed Description
This function manages the termination process for logical replication apply workers in response to subscription parameter changes. It implements different exit strategies based on the worker type: parallel apply workers simply return without terminating to avoid disrupting the leader worker's operation, while leader apply workers perform cleanup by resetting their start time in the launcher's tracking system before terminating. The function ensures proper cleanup and prevents resource leaks while avoiding errors that could inadvertently disable subscriptions.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [am_parallel_apply_worker](am_parallel_apply_worker.md)
  - [am_leader_apply_worker](am_leader_apply_worker.md)
  - [ApplyLauncherForgetWorkerStartTime](../A/ApplyLauncherForgetWorkerStartTime.md)
  - [proc_exit](../p/proc_exit.md)
- Called from (representative examples):
  - [maybe_reread_subscription](../m/maybe_reread_subscription.md) (at lines 3921, 3956, 3974)
  - [InitializeLogRepWorker](../I/InitializeLogRepWorker.md) (at line 4646)

## Notes and Other Information
- This is a static function internal to the worker.c file
- Implements different exit strategies for parallel vs leader apply workers
- Parallel workers don't terminate to prevent communication errors with the leader
- Leader workers clean up their start time tracking to enable immediate restart
- Called in response to subscription parameter changes that require worker restart
- Uses proc_exit(0) to terminate the process cleanly
- Prevents accidental subscription disabling when disable_on_error is configured
- The launcher can restart workers without waiting for wal_retrieve_retry_interval after cleanup

## Simplified Source

```c
static void
apply_worker_exit(void)
{
    // Parallel apply workers don't exit to avoid disrupting the leader
    if (am_parallel_apply_worker()) {
        /*
         * Don't stop parallel workers - let the leader detect parameter
         * changes and restart replication. This prevents communication
         * errors that could accidentally disable subscriptions.
         */
        return;
    }

    // Leader workers clean up their start time tracking before exiting
    if (am_leader_apply_worker()) {
        /*
         * Reset start time so launcher can restart immediately
         * without waiting for retry interval, and avoid hash table leaks
         */
        ApplyLauncherForgetWorkerStartTime(MyLogicalRepWorker->subid);
    }

    // Cleanly terminate the worker process
    proc_exit(0);
}
```