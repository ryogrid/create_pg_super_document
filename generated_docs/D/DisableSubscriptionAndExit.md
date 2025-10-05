# DisableSubscriptionAndExit

## Location
[src/backend/replication/logical/worker.c:4765-4811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4765-L4811)

## Overview
DisableSubscriptionAndExit is a critical error recovery function that handles subscription failures by disabling the subscription and cleanly terminating the logical replication worker process.

## Definition

```c
void
DisableSubscriptionAndExit(void)
```
## Detailed Description
This function is called when a logical replication worker encounters an unrecoverable error during either table synchronization or apply operations. It performs a controlled shutdown sequence that includes error reporting, transaction cleanup, subscription disabling, and process termination. The function ensures that the subscription is properly marked as disabled in the system catalogs before the worker exits, preventing further replication attempts until manual intervention.

The function operates in several phases:
1. Error recovery and cleanup of the current transaction state
2. Statistical reporting of the subscription error
3. Starting a new transaction to disable the subscription
4. Cleaning up worker tracking information
5. Logging the disability and exiting the process

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - HOLD_INTERRUPTS (interrupt handling)
  - [EmitErrorReport](../E/EmitErrorReport.md) (error reporting)
  - [AbortOutOfAnyTransaction](../A/AbortOutOfAnyTransaction.md) (transaction cleanup)
  - [FlushErrorState](../F/FlushErrorState.md) (error state cleanup)
  - RESUME_INTERRUPTS (interrupt handling)
  - [pgstat_report_subscription_error](../p/pgstat_report_subscription_error.md) (statistics reporting)
  - [am_tablesync_worker](../a/am_tablesync_worker.md) (worker type checking)
  - [StartTransactionCommand](../S/StartTransactionCommand.md) (transaction management)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md) (snapshot management)
  - [PushActiveSnapshot](../P/PushActiveSnapshot.md) (snapshot management)
  - [DisableSubscription](DisableSubscription.md) (subscription management)
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md) (snapshot management)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md) (transaction management)
  - [am_leader_apply_worker](../a/am_leader_apply_worker.md) (worker type checking)
  - [ApplyLauncherForgetWorkerStartTime](../A/ApplyLauncherForgetWorkerStartTime.md) (worker tracking cleanup)
  - [proc_exit](../p/proc_exit.md) (process termination)

- Called from:
  - [start_table_sync](../s/start_table_sync.md) (in tablesync.c:1683)
  - [start_apply](../s/start_apply.md) (in worker.c:4455)

## Notes and Other Information
- This function never returns as it calls proc_exit(0)
- The function uses interrupt handling (HOLD/RESUME_INTERRUPTS) to ensure atomic error recovery
- Requires access to global variables MyLogicalRepWorker and MySubscription
- The function ensures TOAST table access is possible by using transaction snapshots
- Statistical reporting differentiates between table sync and apply worker failures
- Only leader apply workers have their start time tracking cleaned up
- The subscription disable operation is performed in a separate transaction to ensure consistency

## Simplified Source

```c
void
DisableSubscriptionAndExit(void)
{
    // Step 1: Error recovery and cleanup
    HOLD_INTERRUPTS();
    EmitErrorReport();
    AbortOutOfAnyTransaction();
    FlushErrorState();
    RESUME_INTERRUPTS();

    // Step 2: Report subscription error statistics
    pgstat_report_subscription_error(MyLogicalRepWorker->subid,
                                     !am_tablesync_worker());

    // Step 3: Disable subscription in new transaction
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    DisableSubscription(MySubscription->oid);
    PopActiveSnapshot();
    CommitTransactionCommand();

    // Step 4: Clean up worker tracking if leader
    if (am_leader_apply_worker())
        ApplyLauncherForgetWorkerStartTime(MyLogicalRepWorker->subid);

    // Step 5: Log and exit
    ereport(LOG,
            errmsg("subscription \"%s\" has been disabled because of an error",
                   MySubscription->name));
    proc_exit(0);
}
```