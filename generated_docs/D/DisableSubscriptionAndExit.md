# DisableSubscriptionAndExit

## Location
src/backend/replication/logical/worker.c: 4765 - 4811

## Overview
DisableSubscriptionAndExit is a critical error recovery function that handles subscription failures by disabling the subscription and cleanly terminating the logical replication worker process.

## Definition


## Detailed Description
This function is called when a logical replication worker encounters an unrecoverable error during either table synchronization or apply operations. It performs a controlled shutdown sequence that includes error reporting, transaction cleanup, subscription disabling, and process termination. The function ensures that the subscription is properly marked as disabled in the system catalogs before the worker exits, preventing further replication attempts until manual intervention.

The function operates in several phases:
1. Error recovery and cleanup of the current transaction state
2. Statistical reporting of the subscription error
3. Starting a new transaction to disable the subscription
4. Cleaning up worker tracking information
5. Logging the disability and exiting the process

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - HOLD_INTERRUPTS (interrupt handling)
  - EmitErrorReport (error reporting)
  - AbortOutOfAnyTransaction (transaction cleanup)
  - FlushErrorState (error state cleanup)
  - RESUME_INTERRUPTS (interrupt handling)
  - pgstat_report_subscription_error (statistics reporting)
  - am_tablesync_worker (worker type checking)
  - StartTransactionCommand (transaction management)
  - GetTransactionSnapshot (snapshot management)
  - PushActiveSnapshot (snapshot management)
  - DisableSubscription (subscription management)
  - PopActiveSnapshot (snapshot management)
  - CommitTransactionCommand (transaction management)
  - am_leader_apply_worker (worker type checking)
  - ApplyLauncherForgetWorkerStartTime (worker tracking cleanup)
  - proc_exit (process termination)

- Called from:
  - start_table_sync (in tablesync.c:1683)
  - start_apply (in worker.c:4455)

## Notes and Other Information
- This function never returns as it calls proc_exit(0)
- The function uses interrupt handling (HOLD/RESUME_INTERRUPTS) to ensure atomic error recovery
- Requires access to global variables MyLogicalRepWorker and MySubscription
- The function ensures TOAST table access is possible by using transaction snapshots
- Statistical reporting differentiates between table sync and apply worker failures
- Only leader apply workers have their start time tracking cleaned up
- The subscription disable operation is performed in a separate transaction to ensure consistency