# start_apply

## Location
src/backend/replication/logical/worker.c: 4438 - 4477

## Overview
A core function that runs the logical replication apply loop with comprehensive error handling and subscription management capabilities.

## Definition
```c
void start_apply(XLogRecPtr origin_startpos)
```

## Detailed Description
start_apply serves as the main entry point for executing the logical replication apply process with robust error handling. The function wraps the core LogicalRepApplyLoop in a PostgreSQL exception handling block (PG_TRY/PG_CATCH) to manage errors that occur during replication processing.

When an error occurs during the apply process, the function takes several critical actions:
1. Resets the replication origin state to prevent incorrect progress advancement
2. Checks the subscription's error handling policy (disableonerr setting)
3. Either disables the subscription and exits, or reports the error and re-throws the exception

The function is designed to handle recoverable errors gracefully while ensuring data consistency. It prevents transaction loss by resetting the origin state when failures occur, ensuring that failed transactions will be retried from the server. For subscriptions configured with disable-on-error, it provides a safety mechanism to prevent continuous failures.

## Parameters / Member Variables
- `origin_startpos`: XLogRecPtr indicating the starting position in the WAL from which to begin applying changes

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md) (main replication apply loop)
  - PG_TRY/PG_CATCH/PG_END_TRY (PostgreSQL exception handling macros)
  - [replorigin_reset](../r/replorigin_reset.md) (resets replication origin progress)
  - [DisableSubscriptionAndExit](../D/DisableSubscriptionAndExit.md) (disables subscription and terminates worker)
  - [AbortOutOfAnyTransaction](../A/AbortOutOfAnyTransaction.md) (aborts current transaction state)
  - [pgstat_report_subscription_error](../p/pgstat_report_subscription_error.md) (reports subscription error statistics)
  - [am_tablesync_worker](../a/am_tablesync_worker.md) (checks if this is a table synchronization worker)
  - PG_RE_THROW (re-throws caught exception)
- Called from (representative examples):
  - [run_tablesync_worker](../r/run_tablesync_worker.md) (at src/backend/replication/logical/tablesync.c:1732)
  - [run_apply_worker](../r/run_apply_worker.md) (at src/backend/replication/logical/worker.c:4579)

## Notes and Other Information
- Does not handle FATAL errors, which are typically system resource issues and non-repeatable
- Implements two error handling strategies based on subscription configuration: disable-on-error or error reporting with re-throw
- Critical for maintaining data consistency by preventing origin progress advancement on failures
- Origin state reset ensures that failed transactions will be retried from the publisher
- Reports error statistics to help with monitoring and debugging subscription issues
- Essential component of PostgreSQL's logical replication worker architecture