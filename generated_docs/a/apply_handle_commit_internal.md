# apply_handle_commit_internal

## Location
[src/backend/replication/logical/worker.c:2243-2302](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L2243-L2302)

## Overview
apply_handle_commit_internal is a helper function that performs the core commit processing logic shared by both regular commits and streaming transaction commits in PostgreSQL logical replication.

## Definition
```c
static void apply_handle_commit_internal(LogicalRepCommitData *commit_data)
```

## Detailed Description
This function encapsulates the common commit logic used by both apply_handle_commit and apply_handle_stream_commit. It handles the completion of transaction processing in logical replication by managing transaction state, updating replication origin information, and ensuring proper cleanup. The function handles two main scenarios:

1. **Active Transaction State**: When there is an active transaction (either non-empty or skipped), it commits the transaction, updates the replication origin LSN and timestamp, handles transaction blocks properly, reports statistics, and stores the flush position.

2. **No Transaction State**: When no transaction is active, it processes any accumulated invalidation messages and potentially re-reads subscription information.

The function also manages the transition out of remote transaction state and handles subscription skip LSN clearing for resumed replication scenarios.

## Parameters / Member Variables
- `commit_data`: Pointer to LogicalRepCommitData structure containing commit LSN, end LSN, and commit timestamp information

## Dependencies
- Functions called/Symbols referenced:
  - is_skipping_changes/stop_skipping_changes (skip logic management)
  - [IsTransactionState](../I/IsTransactionState.md) (transaction state check)
  - [StartTransactionCommand](../S/StartTransactionCommand.md) (transaction initiation)
  - [clear_subscription_skip_lsn](../c/clear_subscription_skip_lsn.md) (subscription state management)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md) (transaction commit)
  - [IsTransactionBlock](../I/IsTransactionBlock.md)/EndTransactionBlock (transaction block handling)
  - [pgstat_report_stat](../p/pgstat_report_stat.md) (statistics reporting)
  - [store_flush_position](../s/store_flush_position.md) (replication position tracking)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md) (cache invalidation)
  - [maybe_reread_subscription](../m/maybe_reread_subscription.md) (subscription configuration refresh)
- Called from (representative examples):
  - [apply_handle_commit](apply_handle_commit.md) (regular commit processing)
  - [apply_handle_stream_commit](apply_handle_stream_commit.md) (streaming commit processing - called twice for different apply actions)

## Notes and Other Information
- Static helper function designed for code reuse between different commit handlers
- Critical for maintaining replication origin state consistency across crashes/restarts
- Handles both transaction and non-transaction contexts appropriately
- Sets in_remote_transaction = false to indicate completion of remote transaction processing
- Includes proper handling of transaction blocks to ensure complete commit processing
- Updates global replication state including replorigin_session_origin_lsn and replorigin_session_origin_timestamp
- Part of the critical path for maintaining logical replication consistency and durability