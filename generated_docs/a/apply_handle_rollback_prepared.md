# apply_handle_rollback_prepared

## Location
[src/backend/replication/logical/worker.c:1220-1272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L1220-L1272)

## Overview
Handles the rollback phase of a previously prepared two-phase transaction in PostgreSQL logical replication, aborting the prepared transaction and updating replication state.

## Definition
static void apply_handle_rollback_prepared(StringInfo s)

## Detailed Description
apply_handle_rollback_prepared processes a ROLLBACK PREPARED message received from the publisher during logical replication. This function is part of PostgreSQL's two-phase commit protocol implementation in logical replication workers. It reads the rollback prepared transaction data, constructs the global transaction identifier (GID), and conditionally calls FinishPreparedTransaction to abort the previously prepared transaction.

A key feature of this function is its conditional execution logic - it first checks if the prepared transaction exists using LookupGXact before attempting to rollback. This handles cases where the PREPARE may not have been received (e.g., if it occurred before the walsender reached a consistent point or two-phase was not yet enabled), in which case the rollback is safely skipped.

Like apply_handle_commit_prepared, this function operates outside of transaction boundaries and manages its own replication step lifecycle. It also handles replication state updates, parallel table synchronization processing, and subscription management.

## Parameters / Member Variables
- : StringInfo containing the serialized ROLLBACK PREPARED message data from the publisher

## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_read_rollback_prepared](../l/logicalrep_read_rollback_prepared.md)
  - [set_apply_error_context_xact](../s/set_apply_error_context_xact.md)
  - [TwoPhaseTransactionGid](../T/TwoPhaseTransactionGid.md)
  - [LookupGXact](../L/LookupGXact.md)
  - [begin_replication_step](../b/begin_replication_step.md)
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md)
  - [end_replication_step](../e/end_replication_step.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [clear_subscription_skip_lsn](../c/clear_subscription_skip_lsn.md)
  - [pgstat_report_stat](../p/pgstat_report_stat.md)
  - [store_flush_position](../s/store_flush_position.md)
  - [process_syncing_tables](../p/process_syncing_tables.md)
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
  - [reset_apply_error_context_info](../r/reset_apply_error_context_info.md)
- Called from:
  - [apply_dispatch](apply_dispatch.md)

## Notes and Other Information
- This function includes conditional rollback logic - it only performs the rollback if the prepared transaction actually exists
- The LookupGXact check prevents errors when trying to rollback non-existent prepared transactions
- Handles scenarios where PREPARE messages may have been missed due to timing or configuration issues
- Like commit prepared, it operates outside transaction boundaries since ROLLBACK PREPARED doesn't run within a transaction
- Origin state updates and parallel table synchronization are handled similarly to the commit case
- The function uses FinishPreparedTransaction with false parameter to indicate rollback rather than commit
- Error context and activity reporting are managed to maintain proper monitoring during replication