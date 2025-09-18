# pa_stream_abort

## Location
src/backend/replication/logical/applyparallelworker.c: 1416 - 1497

## Overview
pa_stream_abort handles STREAM ABORT messages for transactions that were applied in parallel apply workers during PostgreSQL logical replication, managing both toplevel transaction aborts and subtransaction rollbacks.

## Definition


## Detailed Description
This function processes stream abort operations in PostgreSQL's logical replication parallel worker environment. It handles two distinct scenarios based on whether the abort involves a toplevel transaction or a subtransaction:

1. **Toplevel Transaction Abort**: When the main transaction ID (xid) matches the subtransaction ID (subxid), it performs complete transaction cleanup including setting transaction state to finished, releasing locks, aborting the current transaction, ending transaction blocks, and resetting subtransaction state.

2. **Subtransaction Rollback**: When dealing with a subtransaction abort, it searches the subtransaction list to find the appropriate savepoint, rolls back to that savepoint, and truncates the subtransaction list accordingly.

The function also updates replication origin state to ensure proper crash recovery by setting the abort LSN and timestamp from the abort_data.

## Parameters / Member Variables
- : Pointer to LogicalRepStreamAbortData structure containing:
  - : Main transaction ID
  - : Subtransaction ID 
  - : Log sequence number for abort position
  - : Timestamp of the abort operation

## Dependencies
- Functions called/Symbols referenced:
  - [pa_set_xact_state](pa_set_xact_state.md)
  - [pa_unlock_transaction](pa_unlock_transaction.md)  
  - [AbortCurrentTransaction](../A/AbortCurrentTransaction.md)
  - [IsTransactionBlock](../I/IsTransactionBlock.md)
  - [EndTransactionBlock](../E/EndTransactionBlock.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [pa_reset_subtrans](pa_reset_subtrans.md)
  - [pgstat_report_activity](pgstat_report_activity.md)
  - [pa_savepoint_name](pa_savepoint_name.md)
  - [RollbackToSavepoint](../R/RollbackToSavepoint.md)
  - [list_truncate](../l/list_truncate.md)
  - lfirst_xid
  - list_nth_cell
- Called from (representative examples):
  - [apply_handle_stream_abort](../a/apply_handle_stream_abort.md)

## Notes and Other Information
- Handles both complete transaction aborts and partial subtransaction rollbacks
- Updates replication origin state for crash recovery purposes  
- For toplevel aborts, releases transaction locks before aborting to prevent deadlocks
- For subtransaction aborts, searches backwards through the subtransaction list to find the correct savepoint
- Uses savepoint mechanism for subtransaction rollback operations
- Part of PostgreSQL's logical replication parallel worker infrastructure
- Ensures proper cleanup of transaction state and memory management