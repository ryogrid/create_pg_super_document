# pa_stream_abort

## Location
[src/backend/replication/logical/applyparallelworker.c:1416-1497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L1416-L1497)

## Overview
pa_stream_abort handles STREAM ABORT messages for transactions that were applied in parallel apply workers during PostgreSQL logical replication, managing both toplevel transaction aborts and subtransaction rollbacks.

## Definition

```c
void
pa_stream_abort(LogicalRepStreamAbortData *abort_data)
```
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
  - [list_nth_cell](../l/list_nth_cell.md)
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

## Simplified Source

```c
void pa_stream_abort(LogicalRepStreamAbortData *abort_data) {
    TransactionId xid = abort_data->xid;
    TransactionId subxid = abort_data->subxid;

    // Update replication origin for crash recovery
    replorigin_session_origin_lsn = abort_data->abort_lsn;
    replorigin_session_origin_timestamp = abort_data->abort_time;

    if (subxid == xid) {
        // Toplevel transaction abort - complete cleanup
        pa_set_xact_state(MyParallelShared, PARALLEL_TRANS_FINISHED);

        // Release lock before aborting to prevent deadlocks
        pa_unlock_transaction(xid, AccessExclusiveLock);

        AbortCurrentTransaction();

        if (IsTransactionBlock()) {
            EndTransactionBlock(false);
            CommitTransactionCommand();
        }

        pa_reset_subtrans();
        pgstat_report_activity(STATE_IDLE, NULL);
    } else {
        // Subtransaction abort - rollback to savepoint
        char spname[NAMEDATALEN];
        pa_savepoint_name(MySubscription->oid, subxid, spname, sizeof(spname));

        elog(DEBUG1, "rolling back to savepoint %s in logical replication parallel apply worker", spname);

        // Find and rollback to the appropriate savepoint
        for (int i = list_length(subxactlist) - 1; i >= 0; i--) {
            TransactionId xid_tmp = lfirst_xid(list_nth_cell(subxactlist, i));
            if (xid_tmp == subxid) {
                RollbackToSavepoint(spname);
                CommitTransactionCommand();
                subxactlist = list_truncate(subxactlist, i);
                break;
            }
        }
    }
}
```