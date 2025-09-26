# apply_handle_commit_prepared

## Location
[src/backend/replication/logical/worker.c:1171-1219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L1171-L1219)

## Overview
Handles the commit phase of a previously prepared two-phase transaction in PostgreSQL logical replication, finalizing the prepared transaction and updating replication state.

## Definition

```c
static void
apply_handle_commit_prepared(StringInfo s)
```
## Detailed Description
apply_handle_commit_prepared processes a COMMIT PREPARED message received from the publisher during logical replication. This function is part of PostgreSQL's two-phase commit protocol implementation in logical replication workers. It reads the commit prepared transaction data, constructs the global transaction identifier (GID), and calls FinishPreparedTransaction to commit the previously prepared transaction.

The function operates in the context where there is no active transaction (since COMMIT PREPARED is called outside of transaction boundaries), so it manages its own replication step lifecycle. It also handles critical replication state updates including origin LSN positioning for crash recovery, parallel table synchronization processing, and subscription skip LSN management.

Note that if the transaction was prepared in a parallel apply worker, no additional waiting is required here as the wait was already handled in apply_handle_stream_prepare(), ensuring all operations completed on the subscriber.

## Parameters / Member Variables
- : StringInfo containing the serialized COMMIT PREPARED message data from the publisher

## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_read_commit_prepared](../l/logicalrep_read_commit_prepared.md)
  - [set_apply_error_context_xact](../s/set_apply_error_context_xact.md)
  - [TwoPhaseTransactionGid](../T/TwoPhaseTransactionGid.md)
  - [begin_replication_step](../b/begin_replication_step.md)
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md)
  - [end_replication_step](../e/end_replication_step.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [pgstat_report_stat](../p/pgstat_report_stat.md)
  - [store_flush_position](../s/store_flush_position.md)
  - [process_syncing_tables](../p/process_syncing_tables.md)
  - [clear_subscription_skip_lsn](../c/clear_subscription_skip_lsn.md)
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
  - [reset_apply_error_context_info](../r/reset_apply_error_context_info.md)
- Called from:
  - [apply_dispatch](apply_dispatch.md)

## Notes and Other Information
- This function is part of PostgreSQL's logical replication two-phase commit support
- The GID (Global Transaction Identifier) is constructed using the subscription OID and transaction ID
- Origin state is updated to enable proper restart positioning in case of crashes
- Parallel table synchronization is processed after the transaction commit
- The function operates outside of transaction boundaries since COMMIT PREPARED doesn't run within a transaction
- Statistics reporting and activity state management are handled to maintain proper monitoring
- Error context is managed to provide meaningful error messages during replication