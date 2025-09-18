# apply_handle_commit_prepared

## Location
src/backend/replication/logical/worker.c: 1171 - 1219

## Overview
Handles the commit phase of a previously prepared two-phase transaction in PostgreSQL logical replication, finalizing the prepared transaction and updating replication state.

## Definition


## Detailed Description
apply_handle_commit_prepared processes a COMMIT PREPARED message received from the publisher during logical replication. This function is part of PostgreSQL's two-phase commit protocol implementation in logical replication workers. It reads the commit prepared transaction data, constructs the global transaction identifier (GID), and calls FinishPreparedTransaction to commit the previously prepared transaction.

The function operates in the context where there is no active transaction (since COMMIT PREPARED is called outside of transaction boundaries), so it manages its own replication step lifecycle. It also handles critical replication state updates including origin LSN positioning for crash recovery, parallel table synchronization processing, and subscription skip LSN management.

Note that if the transaction was prepared in a parallel apply worker, no additional waiting is required here as the wait was already handled in apply_handle_stream_prepare(), ensuring all operations completed on the subscriber.

## Parameters / Member Variables
- : StringInfo containing the serialized COMMIT PREPARED message data from the publisher

## Dependencies
- Functions called/Symbols referenced:
  - logicalrep_read_commit_prepared
  - set_apply_error_context_xact
  - TwoPhaseTransactionGid
  - begin_replication_step
  - FinishPreparedTransaction
  - end_replication_step
  - CommitTransactionCommand
  - pgstat_report_stat
  - store_flush_position
  - process_syncing_tables
  - clear_subscription_skip_lsn
  - pgstat_report_activity
  - reset_apply_error_context_info
- Called from:
  - apply_dispatch

## Notes and Other Information
- This function is part of PostgreSQL's logical replication two-phase commit support
- The GID (Global Transaction Identifier) is constructed using the subscription OID and transaction ID
- Origin state is updated to enable proper restart positioning in case of crashes
- Parallel table synchronization is processed after the transaction commit
- The function operates outside of transaction boundaries since COMMIT PREPARED doesn't run within a transaction
- Statistics reporting and activity state management are handled to maintain proper monitoring
- Error context is managed to provide meaningful error messages during replication