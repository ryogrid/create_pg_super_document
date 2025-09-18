# apply_handle_stream_prepare

## Location
src/backend/replication/logical/worker.c: 1273 - 1409

## Overview
Handles the prepare phase of streamed transactions in PostgreSQL logical replication, managing the preparation of large transactions across different execution modes including parallel processing.

## Definition
static void apply_handle_stream_prepare(StringInfo s)

## Detailed Description
apply_handle_stream_prepare processes STREAM PREPARE messages received from the publisher during logical replication. This function is a critical component of PostgreSQL's streaming transaction support for large transactions that exceed memory limits. It handles transaction preparation across multiple execution modes: leader apply, parallel apply, and serialization scenarios.

The function performs validation checks to ensure protocol correctness (no nested streaming, no tablesync worker usage) and then determines the appropriate action based on the transaction's current processing state. It supports three main execution paths:

1. TRANS_LEADER_APPLY: Replays spooled operations from serialized files and prepares the transaction
2. TRANS_LEADER_SEND_TO_PARALLEL/TRANS_LEADER_PARTIAL_SERIALIZE: Coordinates with parallel workers or falls back to serialization
3. TRANS_PARALLEL_APPLY: Handles preparation within a parallel apply worker context

The function manages complex state transitions, file cleanup, and synchronization between leader and parallel workers, ensuring data consistency across different processing modes.

## Parameters / Member Variables
- : StringInfo containing the serialized STREAM PREPARE message data from the publisher

## Dependencies
- Functions called/Symbols referenced:
  - [am_tablesync_worker](am_tablesync_worker.md)
  - [logicalrep_read_stream_prepare](../l/logicalrep_read_stream_prepare.md)
  - [set_apply_error_context_xact](../s/set_apply_error_context_xact.md)
  - [get_transaction_apply_action](../g/get_transaction_apply_action.md)
  - [apply_spooled_messages](apply_spooled_messages.md)
  - [apply_handle_prepare_internal](apply_handle_prepare_internal.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [store_flush_position](../s/store_flush_position.md)
  - [stream_cleanup_files](../s/stream_cleanup_files.md)
  - pa_send_data
  - [pa_xact_finish](../p/pa_xact_finish.md)
  - [pa_switch_to_partial_serialize](../p/pa_switch_to_partial_serialize.md)
  - [stream_open_and_write_change](../s/stream_open_and_write_change.md)
  - [pa_set_fileset_state](../p/pa_set_fileset_state.md)
  - [stream_close_file](../s/stream_close_file.md)
  - [begin_replication_step](../b/begin_replication_step.md)
  - [end_replication_step](../e/end_replication_step.md)
  - [pa_set_xact_state](../p/pa_set_xact_state.md)
  - [pa_unlock_transaction](../p/pa_unlock_transaction.md)
  - [pa_reset_subtrans](../p/pa_reset_subtrans.md)
  - [pgstat_report_stat](../p/pgstat_report_stat.md)
  - [process_syncing_tables](../p/process_syncing_tables.md)
  - [stop_skipping_changes](../s/stop_skipping_changes.md)
  - [clear_subscription_skip_lsn](../c/clear_subscription_skip_lsn.md)
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
  - [reset_apply_error_context_info](../r/reset_apply_error_context_info.md)
- Called from:
  - [apply_dispatch](apply_dispatch.md)

## Notes and Other Information
- This function is central to PostgreSQL's large transaction streaming support in logical replication
- Implements sophisticated parallel processing coordination with fallback mechanisms
- Validates protocol state to prevent invalid message sequences (no nested streaming, tablesync restrictions)
- Manages file-based transaction spooling and cleanup for memory-constrained scenarios  
- Handles three distinct transaction processing modes with different synchronization requirements
- Includes comprehensive state management for parallel worker coordination
- The original message is preserved for potential serialization needs
- Implements proper locking and state transitions for parallel transaction completion
- Error handling includes context management and activity reporting for monitoring