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
  - am_tablesync_worker
  - logicalrep_read_stream_prepare
  - set_apply_error_context_xact
  - get_transaction_apply_action
  - apply_spooled_messages
  - apply_handle_prepare_internal
  - CommitTransactionCommand
  - store_flush_position
  - stream_cleanup_files
  - pa_send_data
  - pa_xact_finish
  - pa_switch_to_partial_serialize
  - stream_open_and_write_change
  - pa_set_fileset_state
  - stream_close_file
  - begin_replication_step
  - end_replication_step
  - pa_set_xact_state
  - pa_unlock_transaction
  - pa_reset_subtrans
  - pgstat_report_stat
  - process_syncing_tables
  - stop_skipping_changes
  - clear_subscription_skip_lsn
  - pgstat_report_activity
  - reset_apply_error_context_info
- Called from:
  - apply_dispatch

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