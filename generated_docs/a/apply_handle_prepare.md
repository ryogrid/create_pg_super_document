# apply_handle_prepare

## Location
src/backend/replication/logical/worker.c: 1110 - 1170

## Overview
apply_handle_prepare handles PREPARE messages in PostgreSQL logical replication, processing the prepare phase of two-phase commit transactions on the apply worker side.

## Definition
```c
static void apply_handle_prepare(StringInfo s)
```

## Detailed Description
This function processes logical replication PREPARE messages that signal the prepare phase of a two-phase commit transaction from the publisher. It reads the prepare details from the message stream, validates that the prepare LSN matches the expected final LSN, and always prepares the transaction regardless of whether changes occurred (unlike regular commits). The function wraps the prepare operation in replication steps, handles parallel table synchronization, manages LSN skip logic, and properly cleans up worker state. A key design decision is that transactions are always prepared even if no changes occurred, to simplify the handling of commit prepared messages later.

## Parameters / Member Variables
- `s`: StringInfo containing the serialized PREPARE message data from the logical replication stream

## Dependencies
- Functions called/Symbols referenced:
  - LogicalRepPreparedTxnData (struct for storing prepare message data)
  - logicalrep_read_prepare (deserializes PREPARE message from stream)
  - begin_replication_step (starts replication step tracking)
  - apply_handle_prepare_internal (performs the actual prepare operation)
  - end_replication_step (ends replication step tracking)
  - CommitTransactionCommand (commits the prepare transaction command)
  - pgstat_report_stat (reports statistics)
  - store_flush_position (stores LSN flush position)
  - process_syncing_tables (processes tables being synchronized in parallel)
  - stop_skipping_changes (stops LSN-based change skipping)
  - clear_subscription_skip_lsn (clears subscription skip LSN)
  - pgstat_report_activity (reports worker activity status)
  - STATE_IDLE (activity state constant)
  - reset_apply_error_context_info (cleans up error context)
- Called from:
  - apply_dispatch (main message dispatcher for logical replication)

## Notes and Other Information
- Validates that prepare_data.prepare_lsn matches remote_final_lsn to detect protocol violations
- Always prepares transactions even if no changes occurred, unlike regular commit handling
- Contains detailed comments explaining the rationale for always preparing transactions
- Sets in_remote_transaction to false after successful prepare
- Includes optimization notes about potential future improvements for empty transaction handling
- Handles subscription skip LSN clearing and change skipping termination
- Part of PostgreSQL's two-phase commit support in logical replication
- Contains crash recovery considerations for subskiplsn handling