# apply_handle_prepare

## Location
[src/backend/replication/logical/worker.c:1110-1170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L1110-L1170)

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
  - [LogicalRepPreparedTxnData](../L/LogicalRepPreparedTxnData.md) (struct for storing prepare message data)
  - logicalrep_read_prepare (deserializes PREPARE message from stream)
  - [begin_replication_step](../b/begin_replication_step.md) (starts replication step tracking)
  - [apply_handle_prepare_internal](apply_handle_prepare_internal.md) (performs the actual prepare operation)
  - [end_replication_step](../e/end_replication_step.md) (ends replication step tracking)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md) (commits the prepare transaction command)
  - [pgstat_report_stat](../p/pgstat_report_stat.md) (reports statistics)
  - [store_flush_position](../s/store_flush_position.md) (stores LSN flush position)
  - [process_syncing_tables](../p/process_syncing_tables.md) (processes tables being synchronized in parallel)
  - [stop_skipping_changes](../s/stop_skipping_changes.md) (stops LSN-based change skipping)
  - [clear_subscription_skip_lsn](../c/clear_subscription_skip_lsn.md) (clears subscription skip LSN)
  - [pgstat_report_activity](../p/pgstat_report_activity.md) (reports worker activity status)
  - STATE_IDLE (activity state constant)
  - [reset_apply_error_context_info](../r/reset_apply_error_context_info.md) (cleans up error context)
- Called from:
  - [apply_dispatch](apply_dispatch.md) (main message dispatcher for logical replication)

## Notes and Other Information
- Validates that prepare_data.prepare_lsn matches remote_final_lsn to detect protocol violations
- Always prepares transactions even if no changes occurred, unlike regular commit handling
- Contains detailed comments explaining the rationale for always preparing transactions
- Sets in_remote_transaction to false after successful prepare
- Includes optimization notes about potential future improvements for empty transaction handling
- Handles subscription skip LSN clearing and change skipping termination
- Part of PostgreSQL's two-phase commit support in logical replication
- Contains crash recovery considerations for subskiplsn handling