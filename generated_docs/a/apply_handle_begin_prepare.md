# apply_handle_begin_prepare

## Location
src/backend/replication/logical/worker.c: 1044 - 1072

## Overview
apply_handle_begin_prepare handles BEGIN PREPARE messages in PostgreSQL logical replication, marking the start of a prepared transaction for two-phase commit processing on the apply worker side.

## Definition
```c
static void apply_handle_begin_prepare(StringInfo s)
```

## Detailed Description
This function processes logical replication BEGIN PREPARE messages that signal the start of a prepared transaction from the publisher in a two-phase commit scenario. It reads the prepared transaction details from the message stream, validates that the worker is not a tablesync worker (which should never receive prepare messages), establishes error context for proper error reporting, sets up transaction state tracking, and optionally initiates change skipping based on LSN filtering. The function ensures that no streaming transaction is currently active and prepares the apply worker to process subsequent changes within this prepared transaction context.

## Parameters / Member Variables
- `s`: StringInfo containing the serialized BEGIN PREPARE message data from the logical replication stream

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalRepPreparedTxnData](../L/LogicalRepPreparedTxnData.md) (struct for storing prepared transaction begin data)
  - [am_tablesync_worker](am_tablesync_worker.md) (checks if current worker is a tablesync worker)
  - [logicalrep_read_begin_prepare](../l/logicalrep_read_begin_prepare.md) (deserializes BEGIN PREPARE message from stream)
  - [set_apply_error_context_xact](../s/set_apply_error_context_xact.md) (establishes error context for transaction)
  - [maybe_start_skipping_changes](../m/maybe_start_skipping_changes.md) (initiates LSN-based change filtering if needed)
  - [pgstat_report_activity](../p/pgstat_report_activity.md) (reports worker activity status)
  - STATE_RUNNING (activity state constant)
- Called from:
  - [apply_dispatch](apply_dispatch.md) (main message dispatcher for logical replication)

## Notes and Other Information
- Explicitly prevents tablesync workers from processing prepared transactions with error reporting
- This function asserts that no streaming transaction is currently active (stream_xid must be invalid)
- Sets the global flag in_remote_transaction to true to track transaction state
- Stores the prepare_lsn for the transaction in the global remote_final_lsn variable
- Part of PostgreSQL's two-phase commit support in logical replication
- The function is static and only called internally within the logical replication worker
- Uses prepare_lsn instead of final_lsn for LSN tracking in prepared transaction context