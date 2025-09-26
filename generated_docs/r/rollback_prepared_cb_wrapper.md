# rollback_prepared_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1058-1104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1058-L1104)

## Overview  
A wrapper function that handles the rollback of previously prepared transactions in logical decoding, managing error context and state for two-phase commit abort operations.

## Definition
```c
static void rollback_prepared_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn, XLogRecPtr prepare_end_lsn, TimestampTz prepare_time)
```

## Detailed Description
The `rollback_prepared_cb_wrapper` function manages the rollback phase of two-phase commit transactions in PostgreSQL's logical decoding system. This function is called when a previously prepared transaction is being aborted, completing the two-phase commit process with a rollback decision. It serves as a wrapper around the rollback_prepared callback, providing consistent error handling and context management.

The function ensures that two-phase commit support is enabled and validates that the required rollback_prepared callback is registered. It establishes proper error context, configures the logical decoding context for rollback output generation, and delegates to the output plugin's rollback_prepared callback. This represents the abort of a distributed transaction that was previously prepared but ultimately rejected.

## Parameters / Member Variables
- `cache`: ReorderBuffer instance managing transaction reordering and callbacks
- `txn`: ReorderBufferTXN representing the previously prepared transaction being rolled back
- `prepare_end_lsn`: XLogRecPtr indicating the WAL location where the prepare phase ended
- `prepare_time`: TimestampTz timestamp when the transaction was initially prepared

## Dependencies
- Functions called/Symbols referenced:
  - [output_plugin_error_callback](../o/output_plugin_error_callback.md)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md) (transaction structure)
  - [ReorderBuffer](../R/ReorderBuffer.md) (reorder buffer structure)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (decoding context)
  - [LogicalErrorCallbackState](../L/LogicalErrorCallbackState.md) (error callback state)
- Called from (representative examples):
  - [StartupDecodingContext](../S/StartupDecodingContext.md)

## Notes and Other Information
- This is a static function internal to logical.c, used specifically for rolling back prepared transactions
- Only called when ctx->twophase is true, indicating two-phase commit support is enabled
- Establishes error context stack with callback name "rollback_prepared" for proper error reporting
- Sets context state with accept_writes=true, write_xid, write_location=txn->end_lsn, and end_xact=true
- Validates that rollback_prepared_cb callback is registered, throwing an error if missing for two-phase enabled contexts
- Uses txn->final_lsn as report_location (beginning of rollback record) and txn->end_lsn as write_location (end of record)
- Provides additional context through prepare_end_lsn and prepare_time parameters to help plugins understand the transaction's lifecycle
- Completes the two-phase commit workflow alternatives: prepare -> commit_prepared OR prepare -> rollback_prepared
- Essential for maintaining consistency in distributed transaction scenarios where some participants cannot commit