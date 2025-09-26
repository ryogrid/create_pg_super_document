# prepare_cb_wrapper

## Location
src/backend/replication/logical/logical.c: 968 - 1012

## Overview
A wrapper function that manages the prepare phase of two-phase commit transactions in logical decoding, providing error handling and state setup before calling the prepare callback.

## Definition
```c
static void prepare_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn, XLogRecPtr prepare_lsn)
```

## Detailed Description
The `prepare_cb_wrapper` function handles the prepare phase of two-phase commit transactions in PostgreSQL's logical decoding system. This function is called when a transaction reaches the PREPARE stage of a two-phase commit, indicating that the transaction is ready to be committed but waiting for the final commit decision. It serves as a wrapper around the actual prepare callback, providing consistent error handling and context management.

The function validates that two-phase commit support is enabled and that the required prepare callback is registered. It establishes proper error context, configures the logical decoding context for output generation, and then delegates to the output plugin's prepare callback. This design ensures that prepared transactions are properly handled in the logical replication stream.

## Parameters / Member Variables
- `cache`: ReorderBuffer instance managing transaction reordering and callbacks
- `txn`: ReorderBufferTXN representing the transaction being prepared
- `prepare_lsn`: XLogRecPtr indicating the WAL location of the prepare record

## Dependencies
- Functions called/Symbols referenced:
  - output_plugin_error_callback
  - ReorderBufferTXN (transaction structure)
  - ReorderBuffer (reorder buffer structure)
  - LogicalDecodingContext (decoding context)
  - LogicalErrorCallbackState (error callback state)
- Called from (representative examples):
  - StartupDecodingContext

## Notes and Other Information
- This is a static function internal to logical.c, used specifically for prepared transaction processing
- Only called when ctx->twophase is true, indicating two-phase commit support is enabled
- Establishes error context stack with callback name "prepare" for proper error reporting
- Sets context state with accept_writes=true, write_xid, write_location=txn->end_lsn, and end_xact=true
- Validates that prepare_cb callback is registered, throwing an error if missing for two-phase enabled contexts
- Uses txn->final_lsn as report_location (beginning of prepare record) and txn->end_lsn as write_location (end of record)
- Similar to commit_cb_wrapper in structure but specifically handles the prepare phase of two-phase commits
- Part of PostgreSQL's two-phase commit support in logical decoding, enabling proper handling of distributed transactions