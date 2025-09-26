# commit_prepared_cb_wrapper

## Location
src/backend/replication/logical/logical.c: 1013 - 1057

## Overview
A wrapper function that handles the final commit phase of previously prepared transactions in logical decoding, managing error context and state for two-phase commit completion.

## Definition
```c
static void commit_prepared_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
```

## Detailed Description
The `commit_prepared_cb_wrapper` function manages the final commit phase of two-phase commit transactions in PostgreSQL's logical decoding system. This function is called when a previously prepared transaction is being definitively committed, completing the two-phase commit process. It serves as a wrapper around the commit_prepared callback, providing consistent error handling and context management.

The function ensures that two-phase commit support is enabled and validates that the required commit_prepared callback is registered. It establishes proper error context, configures the logical decoding context for final output generation, and delegates to the output plugin's commit_prepared callback. This represents the completion of a distributed transaction that was previously prepared.

## Parameters / Member Variables
- `cache`: ReorderBuffer instance managing transaction reordering and callbacks
- `txn`: ReorderBufferTXN representing the previously prepared transaction being committed
- `commit_lsn`: XLogRecPtr indicating the WAL location of the commit prepared record

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
- This is a static function internal to logical.c, used specifically for committing prepared transactions
- Only called when ctx->twophase is true, indicating two-phase commit support is enabled
- Establishes error context stack with callback name "commit_prepared" for proper error reporting
- Sets context state with accept_writes=true, write_xid, write_location=txn->end_lsn, and end_xact=true
- Validates that commit_prepared_cb callback is registered, throwing an error if missing for two-phase enabled contexts
- Uses txn->final_lsn as report_location (beginning of commit record) and txn->end_lsn as write_location (end of record)
- Similar structure to commit_cb_wrapper but specifically handles the final commit of prepared transactions
- Part of PostgreSQL's complete two-phase commit workflow in logical decoding: begin_prepare -> prepare -> commit_prepared
- Essential for maintaining consistency in distributed transaction scenarios where transactions span multiple systems