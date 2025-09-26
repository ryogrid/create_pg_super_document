# begin_prepare_cb_wrapper

## Location
src/backend/replication/logical/logical.c: 924 - 967

## Overview
A wrapper function that handles the begin phase of prepared transactions in logical decoding, providing error context and state management for two-phase commit operations.

## Definition
```c
static void begin_prepare_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn)
```

## Detailed Description
The `begin_prepare_cb_wrapper` function is specifically designed for two-phase commit transactions in PostgreSQL's logical decoding system. It serves as a wrapper around the begin_prepare callback, which is called when a transaction enters the prepare phase of a two-phase commit. This function is similar to the regular begin callback but includes additional handling for prepared transactions with global transaction IDs.

The function ensures that two-phase commits are enabled and that the required begin_prepare callback is registered before proceeding. It sets up proper error handling context and configures the logical decoding context for the prepare phase, then delegates to the output plugin's begin_prepare callback. This design maintains clean separation between the replication protocol handling and plugin-specific logic.

## Parameters / Member Variables
- `cache`: ReorderBuffer instance managing transaction reordering and callbacks
- `txn`: ReorderBufferTXN representing the transaction being prepared

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
- Establishes error context stack with callback name "begin_prepare" for proper error reporting
- Sets context state with accept_writes=true, write_xid, write_location=txn->first_lsn, and end_xact=false
- Validates that begin_prepare_cb callback is registered, throwing an error if missing
- Uses txn->first_lsn for both report_location and write_location, representing the start of the transaction
- Part of the logical decoding two-phase commit support introduced for improved replication protocol handling
- Maintains compatibility by not extending the existing begin callback, avoiding protocol breaks