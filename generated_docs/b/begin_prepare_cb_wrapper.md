# begin_prepare_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:924-967](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L924-L967)

## Overview
A wrapper function that handles the begin phase of prepared transactions in logical decoding, providing error context and state management for two-phase commit operations.

## Definition
```c
static void begin_prepare_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn)
```

## Detailed Description
The `begin_prepare_cb_wrapper` function is specifically designed for two-phase commit transactions in PostgreSQL's logical decoding system. It serves as a wrapper around the begin_prepare callback, which is called when a transaction enters the prepare phase of a two-phase commit. This function is similar to the regular begin callback but includes additional handling for prepared transactions with global transaction IDs.

## Simplified Source

```c
// Simplified version of begin_prepare_cb_wrapper
static void begin_prepare_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn) {
    LogicalDecodingContext *ctx = cache->private_data;

    // Set up error context for prepared transaction begin
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;
    state.ctx = ctx;
    state.callback_name = "begin_prepare";
    state.report_location = txn->first_lsn;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *) &state;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Configure output state for prepared transaction
    ctx->accept_writes = true;
    ctx->write_xid = txn->xid;
    ctx->write_location = txn->first_lsn;
    ctx->end_xact = false;

    // Ensure begin_prepare callback is available (required for two-phase)
    if (ctx->callbacks.begin_prepare_cb == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("logical replication at prepare time requires a %s callback",
                        "begin_prepare_cb")));

    // Call the plugin's begin_prepare callback
    ctx->callbacks.begin_prepare_cb(ctx, txn);

    // Restore error context
    error_context_stack = errcallback.previous;
}
```

Key simplifications made:
- Removed assertion checks for clarity
- Grouped error context setup together
- Grouped output state configuration together
- Preserved essential error checking for missing callback
- Added descriptive comments for each section
- Focused on two-phase commit transaction begin functionality

The function ensures that two-phase commits are enabled and that the required begin_prepare callback is registered before proceeding. It sets up proper error handling context and configures the logical decoding context for the prepare phase, then delegates to the output plugin's begin_prepare callback. This design maintains clean separation between the replication protocol handling and plugin-specific logic.

## Parameters / Member Variables
- `cache`: ReorderBuffer instance managing transaction reordering and callbacks
- `txn`: ReorderBufferTXN representing the transaction being prepared

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
- This is a static function internal to logical.c, used specifically for prepared transaction processing
- Only called when ctx->twophase is true, indicating two-phase commit support is enabled
- Establishes error context stack with callback name "begin_prepare" for proper error reporting
- Sets context state with accept_writes=true, write_xid, write_location=txn->first_lsn, and end_xact=false
- Validates that begin_prepare_cb callback is registered, throwing an error if missing
- Uses txn->first_lsn for both report_location and write_location, representing the start of the transaction
- Part of the logical decoding two-phase commit support introduced for improved replication protocol handling
- Maintains compatibility by not extending the existing begin callback, avoiding protocol breaks