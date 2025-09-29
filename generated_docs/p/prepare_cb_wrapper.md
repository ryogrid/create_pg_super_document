# prepare_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:968-1012](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L968-L1012)

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
- Establishes error context stack with callback name "prepare" for proper error reporting
- Sets context state with accept_writes=true, write_xid, write_location=txn->end_lsn, and end_xact=true
- Validates that prepare_cb callback is registered, throwing an error if missing for two-phase enabled contexts
- Uses txn->final_lsn as report_location (beginning of prepare record) and txn->end_lsn as write_location (end of record)
- Similar to commit_cb_wrapper in structure but specifically handles the prepare phase of two-phase commits
- Part of PostgreSQL's two-phase commit support in logical decoding, enabling proper handling of distributed transactions

## Simplified Source

```c
static void prepare_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
                              XLogRecPtr prepare_lsn)
{
    LogicalDecodingContext *ctx = cache->private_data;
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    Assert(!ctx->fast_forward);
    Assert(ctx->twophase);  // Only for two-phase commits

    // Set up error handling context
    state.ctx = ctx;
    state.callback_name = "prepare";
    state.report_location = txn->final_lsn;  // beginning of prepare record
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *) &state;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Configure output state for transaction prepare
    ctx->accept_writes = true;
    ctx->write_xid = txn->xid;
    ctx->write_location = txn->end_lsn;  // points to end of record
    ctx->end_xact = true;

    // Validate callback is available for two-phase commits
    if (ctx->callbacks.prepare_cb == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("logical replication at prepare time requires a %s callback",
                        "prepare_cb")));

    // Call the actual plugin prepare callback
    ctx->callbacks.prepare_cb(ctx, txn, prepare_lsn);

    // Restore error context
    error_context_stack = errcallback.previous;
}
```