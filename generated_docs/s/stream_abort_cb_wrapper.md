# stream_abort_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1384-1424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1384-L1424)

## Overview
A wrapper function that sets up error handling context and calls the plugin's stream_abort callback to notify about the abortion of a streaming transaction in logical replication.

## Definition

```c
static void
stream_abort_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
						XLogRecPtr abort_lsn)
```
## Detailed Description
This function serves as an internal wrapper for the stream_abort callback in PostgreSQL's logical replication system. It is called when a large streaming transaction is aborted/rolled back. The wrapper performs essential setup tasks including error context management, output state configuration, and validation before delegating to the actual plugin callback.

Unlike the start and stop wrappers, this function sets ctx->end_xact to true, indicating that the transaction is ending due to abortion. The abort_lsn parameter is passed through to the plugin callback to provide the exact location where the abortion occurred.

## Parameters / Member Variables
- `cache`: Pointer to the ReorderBuffer containing transaction data and plugin context
- `txn`: Pointer to the ReorderBufferTXN representing the streaming transaction being aborted
- `abort_lsn`: XLogRecPtr indicating the LSN position where the transaction abortion occurred

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBuffer](../R/ReorderBuffer.md)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - [LogicalErrorCallbackState](../L/LogicalErrorCallbackState.md)
  - [output_plugin_error_callback](../o/output_plugin_error_callback.md)
  - ereport (error reporting)
- Called from (representative examples):
  - [StartupDecodingContext](../S/StartupDecodingContext.md)

## Notes and Other Information
- Only called when streaming is supported (ctx->streaming must be true)
- Fast-forward mode is not compatible with streaming (Assert(!ctx->fast_forward))
- The stream_abort_cb callback is mandatory in streaming mode - missing callback results in ERROR
- Sets up error context to provide meaningful error messages during plugin callback execution
- Configures output state including accept_writes=true and current transaction ID
- Sets ctx->end_xact = true, distinguishing this from start/stop callbacks
- Updates write_location to the abort LSN for replication progress tracking
- The plugin callback receives the abort_lsn as an additional parameter
- Part of PostgreSQL's logical replication streaming feature for handling transaction rollbacks
- Critical for proper cleanup when large streaming transactions are aborted

## Simplified Source

```c
static void stream_abort_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
                                   XLogRecPtr abort_lsn)
{
    LogicalDecodingContext *ctx = cache->private_data;
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    Assert(!ctx->fast_forward);
    Assert(ctx->streaming);  // Only for streaming mode

    // Set up error handling context
    state.ctx = ctx;
    state.callback_name = "stream_abort";
    state.report_location = abort_lsn;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *) &state;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Configure output state for transaction abort
    ctx->accept_writes = true;
    ctx->write_xid = txn->xid;
    ctx->write_location = abort_lsn;
    ctx->end_xact = true;

    // Validate callback is available for streaming mode
    if (ctx->callbacks.stream_abort_cb == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("logical streaming requires a %s callback",
                        "stream_abort_cb")));

    // Call the actual plugin stream abort callback
    ctx->callbacks.stream_abort_cb(ctx, txn, abort_lsn);

    // Restore error context
    error_context_stack = errcallback.previous;
}
```