# stream_start_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1286-1334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1286-L1334)

## Overview
A wrapper function that sets up error handling context and calls the plugin's stream_start callback to notify about the start of a streaming transaction in logical replication.

## Definition

```c
static void
stream_start_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
						XLogRecPtr first_lsn)
```
## Detailed Description
This function serves as an internal wrapper for the stream_start callback in PostgreSQL's logical replication system. It is called when a large transaction begins streaming its changes before the transaction commits. The wrapper performs essential setup tasks including error context management, output state configuration, and validation before delegating to the actual plugin callback.

The function ensures proper error reporting by setting up an error context stack that will provide meaningful error messages if the plugin callback fails. It also configures the logical decoding context for write operations and updates location tracking for replication progress monitoring.

## Parameters / Member Variables
- `*cache`: Pointer to the ReorderBuffer containing transaction data and plugin context
- `*txn`: Pointer to the ReorderBufferTXN representing the streaming transaction
- `first_lsn`: XLogRecPtr indicating the LSN position where streaming begins
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
- The stream_start_cb callback is mandatory in streaming mode - missing callback results in ERROR
- Sets up error context to provide meaningful error messages during plugin callback execution
- Configures output state including accept_writes=true and current transaction ID
- Updates write_location for replication progress tracking
- Part of PostgreSQL's logical replication streaming feature for handling large transactions

## Simplified Source

```c
static void stream_start_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn, XLogRecPtr first_lsn)
{
    LogicalDecodingContext *ctx = cache->private_data;
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    // Validate streaming mode is enabled
    Assert(!ctx->fast_forward);
    Assert(ctx->streaming);

    // Set up error context for better error reporting
    state.ctx = ctx;
    state.callback_name = "stream_start";
    state.report_location = first_lsn;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *) &state;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Configure output state for streaming transaction
    ctx->accept_writes = true;
    ctx->write_xid = txn->xid;
    ctx->write_location = first_lsn;  // Report first LSN for replication progress
    ctx->end_xact = false;

    // Verify stream_start_cb callback exists (required in streaming mode)
    if (ctx->callbacks.stream_start_cb == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("logical streaming requires a stream_start_cb callback")));

    // Call the plugin's stream start callback
    ctx->callbacks.stream_start_cb(ctx, txn);

    // Restore previous error context
    error_context_stack = errcallback.previous;
}
```