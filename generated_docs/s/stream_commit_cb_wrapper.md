# stream_commit_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1470-1510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1470-L1510)

## Overview
A wrapper function that sets up error handling context and calls the plugin's stream_commit callback to notify about the commit of a streaming transaction in logical replication.

## Definition

```c
static void
stream_commit_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
						 XLogRecPtr commit_lsn)
```
## Detailed Description
This function serves as an internal wrapper for the stream_commit callback in PostgreSQL's logical replication system. It is called when a large streaming transaction is committed. The wrapper performs essential setup tasks including error context management, output state configuration, and validation before delegating to the actual plugin callback.

Similar to stream_abort_cb_wrapper, this function sets ctx->end_xact to true, indicating that the transaction is ending due to commit. It uses the transaction's final_lsn for error reporting but end_lsn for write location tracking, and passes the commit_lsn parameter to the plugin callback.

## Parameters / Member Variables
- `cache`: Pointer to the ReorderBuffer containing transaction data and plugin context
- `txn`: Pointer to the ReorderBufferTXN representing the streaming transaction being committed
- `commit_lsn`: XLogRecPtr indicating the LSN position where the transaction commit occurred

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
- The stream_commit_cb callback is mandatory in streaming mode - missing callback results in ERROR
- Sets up error context to provide meaningful error messages during plugin callback execution
- Configures output state including accept_writes=true and current transaction ID
- Sets ctx->end_xact = true, indicating transaction end state
- Uses txn->final_lsn for error reporting location but txn->end_lsn for write_location
- The plugin callback receives the commit_lsn as an additional parameter
- Part of PostgreSQL's logical replication streaming feature for handling large transactions
- Counterpart to stream_abort_cb_wrapper, marking successful completion of streaming transactions
- Essential for proper commit processing in streaming logical replication

## Simplified Source

```c
static void stream_commit_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
{
    LogicalDecodingContext *ctx = cache->private_data;
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    // Validate streaming mode is enabled
    Assert(!ctx->fast_forward);
    Assert(ctx->streaming);

    // Set up error context for better error reporting
    state.ctx = ctx;
    state.callback_name = "stream_commit";
    state.report_location = txn->final_lsn;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *) &state;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Configure output state for transaction commit
    ctx->accept_writes = true;
    ctx->write_xid = txn->xid;
    ctx->write_location = txn->end_lsn;
    ctx->end_xact = true;

    // Verify stream_commit_cb callback exists (required in streaming mode)
    if (ctx->callbacks.stream_commit_cb == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("logical streaming requires a stream_commit_cb callback")));

    // Call the plugin's stream commit callback
    ctx->callbacks.stream_commit_cb(ctx, txn, commit_lsn);

    // Restore previous error context
    error_context_stack = errcallback.previous;
}
```