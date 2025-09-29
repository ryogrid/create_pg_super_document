# stream_change_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1511-1559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1511-L1559)

## Overview
A wrapper function that safely invokes the output plugin's stream_change_cb callback during logical replication streaming, providing error context and state management.

## Definition

```c
static void
stream_change_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
						 Relation relation, ReorderBufferChange *change)
```
## Detailed Description
This function serves as a protective wrapper around the output plugin's stream_change_cb callback in PostgreSQL's logical replication streaming mechanism. It sets up proper error context handling, manages the logical decoding context state, and ensures that streaming-specific requirements are met before delegating to the actual plugin callback.

The function performs several critical tasks:
1. Sets up error context with callback information for better error reporting
2. Configures the logical decoding context for write operations
3. Updates the write location for client progress tracking
4. Validates that the required stream_change_cb callback is available
5. Safely invokes the plugin's callback with proper error handling

## Parameters / Member Variables
- : ReorderBuffer instance containing the private logical decoding context
- : ReorderBufferTXN representing the current transaction being processed
- : Relation object for the table being modified
- : ReorderBufferChange containing the specific change being streamed

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBuffer](../R/ReorderBuffer.md)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md)  
  - [ReorderBufferChange](../R/ReorderBufferChange.md)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - [LogicalErrorCallbackState](../L/LogicalErrorCallbackState.md)
  - [output_plugin_error_callback](../o/output_plugin_error_callback.md)
- Called from (representative examples):
  - [StartupDecodingContext](../S/StartupDecodingContext.md)

## Notes and Other Information
- This function is only called when streaming is supported (ctx->streaming must be true)
- The function asserts that fast_forward mode is not active
- It requires that the output plugin provides a stream_change_cb callback, otherwise it raises an error
- The function manages error context stack properly to ensure cleanup on both success and failure paths
- Location tracking is updated to allow clients to provide up-to-date progress information

## Simplified Source

```c
static void stream_change_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
                                    Relation relation, ReorderBufferChange *change)
{
    LogicalDecodingContext *ctx = cache->private_data;
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    Assert(!ctx->fast_forward);
    Assert(ctx->streaming);  // Only for streaming mode

    // Set up error handling context
    state.ctx = ctx;
    state.callback_name = "stream_change";
    state.report_location = change->lsn;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *) &state;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Configure output state
    ctx->accept_writes = true;
    ctx->write_xid = txn->xid;

    // Update location for client progress tracking
    ctx->write_location = change->lsn;
    ctx->end_xact = false;

    // Validate callback is available for streaming mode
    if (ctx->callbacks.stream_change_cb == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("logical streaming requires a %s callback",
                        "stream_change_cb")));

    // Call the actual plugin stream change callback
    ctx->callbacks.stream_change_cb(ctx, txn, relation, change);

    // Restore error context
    error_context_stack = errcallback.previous;
}
```