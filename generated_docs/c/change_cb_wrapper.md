# change_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1105-1143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1105-L1143)

## Overview
A wrapper function that provides error handling and context management when calling logical replication output plugin change callbacks during logical decoding.

## Definition

```c
static void
change_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
				  Relation relation, ReorderBufferChange *change)
```
## Detailed Description
The  function serves as an intermediary layer between PostgreSQL's logical replication infrastructure and output plugin change callbacks. It establishes proper error context, manages output state, and ensures that LSN (Log Sequence Number) information is correctly propagated for client communication. This wrapper is crucial for maintaining consistency and providing meaningful error messages during logical decoding operations.

The function sets up an error callback context that will provide detailed information if the plugin's change callback fails. It also manages the logical decoding context's output state, including setting the current transaction ID and LSN position for proper client synchronization.

## Parameters / Member Variables
- `*cache`: ReorderBuffer instance containing the logical decoding state and private plugin data
- `*txn`: ReorderBufferTXN representing the current transaction being processed
- `relation`: Relation object representing the table being modified
- `*change`: ReorderBufferChange containing the specific change details (INSERT, UPDATE, DELETE, etc.)
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
- This function is only called when  is false, ensuring it's not used during fast-forward mode
- Sets  to enable output plugin writing
- Updates  with the change's LSN for client reply coordination
- Manages error context stack to provide meaningful error messages if the plugin callback fails
- The LSN tracking allows clients to acknowledge receipt of changes up to a specific point, enabling efficient replication confirmation
- Sets  to indicate this is not a transaction end event

## Simplified Source

```c
static void change_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
                             Relation relation, ReorderBufferChange *change)
{
    LogicalDecodingContext *ctx = cache->private_data;
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    Assert(!ctx->fast_forward);

    // Set up error handling context
    state.ctx = ctx;
    state.callback_name = "change";
    state.report_location = change->lsn;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *) &state;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Configure output state
    ctx->accept_writes = true;
    ctx->write_xid = txn->xid;
    ctx->write_location = change->lsn;
    ctx->end_xact = false;

    // Call the actual plugin change callback
    ctx->callbacks.change_cb(ctx, txn, relation, change);

    // Restore error context
    error_context_stack = errcallback.previous;
}
```